#!/usr/bin/env python3
"""Exercise the shipped Compose file with a local image, fake account and local HTTP proxy.

Requires Python 3.12+, Docker Compose v2 and a local Linux Docker daemon. No image pulls,
real credentials or remote upstreams. Retains a private evidence directory; removes only
its own Compose project. Optionally starts from another local image to check replacement
with persisted state. Also checks concurrent SSE/WS drain, Force and final usage flush.
"""
import argparse
import base64
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import shutil
import socket
import sqlite3
import struct
import subprocess
import tarfile
import tempfile
import threading
import time
import tomllib
from urllib.parse import urlsplit
import uuid


ROOT = Path(__file__).resolve().parent.parent
TOKEN = 'fake-compose-smoke-access-token'
UPSTREAM = 'ciallopa-smoke.invalid'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(text, pattern, replacement):
    text, count = re.subn(pattern, lambda _: replacement, text, flags=re.MULTILINE)
    require(count == 1, f'expected one match for {pattern!r}, got {count}')
    return text


def compose_text(image, port):
    text = (ROOT / 'packaging/docker/compose.yaml').read_text()
    text = replace_once(text, r'^    image: .*$', '    image: ' + json.dumps(image))
    text = replace_once(text, r'^    user: .*$', f'    user: "{os.getuid()}:{os.getgid()}"')
    return replace_once(text, r'^      - "127\.0\.0\.1:8317:8317"$',
                        f'      - "127.0.0.1:{port}:8317"')


def read_exact(stream, size):
    result = bytearray()
    while len(result) < size:
        chunk = stream.read(size - len(result))
        if not chunk:
            raise EOFError('WebSocket peer closed')
        result.extend(chunk)
    return bytes(result)


def ws_read(stream):
    first, second = read_exact(stream, 2)
    require(first & 0x80 and not first & 0x70, 'fixture expects unfragmented, uncompressed WS frames')
    size = second & 0x7f
    if size in (126, 127):
        size = int.from_bytes(read_exact(stream, 2 if size == 126 else 8), 'big')
    require(size <= 1024 * 1024, 'unexpected oversized fixture frame')
    mask = read_exact(stream, 4) if second & 0x80 else b''
    body = read_exact(stream, size)
    return first & 15, bytes(b ^ mask[i % 4] for i, b in enumerate(body)) if mask else body


def ws_write(sock, opcode, body, masked=False):
    if isinstance(body, dict):
        body = json.dumps(body).encode()
    size = len(body)
    marker = 0x80 if masked else 0
    header = bytes([0x80 | opcode])
    if size < 126:
        header += bytes([marker | size])
    else:
        header += bytes([marker | 126]) + struct.pack('!H', size)
    mask = os.urandom(4) if masked else b''
    data = bytes(b ^ mask[i % 4] for i, b in enumerate(body)) if mask else body
    sock.sendall(header + mask + data)


class Flow:
    def __init__(self, model, protocol):
        self.model, self.protocol = model, protocol
        self.release = threading.Event()
        self.closed = threading.Event()
        self.received = threading.Event()
        self.client = {'events': []}
        self.result = {'model': model, 'protocol': protocol, 'terminal': False, 'peer_closed': False}

    def events(self):
        return [
            {'type': 'response.created', 'response': {'id': 'resp_' + self.model, 'status': 'in_progress'}},
            {'type': 'response.output_text.delta', 'delta': self.model},
            {'type': 'response.completed', 'response': {
                'id': 'resp_' + self.model, 'status': 'completed', 'output': [],
                'usage': {'input_tokens': 7, 'output_tokens': 3, 'total_tokens': 10}}},
        ]


class MockProxy(BaseHTTPRequestHandler):
    """Serve the sole synthetic upstream locally; never relay or tunnel any connection."""
    protocol_version = 'HTTP/1.1'
    rbufsize = 0

    def log_message(self, *_):
        pass

    def handle_request(self):
        target = urlsplit(self.path)
        entry = {'method': self.command, 'url': self.path,
                 'account': self.headers.get('chatgpt-account-id')}
        self.server.requests.append(entry)
        size = int(self.headers.get('Content-Length', '0'))
        body = read_exact(self.rfile, size)
        accepted = (self.command == 'POST' and target.hostname == UPSTREAM
                    and target.path.endswith('/responses')
                    and self.headers.get('Authorization') == 'Bearer ' + TOKEN
                    and not self.headers.get('Content-Encoding'))
        if accepted:
            request = json.loads(body)
            flow = self.server.flows.get(request.get('model'))
            accepted = (request.get('model') == 'smoke-model' or flow is not None) and request.get('stream') is True
        entry['accepted'] = accepted
        if accepted and flow is not None:
            self.stream(flow)
            return
        if accepted:
            events = [
                {'type': 'response.created', 'response': {'id': 'resp_fixture', 'status': 'in_progress'}},
                {'type': 'response.output_text.delta', 'delta': 'compose fixture ok'},
                {'type': 'response.completed', 'response': {
                    'id': 'resp_fixture', 'status': 'completed', 'output': [],
                    'usage': {'input_tokens': 7, 'output_tokens': 3, 'total_tokens': 10}}},
            ]
            output = ''.join('data: ' + json.dumps(event) + '\n\n' for event in events).encode()
            status, content_type = 200, 'text/event-stream'
        else:
            # Includes CONNECT: an accidental OAuth/remote request cannot escape the fixture.
            output = b'{"error":"unexpected destination or request in compose smoke"}'
            status, content_type = 502, 'application/json'
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(output)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(output)
        self.close_connection = True

    do_POST = handle_request

    def do_CONNECT(self):
        # The WS dialer uses CONNECT even for ws://. Terminate the one fake origin
        # here; this handler never opens another socket or tunnels arbitrary traffic.
        if self.path != UPSTREAM + ':80':
            self.handle_request()
            return
        self.send_response(200)
        self.end_headers()
        self.close_connection = False
        self.handle_one_request()

    def do_GET(self):
        if (self.headers.get('Upgrade', '').lower() != 'websocket'
                or self.headers.get('Host') not in (UPSTREAM, UPSTREAM + ':80')
                or self.path != '/backend-api/codex/responses'
                or self.headers.get('Authorization') != 'Bearer ' + TOKEN):
            self.handle_request()
            return
        key = self.headers['Sec-WebSocket-Key']
        accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        self.send_response(101)
        self.send_header('Upgrade', 'websocket')
        self.send_header('Connection', 'Upgrade')
        self.send_header('Sec-WebSocket-Accept', accept)
        self.end_headers()
        self.connection.settimeout(10)
        opcode, body = ws_read(self.rfile)
        request = json.loads(body)
        flow = self.server.flows.get(request.get('model'))
        require(opcode == 1 and flow is not None and flow.protocol == 'ws', 'unexpected WS fixture request')
        self.server.requests.append({'method': 'WS', 'url': self.path, 'accepted': True,
                                     'account': self.headers.get('chatgpt-account-id'), 'model': flow.model})
        self.stream(flow)

    def stream(self, flow):
        self.close_connection = True
        if flow.protocol == 'sse':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Connection', 'close')
            self.end_headers()

        def send(event):
            if flow.protocol == 'ws':
                ws_write(self.connection, 1, event)
            else:
                self.wfile.write(('data: ' + json.dumps(event) + '\n\n').encode())

        try:
            created, delta, terminal = flow.events()
            send(created)
            send(delta)
            deadline = time.monotonic() + 85
            while time.monotonic() < deadline:
                if flow.release.is_set() and not flow.result['terminal']:
                    send(terminal)
                    flow.result['terminal'] = True
                    if flow.protocol == 'sse':
                        break
                if select.select([self.connection], [], [], 0.05)[0]:
                    if flow.protocol == 'ws':
                        opcode, payload = ws_read(self.rfile)
                        if opcode == 8:
                            flow.result['peer_closed'] = True
                            ws_write(self.connection, 8, payload)
                            break
                        if opcode == 9:
                            ws_write(self.connection, 10, payload)
                    elif not self.connection.recv(1, socket.MSG_PEEK):
                        flow.result['peer_closed'] = True
                        break
            else:
                raise TimeoutError('fixture did not finish or receive cancellation')
        except (EOFError, ConnectionResetError, BrokenPipeError):
            flow.result['peer_closed'] = True
        except Exception as exc:
            flow.result['error'] = str(exc)
        finally:
            flow.closed.set()


class Smoke:
    def __init__(self, image, output, upgrade_from=None):
        self.image = image
        self.upgrade_from = upgrade_from
        self.work = output
        self.project = 'ciallopa-smoke-' + uuid.uuid4().hex[:10]
        self.active = self.work / 'source'
        self.active.mkdir(mode=0o700)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        self.mock = None
        self.thread = None
        self.key = ''
        self.evidence = {'project': self.project, 'image': image, 'checks': [], 'commands': []}

    def command(self, *args, check=True, timeout=90):
        result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
        self.evidence['commands'].append({'args': args, 'exit': result.returncode,
                                          'stdout': result.stdout, 'stderr': result.stderr})
        if check and result.returncode:
            raise RuntimeError(f'{args}: exit {result.returncode}\n{result.stderr[-4000:]}')
        return result

    def docker_json(self, *args):
        return json.loads(self.command('docker', *args).stdout)

    def compose_args(self, *args):
        return ('docker', 'compose', '--project-name', self.project,
                '--project-directory', str(self.active), '-f',
                str(self.active / 'compose.yaml'), *args)

    def compose(self, *args, **kwargs):
        return self.command(*self.compose_args(*args), **kwargs)

    def checked(self, name):
        self.evidence['checks'].append(name)
        print('PASS ' + name, flush=True)

    def request(self, path, method='GET', body=None, headers=None, raw=False):
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        try:
            hdr = {'X-Api-Key': self.key, **(headers or {})}
            if body is not None:
                body = json.dumps(body)
                hdr['Content-Type'] = 'application/json'
            conn.request(method, path, body=body, headers=hdr)
            response = conn.getresponse()
            data = response.read()
            require(response.status == 200, f'{method} {path}: {response.status}: {data[:300]!r}')
            return data if raw else json.loads(data)
        finally:
            conn.close()

    def inspect(self):
        cid = self.compose('ps', '-aq', 'ciallopa').stdout.strip()
        require(bool(cid), 'expected a ciallopa container')
        return self.docker_json('inspect', cid)[0]

    def ready(self):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            state = self.inspect()['State']
            require(state['Running'], f'container stopped before readiness: {state}')
            if state.get('Health', {}).get('Status') == 'healthy':
                require(self.request('/healthz', raw=True) == b'ok\n', 'unexpected health response')
                return
            time.sleep(0.2)
        raise RuntimeError('container did not become healthy in 40 seconds')

    def private_files(self):
        for directory in ['config', 'data']:
            for path in [self.active / directory, *(self.active / directory).rglob('*')]:
                st = path.stat()
                require(st.st_uid == os.getuid() and st.st_gid == os.getgid(), f'wrong owner: {path}')
                require(st.st_mode & 0o077 == 0, f'file not private: {path}')

    def persisted(self, row_id):
        cfg = self.request('/admin/v1/config')
        require(cfg['running']['routing.strategy'] == 'fill-first', 'setting did not persist')
        require(cfg['runtime']['inflight'] == 0, 'unexpected inflight request')
        accounts = self.request('/admin/v1/accounts')['accounts']
        require(len(accounts) == 1 and accounts[0]['id'] == 'fixture', 'account did not persist')
        records = self.request('/usage/v1/records?limit=10')
        require(records['total'] == 1, f'expected one usage record: {records}')
        row = records['records'][0]
        require(row['request_id'] == row_id and row['account'] == 'fixture', 'wrong persisted record/account')
        require(row['status'] == 200 and not row['error'] and not row['incomplete'], 'request was not completed')
        require(row['input_tokens'] == 7 and row['output_tokens'] == 3, 'usage tokens did not persist')
        self.private_files()
        return row

    def run(self):
        require(os.getuid() != 0, 'run as a non-root user with Docker access, as in the Compose guide')
        require(self.command('docker', 'info', '--format', '{{.OSType}}').stdout.strip() == 'linux',
                'this smoke requires a local Linux Docker daemon')
        self.command('docker', 'compose', 'version')
        image = self.docker_json('image', 'inspect', self.image)[0]
        self.evidence['image_id'] = image['Id']
        initial_image = self.image
        if self.upgrade_from:
            previous = self.docker_json('image', 'inspect', self.upgrade_from)[0]
            require(previous['Id'] != image['Id'], '--upgrade-from must resolve to a different image ID')
            self.evidence['upgrade'] = {}
            for name, reference, metadata in [
                ('from', self.upgrade_from, previous), ('to', self.image, image)
            ]:
                labels = metadata.get('Config', {}).get('Labels') or {}
                self.evidence['upgrade'][name] = {
                    'reference': reference, 'image_id': metadata['Id'],
                    'version': labels.get('org.opencontainers.image.version'),
                    'revision': labels.get('org.opencontainers.image.revision'),
                }
            print('Upgrade images: ' + json.dumps(self.evidence['upgrade']), flush=True)
            # Pin the resolved ID throughout the old-image phase; never move a tag.
            initial_image = previous['Id']
        (self.active / 'compose.yaml').write_text(compose_text(initial_image, self.port))
        result = self.compose('up', '-d', '--pull', 'never', '--no-build', check=False)
        require(result.returncode != 0 and 'does not exist' in result.stderr
                and not (self.active / 'config').exists() and not (self.active / 'data').exists(),
                'missing bind directories must fail without being created')
        self.checked('missing bind directories fail')
        self.compose('down', '--remove-orphans')
        for directory in ['config', 'data']:
            (self.active / directory).mkdir(mode=0o700)
        self.compose('run', '--rm', '--no-deps', '--pull', 'never', 'ciallopa', 'init')
        config_path = self.active / 'config/config.toml'
        config = config_path.read_text()
        parsed = tomllib.loads(config)
        self.key = parsed['api_keys'][0]['key']
        require(parsed['listen'] == '0.0.0.0:8317' and self.key.startswith('sk-cpa-'), 'init configuration mismatch')
        self.private_files()
        self.checked('Compose init with host UID and private config')

        network = self.docker_json('network', 'inspect', self.project + '_default')[0]
        gateway = next(c['Gateway'] for c in network['IPAM']['Config']
                       if ipaddress.ip_address(c['Gateway']).version == 4)
        self.mock = ThreadingHTTPServer((gateway, 0), MockProxy)
        self.mock.requests = []
        self.mock.flows = {}
        self.thread = threading.Thread(target=self.mock.serve_forever, daemon=True)
        self.thread.start()
        # All outbound clients use this proxy, which serves fixtures and cannot relay traffic.
        config = replace_once(config, r'^upstream = .*$', f'upstream = "http://{UPSTREAM}/backend-api"')
        config = replace_once(config, r'^proxy_url = .*$', f'proxy_url = "http://{gateway}:{self.mock.server_port}"')
        config = replace_once(config, r'^proxy_enabled = .*$', 'proxy_enabled = true')
        config = replace_once(config, r'^allow_config_write = .*$', 'allow_config_write = true')
        config = replace_once(config, r'^reset_credits_refresh_mins = .*$', 'reset_credits_refresh_mins = 0')
        config = config.replace('[usage]\n', '[usage]\nprices_source = ""\nflush_ms = 20\n', 1)
        config = config.replace('[upstream_opts]\n', '[upstream_opts]\ncompress_requests = false\n', 1)
        tomllib.loads(config)  # Reject duplicate or malformed fixture settings before starting.
        config_path.write_text(config)
        auth = self.active / 'data/auths'
        auth.mkdir(mode=0o700)
        account_path = auth / 'fixture.json'
        account_path.write_text(json.dumps({'type': 'codex', 'email': 'compose-fixture@invalid',
            'account_id': 'compose-fixture', 'access_token': TOKEN, 'expired': '2099-01-01T00:00:00Z'}))
        account_path.chmod(0o600)
        account_hash = digest(account_path)
        self.compose('up', '-d', '--pull', 'never', '--no-build')
        self.ready()
        cfg = self.request('/admin/v1/config')
        self.request('/admin/v1/config', 'PUT', {'changes': {'routing.strategy': 'fill-first'}},
                     {'If-Match': '"' + cfg['version'] + '"'})
        config_hash = digest(config_path)
        require(config_path.with_suffix('.toml.bak').is_file()
                and config_path.with_suffix('.toml.audit.jsonl').is_file(), 'settings backup/audit missing')
        output = self.request('/v1/responses', 'POST', {'model': 'smoke-model', 'stream': True,
                                                       'input': 'Compose fixture'}, raw=True)
        require(b'compose fixture ok' in output and b'response.completed' in output, 'SSE response incomplete')
        deadline = time.monotonic() + 5
        while True:
            records = self.request('/usage/v1/records?limit=10')
            if records['total']:
                break
            require(time.monotonic() < deadline, 'usage writer did not record the request')
            time.sleep(0.05)
        row_id = records['records'][0]['request_id']
        self.evidence['record'] = self.persisted(row_id)
        self.checked('settings write and one real forwarded fixture usage record')

        before = self.inspect()
        if self.upgrade_from:
            require(before['Image'] == previous['Id'], 'fixture was not created by the old image')
            (self.active / 'compose.yaml').write_text(compose_text(image['Id'], self.port))
        self.compose('up', '-d', '--force-recreate', '--pull', 'never', '--no-build')
        self.ready()
        after = self.inspect()
        require(after['Id'] != before['Id'], 'force-recreate did not replace the container')
        require(after['Image'] == image['Id'], 'recreated container did not use the candidate image')
        self.persisted(row_id)
        self.checked(('image upgrade' if self.upgrade_from else 'force-recreate')
                     + ' preserves config, account, usage and permissions')

        archive = self.work / 'backup'
        archive.mkdir(mode=0o700)
        self.command('docker', 'image', 'save', '-o', str(archive / 'image.tar'), image['Id'])
        (archive / 'image.json').write_text(json.dumps(image, indent=2))
        (archive / 'image-id').write_text(image['Id'] + '\n')
        self.compose('stop', 'ciallopa')
        require(self.inspect()['State']['ExitCode'] == 0, 'backup requires a clean stop')
        with tarfile.open(archive / 'state.tar.gz', 'w:gz') as tar:
            for name in ['config', 'data']:
                tar.add(self.active / name, arcname=name)
        shutil.copyfile(self.active / 'compose.yaml', archive / 'compose.yaml')
        source = self.active
        self.active = self.work / 'restored'
        self.active.mkdir(mode=0o700)
        # Same project/network, but empty new bind directories. The old instance is stopped.
        (self.active / 'compose.yaml').write_text(compose_text(image['Id'], self.port))
        with tarfile.open(archive / 'state.tar.gz') as tar:
            tar.extractall(self.active, filter='data')
        self.command('docker', 'image', 'load', '-i', str(archive / 'image.tar'))
        self.compose('up', '-d', '--force-recreate', '--pull', 'never', '--no-build')
        self.ready()
        require(self.inspect()['Image'] == image['Id'], 'restored image differs from archived image')
        self.persisted(row_id)
        for directory in [source, self.active]:
            require(digest(directory / 'config/config.toml') == config_hash, 'config changed across recovery')
            require(digest(directory / 'data/auths/fixture.json') == account_hash, 'account changed across recovery')
        require(len(self.mock.requests) == 1 and self.mock.requests[0]['accepted']
                and self.mock.requests[0]['account'] == 'compose-fixture',
                f'unexpected upstream activity: {self.mock.requests}')
        self.checked('offline image load and empty-directory recovery preserve state')
        self.active_stream_shutdown()

    def stream_client(self, flow):
        def event(data):
            message = json.loads(data)
            flow.client['events'].append(message)
            if message.get('type') == 'response.output_text.delta':
                flow.received.set()

        try:
            if flow.protocol == 'sse':
                conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=85)
                try:
                    conn.request('POST', '/v1/responses', body=json.dumps({
                        'model': flow.model, 'stream': True, 'input': 'shutdown fixture'}),
                        headers={'Authorization': 'Bearer ' + self.key, 'Content-Type': 'application/json'})
                    response = conn.getresponse()
                    require(response.status == 200, f'SSE handshake: {response.status}')
                    try:
                        while line := response.readline():
                            if line.startswith(b'data: '):
                                event(line[6:])
                    except http.client.IncompleteRead:
                        flow.client['truncated'] = True
                finally:
                    conn.close()
            else:
                with socket.create_connection(('127.0.0.1', self.port), timeout=85) as sock:
                    key = base64.b64encode(os.urandom(16)).decode()
                    sock.sendall((f'GET /v1/responses HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n'
                                  f'Authorization: Bearer {self.key}\r\nUpgrade: websocket\r\n'
                                  f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
                                  'Sec-WebSocket-Version: 13\r\n\r\n').encode())
                    with sock.makefile('rb', buffering=0) as stream:
                        require(b' 101 ' in stream.readline(), 'WS upgrade failed')
                        while True:
                            header = stream.readline()
                            require(bool(header), 'WS handshake ended before headers')
                            if header == b'\r\n':
                                break
                        ws_write(sock, 1, {'type': 'response.create', 'model': flow.model,
                                          'stream': True, 'input': []}, masked=True)
                        while True:
                            opcode, payload = ws_read(stream)
                            if opcode == 1:
                                event(payload)
                            elif opcode == 8:
                                flow.client['close_code'] = int.from_bytes(payload[:2], 'big')
                                ws_write(sock, 8, payload, masked=True)
                                break
                            elif opcode == 9:
                                ws_write(sock, 10, payload, masked=True)
        except Exception as exc:
            flow.client['error'] = str(exc)

    def active_stream_shutdown(self):
        # This is a candidate-only check, after the old-image and recovery checks.
        # A long batching interval makes the exit flush observable, instead of
        # letting the ordinary 20 ms writer tick hide a broken shutdown barrier.
        self.compose('stop', 'ciallopa')
        config_path = self.active / 'config/config.toml'
        config = replace_once(config_path.read_text(), r'^flush_ms = .*$', 'flush_ms = 60000')
        config_path.write_text(config)
        self.evidence['shutdown'] = []
        for force in (False, True):
            mode = 'force' if force else 'drain'
            self.compose('start', 'ciallopa')
            self.ready()
            flows = [Flow('shutdown-' + mode + '-' + protocol, protocol) for protocol in ('sse', 'ws')]
            for flow in flows:
                self.mock.flows[flow.model] = flow
            clients = [threading.Thread(target=self.stream_client, args=(flow,), daemon=True) for flow in flows]
            for client in clients:
                client.start()
            for flow in flows:
                require(flow.received.wait(10), f'{flow.model}: client did not receive first delta: {flow.client}')
            runtime = self.request('/admin/v1/config')['runtime']
            require(runtime['inflight'] == 2 and runtime['ws_connections'] == 1, f'wrong active counts: {runtime}')
            cid = self.inspect()['Id']
            marker = 'draining forwarded requests'
            logs = self.command('docker', 'logs', cid)
            prior = logs.stdout + logs.stderr
            command = self.compose_args('stop', 'ciallopa')
            started = time.monotonic()
            stopper = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = started + 10
                while True:
                    logs = self.command('docker', 'logs', cid)
                    if (logs.stdout + logs.stderr).count(marker) > prior.count(marker):
                        break
                    require(stopper.poll() is None and time.monotonic() < deadline, 'no drain notification from container')
                    time.sleep(0.05)
                if force:
                    self.command('docker', 'kill', '--signal', 'TERM', cid)
                else:
                    for flow in flows:
                        flow.release.set()
                out, err = stopper.communicate(timeout=20)
                self.evidence['commands'].append({'args': command, 'exit': stopper.returncode, 'stdout': out, 'stderr': err})
                require(stopper.returncode == 0, 'Compose stop failed: ' + err)
            finally:
                if stopper.poll() is None:
                    stopper.terminate()
                    stopper.communicate(timeout=5)
            elapsed = time.monotonic() - started
            state = self.inspect()['State']
            require(not state['Running'] and state['ExitCode'] == 0 and not state['OOMKilled'], f'unclean stop: {state}')
            require(elapsed < 75, 'application exceeded the Compose stop grace')
            for client, flow in zip(clients, flows):
                client.join(timeout=5)
                require(not client.is_alive() and not flow.client.get('error'), f'client did not finish: {flow.client}')
                require(flow.closed.wait(5) and not flow.result.get('error'), f'upstream did not close: {flow.result}')
                completed = any(e.get('type') == 'response.completed' for e in flow.client['events'])
                require(completed != force and flow.result['terminal'] != force, f'wrong completion: {flow.client}')
                if force:
                    require(flow.result['peer_closed'], 'Force did not cancel upstream')
                if flow.protocol == 'ws':
                    require(flow.client.get('close_code') == (1001 if force else 1000), f'wrong WS close: {flow.client}')
            database = self.active / 'data/usage.db'
            with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as db:
                db.row_factory = sqlite3.Row
                rows = [dict(row) for row in db.execute('''
                    SELECT printf('%016x', r.id) AS request_id, m.name AS model,
                           a.name AS account, r.transport, r.status, e.name AS error,
                           s.name AS err_source, r.in_tok, r.out_tok
                    FROM requests r LEFT JOIN labels m ON m.id=r.model
                    LEFT JOIN labels a ON a.id=r.account LEFT JOIN labels e ON e.id=r.error
                    LEFT JOIN labels s ON s.id=r.err_source WHERE m.name IN (?,?)
                    ORDER BY m.name''', tuple(flow.model for flow in flows))]
            require(len(rows) == 2, f'final usage flush lost records: {rows}')
            require({row['transport'] for row in rows} == {1, 2}, f'wrong persisted protocols: {rows}')
            for row in rows:
                require(row['account'] == 'fixture', f'wrong account: {row}')
                if force:
                    require(row['error'] == 'shutdown' and row['err_source'] == 'local', f'wrong Force cause: {row}')
                else:
                    require(not row['error'] and row['in_tok'] == 7 and row['out_tok'] == 3, f'wrong completed usage: {row}')
            self.compose('start', 'ciallopa')
            self.ready()
            runtime = self.request('/admin/v1/config')['runtime']
            require(runtime['inflight'] == 0 and runtime['ws_connections'] == 0, f'restart retained occupancy: {runtime}')
            records = self.request('/usage/v1/records?limit=20')['records']
            ids = {row['request_id'] for row in rows}
            require(len([row for row in records if row['request_id'] in ids]) == 2, 'flushed records unreadable after restart')
            self.evidence['shutdown'].append({'mode': mode, 'seconds': elapsed, 'exit': state,
                'records': rows, 'flows': [{'upstream': flow.result, 'client': flow.client} for flow in flows]})
            self.checked('concurrent SSE/WS ' + mode + ', final flush and readable restart')
            self.compose('stop', 'ciallopa')

    def finish(self):
        cleanup_error = None
        if self.mock:
            for flow in self.mock.flows.values():
                flow.release.set()
        try:
            self.compose('logs', '--no-color', check=False)
            self.compose('down', '--remove-orphans', '--timeout', '75')
        except Exception as exc:
            cleanup_error = exc
        if self.mock:
            self.mock.shutdown()
            self.mock.server_close()
            self.thread.join(timeout=3)
            self.evidence['upstream_requests'] = self.mock.requests
        if cleanup_error:
            raise cleanup_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('image', help='already built local image (for example ciallopa:smoke)')
    parser.add_argument('--upgrade-from', metavar='IMAGE',
                        help='already local old image used for init, settings and the fixture request')
    args = parser.parse_args()
    os.umask(0o077)
    work = Path(tempfile.mkdtemp(prefix='ciallopa-compose-smoke-'))
    print('Compose smoke evidence: ' + str(work), flush=True)
    smoke = Smoke(args.image, work, args.upgrade_from)
    error = None
    try:
        smoke.run()
    except Exception as exc:
        error = exc
    finally:
        try:
            smoke.finish()
        except Exception as exc:
            error = error or exc
        smoke.evidence['passed'] = error is None
        if error:
            smoke.evidence['error'] = str(error)
        (work / 'result.json').write_text(json.dumps(smoke.evidence, indent=2) + '\n')
    if error:
        raise SystemExit(f'Compose smoke failed: {error}; evidence: {work}')
    print('Compose smoke passed; evidence: ' + str(work), flush=True)


if __name__ == '__main__':
    main()
