#!/usr/bin/env python3
"""Opt-in Codex WS recovery and finite grace through a real Caddy reload.

Supply already downloaded/built binaries. Copies them into a private evidence directory
before starting, and reuses cli-smoke.py's isolation and mock lifecycle helpers. No TLS,
real credentials, provider calls, installs or global service changes.
"""
import argparse
import http.client
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


spec = importlib.util.spec_from_file_location('cli_smoke', Path(__file__).with_name('cli-smoke.py'))
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def wait_wire(directory, predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for entry in cli.read_wire(directory):
            if predicate(entry):
                return entry
        time.sleep(0.025)
    raise RuntimeError('mock did not reach the expected reload stage')


MODES = {'default': 0, 'finish-within-grace': 3, 'grace-expires': 1}
CASES = {f'{stage}-{mode}': (stage, mode) for stage in ('before_output', 'after_output')
         for mode in MODES}


def revision(front):
    conn = http.client.HTTPConnection('127.0.0.1', front, timeout=3)
    try:
        conn.request('GET', '/__smoke/revision')
        response = conn.getresponse()
        body = response.read().decode()
        require(response.status == 200, 'Caddy revision endpoint did not respond')
        return body
    finally:
        conn.close()


def run_case(args, directory, stage, mode):
    grace = MODES[mode]
    preserved = mode == 'finish-within-grace'
    client_dir, work, upstream, listen, config = cli.prepare(
        directory, 'ws', fixture={'capture': True, 'hold_stream': stage},
        provider_options='request_max_retries = 0\nstream_max_retries = 1\n')
    front, admin = cli.port(), cli.port()
    client_config = client_dir / 'config.toml'
    client_config.write_text(client_config.read_text().replace(
        f'http://127.0.0.1:{listen}/v1', f'http://127.0.0.1:{front}/v1'))
    caddyfile = directory / 'Caddyfile'

    def write_config(revision):
        reverse_proxy = f'reverse_proxy 127.0.0.1:{listen}'
        if grace:
            reverse_proxy += f' {{\n        stream_close_delay {grace}s\n    }}'
        caddyfile.write_text(f'''{{
    admin 127.0.0.1:{admin}
    auto_https off
}}
http://127.0.0.1:{front} {{
    header X-Smoke-Revision {revision}
    respond /__smoke/revision {revision} 200
    {reverse_proxy}
}}
''')

    write_config('before')
    caddy_env = {**cli.ENV, 'XDG_DATA_HOME': str(directory / 'caddy-data'),
                 'XDG_CONFIG_HOME': str(directory / 'caddy-config')}
    owned, rpc = [], None
    try:
        cli.start_servers(args, directory, upstream, listen, config, owned)
        caddy = cli.spawn([str(args.caddy_bin), 'run', '--config', str(caddyfile), '--adapter', 'caddyfile'],
                          directory, 'caddy', env=caddy_env)
        owned.append(caddy)
        cli.ready(caddy, front)
        require(revision(front) == 'before', 'Caddy did not load the initial configuration')
        rpc = cli.AppServer(args, directory, client_dir, work)
        owned.append(rpc.child)
        rpc.request('initialize', {'clientInfo': {'name': 'ciallopa_caddy_probe', 'version': '1'},
                                   'capabilities': {'experimentalApi': True}})
        rpc.send({'method': 'initialized', 'params': {}})
        thread = rpc.request('thread/start', {
            'model': 'gpt-5.5', 'modelProvider': 'smoke', 'cwd': str(work), 'ephemeral': True,
            'approvalPolicy': 'never', 'sandbox': 'read-only', 'environments': [],
        })['thread']['id']
        turn = rpc.request('turn/start', {'threadId': thread,
                           'input': [{'type': 'text', 'text': 'Say hello. Do not use tools.', 'textElements': []}]})['turn']['id']
        held = wait_wire(directory, lambda e: e.get('t') == 'held' and e.get('transport') == 'ws')
        if stage == 'after_output':
            # Establish receipt by the actual Codex parser, not merely a mock send.
            rpc.wait(lambda m: m.get('method') == 'item/agentMessage/delta'
                     and m.get('params', {}).get('turnId') == turn)
        write_config('after')  # A genuinely different configuration, not a reload no-op.
        reload_started = time.monotonic()
        reload_result = subprocess.run([str(args.caddy_bin), 'reload', '--config', str(caddyfile),
                                        '--adapter', 'caddyfile', '--address', f'127.0.0.1:{admin}'],
                                       capture_output=True, text=True, timeout=10, env=caddy_env)
        cli.save(directory / 'reload.json', {'exit': reload_result.returncode,
                                           'stdout': reload_result.stdout, 'stderr': reload_result.stderr})
        require(reload_result.returncode == 0, 'Caddy reload failed: ' + reload_result.stderr)
        require(revision(front) == 'after', 'new HTTP requests did not reach the reloaded configuration')
        timeline = {'reload_command_ms': round((time.monotonic() - reload_started) * 1000)}
        retained = None
        if preserved:
            # The original response remains gated until the new route is observed.
            # There is no timing race with the mock finishing before the reload.
            wire = cli.read_wire(directory)
            require(not any(e.get('t') == 'closed' and e.get('id') == held['id'] for e in wire),
                    'grace did not retain the original response across reload')
            require(sum(e.get('t') == 'business' for e in wire) == 1,
                    'client already generated again while the first response was held')
        else:
            closed = wait_wire(directory, lambda e: e.get('t') == 'closed' and e.get('id') == held['id'])
            timeline['original_closed_ms'] = round((time.monotonic() - reload_started) * 1000)
            require(not closed['terminal'], 'reload happened after the first generation had finished')
            if grace:
                require(timeline['original_closed_ms'] >= grace * 1000 - 150,
                        'old response closed before the configured grace expired')
        timeline['release_ms'] = round((time.monotonic() - reload_started) * 1000)
        (directory / 'release_stream').touch()
        terminal = rpc.wait(lambda m: m.get('method') == 'turn/completed'
                            and m.get('params', {}).get('turn', {}).get('id') == turn)['params']['turn']
        timeline['turn_completed_ms'] = round((time.monotonic() - reload_started) * 1000)
        if preserved:
            # Completion does not itself retire a reusable Codex socket. Keep the
            # actual client alive and observe Caddy's old connection until expiry.
            retained = cli.get(listen, '/admin/v1/config')['runtime']
            require(retained['inflight'] == 1 and retained['ws_connections'] == 1,
                    'expected one idle old WebSocket retaining its admission slot')
            deadline = reload_started + grace + 5
            while cli.get(listen, '/admin/v1/config')['runtime']['ws_connections']:
                require(time.monotonic() < deadline, 'old idle WebSocket outlived its finite grace')
                time.sleep(0.025)
            timeline['idle_socket_closed_ms'] = round((time.monotonic() - reload_started) * 1000)
            require(timeline['idle_socket_closed_ms'] >= grace * 1000 - 150,
                    'idle old WebSocket closed before the configured grace expired')
        rpc.child.stdin.close()
        try:
            rpc.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cli.stop(rpc.child)
        rpc.reader.join(timeout=2)
        runtime, records = cli.observations(listen, expected_records=1 if preserved else 2)
        wire = cli.read_wire(directory)
        business = [e for e in wire if e['t'] == 'business']
        rows = records['records']
        completed_items = [m['params']['item'] for m in rpc.messages
                           if m.get('method') == 'item/completed'
                           and m.get('params', {}).get('item', {}).get('type') == 'agentMessage']
        checks = {
            'client_turn_completed': terminal['status'] == 'completed',
            'client_answer_received': any(item.get('type') == 'agentMessage'
                and item.get('text') == 'ok from acc-a' for item in terminal.get('items', [])),
            'native_ws_retained': all(e['transport'] == 'ws' for e in business),
            'session_preserved': all(e.get('session') for e in business)
                and len({e.get('session') for e in business}) == 1,
            'gateway_released': runtime['inflight'] == 0 and runtime['ws_connections'] == 0,
        }
        if preserved:
            checks.update({
                'one_original_generation': len(business) == 1 and business[0]['id'] == held['id'],
                'one_complete_record': len(rows) == 1 and not rows[0]['error'] and not rows[0]['incomplete'],
                'completion_before_expiry': timeline['turn_completed_ms'] < grace * 1000,
                'original_terminal_received': any(e.get('t') == 'closed' and e.get('id') == held['id']
                                                  and e.get('terminal') for e in wire),
            })
        else:
            checks.update({
                'two_upstream_generations': len(business) == 2 and len({e['id'] for e in business}) == 2,
                'one_interrupted_one_complete_record': len(rows) == 2
                    and sum(row['error'] == 'client_closed' and row['err_source'] == 'client' for row in rows) == 1
                    and sum(not row['error'] and not row['incomplete'] for row in rows) == 1,
            })
        result = {'stage': stage, 'mode': mode, 'stream_close_delay_secs': grace,
                  'passed': all(checks.values()), 'checks': checks, 'timeline_from_reload_ms': timeline,
                  'timing_note': 'Elapsed from parent reload-command start; close times are polling observations, '
                                 'not exact server timestamps.',
                  'runtime_after_completion_before_expiry': retained,
                  'turn': terminal, 'completed_items': completed_items, 'business': business,
                  'client_deltas': [m['params'] for m in rpc.messages if m.get('method') == 'item/agentMessage/delta'],
                  'records': records, 'runtime': runtime, 'upstream': wire,
                  'note': 'Finite grace preserves a response that finishes in time; expiry still requires client recovery. '
                          'The old socket retains its old handler until close. Repeated generations do not prove duplicate '
                          'billing or visible final text; these fixtures contain no tool calls.'}
        cli.save(directory / 'result.json', result)
        require(result['passed'], 'failed checks: ' + ', '.join(k for k, ok in checks.items() if not ok))
        return result
    finally:
        for child in reversed(owned):
            cli.stop(child)
        if rpc:
            rpc.reader.join(timeout=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('codex-bin', 'caddy-bin', 'binary', 'mock'):
        parser.add_argument('--' + option, type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=30, help='seconds per client event wait')
    parser.add_argument('--case', action='append', choices=CASES,
                        help='run selected cases; defaults to all six reload/grace comparisons')
    args = parser.parse_args()
    require(args.timeout > 0, '--timeout must be positive')
    os.umask(0o077)
    directory = Path(tempfile.mkdtemp(prefix='ciallopa-caddy-smoke-'))
    print('Caddy smoke evidence: ' + str(directory), flush=True)
    manifest = {'binaries': {}, 'cases': [], 'drivers': {
        name: cli.digest(Path(__file__).with_name(name)) for name in ('caddy-smoke.py', 'cli-smoke.py')}}
    try:
        for name in ('codex_bin', 'caddy_bin', 'binary', 'mock'):
            source = getattr(args, name).resolve(strict=True)
            copied = directory / name
            before = cli.digest(source)
            shutil.copy2(source, copied)
            require(cli.digest(copied) == before == cli.digest(source), 'binary changed during copy: ' + name)
            manifest['binaries'][name] = {'source': str(source), 'sha256': before}
            setattr(args, name, copied)
        version_env = {**cli.ENV, 'XDG_CONFIG_HOME': str(directory / 'version-config'),
                       'XDG_DATA_HOME': str(directory / 'version-data')}
        for name, command in [('codex_bin', '--version'), ('caddy_bin', 'version'), ('binary', '--version')]:
            manifest['binaries'][name]['version'] = subprocess.check_output(
                [str(getattr(args, name)), command], text=True, env=version_env).strip()
        for case in args.case or CASES:
            stage, mode = CASES[case]
            result = run_case(args, directory / case, stage, mode)
            manifest['cases'].append({'case': case, 'stage': stage, 'mode': mode, 'passed': result['passed']})
            print('PASS Caddy reload ' + case, flush=True)
        require(all(child.poll() is not None for child in cli.CHILDREN), 'owned processes remain alive')
        manifest['passed'] = True
    except Exception as exc:
        manifest.update(passed=False, error=str(exc))
        raise
    finally:
        manifest['processes'] = [{'pid': child.pid, 'exit': child.poll()} for child in cli.CHILDREN]
        cli.save(directory / 'manifest.json', manifest)


if __name__ == '__main__':
    main()
