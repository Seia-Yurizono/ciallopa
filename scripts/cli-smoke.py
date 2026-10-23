#!/usr/bin/env python3
"""Opt-in real Codex CLI smoke against local fake upstreams; Python stdlib only.

Four default cases, plus opt-in recovery, conversation, tool-recovery and tool-faults suites. No builds or
downloads. Retains its fresh temporary directory on success and failure. Only owned
subprocess handles are stopped; user config is untouched.
"""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import queue
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


KEY = "sk-local-cli-smoke-fake-0123456789"
# Do not inherit credentials, proxy overrides, application configuration, or
# telemetry endpoints. CODEX_HOME below is Codex's documented state location,
# set only in the client subprocess environment, never in the parent process.
ENV = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL", "TERM", "TMPDIR") if k in os.environ}
ENV["PYTHONDONTWRITEBYTECODE"] = "1"
CHILDREN = []
PORTS = set()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def spawn(command, directory, name, env=None, cwd=None):
    with (directory / (name + ".stdout")).open("w") as out, (directory / (name + ".stderr")).open("w") as err:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                 env=env or ENV, cwd=cwd or directory)
    CHILDREN.append(child)
    return child


def stop(child):
    if child.poll() is None:
        for _ in range(2):
            child.send_signal(signal.SIGTERM)
            try:
                child.wait(timeout=5)
                return
            except subprocess.TimeoutExpired:
                pass
        child.kill()
        child.wait(timeout=3)


def port():
    # Closing a probe socket lets the OS immediately return the same port again.
    # Keep roles distinct within this run; an external bind race still fails startup.
    for _ in range(64):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            candidate = sock.getsockname()[1]
        if candidate not in PORTS:
            PORTS.add(candidate)
            return candidate
    raise RuntimeError("could not select distinct local fixture ports")


def ready(child, listen):
    deadline = time.monotonic() + 5
    while child.poll() is None and time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", listen), timeout=0.2):
                return
        except OSError:
            time.sleep(0.025)
    raise RuntimeError(f"child {child.pid} did not listen on {listen}; inspect its stderr")


def get(listen, path):
    conn = http.client.HTTPConnection("127.0.0.1", listen, timeout=3)
    try:
        conn.request("GET", path, headers={"Authorization": "Bearer " + KEY})
        response = conn.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"{path}: HTTP {response.status}: {body[:200]!r}")
        return json.loads(body)
    finally:
        conn.close()


def client_env(client_dir):
    return {**ENV, "CODEX_HOME": str(client_dir), "CIALLOPA_SMOKE_KEY": KEY}


def read_wire(directory):
    path = directory / "upstream.log"
    # The mock may be appending a large captured request while we poll. Ignore
    # only the unfinished final record; malformed complete records still fail.
    return [json.loads(line) for line in path.read_bytes().split(b"\n")[:-1] if line.strip()] if path.exists() else []


def read_stream_diagnostics(directory):
    """Parse only our metadata warning, preserving its text for payload checks."""
    found = []
    for name in ("gateway.stdout", "gateway.stderr"):
        for line in (directory / name).read_text().splitlines():
            if "upstream stream ended without an observed terminal event" in line:
                fields = dict(token.split("=", 1) for token in shlex.split(line) if "=" in token)
                found.append({"fields": fields, "line": line})
    return found


def observations(listen, timeout=5, expected_records=None):
    deadline = time.monotonic() + timeout
    while True:
        runtime = get(listen, "/admin/v1/config")["runtime"]
        records = get(listen, "/usage/v1/records?exclude_prewarm=1&limit=100")
        if runtime["inflight"] == 0 and (expected_records is None or records["total"] >= expected_records):
            return runtime, records
        if time.monotonic() >= deadline:
            raise RuntimeError("client stopped but gateway slots or expected usage records did not settle")
        time.sleep(0.025)


def start_servers(args, directory, upstream, listen, config, owned):
    mock = spawn([str(args.mock), f"127.0.0.1:{upstream}"], directory, "mock",
                 env={**ENV, "MOCK_DIR": str(directory)})
    owned.append(mock)
    ready(mock, upstream)
    gateway = spawn([str(args.binary), "-c", str(config), "serve"], directory, "gateway")
    owned.append(gateway)
    ready(gateway, listen)
    get(listen, "/admin/v1/config")


RECOVERY_CASES = {
    "http-transient": {"fixture": {"http_statuses": [503, 200]}, "request": 1, "stream": 0,
                       "sse": 2, "ws": 0, "exit": 0},
    "http-small-budget": {"fixture": {"http_statuses": [503]}, "request": 1, "stream": 1,
                          "sse": 4, "ws": 0, "exit": 1},
    "http-default-budget": {"fixture": {"http_statuses": [503]}, "request": None, "stream": None,
                            "sse": 30, "ws": 0, "exit": 1},
    "retry-after-short": {"fixture": {"http_statuses": [503, 200], "retry_after": "1"},
                          "request": 1, "stream": 0, "sse": 2, "ws": 0, "exit": 0, "wait_min_ms": 800},
    "retry-after-long": {"fixture": {"http_statuses": [503, 200], "retry_after": "120"},
                         "request": 1, "stream": 0, "sse": 2, "ws": 0, "exit": 0, "wait_max_ms": 5000},
    "retry-after-date": {"fixture": {"http_statuses": [503, 200], "retry_after": "Thu, 01 Jan 2099 00:00:00 GMT"},
                         "request": 1, "stream": 0, "sse": 2, "ws": 0, "exit": 0, "wait_max_ms": 5000},
    "rate-limit-no-hold": {"fixture": {"http_statuses": [429, 200], "error_code": "rate_limit_exceeded", "retry_after": "1"},
                           "request": 0, "stream": 1, "sse": 1, "ws": 0, "exit": 1, "records": 2,
                           "wait_min_ms": 800, "wait_clock": "client", "record_errors": ["rate_limit_exceeded", "all_cooling"]},
    "rate-limit-default-hold": {"fixture": {"http_statuses": [429, 200], "error_code": "rate_limit_exceeded", "retry_after": "1"},
                                "request": 0, "stream": 1, "sse": 2, "ws": 0, "exit": 0,
                                "routing": "", "wait_min_ms": 9000},
    "usage-limit": {"fixture": {"http_statuses": [429], "error_code": "usage_limit_reached", "retry_after": "1"},
                    "request": 1, "stream": 1, "sse": 1, "ws": 0, "exit": 1},
    "ws-transient": {"transport": "ws", "fixture": {"ws_statuses": [503, 200]},
                     "request": 0, "stream": 0, "sse": 0, "ws": 2, "exit": 0},
    "ws-budget-fallback": {"transport": "ws", "fixture": {"ws_statuses": [503]},
                           "request": 0, "stream": 1, "sse": 1, "ws": 15, "exit": 0, "records": 4},
    "ws-426-fallback": {"transport": "ws", "fixture": {"ws_statuses": [426]},
                        "request": 0, "stream": 0, "sse": 1, "ws": 1, "exit": 0, "records": 2},
    "ws-handshake-cancel": {"transport": "ws", "fixture": {"handshake_delay_ms": 600},
                            "request": 0, "stream": 0, "connect_ms": 250,
                            "sse": 1, "ws": 2, "exit": 0, "records": 3, "handshake_error": "client_closed"},
    "ws-default-connect-deadline": {"transport": "ws", "fixture": {"handshake_delay_ms": 16000},
                                    "request": 0, "stream": 0, "sse": 1, "ws": 2, "exit": 0,
                                    "records": 3, "handshake_error": "client_closed", "handshake_min_ms": 14500},
    "ws-long-advice-budget": {"transport": "ws", "fixture": {"ws_statuses": [503], "retry_after": "120"},
                              "request": 0, "stream": 0, "gateway": "preparation_timeout_secs = 1\n",
                              "sse": 1, "ws": 2, "exit": 0, "records": 3, "handshake_error": "preparation_timeout"},
}


def run_recovery(args, directory, spec):
    provider = "".join(f"{key} = {spec[name]}\n" for name, key in
                       (("request", "request_max_retries"), ("stream", "stream_max_retries"))
                       if spec[name] is not None)
    if "connect_ms" in spec:
        provider += f"websocket_connect_timeout_ms = {spec['connect_ms']}\n"
    client_dir, work, upstream, listen, config = prepare(
        directory, spec.get("transport", "sse"), fixture={"capture": True, **spec["fixture"]},
        provider_options=provider, gateway_options="transient_cooldown_secs = 0\n" + spec.get("gateway", ""),
        routing_options=spec.get("routing", "cooldown_wait_secs = 0\n"))
    owned = []
    try:
        start_servers(args, directory, upstream, listen, config, owned)
        started = time.monotonic()
        client = spawn([str(args.codex_bin), "exec", "--json", "--ephemeral", "--skip-git-repo-check",
                        "--sandbox", "read-only", "--color", "never", "Say hello. Do not use tools."],
                       directory, "client", env=client_env(client_dir), cwd=work)
        owned.append(client)
        client.wait(timeout=args.timeout)
        elapsed_ms = round((time.monotonic() - started) * 1000)
        runtime, records = observations(listen, expected_records=spec.get("records", spec["sse"] or 1))
        wire = read_wire(directory)
        requests = [x for x in wire if x["t"] in ("sse", "ws")]
        intervals = [b["t_ms"] - a["t_ms"] for a, b in zip(requests, requests[1:])]
        client_times = sorted(row["ts_ms"] for row in records["records"])
        client_intervals = [b - a for a, b in zip(client_times, client_times[1:])]
        events = [json.loads(line) for line in (directory / "client.stdout").read_text().splitlines() if line.strip()]
        checks = {
            "expected_client_exit": client.returncode == spec["exit"],
            "expected_http_attempts": sum(x["t"] == "sse" for x in requests) == spec["sse"],
            "expected_ws_attempts": sum(x["t"] == "ws" for x in requests) == spec["ws"],
            "turn_outcome": any(x["type"] == ("turn.completed" if spec["exit"] == 0 else "turn.failed") for x in events),
            "gateway_released": runtime["inflight"] == 0 and runtime["rejected"] == 0,
            "usage_written": records["total"] > 0,
        }
        waits = client_intervals if spec.get("wait_clock") == "client" else intervals
        if "wait_min_ms" in spec:
            checks["retry_advice_honored"] = bool(waits) and waits[0] >= spec["wait_min_ms"]
        if "wait_max_ms" in spec:
            checks["unsupported_advice_removed"] = bool(intervals) and intervals[0] < spec["wait_max_ms"]
        if "record_errors" in spec:
            checks["expected_gateway_errors"] = [row["error"] for row in sorted(records["records"], key=lambda r: r["ts_ms"])] == spec["record_errors"]
        if "handshake_error" in spec:
            matches = [
                row for row in records["records"] if
                row["transport"] == "ws" and row["error"] == spec["handshake_error"] and row["account"] == "acc-a"
                and row["err_source"] == ("client" if spec["handshake_error"] == "client_closed" else "network")
                and row["status"] == (0 if spec["handshake_error"] == "client_closed" else 504)
            ]
            checks["handshake_cause_recorded"] = len(matches) == spec["ws"]
            if spec["handshake_error"] == "client_closed":
                checks["upstream_preparation_cancelled"] = sum(
                    x["t"] == "closed" and x.get("transport") == "ws-handshake" and not x["terminal"]
                    for x in wire) == spec["ws"]
            if "handshake_min_ms" in spec:
                checks["default_handshake_deadline"] = all(row["duration_ms"] >= spec["handshake_min_ms"] for row in matches)
        result = {"passed": all(checks.values()), "checks": checks, "spec": spec,
                  "client_exit": client.returncode, "elapsed_ms": elapsed_ms, "request_intervals_ms": intervals,
                  "client_request_intervals_ms": client_intervals,
                  "upstream": wire, "records": records, "runtime": runtime}
        save(directory / "result.json", result)
        if not result["passed"]:
            raise RuntimeError("failed checks: " + ", ".join(k for k, ok in checks.items() if not ok))
        return result
    finally:
        for child in reversed(owned):
            stop(child)


class AppServer:
    """Small stdio JSON-RPC driver; no real command execution or external tools."""

    def __init__(self, args, directory, client_dir, work, tool_handler=None, name="client"):
        self.directory, self.timeout = directory, args.timeout
        self.name = name
        self.tool_handler = tool_handler
        self.messages, self.next_id, self.tool_calls = [], 0, []
        self.tool_replies = []
        self.incoming = queue.Queue()
        with (directory / (name + ".stderr")).open("w") as err:
            self.child = subprocess.Popen([str(args.codex_bin), "app-server", "--stdio"],
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
                                          text=True, bufsize=1, env=client_env(client_dir), cwd=work)
        CHILDREN.append(self.child)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        with (self.directory / (self.name + ".stdout")).open("w") as out:
            for line in self.child.stdout:
                out.write(line)
                out.flush()
                try:
                    self.incoming.put(json.loads(line))
                except ValueError as exc:
                    self.incoming.put(exc)
        self.incoming.put(None)

    def send(self, message):
        encoded = json.dumps(message) + "\n"
        with (self.directory / (self.name + ".stdin")).open("a") as log:
            log.write(encoded)
        self.child.stdin.write(encoded)
        self.child.stdin.flush()

    def receive(self, timeout, allow_timeout=False):
        try:
            message = self.incoming.get(timeout=max(0.001, timeout))
        except queue.Empty as exc:
            if allow_timeout:
                return None
            raise RuntimeError("Codex app-server did not produce the expected event") from exc
        if message is None or isinstance(message, Exception):
            raise RuntimeError(f"Codex app-server stdout ended or was invalid: {message}")
        self.messages.append(message)
        if "method" in message and "id" in message:
            if message["method"] != "item/tool/call":
                raise RuntimeError(f"unexpected server request: {message['method']}")
            params = message["params"]
            if params["tool"] != "test_echo" or params["arguments"] != {"text": "fixture tool input"}:
                raise RuntimeError(f"unexpected dynamic tool call: {params}")
            self.tool_calls.append(params)
            output = self.tool_handler(params) if self.tool_handler else "fixture tool output"
            # None deliberately leaves the request pending for a fault-window probe.
            if output is not None:
                self.send({"id": message["id"], "result": {
                    "contentItems": [{"type": "inputText", "text": output}], "success": True}})
                self.tool_replies.append(params["callId"])
        return message

    def wait(self, predicate, start=0):
        deadline = time.monotonic() + self.timeout
        while True:
            for message in self.messages[start:]:
                if predicate(message):
                    return message
            start = len(self.messages)
            if time.monotonic() >= deadline:
                raise RuntimeError("Codex app-server event deadline expired")
            self.receive(deadline - time.monotonic())

    def request(self, method, params):
        self.next_id += 1
        ident = "probe-" + str(self.next_id)
        mark = len(self.messages)
        self.send({"id": ident, "method": method, "params": params})
        message = self.wait(lambda m: m.get("id") == ident and "method" not in m, mark)
        if "error" in message:
            raise RuntimeError(f"{method}: {message['error']}")
        return message["result"]


def run_conversation(args, directory, transport, native_steer=False):
    client_dir, work, upstream, listen, config = prepare(
        directory, transport, fixture={"capture": True, "conversation": True})
    if native_steer:
        # Codex 0.164's bundled gpt-5.5 metadata sets use_responses_lite=false.
        # The wire-interrupt branch requires this capability. Exercise it explicitly
        # with a synthetic local catalog, never claim it is the shipping default.
        catalog = client_dir / "fixture-models.json"
        save(catalog, {"models": [{
            "slug": "gpt-5.5", "display_name": "Local interrupt fixture", "description": "Synthetic capability probe",
            "base_instructions": "Follow the local fixture conversation.",
            "supported_reasoning_levels": [{"effort": "low", "description": "Fixture"}],
            "shell_type": "shell_command", "visibility": "list", "supported_in_api": True, "priority": 0,
            "support_verbosity": False, "truncation_policy": {"mode": "tokens", "limit": 10000},
            "experimental_supported_tools": [], "use_responses_lite": True,
        }]})
        path = client_dir / "config.toml"
        path.write_text("model_catalog_json = " + json.dumps(str(catalog)) + "\n" + path.read_text())
    owned = []
    rpc = None
    try:
        start_servers(args, directory, upstream, listen, config, owned)
        rpc = AppServer(args, directory, client_dir, work)
        owned.append(rpc.child)
        rpc.request("initialize", {"clientInfo": {"name": "ciallopa_probe", "version": "1"},
                                   "capabilities": {"experimentalApi": True}})
        rpc.send({"method": "initialized", "params": {}})
        thread = rpc.request("thread/start", {
            "model": "gpt-5.5", "modelProvider": "smoke", "cwd": str(work), "ephemeral": True,
            "approvalPolicy": "never", "sandbox": "read-only", "environments": [],
            "dynamicTools": [{"name": "test_echo", "description": "Local fixture echo",
                              "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}},
                                              "required": ["text"], "additionalProperties": False}}],
        })["thread"]["id"]
        completed = []
        turns = []
        for prompt in ("Call test_echo once, then answer.", "Second turn: answer briefly.",
                       "Keep answering until interrupted.", "Answer after cancellation."):
            mark = len(rpc.messages)
            turn = rpc.request("turn/start", {"threadId": thread,
                               "input": [{"type": "text", "text": prompt, "textElements": []}]})["turn"]["id"]
            turns.append(turn)
            if len(turns) == 3:
                rpc.wait(lambda m: m.get("method") == "item/agentMessage/delta"
                         and m.get("params", {}).get("turnId") == turn, mark)
                if native_steer:
                    rpc.request("turn/steer", {"threadId": thread, "expectedTurnId": turn,
                                "input": [{"type": "text", "text": "Finish using this new input.", "textElements": []}]})
                else:
                    rpc.request("turn/interrupt", {"threadId": thread, "turnId": turn})
            result = rpc.wait(lambda m: m.get("method") == "turn/completed"
                              and m.get("params", {}).get("turn", {}).get("id") == turn, mark)
            completed.append(result["params"]["turn"])
        # Close our app-server transport before checking that its gateway connection left.
        rpc.child.stdin.close()
        try:
            rpc.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            stop(rpc.child)
        rpc.reader.join(timeout=2)
        expected_count = 6 if native_steer else 5
        runtime, records = observations(listen, expected_records=expected_count)
        wire = read_wire(directory)
        requests = [x for x in wire if x["t"] == "sse" or
                    (x["t"] == "ws-frame" and x.get("generate") is not False and x.get("type") == "response.create")]
        bodies = [x.get("body", {}) for x in requests]
        outputs = [item for item in bodies[1].get("input", [])
                   if isinstance(item, dict) and item.get("type") == "function_call_output"]
        controls = [x for x in wire if x["t"] == "ws-control"]
        rows = records["records"]
        business_ids = [x["id"] for x in wire if x["t"] == "business"]
        body_turns = [json.loads(body.get("client_metadata", {}).get("x-codex-turn-metadata", "{}")).get("turn_id") for body in bodies]
        expected_turns = [turns[0], turns[0], turns[1], turns[2]] + ([turns[2]] if native_steer else []) + [turns[3]]
        checks = {
            "four_turn_outcomes": [x["status"] for x in completed] ==
                                  ["completed", "completed", "completed" if native_steer else "interrupted", "completed"],
            "one_dynamic_tool": len(rpc.tool_calls) == 1 and rpc.tool_calls[0]["callId"] == "call_fixture",
            "tool_result_returned": any(x.get("call_id") == "call_fixture" and "fixture tool output" in json.dumps(x) for x in outputs),
            "expected_model_requests": len(requests) == expected_count,
            "one_account_and_session": {x["acct"] for x in requests} == {"acc-a"}
                                       and len({x.get("session") for x in requests}) == 1 and bool(requests[0].get("session")),
            "turn_ids_preserved": body_turns == expected_turns,
            "actual_transport": not any(x["t"] == ("sse" if transport == "ws" else "ws") for x in wire),
            "expected_business_records": records["total"] == expected_count,
            "gateway_released": runtime["inflight"] == 0 and runtime["rejected"] == 0,
        }
        if transport == "ws":
            checks.update({
                "incremental_continuations": [body.get("previous_response_id") for body in bodies[1:4]] == business_ids[:3],
            })
            if native_steer:
                checks.update({
                    "connection_reused": sum(x["t"] == "ws" for x in wire) == 1,
                    "native_interrupt": len(controls) == 1 and controls[0]["body"].get("type") == "response.interrupt",
                    "interrupt_ack_and_terminal": sum(x["t"] == "interrupted" for x in wire) == 1,
                    "usage_marks_interrupted": sum(x.get("incomplete") == "interrupted" for x in rows) == 1,
                    "incremental_after_steer": [body.get("previous_response_id") for body in bodies[4:]] == business_ids[3:5],
                })
            else:
                checks.update({
                    "cancel_reconnects": sum(x["t"] == "ws" for x in wire) == 2 and not bodies[4].get("previous_response_id"),
                    "cancel_uses_socket_close": not controls and sum(x.get("error") == "client_closed" for x in rows) == 1,
                    "reconnect_preserves_encrypted_history": any(item.get("encrypted_content") == "fake-encrypted-history-owned-by-acc-a"
                        for item in bodies[4].get("input", []) if isinstance(item, dict)),
                })
        else:
            checks.update({
                "encrypted_history_preserved": any(item.get("encrypted_content") == "fake-encrypted-history-owned-by-acc-a"
                                                   for item in bodies[1].get("input", []) if isinstance(item, dict)),
                "cancel_closes_stream": any(x["t"] == "closed" and x["transport"] == "sse" and not x["terminal"] for x in wire),
                "usage_marks_client_close": sum(x.get("error") == "client_closed" for x in rows) == 1,
            })
        result = {"passed": all(checks.values()), "checks": checks, "transport": transport, "native_steer": native_steer,
                  "thread": thread, "turns": completed, "tools": rpc.tool_calls,
                  "upstream": wire, "records": records, "runtime": runtime,
                  "note": "Synthetic encrypted content checks preservation only, not provider cryptography."}
        save(directory / "result.json", result)
        if not result["passed"]:
            raise RuntimeError("failed checks: " + ", ".join(k for k, ok in checks.items() if not ok))
        return result
    finally:
        for child in reversed(owned):
            stop(child)
        if rpc:
            rpc.reader.join(timeout=2)


def prepare(directory, transport, failover=False, fixture=None,
            provider_options="request_max_retries = 0\nstream_max_retries = 0\n",
            gateway_options="", routing_options=""):
    directory.mkdir()
    client_dir = directory / "client"
    client_dir.mkdir(mode=0o700)
    work = directory / "work"
    work.mkdir()
    if fixture is not None:
        save(directory / "fixture.json", fixture)
    upstream, listen = port(), port()
    auth = directory / "auth"
    auth.mkdir(mode=0o700)
    for name, priority in [("acc-a", 10), ("acc-b", 0)][:2 if failover else 1]:
        account = auth / (name + ".json")
        save(account, {"type": "codex", "email": name + "@invalid", "account_id": name,
                       "access_token": "fake-local-cli-smoke-token", "expired": "2099-01-01T00:00:00Z",
                       "priority": priority})
        account.chmod(0o600)
    if failover:
        # HTTP 200 + response.created, then quota error before any model output.
        (directory / "midlimited").write_text("acc-a\n")
        # Codex 0.163 sends a separate WS prewarm. Keep the injected quota error
        # for the actual business turn; otherwise only the prewarm switches.
        if transport == "ws":
            (directory / "midlimit_business_only").touch()
    config = directory / "config.toml"
    config.write_text(f'''listen = "127.0.0.1:{listen}"
api_keys = [{{label = "cli-smoke", key = "{KEY}", admin = true}}]
instance_id = "cli-smoke"
upstream = "http://127.0.0.1:{upstream}/backend-api"
auth_dir = {json.dumps(str(auth))}
proxy_enabled = false
[routing]
strategy = "round-robin"
quota_avoid_threshold = 0.0
{routing_options}
[usage]
enabled = true
path = {json.dumps(str(directory / "usage.db"))}
prices = {json.dumps(str(directory / "prices.toml"))}
flush_ms = 20
[ui]
reset_credits_refresh_mins = 0
[log]
level = "info"
[upstream_opts]
{gateway_options}
''')
    (client_dir / "config.toml").write_text(f'''model = "gpt-5.5"
model_provider = "smoke"
model_reasoning_effort = "low"
approval_policy = "never"
sandbox_mode = "read-only"
cli_auth_credentials_store = "file"
web_search = "disabled"
[history]
persistence = "none"
[analytics]
enabled = false
[feedback]
enabled = false
[model_providers.smoke]
name = "Local smoke"
base_url = "http://127.0.0.1:{listen}/v1"
wire_api = "responses"
env_key = "CIALLOPA_SMOKE_KEY"
requires_openai_auth = false
supports_websockets = {str(transport == "ws").lower()}
{provider_options}
[features]
unbounded_connection_retries = false
plugins = false
''')
    return client_dir, work, upstream, listen, config


def run_tool_recovery(args, directory, transport, stage):
    """A real dynamic-tool callback writes a local counter; no external side effects.

    Disconnect only after Codex has sent that tool result in a following model
    request. This tests one committed-history boundary, not global exactly-once.
    """
    client_dir, work, upstream, listen, config = prepare(
        directory, transport, fixture={"capture": True, "tool_recovery": stage},
        provider_options="request_max_retries = 0\nstream_max_retries = 1\n")
    owned, executions = [], []
    rpc = None

    def count_tool(params):
        ordinal = len(executions) + 1
        output = f"fixture tool output; execution={ordinal}"
        executions.append({"call_id": params["callId"], "ordinal": ordinal, "output": output})
        save(directory / "tool-executions.json", executions)
        (directory / "tool-counter").write_text(str(ordinal) + "\n")
        return output

    try:
        start_servers(args, directory, upstream, listen, config, owned)
        rpc = AppServer(args, directory, client_dir, work, tool_handler=count_tool)
        owned.append(rpc.child)
        rpc.request("initialize", {"clientInfo": {"name": "ciallopa_probe", "version": "1"},
                                   "capabilities": {"experimentalApi": True}})
        rpc.send({"method": "initialized", "params": {}})
        thread = rpc.request("thread/start", {
            "model": "gpt-5.5", "modelProvider": "smoke", "cwd": str(work), "ephemeral": True,
            "approvalPolicy": "never", "sandbox": "read-only", "environments": [],
            "dynamicTools": [{"name": "test_echo", "description": "Local fixture counting tool",
                              "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}},
                                              "required": ["text"], "additionalProperties": False}}],
        })["thread"]["id"]
        mark = len(rpc.messages)
        turn = rpc.request("turn/start", {"threadId": thread,
                           "input": [{"type": "text", "text": "Call test_echo once, then answer.",
                                      "textElements": []}]})["turn"]["id"]
        rpc.wait(lambda m: m.get("method") == "item/tool/call", mark)
        deadline = time.monotonic() + args.timeout
        while not any(x["t"] == "held" for x in read_wire(directory)):
            if time.monotonic() >= deadline:
                raise RuntimeError("tool result did not reach the held model response")
            time.sleep(0.025)
        if stage == "after_output":
            rpc.wait(lambda m: m.get("method") == "item/agentMessage/delta"
                     and m.get("params", {}).get("turnId") == turn, mark)
        # The fixture is held only for the first post-tool response. This command
        # cannot disconnect the recovered response or a later turn.
        (directory / "disconnect_stream").touch()
        completed = rpc.wait(lambda m: m.get("method") == "turn/completed"
                             and m.get("params", {}).get("turn", {}).get("id") == turn, mark)["params"]["turn"]
        rpc.child.stdin.close()
        try:
            rpc.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            stop(rpc.child)
        rpc.reader.join(timeout=2)
        runtime, records = observations(listen, expected_records=3)
        wire = read_wire(directory)
        requests = [x for x in wire if x["t"] == "sse" or
                    (x["t"] == "ws-frame" and x.get("generate") is not False and x.get("type") == "response.create")]
        bodies = [x.get("body", {}) for x in requests]
        business = [x for x in wire if x["t"] == "business"]
        disconnects = [x for x in wire if x["t"] == "injected-disconnect"]
        followup = bodies[1].get("input", []) if len(bodies) > 1 else []
        recovered = bodies[2].get("input", []) if len(bodies) > 2 else []
        outputs = lambda items: [item for item in items if isinstance(item, dict)
                                 and item.get("type") == "function_call_output"]
        calls = [item for item in recovered if isinstance(item, dict) and item.get("type") == "function_call"]
        deltas = [m["params"] for m in rpc.messages if m.get("method") == "item/agentMessage/delta"
                  and m.get("params", {}).get("turnId") == turn]
        rows = records["records"]
        diagnostics = read_stream_diagnostics(directory)
        observed = diagnostics[0]["fields"] if len(diagnostics) == 1 else {}
        faults = [row for row in rows if row.get("error")]
        checks = {
            "turn_completed": completed["status"] == "completed",
            "tool_executed_once": len(executions) == len(rpc.tool_calls) == 1
                                  and executions[0]["call_id"] == "call_fixture"
                                  and (directory / "tool-counter").read_text().strip() == "1",
            "tool_result_committed_before_fault": len(outputs(followup)) == 1
                and outputs(followup)[0].get("call_id") == "call_fixture"
                and "fixture tool output; execution=1" in json.dumps(outputs(followup)[0]),
            "recovered_tool_history": len(calls) == 1 and calls[0].get("call_id") == "call_fixture"
                and len(outputs(recovered)) == 1 and outputs(recovered) == outputs(followup),
            "recovered_encrypted_history": any(item.get("encrypted_content") == "fake-encrypted-history-owned-by-acc-a"
                for item in recovered if isinstance(item, dict)),
            "three_model_requests": len(requests) == len(business) == 3,
            "one_injected_disconnect": len(disconnects) == 1 and len(business) > 1
                and disconnects[0]["id"] == business[1]["id"],
            "recovery_starts_without_previous_id": len(bodies) == 3 and not bodies[2].get("previous_response_id"),
            "one_account_and_session": {x["acct"] for x in requests} == {"acc-a"}
                and len({x.get("session") for x in requests}) == 1 and bool(requests[0].get("session")),
            "turn_id_preserved": all(json.loads(body.get("client_metadata", {}).get("x-codex-turn-metadata", "{}")).get("turn_id") == turn
                for body in bodies),
            "expected_output_deltas": len(deltas) == (2 if stage == "after_output" else 1),
            "final_answer": [item.get("text") for item in completed.get("items", [])
                if item.get("type") == "agentMessage"] == ["tool received"],
            "actual_transport": not any(x["t"] == ("sse" if transport == "ws" else "ws") for x in wire),
            "three_gateway_records": records["total"] == 3,
            "two_successes_one_fault": sum(x.get("error") is None for x in rows) == 2
                and sum(x.get("error") == ("stream_truncated" if transport == "sse" else "upstream_closed") for x in rows) == 1,
            "gateway_did_not_replay": all(x.get("retries") == 0 and x.get("switches") == 0 for x in rows),
            "gateway_released": runtime["inflight"] == 0 and runtime["ws_connections"] == 0 and runtime["rejected"] == 0,
            "one_diagnostic_correlates_fault": len(diagnostics) == len(faults) == 1
                and observed.get("record_id") == faults[0]["request_id"]
                and observed.get("thread_id") == thread and observed.get("turn_id") == turn,
            "diagnostic_stream_progress": observed.get("transport") == transport
                and observed.get("terminal_received") == "false" and observed.get("observation_limited") == "false"
                and observed.get("activity_received") == "true"
                and observed.get("last_event") == ("response.created" if stage == "before_output" else "response.output_text.delta")
                and observed.get("event_count") == ("1" if stage == "before_output" else "3"),
            "diagnostic_idle_durations": all(observed.get(key, "").isdigit() for key in ("activity_idle_ms", "event_idle_ms")),
            "diagnostic_error_class": observed.get("error_class") in
                (("eof",) if transport == "sse" else ("read_error", "eof", "close_frame")),
            "diagnostic_contains_no_payload": all(secret not in entry["line"] for entry in diagnostics
                for secret in (KEY, "fake-local-cli-smoke-token", "fixture tool", "fake-encrypted-history")),
        }
        if transport == "ws":
            checks["tool_followup_was_incremental"] = len(bodies) > 1 and bodies[1].get("previous_response_id") == business[0]["id"]
        result = {"passed": all(checks.values()), "checks": checks, "transport": transport, "stage": stage,
                  "thread": thread, "turn": completed, "tool_executions": executions,
                  "stream_diagnostics": diagnostics,
                  "client_deltas": deltas, "upstream": wire, "records": records, "runtime": runtime,
                  "note": "A local counting callback tests a completed tool result already sent in the next model request. "
                          "It does not establish exactly-once behavior for an unacknowledged tool or a model that generates another call."}
        save(directory / "result.json", result)
        if not result["passed"]:
            raise RuntimeError("failed checks: " + ", ".join(k for k, ok in checks.items() if not ok))
        return result
    finally:
        for child in reversed(owned):
            stop(child)
        if rpc:
            rpc.reader.join(timeout=2)


def wait_local(predicate, timeout, description):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise RuntimeError("deadline waiting for " + description)
        time.sleep(0.025)


def rollout_records(path):
    # Read only complete records; the official client may still be appending.
    return [json.loads(line) for line in path.read_bytes().split(b"\n")[:-1]
            if line.strip()] if path.exists() else []


def tool_items(items, kind):
    return [item for item in items if isinstance(item, dict) and item.get("type") == kind
            and item.get("call_id") == "call_fixture"]


def run_tool_fault(args, directory, transport, fault):
    """Observe missing tool acknowledgements, never infer an execution from history.

    All new cases intentionally use a fresh persistent thread and CODEX_HOME.
    The fixed worker is the only side effect; no model-supplied command is run.
    """
    client_dir, work, upstream, listen, config = prepare(
        directory, transport, fixture={"capture": True, "tool_fault": True,
                                       "hold_timeout_ms": (args.timeout + 10) * 1000})
    owned, clients = [], []
    counter = directory / "tool-counter"
    counter.write_text("0\n")
    crash = fault in ("pending-crash", "executed-unacknowledged-crash")
    executed = fault == "executed-unacknowledged-crash"
    worker = None

    def launch(name):
        rpc = AppServer(args, directory, client_dir, work, tool_handler=lambda _: None, name=name)
        owned.append(rpc.child)
        clients.append(rpc)
        rpc.request("initialize", {"clientInfo": {"name": "ciallopa_probe", "version": "1"},
                                   "capabilities": {"experimentalApi": True}})
        rpc.send({"method": "initialized", "params": {}})
        return rpc

    def response_items(records):
        return [record.get("payload", {}) for record in records if record.get("type") == "response_item"]

    def snapshot(path, name):
        records = rollout_records(path)
        save(directory / (name + ".json"), records)
        return response_items(records)

    def completed_message(message, turn):
        return message.get("method") == "turn/completed" and message.get("params", {}).get("turn", {}).get("id") == turn

    try:
        start_servers(args, directory, upstream, listen, config, owned)
        rpc = launch("client")
        started_thread = rpc.request("thread/start", {
            "model": "gpt-5.5", "modelProvider": "smoke", "cwd": str(work), "ephemeral": False,
            "approvalPolicy": "never", "sandbox": "read-only", "environments": [],
            "dynamicTools": [{"name": "test_echo", "description": "Local fixture counting tool",
                              "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}},
                                              "required": ["text"], "additionalProperties": False}}],
        })["thread"]
        save(directory / "thread-start.json", started_thread)
        thread = started_thread["id"]
        if started_thread.get("ephemeral") or not started_thread.get("path"):
            raise RuntimeError("this Codex build did not create a persistent thread")
        rollout = Path(started_thread["path"]).resolve()
        if not rollout.is_relative_to(client_dir.resolve()):
            raise RuntimeError("thread rollout escaped the isolated CODEX_HOME")
        mark = len(rpc.messages)
        turn = rpc.request("turn/start", {"threadId": thread,
                           "input": [{"type": "text", "text": "Call test_echo once, then answer.",
                                      "textElements": []}]})["turn"]["id"]
        pending = rpc.wait(lambda m: m.get("method") == "item/tool/call", mark)
        if pending["params"]["callId"] != "call_fixture":
            raise RuntimeError("unexpected fixture call ID")
        held = wait_local(lambda: next((x for x in read_wire(directory) if x["t"] == "held"), None),
                          args.timeout, "initial tool response barrier")
        wait_local(lambda: tool_items(response_items(rollout_records(rollout)), "function_call"),
                   args.timeout, "tool call persisted before fault")
        before = snapshot(rollout, "rollout-before-fault")
        save(directory / "pending-call.json", pending)
        if executed:
            # Fixed code only. Persist the side effect, then die without printing
            # any result. The driver never acknowledges item/tool/call to Codex.
            worker = spawn([sys.executable, "-c", """
import os, signal, sys
with open(sys.argv[1], "r+") as counter:
    count = int(counter.read()) + 1
    counter.seek(0)
    counter.write(str(count) + "\\n")
    counter.truncate()
    counter.flush()
    os.fsync(counter.fileno())
os.kill(os.getpid(), signal.SIGKILL)
""", str(counter)], directory, "tool-worker")
            owned.append(worker)
            worker.wait(timeout=5)
        fault_boundary = {"call_id": pending["params"]["callId"], "rpc_request_id": pending["id"],
                          "actual_count": int(counter.read_text()), "tool_replies": rpc.tool_replies[:],
                          "worker_exit": worker.returncode if worker else None, "held": held}
        save(directory / "fault-boundary.json", fault_boundary)
        first_outcome, resumed, disconnect_observation = None, None, None
        if crash:
            # No graceful EOF/interrupt: lose the official app-server process too.
            rpc.child.kill()
            rpc.child.wait(timeout=5)
            rpc.reader.join(timeout=2)
            runtime_at_fault, records_at_fault = observations(listen, expected_records=1)
            after_fault = snapshot(rollout, "rollout-after-fault")
            rpc = launch("client-restarted")
            resumed = rpc.request("thread/resume", {"threadId": thread, "cwd": str(work),
                                   "model": "gpt-5.5", "modelProvider": "smoke",
                                   "approvalPolicy": "never", "sandbox": "read-only"})["thread"]
            save(directory / "thread-resumed.json", resumed)
        else:
            if fault == "pending-disconnect":
                (directory / "disconnect_stream").touch()
                wait_local(lambda: any(x["t"] == "injected-disconnect" for x in read_wire(directory)),
                           args.timeout, "commanded upstream disconnect")
                # A broken network need not resolve a client-side tool callback.
                # Observe a finite window, record it, then cancel explicitly if
                # still pending. This is not a gateway retry/policy change.
                deadline = time.monotonic() + min(2, args.timeout)
                while time.monotonic() < deadline and not any(completed_message(m, turn) for m in rpc.messages[mark:]):
                    rpc.receive(deadline - time.monotonic(), allow_timeout=True)
                ended = next((m["params"]["turn"] for m in rpc.messages[mark:] if completed_message(m, turn)), None)
                disconnected_runtime, disconnected_records = observations(listen, expected_records=1)
                disconnect_observation = {"observation_seconds": min(2, args.timeout),
                                          "turn_completed": ended, "explicit_interrupt_needed": ended is None,
                                          "runtime_before_interrupt": disconnected_runtime,
                                          "records_before_interrupt": disconnected_records}
                save(directory / "disconnect-observation.json", disconnect_observation)
            else:
                ended = None
            if ended is None:
                rpc.request("turn/interrupt", {"threadId": thread, "turnId": turn})
            first_outcome = rpc.wait(lambda m: completed_message(m, turn), mark)["params"]["turn"]
            runtime_at_fault, records_at_fault = observations(listen, expected_records=1)
            after_fault = snapshot(rollout, "rollout-after-fault")
        read_before_next = rpc.request("thread/read", {"threadId": thread, "includeTurns": True})["thread"]
        save(directory / "thread-before-next-input.json", read_before_next)
        next_text = "Continue after the local fault. Do not call any tools."
        next_mark = len(rpc.messages)
        next_turn = rpc.request("turn/start", {"threadId": thread,
                                "input": [{"type": "text", "text": next_text, "textElements": []}]})["turn"]["id"]
        completed = rpc.wait(lambda m: completed_message(m, next_turn), next_mark)["params"]["turn"]
        final_thread = rpc.request("thread/read", {"threadId": thread, "includeTurns": True})["thread"]
        save(directory / "thread-final.json", final_thread)
        rpc.child.stdin.close()
        try:
            rpc.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            stop(rpc.child)
        rpc.reader.join(timeout=2)
        after_next = snapshot(rollout, "rollout-after-next-input")
        runtime, records = observations(listen, expected_records=2)
        wire = read_wire(directory)
        requests = [x for x in wire if x["t"] == "sse" or
                    (x["t"] == "ws-frame" and x.get("generate") is not False and x.get("type") == "response.create")]
        next_body = requests[-1].get("body", {}) if len(requests) > 1 else {}
        next_input = next_body.get("input", [])
        next_calls = tool_items(next_input, "function_call")
        next_outputs = tool_items(next_input, "function_call_output")
        tool_calls = [call for client in clients for call in client.tool_calls]
        rows = records["records"]
        first_events = [x["event"] for x in wire if x["t"] == "response" and x["id"] == held["id"]]
        expected_error = ("stream_truncated" if transport == "sse" else "upstream_closed") \
            if fault == "pending-disconnect" else "client_closed"
        checks = {
            "persistent_isolated_thread": started_thread["ephemeral"] is False and rollout.is_relative_to(client_dir.resolve()),
            "pending_call_persisted_before_fault": len(tool_items(before, "function_call")) == 1,
            "no_result_persisted_before_fault": not tool_items(before, "function_call_output"),
            "no_tool_result_replied": not any(client.tool_replies for client in clients),
            "actual_side_effect_count": int(counter.read_text()) == fault_boundary["actual_count"] == int(executed),
            "one_tool_dispatch": len(tool_calls) == 1 and tool_calls[0]["callId"] == "call_fixture",
            "tool_call_matches_thread_and_turn": pending["params"]["threadId"] == thread
                and pending["params"]["turnId"] == turn,
            "fault_before_response_terminal": len(first_events) == 4
                and first_events[-1]["type"] == "response.output_item.done"
                and first_events[-1]["item"].get("call_id") == "call_fixture",
            "next_input_preserves_call_and_outcome": len(next_calls) == 1 and len(next_outputs) == 1,
            "missing_result_not_fabricated_as_success": len(next_outputs) == 1
                and next_outputs[0].get("output") == ("aborted" if crash else
                    "dynamic tool call was cancelled before receiving a response"),
            "next_user_input_forwarded": next_text in json.dumps(next_input),
            "next_turn_completed": completed["status"] == "completed",
            "next_turn_same_thread": read_before_next["id"] == final_thread["id"] == thread,
            "next_turn_answer": [item.get("text") for item in completed.get("items", [])
                                 if item.get("type") == "agentMessage"] == ["tool received"],
            "two_model_requests": len(requests) == 2,
            "actual_transport": not any(x["t"] == ("sse" if transport == "ws" else "ws") for x in wire),
            "no_barrier_timeout": not any(x["t"] == "hold-timeout" for x in wire),
            "gateway_did_not_replay": all(row.get("retries") == 0 and row.get("switches") == 0 for row in rows),
            "gateway_released_at_fault": runtime_at_fault["inflight"] == 0
                and runtime_at_fault["ws_connections"] == 0 and runtime_at_fault["rejected"] == 0,
            "gateway_released": runtime["inflight"] == 0 and runtime["ws_connections"] == 0 and runtime["rejected"] == 0,
            "two_gateway_records": records["total"] == 2,
            "one_success_one_expected_fault": sum(row.get("error") is None for row in rows) == 1
                and sum(row.get("error") == expected_error for row in rows) == 1,
        }
        if crash:
            checks.update({
                "client_abruptly_killed": clients[0].child.returncode == -signal.SIGKILL,
                "resume_same_persistent_thread": resumed["id"] == thread and resumed["path"] == str(rollout),
                "call_survived_crash": len(tool_items(after_fault, "function_call")) == 1,
                "no_result_survived_crash": not tool_items(after_fault, "function_call_output"),
                "synthetic_abort_not_persisted_as_tool_result": not tool_items(after_next, "function_call_output"),
                "resume_starts_without_previous_id": not next_body.get("previous_response_id"),
                "worker_crashed_after_side_effect": not executed or (worker.returncode == -signal.SIGKILL
                    and not (directory / "tool-worker.stdout").read_text()),
            })
        else:
            checks["fault_turn_interrupted"] = first_outcome["status"] == "interrupted"
            checks["cancel_result_persisted"] = len(tool_items(after_next, "function_call_output")) == 1 \
                and tool_items(after_next, "function_call_output")[0].get("output") == \
                    "dynamic tool call was cancelled before receiving a response"
            checks["expected_disconnect"] = sum(x["t"] == "injected-disconnect" for x in wire) == int(fault == "pending-disconnect")
            if disconnect_observation:
                checks["disconnect_left_tool_pending_during_observation"] = disconnect_observation["explicit_interrupt_needed"]
                checks["gateway_released_before_tool_interruption"] = disconnected_runtime["inflight"] == 0 \
                    and disconnected_runtime["ws_connections"] == 0
        result = {"passed": all(checks.values()), "checks": checks, "transport": transport, "fault": fault,
                  "thread": thread, "call_id": pending["params"]["callId"], "actual_count": int(counter.read_text()),
                  "fault_boundary": fault_boundary, "fault_turn": first_outcome,
                  "fault_outcome": "client_process_killed" if crash else first_outcome["status"],
                  "disconnect_observation": disconnect_observation, "next_turn": completed,
                  "next_input": next_input, "persisted_tool_items": {
                      name: [item for item in items if item.get("call_id") == "call_fixture"]
                      for name, items in (("before_fault", before), ("after_fault", after_fault), ("after_next_input", after_next))},
                  "runtime_at_fault": runtime_at_fault, "records_at_fault": records_at_fault,
                  "upstream": wire, "records": records, "runtime": runtime,
                  "note": "The fixed local mock never generates a second tool call. Missing/aborted history does not "
                          "prove a side effect did not happen: compare actual_count=0 and 1 crash cases. This establishes "
                          "neither gateway exactly-once execution nor that a real model will not issue another call. "
                          "Filesystem evidence is process-crash evidence, not a power-loss durability guarantee."}
        save(directory / "result.json", result)
        if not result["passed"]:
            raise RuntimeError("failed checks: " + ", ".join(k for k, ok in checks.items() if not ok))
        return result
    finally:
        for child in reversed(owned):
            stop(child)
        for client in clients:
            client.reader.join(timeout=2)


def run_case(args, directory, transport, failover):
    client_dir, work, upstream, listen, config = prepare(directory, transport, failover)
    owned = []
    try:
        mock = spawn([str(args.mock), f"127.0.0.1:{upstream}"], directory, "mock",
                     env={**ENV, "MOCK_DIR": str(directory)})
        owned.append(mock)
        ready(mock, upstream)
        gateway = spawn([str(args.binary), "-c", str(config), "serve"], directory, "gateway")
        owned.append(gateway)
        ready(gateway, listen)
        get(listen, "/admin/v1/config")
        client = spawn([str(args.codex_bin), "exec", "--json", "--ephemeral", "--skip-git-repo-check",
                        "--sandbox", "read-only", "--color", "never", "Say hello. Do not use tools."],
                       directory, "client", env=client_env(client_dir), cwd=work)
        owned.append(client)
        try:
            client.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"CLI exceeded {args.timeout}s") from exc
        events = [json.loads(line) for line in (directory / "client.stdout").read_text().splitlines() if line.strip()]
        # Wait for the completed request to leave the stream and reach SQLite.
        deadline = time.monotonic() + 5
        while True:
            runtime = get(listen, "/admin/v1/config")["runtime"]
            records = get(listen, "/usage/v1/records?exclude_prewarm=1&limit=10")
            if runtime["inflight"] == 0 and records["total"]:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("CLI finished but no completed business record became available")
            time.sleep(0.025)
        wire = [json.loads(line) for line in (directory / "upstream.log").read_text().splitlines() if line.strip()]
        expected_accounts = ["acc-a", "acc-b"] if failover else ["acc-a"]
        final_account = expected_accounts[-1]
        attempts = [x["acct"] for x in wire if x["t"] == transport]
        business_frames = [x["acct"] for x in wire if x["t"] == "ws-frame"
                           and x.get("type") == "response.create" and x.get("generate") is not False]
        row = records["records"][0]
        messages = [e.get("item", {}).get("text", "") for e in events
                    if e.get("type") == "item.completed" and e.get("item", {}).get("type") == "agent_message"]
        checks = {
            "client_exit_zero": client.returncode == 0,
            "client_completed_turn": any(e.get("type") == "turn.completed" for e in events),
            "client_has_expected_answer": messages == ["ok from " + final_account],
            "client_no_failure_event": not any(e.get("type") in ("error", "turn.failed") for e in events),
            "actual_protocol_and_account_chain": attempts == expected_accounts,
            "ws_business_frame_chain": transport != "ws" or business_frames == expected_accounts,
            "no_transport_fallback": not any(x["t"] == ("sse" if transport == "ws" else "ws") for x in wire),
            "one_business_record": records["total"] == 1,
            "usage_completed": row["status"] == 200 and not row["error"] and not row["incomplete"],
            "usage_transport": row["transport"] == transport,
            "usage_account": row["account"] == final_account,
            "usage_switches": row["switches"] == int(failover),
            "usage_first_account": row["first_account"] == ("acc-a" if failover else None),
            "usage_tokens_observed": row["input_tokens"] == 1 and row["output_tokens"] == 1,
            "runtime_idle_without_rejection": runtime["inflight"] == 0 and runtime["rejected"] == 0,
        }
        result = {"transport": transport, "failover": failover, "passed": all(checks.values()),
                  "checks": checks, "client_exit": client.returncode, "messages": messages,
                  "upstream": wire, "records": records, "runtime": runtime}
        save(directory / "result.json", result)
        if not result["passed"]:
            raise RuntimeError("failed checks: " + ", ".join(k for k, ok in checks.items() if not ok))
        return result
    finally:
        for child in reversed(owned):
            stop(child)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-bin", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True, help="built ciallopa executable")
    parser.add_argument("--mock", type=Path, required=True, help="built mock_upstream example")
    parser.add_argument("--timeout", type=int, default=45, help="seconds allowed for each CLI process")
    parser.add_argument("--suite", choices=("smoke", "recovery", "conversation", "tool-recovery", "tool-faults", "all"), default="smoke")
    parser.add_argument("--case", action="append", default=[], help="run only a named case in the selected suite (repeatable)")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    for name in ("codex_bin", "binary", "mock"):
        path = getattr(args, name).expanduser().resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f"{name}: not an executable: {path}")
        setattr(args, name, path)
    cases = []
    if args.suite in ("smoke", "all"):
        cases.extend((transport + ("-quota-failover" if failover else "-success"),
                      "smoke", (transport, failover))
                     for transport in ("sse", "ws") for failover in (False, True))
    if args.suite in ("recovery", "all"):
        cases.extend((name, "recovery", spec) for name, spec in RECOVERY_CASES.items())
    if args.suite in ("conversation", "all"):
        cases.extend((transport + "-conversation", "conversation", (transport, False)) for transport in ("sse", "ws"))
        cases.append(("ws-native-steer", "conversation", ("ws", True)))
    if args.suite in ("tool-recovery", "all"):
        cases.extend((transport + "-tool-recovery-" + stage.replace("_", "-"), "tool-recovery", (transport, stage))
                     for transport in ("sse", "ws") for stage in ("before_output", "after_output"))
    if args.suite in ("tool-faults", "all"):
        cases.extend((transport + "-tool-" + fault, "tool-faults", (transport, fault))
                     for transport in ("sse", "ws") for fault in
                     ("pending-disconnect", "pending-cancel", "pending-crash", "executed-unacknowledged-crash"))
    if args.case:
        unknown = set(args.case) - {name for name, _, _ in cases}
        if unknown:
            parser.error("unknown case for this suite: " + ", ".join(sorted(unknown)))
        cases = [case for case in cases if case[0] in args.case]
    run = Path(tempfile.mkdtemp(prefix="ciallopa-cli-smoke-"))
    print("artifacts=" + str(run), flush=True)
    results = []
    try:
        client_dir = run / "version-client"
        client_dir.mkdir(mode=0o700)
        version = spawn([str(args.codex_bin), "--version"], run, "version", env=client_env(client_dir))
        version.wait(timeout=10)
        if version.returncode:
            raise RuntimeError("codex --version failed")
        repo = Path(__file__).resolve().parents[1]
        git = lambda *a: subprocess.check_output(["git", *a], cwd=repo, text=True).strip()
        binaries = {str(p): {"sha256": digest(p), "bytes": p.stat().st_size}
                    for p in (args.codex_bin, args.binary, args.mock)}
        save(run / "manifest.json", {
            "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "codex_version": (run / "version.stdout").read_text().strip(),
            "source_head_at_run": git("rev-parse", "HEAD"), "source_status_at_run": git("status", "--short"),
            "binaries": binaries,
            "driver_sha256": digest(Path(__file__)), "timeout_seconds": args.timeout,
            "suite": args.suite, "cases": [case[0] for case in cases],
            "note": "Executable hashes identify the tested builds; source HEAD alone does not establish binary provenance.",
        })
        for name, suite, spec in cases:
            if suite == "smoke":
                result = run_case(args, run / name, *spec)
            elif suite == "recovery":
                result = run_recovery(args, run / name, spec)
            elif suite == "tool-recovery":
                result = run_tool_recovery(args, run / name, *spec)
            elif suite == "tool-faults":
                result = run_tool_fault(args, run / name, *spec)
            else:
                result = run_conversation(args, run / name, *spec)
            results.append({"name": name, **result})
            save(run / "results.json", results)
            print("PASS " + name, flush=True)
        if any(digest(Path(p)) != info["sha256"] for p, info in binaries.items()):
            raise RuntimeError("an executable changed during the smoke; rerun after its build finishes")
    except BaseException as exc:
        save(run / "failure.json", {"type": type(exc).__name__, "message": str(exc)})
        raise
    finally:
        for child in reversed(CHILDREN):
            stop(child)
        save(run / "processes.json", [{"pid": c.pid, "command": c.args, "exit_code": c.poll()} for c in CHILDREN])


if __name__ == "__main__":
    main()
