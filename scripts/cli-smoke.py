#!/usr/bin/env python3
"""Opt-in real Codex CLI smoke against local fake upstreams; Python stdlib only.

Four cases, no builds or downloads. Retains its fresh /tmp directory on success
and failure. Only owned subprocess handles are stopped; user config is untouched.
"""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


KEY = "sk-local-cli-smoke-fake-0123456789"
# Do not inherit credentials, proxy overrides, application configuration, or
# telemetry endpoints. CODEX_HOME below is Codex's documented state location,
# set only in the client subprocess environment, never in the parent process.
ENV = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL", "TERM") if k in os.environ}
CHILDREN = []


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
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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


def run_case(args, directory, transport, failover):
    directory.mkdir()
    client_dir = directory / "client"
    client_dir.mkdir(mode=0o700)
    work = directory / "work"
    work.mkdir()
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
[usage]
enabled = true
path = {json.dumps(str(directory / "usage.db"))}
prices = {json.dumps(str(directory / "prices.toml"))}
flush_ms = 20
[ui]
reset_credits_refresh_mins = 0
[log]
level = "info"
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
request_max_retries = 0
stream_max_retries = 0
''')
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
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    for name in ("codex_bin", "binary", "mock"):
        path = getattr(args, name).expanduser().resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f"{name}: not an executable: {path}")
        setattr(args, name, path)
    run = Path(tempfile.mkdtemp(prefix="ciallopa-cli-smoke-", dir="/tmp"))
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
            "note": "Executable hashes identify the tested builds; source HEAD alone does not establish binary provenance.",
        })
        for transport in ("sse", "ws"):
            for failover in (False, True):
                name = transport + ("-quota-failover" if failover else "-success")
                result = run_case(args, run / name, transport, failover)
                results.append(result)
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
