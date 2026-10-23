#!/usr/bin/env python3
"""Small loopback comparison using already-built release binaries; fake data only.

No build, external service, existing-directory cleanup, or process-name killing.
Artifacts are retained in a new /tmp directory, including on failure. Linux /proc
sampling is optional elsewhere. This is a short closed-loop experiment, not an SLO.
"""

import argparse
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
KEY = "sk-local-benchmark-fake-0123456789"
BODY = b'{"model":"gpt-5.5","reasoning":{"effort":"low"},"stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}'
SAMPLE_SECONDS = 0.025
CHILDREN = []
ENV = {k: v for k, v in os.environ.items()
       if k.lower() not in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}
       and not k.startswith(("CIALLOPA_", "CPA_LITE_"))}


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def port():
    # The OS assigns a free loopback port. A bind race fails readiness; no foreign
    # process is terminated to reclaim it.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def spawn(command, directory, name):
    out = (directory / (name + ".stdout")).open("w")
    err = (directory / (name + ".stderr")).open("w")
    try:
        child = subprocess.Popen(command, stdout=out, stderr=err, env=ENV)
    finally:
        out.close()
        err.close()
    CHILDREN.append(child)
    return child


def stop(child):
    if child.poll() is None:
        # A second TERM asks ciallopa to skip its natural drain, if necessary.
        for _ in range(2):
            child.send_signal(signal.SIGTERM)
            try:
                child.wait(timeout=5)
                return
            except subprocess.TimeoutExpired:
                pass
        child.kill()
        child.wait(timeout=3)


def ready(child, listen):
    deadline = time.monotonic() + 5
    while child.poll() is None and time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", listen), timeout=0.2):
                return
        except OSError:
            time.sleep(0.025)
    raise RuntimeError(f"local child {child.pid} did not listen on {listen}; inspect its logs")


def request(listen, path, method="GET", body=None, timeout=3):
    conn = http.client.HTTPConnection("127.0.0.1", listen, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers={
            "Authorization": "Bearer " + KEY, "Content-Type": "application/json"})
        response = conn.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        conn.close()


def runtime(listen):
    status, _, body = request(listen, "/admin/v1/config")
    if status != 200:
        raise RuntimeError(f"admin readiness failed: HTTP {status}")
    return json.loads(body)["runtime"]


def idle_after_warmup(listen):
    deadline = time.monotonic() + 3
    while True:
        value = runtime(listen)
        if value["inflight"] == 0:
            return value
        if time.monotonic() >= deadline:
            raise RuntimeError("warmup finished but its gateway slot did not become idle")
        time.sleep(0.025)


def fixture(args, directory, usage=None, limit=64, events=None):
    directory.mkdir()
    upstream = port()
    mock = spawn([str(args.loadbench), "mock", f"127.0.0.1:{upstream}",
                  str(events or args.events), str(args.gap_ms)], directory, "mock")
    ready(mock, upstream)
    observed = {"mock": mock}
    if usage is None:
        return upstream, f"http://127.0.0.1:{upstream}/backend-api/codex/responses", observed
    auth = directory / "auth"
    auth.mkdir(mode=0o700)
    account = auth / "bench.json"
    save(account, {"type": "codex", "email": "bench@invalid", "account_id": "bench",
                   "access_token": "fake-local-benchmark-token", "expired": "2099-01-01T00:00:00Z"})
    account.chmod(0o600)
    listen = port()
    # JSON-quoted strings are valid TOML strings here; no shell interpolation.
    config = directory / "config.toml"
    config.write_text(f'''listen = "127.0.0.1:{listen}"
api_keys = [{{label = "local-benchmark", key = "{KEY}", admin = true}}]
instance_id = "local-benchmark"
upstream = "http://127.0.0.1:{upstream}/backend-api"
auth_dir = {json.dumps(str(auth))}
proxy_enabled = false
max_inflight_requests = {limit}
[upstream_opts]
compress_requests = true
[usage]
enabled = {str(usage).lower()}
path = {json.dumps(str(directory / "usage.db"))}
prices = {json.dumps(str(directory / "prices.toml"))}
[ui]
reset_credits_refresh_mins = 0
[log]
level = "warn"
''')
    gateway = spawn([str(args.binary), "-c", str(config), "serve"], directory, "gateway")
    ready(gateway, listen)
    runtime(listen)
    usage_status = request(listen, "/usage/v1/records?limit=1")[0]
    if usage_status != (200 if usage else 404):
        raise RuntimeError(f"usage={usage} did not match the running recorder: HTTP {usage_status}")
    observed["gateway"] = gateway
    return listen, f"http://127.0.0.1:{listen}/v1/responses", observed


def sample(pid):
    try:
        proc = Path("/proc") / str(pid)
        status = {line.split(":", 1)[0]: line.split(":", 1)[1].split()[0]
                  for line in (proc / "status").read_text().splitlines()
                  if line.startswith(("VmRSS:", "VmHWM:"))}
        stat = (proc / "stat").read_text().rpartition(")")[2].split()
        return {"rss_kib": int(status["VmRSS"]), "vm_hwm_kib": int(status["VmHWM"]),
                "fds": len(list((proc / "fd").iterdir())),
                "cpu_ticks": int(stat[11]) + int(stat[12])}
    except (OSError, KeyError, IndexError):
        return None


def load(args, directory, name, url, concurrency, observed):
    before = {role: sample(child.pid) for role, child in observed.items()}
    peaks = {role: dict(value) for role, value in before.items() if value}
    child = spawn([str(args.loadbench), "load", url, str(concurrency), str(concurrency),
                   KEY, str(args.timeout_secs)], directory, name)
    deadline = time.monotonic() + args.timeout_secs + 5
    while child.poll() is None:
        for role, process in observed.items():
            value = sample(process.pid)
            if value and role in peaks:
                peaks[role] = {key: max(peaks[role][key], value[key]) for key in value}
        if time.monotonic() > deadline:
            raise RuntimeError("load child exceeded its whole-request timeout plus startup allowance")
        time.sleep(SAMPLE_SECONDS)
    if child.returncode:
        raise RuntimeError(f"load exited {child.returncode}; inspect {directory / (name + '.stderr')}")
    result = json.loads((directory / (name + ".stdout")).read_text())
    resources = {}
    for role, process in observed.items():
        final = sample(process.pid)
        first = before[role]
        if not final or not first:
            resources[role] = None
            continue
        peak = peaks[role]
        resources[role] = {
            "pid": process.pid, "baseline_rss_kib": first["rss_kib"],
            "peak_sampled_rss_kib": max(peak["rss_kib"], final["rss_kib"]),
            "process_vm_hwm_kib": final["vm_hwm_kib"],
            "peak_sampled_fds": max(peak["fds"], final["fds"]),
            "cpu_delta_ms": (final["cpu_ticks"] - first["cpu_ticks"]) * 1000 / os.sysconf("SC_CLK_TCK"),
        }
    return {"load": result, "resources": resources}


def lifecycle_sanity(args, directory):
    # A longer stream keeps its slot during the checks, without generating big
    # buffers. Not reading this tiny stream is not a backpressure stress test.
    events = max(args.events, math.ceil(4000 / args.gap_ms))
    listen, url, observed = fixture(args, directory, usage=True, limit=1, events=events)
    conn = http.client.HTTPConnection("127.0.0.1", listen, timeout=3)
    response = None
    try:
        conn.request("POST", "/v1/responses", body=BODY,
                     headers={"Authorization": "Bearer " + KEY, "Content-Type": "application/json"})
        response = conn.getresponse()
        first_status = response.status
        response.read(1)
        time.sleep(0.1)
        held = runtime(listen)
        status, headers, body = request(listen, "/v1/responses", "POST", BODY)
        health = request(listen, "/healthz")[0]
        occupied = runtime(listen)
        started = time.monotonic()
        response.close()
        conn.close()
        while runtime(listen)["inflight"] and time.monotonic() - started < 2:
            time.sleep(0.025)
        after = runtime(listen)
        released_ms = (time.monotonic() - started) * 1000
        recovery = load(args, directory, "recovery", url, 1, observed)
        checks = {
            "initial_http_200": first_status == 200,
            "slot_held_after_headers": held["inflight"] == 1,
            "overflow_is_gateway_busy": status == 503 and json.loads(body).get("error", {}).get("type") == "gateway_busy",
            "retry_after_is_1": headers.get("retry-after") == "1",
            "health_available": health == 200,
            "admin_available_at_capacity": occupied["inflight"] == 1,
            "rejection_counted": occupied["rejected"] == held["rejected"] + 1,
            "slot_released_before_original_stream_end": after["inflight"] == 0,
            "new_request_completes": recovery["load"]["errors"] == 0,
        }
        result = {"kind": "lifecycle_sanity", "mock_events": events,
                  "mock_gap_ms": args.gap_ms, "checks": checks, "passed": all(checks.values()),
                  "runtime_held": held, "runtime_after_rejection": occupied, "runtime_after_close": after,
                  "observed_release_ms": released_ms, "overflow_http_status": status,
                  "overflow_body": json.loads(body), "recovery": recovery}
        save(directory / "result.json", result)
        print(json.dumps(result), flush=True)
        return result["passed"]
    finally:
        if response:
            response.close()
        conn.close()
        for child in reversed(list(observed.values())):
            stop(child)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/ciallopa")
    parser.add_argument("--loadbench", type=Path, default=ROOT / "target/release/examples/loadbench")
    parser.add_argument("--build-ref", default="unspecified: verify binary provenance separately")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[32, 64])
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--gap-ms", type=int, default=100)
    parser.add_argument("--timeout-secs", type=int, default=10)
    parser.add_argument("--skip-lifecycle", action="store_true")
    args = parser.parse_args()
    if any(n <= 0 for n in [*args.concurrency, args.rounds, args.events, args.gap_ms, args.timeout_secs]):
        parser.error("counts and time settings must be positive")
    if max(args.concurrency) > 64:
        parser.error("this comparison is limited to 64 concurrent requests; it is not a capacity test")
    if max(args.events * args.gap_ms, 4000) >= args.timeout_secs * 1000:
        parser.error("request timeout must exceed both the mock stream and four-second lifecycle stream")
    for binary in [args.binary, args.loadbench]:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"build the release binary first: {binary}")
    run = Path(tempfile.mkdtemp(prefix="ciallopa-bench-local-", dir="/tmp"))
    print("artifacts=" + str(run), flush=True)
    git = lambda *command: subprocess.check_output(["git", *command], cwd=ROOT, text=True).strip()
    manifest = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source_head_at_run": git("rev-parse", "HEAD"), "source_status": git("status", "--short"),
                "declared_build_ref": args.build_ref, "platform": platform.platform(), "cpus": os.cpu_count(),
                "python": platform.python_version(), "sample_seconds": SAMPLE_SECONDS,
                "tokio_worker_threads_env": ENV.get("TOKIO_WORKER_THREADS"),
                "logical_request_bytes": len(BODY), "compression": "gateway default zstd enabled; direct plaintext",
                "workload": "fresh processes; one warmup; total=concurrency; success-only percentiles",
                "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "binaries": {str(p): {"sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                                      "mtime": p.stat().st_mtime} for p in [args.binary, args.loadbench]}}
    for name, path, prefix in [("cpu_model", "/proc/cpuinfo", "model name"),
                               ("mem_total", "/proc/meminfo", "MemTotal")]:
        try:
            manifest[name] = next(line.split(":", 1)[1].strip()
                                  for line in Path(path).read_text().splitlines() if line.startswith(prefix))
        except (OSError, StopIteration):
            manifest[name] = None
    manifest["driver_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    save(run / "manifest.json", manifest)
    results = []
    try:
        for repeat in range(args.rounds):
            cases = [(c, kind, usage) for c in args.concurrency
                     for kind, usage in [("direct", None), ("gateway-usage-off", False), ("gateway-usage-on", True)]]
            if repeat % 2:
                cases.reverse()
            for concurrency, kind, usage in cases:
                case = run / f"round-{repeat + 1}-{concurrency}-{kind}"
                listen, url, observed = fixture(args, case, usage=usage)
                try:
                    warmup = load(args, case, "warmup", url, 1, observed)
                    if warmup["load"]["errors"]:
                        raise RuntimeError("warmup failed; inspect local fixture logs")
                    # loadbench stops at the terminal event; the Body lease can
                    # disappear slightly later. Do not spend one of the 64 slots
                    # on a warmup that is still finishing.
                    initial = idle_after_warmup(listen) if usage is not None else None
                    result = load(args, case, "load", url, concurrency, observed)
                    final = runtime(listen) if usage is not None else None
                    result.update({"kind": kind, "round": repeat + 1, "concurrency": concurrency,
                                   "usage": usage, "warmup": warmup["load"], "runtime_after": final,
                                   "gateway_rejections": final["rejected"] - initial["rejected"] if final else None})
                    if usage:
                        status, _, body = request(listen, "/usage/v1/summary?range=1m")
                        if status != 200:
                            raise RuntimeError("usage recorder unavailable after the measurement")
                        result["recorder_after_load"] = json.loads(body)["recorder"]
                    save(case / "result.json", result)
                    results.append(result)
                    save(run / "results.json", results)
                    print(json.dumps(result), flush=True)
                finally:
                    for child in reversed(list(observed.values())):
                        stop(child)
        if not args.skip_lifecycle and not lifecycle_sanity(args, run / "lifecycle"):
            raise RuntimeError("lifecycle sanity failed; inspect its result.json")
    except BaseException as error:
        save(run / "failure.json", {"error": str(error), "type": type(error).__name__})
        raise
    finally:
        for child in reversed(CHILDREN):
            stop(child)
        save(run / "processes.json", [{"pid": child.pid, "command": child.args,
                                       "exit_code": child.poll()} for child in CHILDREN])
        print("artifacts=" + str(run), flush=True)


if __name__ == "__main__":
    main()
