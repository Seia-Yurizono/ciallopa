#!/usr/bin/env python3
"""Exact no-gap short-SSE reproduction (fake data only).

BENCH_HX=... BENCH_BINS='{"main":"...","new":"..."}' \
  python3 scripts/bench-sse-compare.py 8 8 /new/output/dir
python3 scripts/bench-sse-compare.py --compare /new/output/dir/results.jsonl

Defaults preserve the supplied 2026-11-15 cloud kit: terminal-stop, N20/22
SSE events, delta text 30B, body parameter 200 (actual 192B), usage on/off,
concurrency 64/4. No claims about client->gateway keepalive. Raw process logs,
configs and load results are kept per round; usage DB lives in a fresh /tmp
directory (recorded, never deleted). Set BENCH_GW_CPUS / BENCH_LOAD_CPUS for
available disjoint CPUs; defaults 0-3 / 4-7. Compare requires explicit rep and
valid fields: legacy missing-rep output cannot establish paired identity.
"""
import json, os, signal, socket, subprocess, sys, tempfile, time, http.client
import hashlib, platform, statistics
from pathlib import Path

BIN = {}  # set BENCH_BINS='{"main":"/path/ciallopa","new":"/path/ciallopa"}'
BIN.update(json.loads(os.environ.get("BENCH_BINS", "{}")))
HX = os.environ.get("BENCH_HX", "")  # path to built benchx example
KEY = "sk-local-benchmark-fake-0123456789"
GW_CPUS = os.environ.get("BENCH_GW_CPUS", "0-3"); LOAD_CPUS = os.environ.get("BENCH_LOAD_CPUS", "4-7")
ENV = {k: v for k, v in os.environ.items() if k.lower() not in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}
       and not k.startswith(("CIALLOPA_", "CPA_LITE_"))}
TICK = os.sysconf("SC_CLK_TCK")

SCEN = {
    # name: (transport, conc, events, delta_bytes, body_bytes, usage, accounts)
    "sse-small-usage-on":  ("sse", 64, 20, 30, 200, True, 1),
    "sse-small-usage-off": ("sse", 64, 20, 30, 200, False, 1),
    "sse-latency-c4":      ("sse", 4, 20, 30, 200, True, 1),
}


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_listen(p, child, t=10):
    d = time.monotonic() + t
    while time.monotonic() < d and child.poll() is None:
        try:
            socket.create_connection(("127.0.0.1", p), timeout=0.2).close()
            return True
        except OSError:
            time.sleep(0.01)
    raise RuntimeError(f"port {p} not listening")


def get(p, path):
    c = http.client.HTTPConnection("127.0.0.1", p, timeout=10)
    c.request("GET", path, headers={"Authorization": "Bearer " + KEY})
    r = c.getresponse(); b = r.read(); c.close()
    return r.status, b


def proc(pid):
    try:
        st = {l.split(":")[0]: int(l.split()[1]) for l in open(f"/proc/{pid}/status") if l.startswith(("VmRSS", "VmHWM", "Threads"))}
        s = open(f"/proc/{pid}/stat").read().rpartition(")")[2].split()
        io = {l.split(":")[0]: int(l.split()[1]) for l in open(f"/proc/{pid}/io")}
        return {"rss": st["VmRSS"], "hwm": st["VmHWM"], "threads": st["Threads"], "utime": int(s[11]), "stime": int(s[12]),
                "write_bytes": io["write_bytes"], "syscw": io["syscw"], "wchar": io["wchar"], "fds": len(os.listdir(f"/proc/{pid}/fd"))}
    except OSError:
        return None


def stop(ch):
    if ch.poll() is None:
        for _ in range(2):
            ch.send_signal(signal.SIGTERM)
            try:
                ch.wait(timeout=5); return
            except subprocess.TimeoutExpired:
                pass
        ch.kill(); ch.wait()


def run(build, name, secs, outdir, rep):
    tr, conc, events, delta, body, usage, accounts = SCEN[name]
    d = outdir / "raw" / f"{name}-{rep}-{build}"
    d.mkdir(parents=True)
    usage_dir = Path(tempfile.mkdtemp(prefix="bench-sse-usage-", dir="/tmp"))
    kids = []
    try:
        up = port()
        mock = subprocess.Popen(["taskset", "-c", LOAD_CPUS, HX, "mock", f"127.0.0.1:{up}", str(events), str(delta)],
                                stdout=open(d / "mock.out", "w"), stderr=subprocess.STDOUT, env=ENV)
        kids.append(mock); wait_listen(up, mock)
        auth = d / "auth"; auth.mkdir(mode=0o700)
        for i in range(accounts):
            f = auth / f"bench{i}.json"
            f.write_text(json.dumps({"type": "codex", "email": f"bench{i}@invalid", "account_id": f"bench{i}",
                                     "access_token": f"fake-local-benchmark-token-{i}", "expired": "2099-01-01T00:00:00Z"}))
            f.chmod(0o600)
        listen = port()
        (d / "config.toml").write_text(f'''listen = "127.0.0.1:{listen}"
api_keys = [{{label = "local-benchmark", key = "{KEY}", admin = true}}]
instance_id = "local-benchmark"
upstream = "http://127.0.0.1:{up}/backend-api"
auth_dir = {json.dumps(str(auth))}
proxy_enabled = false
max_inflight_requests = 256
[upstream_opts]
compress_requests = true
[usage]
enabled = {str(usage).lower()}
path = {json.dumps(str(usage_dir / "usage.db"))}
prices = {json.dumps(str(usage_dir / "prices.toml"))}
[ui]
reset_credits_refresh_mins = 0
[log]
level = "warn"
''')
        t0 = time.monotonic()
        gw = subprocess.Popen(["taskset", "-c", GW_CPUS, BIN[build], "-c", str(d / "config.toml"), "serve"],
                              stdout=open(d / "gw.out", "w"), stderr=subprocess.STDOUT, env=ENV)
        kids.append(gw); wait_listen(listen, gw)
        startup_ms = (time.monotonic() - t0) * 1000
        get(listen, "/admin/v1/config")
        time.sleep(0.5)
        idle = proc(gw.pid)
        url = f"http://127.0.0.1:{listen}/v1/responses" if tr == "sse" else f"ws://127.0.0.1:{listen}/v1/responses"
        load = lambda s, f: subprocess.run(["taskset", "-c", LOAD_CPUS, HX, tr, url, str(conc), str(s), KEY, str(body)],
                                           stdout=f, stderr=subprocess.STDOUT, env=ENV, check=True, timeout=s + 35)
        with open(d / "warmup.json", "w") as f:
            load(2, f)
        before = proc(gw.pid)
        peak_rss = before["rss"]
        lp = subprocess.Popen(["taskset", "-c", LOAD_CPUS, HX, tr, url, str(conc), str(secs), KEY, str(body)],
                              stdout=open(d / "load.json", "w"), stderr=subprocess.STDOUT, env=ENV)
        kids.append(lp)
        deadline = time.monotonic() + secs + 35
        while lp.poll() is None:
            if time.monotonic() > deadline:
                raise RuntimeError("load exceeded deadline")
            s = proc(gw.pid)
            if s:
                peak_rss = max(peak_rss, s["rss"])
            time.sleep(0.05)
        after = proc(gw.pid)
        res = json.loads((d / "load.json").read_text().strip().splitlines()[-1])
        warm = json.loads((d / "warmup.json").read_text().strip().splitlines()[-1])
        valid = valid_load(res, lp.returncode) and valid_load(warm, 0) and gw.poll() is None and mock.poll() is None
        cpu_ms = (after["utime"] + after["stime"] - before["utime"] - before["stime"]) * 1000 / TICK
        rec = None
        if usage:
            time.sleep(1.0)  # let the writer flush (flush_ms default)
            st, b = get(listen, "/usage/v1/summary?range=1m")
            rec = json.loads(b).get("recorder") if st == 200 else {"status": st}
        final = proc(gw.pid)
        out = {"build": build, "scenario": name, "rep": rep, "valid": valid,
               "load_exit": lp.returncode,
               "invalid_reason": None if valid else "load/warmup failed, incomplete, or child exited", "raw_dir": str(d), "usage_dir": str(usage_dir),
               "contract": {"delta_events": events, "total_events": events + 2, "delta_text_bytes": delta,
                            "mock_sse_wire_bytes": 3009, "body_bytes_parameter": body,
                            "request_body_bytes": res.get("request_body_bytes"), "chunk_strategy": "one-event-per-Body-chunk-no-sleep",
                            "mock_tcp_nodelay": True, "sse_read_mode": "terminal-stop", "usage": usage,
                            "accounts": accounts, "compress_requests": True, "log_level": "warn"}, "secs": secs, "startup_ms": startup_ms, "idle_rss_kib": idle["rss"],
               "load": res, "warmup_ok": warm["ok"], "warmup_err": warm["errors"],
               "cpu_ms": cpu_ms, "user_ms": (after["utime"] - before["utime"]) * 1000 / TICK,
               "sys_ms": (after["stime"] - before["stime"]) * 1000 / TICK,
               "cpu_us_per_req": cpu_ms * 1000 / res["ok"] if valid else None,
               "peak_rss_kib": peak_rss, "vm_hwm_kib": after["hwm"], "threads": after["threads"], "fds_after": after["fds"],
               "write_bytes_incl_flush": final["write_bytes"] - before["write_bytes"],
               "syscw_incl_flush": final["syscw"] - before["syscw"], "recorder": rec,
               "loadavg": open("/proc/loadavg").read().split()[:3]}
        print(json.dumps(out), flush=True)
        with open(outdir / "results.jsonl", "a") as f:
            f.write(json.dumps(out) + "\n")
        return out
    finally:
        for k in reversed(kids):
            stop(k)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def command(*args):
    try:
        return subprocess.check_output(args, text=True).strip()
    except (OSError, subprocess.CalledProcessError) as e:
        return str(e)


def tokio_lock():
    lock = (Path(__file__).resolve().parents[1] / "Cargo.lock").read_text()
    return next((line for part in lock.split("[[package]]") if 'name = "tokio"' in part
                 for line in part.splitlines() if line.startswith("version =")), "unknown")


def valid_load(row, exit_code):
    return (exit_code == 0 and row.get("ok", 0) > 0 and row.get("errors") == 0
            and row.get("started") == row.get("ok") and row.get("incomplete") == 0)


def paired(rows, baseline, candidate, metric):
    """Only explicit, unique (scenario, rep) keys in this dataset may be paired."""
    indexed = {}
    for row in rows:
        if "rep" not in row:
            raise ValueError("missing rep; legacy JSONL cannot be safely paired")
        key = (row["scenario"], row["rep"], row["build"])
        if key in indexed:
            raise ValueError(f"duplicate key: {key}; compare each run separately")
        indexed[key] = row
    ratios = []
    for (scenario, rep, build), row in indexed.items():
        if build != baseline:
            continue
        other = indexed.get((scenario, rep, candidate))
        if row.get("valid") is not True or not other or other.get("valid") is not True:
            continue
        a, b = metric(row), metric(other)
        if a is not None and b is not None and a > 0:
            ratios.append(b / a)
    return ratios


def compare(paths):
    # Separate files are separate datasets: identical rep numbers cannot cross-pair.
    datasets = [[json.loads(line) for line in Path(p).read_text().splitlines()] for p in paths]
    baseline = os.environ.get("BENCH_BASELINE", "main")
    metrics = {"cpu_us_per_req": lambda r: r["cpu_us_per_req"],
               "total_p50": lambda r: r["load"]["total_p50"], "rps": lambda r: r["load"]["rps"]}
    for scenario in sorted({r["scenario"] for rows in datasets for r in rows}):
        for build in sorted({r["build"] for rows in datasets for r in rows} - {baseline}):
            for name, metric in metrics.items():
                ratios = [q for rows in datasets for q in paired([r for r in rows if r["scenario"] == scenario], baseline, build, metric)]
                print(json.dumps({"scenario": scenario, "baseline": baseline, "build": build,
                                  "metric": name, "pairs": len(ratios),
                                  "median_ratio": statistics.median(ratios) if ratios else None,
                                  "worse": sum(q < 1 if name == "rps" else q > 1 for q in ratios)}))


def main():
    reps = int(sys.argv[1]); secs = int(sys.argv[2]); outdir = Path(sys.argv[3]); names = sys.argv[4:] or list(SCEN)
    if reps <= 0 or secs <= 0:
        raise ValueError("reps and secs must be positive")
    builds = json.loads(os.environ.get("BENCH_ORDER", json.dumps(list(BIN))))
    if not builds or len(set(builds)) != len(builds):
        raise ValueError("BENCH_ORDER must contain distinct build names")
    for b in builds:
        if not b.replace("-", "").replace("_", "").isalnum():
            raise ValueError("build names must be alphanumeric, '-' or '_'")
        sha256(BIN[b])
    sha256(HX)
    for name in names:
        if name not in SCEN:
            raise ValueError(f"unknown scenario: {name}")
    outdir = outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=False)  # never overwrite or append to someone else's run
    meta = {"argv": sys.argv, "kernel": platform.platform(),
            "affinity": sorted(os.sched_getaffinity(0)), "gateway_cpus": GW_CPUS, "load_cpus": LOAD_CPUS,
            "checkout_tokio_lock": tokio_lock(), "rustc": command("rustc", "-Vv"),
            "tmp_filesystem": command("findmnt", "-n", "-o", "FSTYPE,OPTIONS", "-T", "/tmp"),
            "bins": {b: {"path": str(Path(BIN[b]).resolve()), "sha256": sha256(BIN[b])} for b in builds},
            "benchx": {"path": str(Path(HX).resolve()), "sha256": sha256(HX)},
            "note": "terminal-stop does not prove client connection reuse; /tmp usage IO depends on filesystem"}
    (outdir / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    failed = False
    for r in range(reps):
        k = r % len(builds)
        order = builds[k:] + builds[:k]
        if (r // len(builds)) % 2:
            order.reverse()
        for name in names:
            for b in order:
                try:
                    result = run(b, name, secs, outdir, r)
                    failed |= not result["valid"]
                except Exception as e:
                    failed = True
                    print(json.dumps({"build": b, "scenario": name, "rep": r, "valid": False, "error": str(e)}), flush=True)
                    with open(outdir / "results.jsonl", "a") as f:
                        f.write(json.dumps({"build": b, "scenario": name, "rep": r, "valid": False, "error": str(e)}) + "\n")

    return 1 if failed else 0


if __name__ == "__main__":
    if sys.argv[1:2] == ["--compare"]:
        compare(sys.argv[2:])
    else:
        sys.exit(main())
