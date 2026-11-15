#!/usr/bin/env python3
"""Opt-in Linux/user-systemd migration probe. Exit 0 pass, 1 fail, 77 unavailable."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def connect(path):
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)


# Current schema label references; resolve names so dictionary pruning cannot hide loss.
LABEL_COLUMNS = {
    "requests": "account first_account model effort endpoint error key instance plan tier req_tier resp_model resp_effort incomplete err_source",
    "quota_obs": "account instance plan",
    "quota_reset_events": "account", "quota_cycle_estimates": "account",
    "refusals": "key route ua model account instance", "occupancy": "instance",
}


def inspect(path):
    """v15+ invariants: all non-dictionary data, resolved thread names, lowercase codes.

    v16 moves session names from labels to threads and lowercases reset codes.
    Dictionary counts are evidence, not row-preservation assertions.
    """
    with connect(path) as c:
        version = c.execute("PRAGMA user_version").fetchone()[0]
        if version < 15:
            raise ValueError("this probe supports v15+ fixtures only")
        result = {"version": version, "sqlite_version": sqlite3.sqlite_version,
                  "page_size": c.execute("PRAGMA page_size").fetchone()[0],
                  "integrity": [r[0] for r in c.execute("PRAGMA integrity_check")],
                  "tables": {}}
        tables = [r[0] for r in c.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        for table in tables:
            # Identifiers come from SQLite; quote them even for synthetic fixtures.
            quote = lambda s: '"' + s.replace('"', '""') + '"'
            info = list(c.execute(f"PRAGMA table_info({quote(table)})"))
            columns = [r[1] for r in info]
            keys = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]
            ordering = ','.join(quote(k) for k in (keys or columns))
            count = c.execute(f"SELECT count(*) FROM {quote(table)}").fetchone()[0]
            entry = {"rows": count}
            if table not in ("labels", "threads"):
                expressions = []
                for col in columns:
                    if table == "requests" and col == "thread":
                        dictionary = "labels" if version < 16 else "threads"
                        expressions.append(f"(SELECT name FROM {dictionary} WHERE id=t.thread)")
                    elif col in LABEL_COLUMNS.get(table, "").split():
                        expressions.append(f"(SELECT name FROM labels WHERE id=t.{quote(col)})")
                    elif table == "reset_consume" and col == "code":
                        expressions.append('lower("code")')
                    else:
                        expressions.append(quote(col))
                h = hashlib.sha256()
                for row in c.execute(f"SELECT {','.join(expressions)} FROM {quote(table)} t ORDER BY {ordering}"):
                    h.update((json.dumps(row, ensure_ascii=True, default=repr) + "\n").encode())
                entry.update(columns=columns, sha256=h.hexdigest())
            result["tables"][table] = entry
        return result


def preserved(before, after):
    # New tables are allowed. Existing non-dictionary rows/values must survive.
    return all(after["tables"].get(t) == v for t, v in before["tables"].items()
               if t not in ("labels", "threads"))


def worker(binary, db, directory, limit):
    """Run inside the owned scope; save cgroup counters before the scope disappears."""
    directory = Path(directory)
    try:
        relative = next(line[3:] for line in Path("/proc/self/cgroup").read_text().splitlines()
                        if line.startswith("0::"))
        group = Path("/sys/fs/cgroup") / relative.lstrip("/")
        controls = {name: (group / name).read_text().strip()
                    for name in ("memory.max", "memory.swap.max")}
        if controls != {"memory.max": str(int(limit) * 1024 * 1024), "memory.swap.max": "0"}:
            raise ValueError(f"limits not applied: {controls}")
    except (OSError, ValueError, StopIteration) as e:
        save(directory / "child.json", {"status": "unavailable", "error": str(e)})
        return 77
    save(directory / "child.json", {"status": "running", "controls": controls, "cgroup": str(group)})
    start = time.monotonic()
    with (directory / "stdout").open("wb") as out, (directory / "stderr").open("wb") as err:
        child = subprocess.run([binary, "open", db], stdout=out, stderr=err, check=False)
    evidence = {"status": "finished", "exit_code": child.returncode,
                "wall_secs": time.monotonic() - start,
                "child_ru_maxrss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                "cgroup": str(group), "controls": controls}
    for name in ("memory.peak", "memory.events"):
        try:
            evidence[name] = (group / name).read_text().strip()
        except OSError as e:
            evidence[name] = {"unavailable": str(e)}
    save(directory / "child.json", evidence)
    return 0


def run(args):
    output = args.output.resolve()
    output.mkdir(mode=0o700)  # Exclusive: never reuse or overwrite evidence.
    binary, fixture = args.binary.resolve(), args.fixture.resolve()
    report = {"status": "failed", "expected_version": args.expected_version,
              "binary": str(binary), "fixture": str(fixture), "runs": []}
    try:
        report["binary_sha256"] = sha(binary)
        # SQLite backup includes committed WAL content and captures one consistent snapshot.
        snapshot = output / "snapshot.db"
        with connect(fixture) as source, sqlite3.connect(snapshot) as target:
            source.backup(target)
        report["snapshot_sha256"] = sha(snapshot)
        before = inspect(snapshot)
        report["before"] = before
        if before["integrity"] != ["ok"]:
            raise ValueError("fixture integrity check failed")
        fs = subprocess.run(["stat", "-f", "-c", "%T", str(output)], capture_output=True, text=True, check=True)
        report["filesystem"] = fs.stdout.strip()
        if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
            report.update(status="unavailable", error="Linux user-systemd required")
            return 77
        for limit in args.memory_mib:
            directory = output / f"{limit}mib"
            directory.mkdir()
            db = directory / "migration.db"
            shutil.copyfile(snapshot, db)  # Outside measured child process.
            scope = f"ciallopa-migration-{uuid.uuid4().hex}.scope"
            command = ["systemd-run", "--user", "--scope", "--quiet", "--unit", scope,
                       "-p", f"MemoryMax={limit}M", "-p", "MemorySwapMax=0",
                       sys.executable, str(Path(__file__).resolve()), "--worker",
                       str(binary), str(db), str(directory), str(limit)]
            item = {"memory_mib": limit, "scope": scope, "command": command}
            report["runs"].append(item)
            try:
                completed = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout, check=False)
                item.update(launcher_exit=completed.returncode)
                (directory / "launcher.stdout").write_text(completed.stdout)
                (directory / "launcher.stderr").write_text(completed.stderr)
            except subprocess.TimeoutExpired:
                item.update(status="failed", error="scope timeout")
                # Only this randomly named scope belongs to this run.
                subprocess.run(["systemctl", "--user", "stop", scope], capture_output=True, check=False, timeout=15)
            child_path = directory / "child.json"
            if child_path.exists():
                item["child"] = json.loads(child_path.read_text())
            else:
                if item.get("status") != "failed":
                    item.update(status="unavailable", error="scope produced no child evidence; see launcher.stderr")
            # Reopen only our copy after the scope exits, including on child failure.
            try:
                after = inspect(db)
                item["after"] = after
                item["data_preserved"] = preserved(before, after)
                item["rollback_matches_snapshot"] = (after == before)
                child = item.get("child", {})
                if child.get("status") == "unavailable" or item.get("status") == "unavailable":
                    item["status"] = "unavailable"
                elif (item.get("status") != "failed" and child.get("exit_code") == 0 and after["integrity"] == ["ok"]
                      and after["version"] == args.expected_version and item["data_preserved"]
                      and sha(binary) == report["binary_sha256"]
                      and isinstance(child.get("memory.peak"), str)
                      and isinstance(child.get("memory.events"), str)):
                    item["status"] = "passed"
                else:
                    item["status"] = "failed"
            except (sqlite3.Error, ValueError) as e:
                item.update(status="failed", reopen_error=str(e))
        statuses = [r["status"] for r in report["runs"]]
        report["status"] = "failed" if "failed" in statuses else ("unavailable" if "unavailable" in statuses else "passed")
        return {"passed": 0, "failed": 1, "unavailable": 77}[report["status"]]
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as e:
        report["error"] = str(e)
        return 1
    finally:
        save(output / "result.json", report)
        print(json.dumps({"status": report["status"], "evidence": str(output / "result.json")}))


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker(*sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--memory-mib", required=True, nargs="+", type=int)
    parser.add_argument("--expected-version", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    if min(args.memory_mib) <= 0 or len(set(args.memory_mib)) != len(args.memory_mib) or args.timeout <= 0:
        parser.error("limits must be positive and unique; timeout must be positive")
    try:
        return run(args)
    except FileExistsError:
        parser.error("output must be a new directory")


if __name__ == "__main__":
    sys.exit(main())
