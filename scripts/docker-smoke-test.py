#!/usr/bin/env python3
"""Exercise the real smoke script's first-run checks without Docker or sudo."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("docker-smoke.sh").resolve()
MISSING_CONFIG = ('no config file at /etc/ciallopa/config.toml (config/config.toml on the host, '
                  'with the example compose file); run "ciallopa init"')


class DockerSmokePipeTests(unittest.TestCase):
    def run_smoke(self, mode="ok", script=SCRIPT):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = root / "bin"
            commands.mkdir()
            docker = commands / "docker"
            docker.write_text(f"#!{sys.executable}\n" + r'''
import os, signal, sys
signal.signal(signal.SIGPIPE, signal.SIG_DFL)
args = sys.argv[1:]
mode = os.environ["SMOKE_FIXTURE_MODE"]
def emit(text):
    data = text.encode()
    while data:
        data = data[os.write(1, data):]
if args[0] == "wait":
    emit("1\n")
elif args[0] == "logs":
    emit((os.environ["SMOKE_FIXTURE_MESSAGE"] if mode != "missing_log" else "wrong message") + "\n")
    # Far larger than a pipe buffer, including after the matching first line.
    for _ in range(128):
        emit("log padding " * 1024 + "\n")
    if mode == "producer_failure":
        sys.exit(42)
elif args[0] == "run" and args[-1] == "init":
    key = "sk-cpa-" + "a" * 48 if mode != "missing_key" else "absent"
    emit("Wrote /etc/ciallopa/config.toml (mode 0600; config/config.toml on the host, with the example compose file)\n")
    emit("CPA_KEY=" + key + "\n")
    emit("docker compose exec ciallopa ciallopa login\n")
    for _ in range(4096):
        emit(key + " init padding\n")
elif args[0] == "run":
    emit("fixture-container\n")
elif args[0] != "rm":
    sys.exit("unexpected docker command")
''')
            sudo = commands / "sudo"
            sudo.write_text(f"#!{sys.executable}\n" + r'''
import os, sys
args = sys.argv[1:]
if args[0] == "stat":
    # Stop before real container filesystem checks: first-run assertions passed.
    print("reached-config-validation", file=sys.stderr)
    sys.exit(77)
if args[0] in ("chown", "chmod"):
    sys.exit(0)
if args[0] == "rm" and args[1] == "-rf":
    assert os.path.commonpath([args[2], os.environ["TMPDIR"]]) == os.environ["TMPDIR"]
    os.execvp("rm", args)
sys.exit("unexpected sudo command")
''')
            docker.chmod(0o755)
            sudo.chmod(0o755)
            env = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}",
                   "TMPDIR": directory, "SMOKE_FIXTURE_MODE": mode,
                   "SMOKE_FIXTURE_MESSAGE": MISSING_CONFIG}
            return subprocess.run(["bash", str(script), "fixture:image"], env=env,
                                  capture_output=True, text=True, timeout=15)

    def test_large_matching_logs_and_init_output_are_fully_consumed(self):
        result = self.run_smoke()
        # The surrounding `test "$(sudo stat ...)" = ...` returns 1 at our
        # deliberate fixture boundary; the marker proves earlier checks passed.
        self.assertEqual(result.returncode, 1, result.stderr[-1000:])
        self.assertIn("reached-config-validation", result.stderr)

    def test_missing_log_match_still_fails(self):
        result = self.run_smoke("missing_log")
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("reached-config-validation", result.stderr)

    def test_matching_logs_do_not_hide_producer_failure(self):
        result = self.run_smoke("producer_failure")
        self.assertEqual(result.returncode, 42)
        self.assertNotIn("reached-config-validation", result.stderr)

    def test_missing_init_key_still_fails(self):
        result = self.run_smoke("missing_key")
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("reached-config-validation", result.stderr)


if __name__ == "__main__":
    unittest.main()
