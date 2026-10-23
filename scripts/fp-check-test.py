#!/usr/bin/env python3
"""Check the real fingerprint helpers using fixtures; no sudo, hosts edits or network."""
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('fp-check.sh').resolve()


def shell(body, *args):
    # Override sudo only inside this child; every path passed to a helper is a fixture.
    proc = subprocess.Popen(
        ['bash', '-c', 'source "$1"; shift; sudo() { "$@"; }; ' + body,
         'fp-check-test', str(SCRIPT), *map(str, args)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        raise
    return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)


class FingerprintTests(unittest.TestCase):
    def test_both_protocols_are_required_and_checked(self):
        ws = {'label': 'ciallopa', 'method': 'GET', 'path': '/backend-api/codex/responses',
              'ja4': 't13d101000_61a7ad8aa9b6_0d308c48d2a3',
              'headers': [[name, 'fixture'] for name in [
                  'Host', 'Connection', 'Upgrade', 'Sec-WebSocket-Version',
                  'Sec-WebSocket-Key', 'Sec-WebSocket-Extensions']]}
        http = {'label': 'ciallopa', 'method': 'GET', 'path': '/backend-api/codex/models',
                'ja4': 't13d1011h2_61a7ad8aa9b6_0d308c48d2a3',
                'h2': {'akamai': '2:0;4:2097152;5:16384;6:16384|5177345|0|m,s,a,p'},
                'hello': {'HasGREASE': False}}
        cases = [('ws-only', [ws], 1), ('http-only', [http], 1),
                 ('both', [http, ws], 0), ('empty', [], 1),
                 ('wrong-http', [dict(http, ja4='wrong'), ws], 1)]
        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / 'captures.jsonl'
            for name, rows, expected in cases:
                with self.subTest(name=name):
                    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                    result = shell('check_captures "$1"', path)
                    self.assertEqual(result.returncode, expected, result.stdout + result.stderr)

    def test_hosts_cleanup_preserves_later_unrelated_edits(self):
        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / 'hosts'
            original = b'127.0.0.1 localhost\n::1 localhost\n'
            later = b'192.0.2.1 changed.example # added while the probe ran\n'
            path.write_bytes(original)
            result = shell('hosts_entry add "$1" fixture-marker', path)
            self.assertEqual(result.returncode, 0, result.stderr)
            with path.open('ab') as f:
                f.write(later)
            result = shell('hosts_entry remove "$1" fixture-marker', path)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(path.read_bytes(), original + later)

    def test_existing_hosts_mapping_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / 'hosts'
            original = b'192.0.2.1 chatgpt.com # somebody else\n'
            path.write_bytes(original)
            result = shell('hosts_entry add "$1" fixture-marker', path)
            self.assertNotEqual(result.returncode, 0)
            # Main's exit trap also removes its own marker after an unsuccessful add.
            result = shell('hosts_entry remove "$1" fixture-marker', path)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(path.read_bytes(), original)

    def test_stop_only_terminates_the_started_server(self):
        with tempfile.TemporaryDirectory() as work:
            server = Path(work) / 'fpserver'
            server.write_text('#!/bin/sh\nexec sleep 30\n')
            server.chmod(0o700)
            result = shell('''
                sleep 30 & unrelated=$!
                owned=
                trap 'kill "$unrelated" ${owned:+"$owned"} 2>/dev/null || true; wait || true' EXIT
                start_server "$1" "$1"
                for _ in {1..50}; do
                    [[ -s "$SERVER_PID_FILE" ]] && break
                    sleep 0.02
                done
                read -r owned < "$SERVER_PID_FILE"
                stop_server
                if kill -0 "$owned" 2>/dev/null; then exit 1; fi
                kill -0 "$unrelated"
            ''', work)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
