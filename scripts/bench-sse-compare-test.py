#!/usr/bin/env python3
"""Small regression fixtures; no benchmark load."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("bench", Path(__file__).with_name("bench-sse-compare.py"))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def row(rep, build, value, valid=True):
    return {"scenario": "sse-latency-c4", "rep": rep, "build": build,
            "valid": valid, "cpu_us_per_req": value}


class Contract(unittest.TestCase):
    def test_cloud_payload_and_defaults(self):
        self.assertEqual(set(bench.SCEN), {"sse-small-usage-on", "sse-small-usage-off", "sse-latency-c4"})
        for scenario in bench.SCEN.values():
            self.assertEqual(scenario[2:5], (20, 30, 200))

    def test_pair_by_key_not_position_and_skip_missing_failed(self):
        rows = [row(0, "main", 100), row(1, "main", 200), row(2, "main", 300),
                row(1, "new", 220), row(0, "new", 110), row(2, "new", 99, False),
                row(3, "new", 400)]
        self.assertEqual(bench.paired(rows, "main", "new", lambda r: r["cpu_us_per_req"]), [1.1, 1.1])

    def test_ambiguous_identity_rejected(self):
        for rows in [[row(0, "main", 1)] * 2, [{"scenario": "sse-latency-c4", "build": "main"}]]:
            with self.assertRaises(ValueError):
                bench.paired(rows, "main", "new", lambda r: r["cpu_us_per_req"])

    def test_failures_never_become_success(self):
        good = {"started": 10, "ok": 10, "errors": 0, "incomplete": 0}
        self.assertTrue(bench.valid_load(good, 0))
        for bad, code in [(good, 1), ({**good, "errors": 1}, 0),
                          ({**good, "started": 11, "incomplete": 1}, 0),
                          ({"started": 0, "ok": 0, "errors": 0, "incomplete": 0}, 0)]:
            self.assertFalse(bench.valid_load(bad, code))

    def test_existing_output_directory_is_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "results.jsonl"
            marker.write_text("keep\n")
            env = {**os.environ, "BENCH_HX": "/bin/true", "BENCH_BINS": json.dumps({"main": "/bin/true"}), "BENCH_ORDER": '["main"]'}
            result = subprocess.run(["python3", str(Path(bench.__file__)), "1", "1", tmp], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(marker.read_text(), "keep\n")
            self.assertEqual(list(Path(tmp).iterdir()), [marker])


if __name__ == "__main__":
    unittest.main()
