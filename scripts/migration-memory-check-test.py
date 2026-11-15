#!/usr/bin/env python3
"""Small tool regressions; fake launcher tests do not establish resource enforcement."""
import importlib.util
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("probe", Path(__file__).with_name("migration-memory-check.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ProbeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fixture = self.root / "fixture.db"
        self.connection = sqlite3.connect(self.fixture)
        self.addCleanup(self.connection.close)
        self.connection.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA user_version=15;
            CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE requests(id INTEGER PRIMARY KEY, thread INTEGER, status INTEGER);
            CREATE TABLE reset_consume(id INTEGER PRIMARY KEY, code TEXT);
            INSERT INTO labels VALUES(1,'session');
            INSERT INTO requests VALUES(100,1,200);
            INSERT INTO reset_consume VALUES(1,'ABC');
        """)
        # Deliberately retain committed data in WAL while the runner snapshots it.
        self.binary = self.root / "binary"
        self.binary.write_text("synthetic test executable")

    def args(self):
        return SimpleNamespace(output=self.root / "evidence", fixture=self.fixture,
                               binary=self.binary, expected_version=17, memory_mib=[128], timeout=5)

    def launcher(self, outcome):
        def call(command, **kwargs):
            if command[0] == "stat":
                return subprocess.CompletedProcess(command, 0, "test-fs\n", "")
            directory = Path(command[-2])
            if outcome == "unavailable":
                return subprocess.CompletedProcess(command, 1, "", "user bus unavailable")
            db = Path(command[-3])
            if outcome == "passed":
                with sqlite3.connect(db) as c:
                    c.executescript("CREATE TABLE threads(id INTEGER PRIMARY KEY,name TEXT);"
                                    "INSERT INTO threads SELECT * FROM labels; DELETE FROM labels;"
                                    "UPDATE reset_consume SET code=lower(code); PRAGMA user_version=17;")
            probe.save(directory / "child.json", {"status": "finished", "exit_code": 0 if outcome == "passed" else 9,
                                                   "memory.peak": "100", "memory.events": "oom_kill 0"})
            return subprocess.CompletedProcess(command, 0, "", "")
        return call

    def exercise(self, outcome, expected):
        with patch.object(probe.subprocess, "run", side_effect=self.launcher(outcome)), patch.object(probe.shutil, "which", return_value="systemd-run"):
            self.assertEqual(probe.run(self.args()), expected)
        import json
        result = json.loads((self.root / "evidence/result.json").read_text())
        self.assertEqual(result["status"], outcome if outcome != "failed" else "failed")
        self.assertEqual(result["before"]["tables"]["requests"]["rows"], 1)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 15)
        return result

    def test_snapshot_includes_wal_and_allows_dictionary_migration(self):
        result = self.exercise("passed", 0)
        self.assertTrue(result["runs"][0]["data_preserved"])
        with self.assertRaises(FileExistsError):
            probe.run(self.args())

    def test_child_failure_is_not_pass(self):
        result = self.exercise("failed", 1)
        self.assertTrue(result["runs"][0]["rollback_matches_snapshot"])

    def test_backend_unavailable_is_not_pass(self):
        self.exercise("unavailable", 77)

    def test_data_loss_fails_invariants(self):
        before = probe.inspect(self.fixture)
        self.connection.execute("DELETE FROM requests")
        self.connection.commit()
        self.assertFalse(probe.preserved(before, probe.inspect(self.fixture)))


if __name__ == "__main__":
    unittest.main()
