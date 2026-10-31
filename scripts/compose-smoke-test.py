#!/usr/bin/env python3
"""Check Compose smoke's persisted-account assertions using a synthetic SQLite file."""

import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('compose_smoke', Path(__file__).with_name('compose-smoke.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class ShutdownRecordsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / 'usage.db'
        self.models = ('shutdown-drain-sse', 'shutdown-drain-ws')
        with sqlite3.connect(self.database) as db:
            db.executescript('''
                CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                CREATE TABLE requests(id INTEGER PRIMARY KEY, model INTEGER, account INTEGER,
                    transport INTEGER, status INTEGER, error INTEGER, err_source INTEGER,
                    in_tok INTEGER, out_tok INTEGER);
                INSERT INTO labels VALUES
                    (1,'acct_ba2c09ba'), (2,'acct_12345678'), (3,'smoke-model'),
                    (4,'shutdown-drain-sse'), (5,'shutdown-drain-ws'),
                    (6,'shutdown'), (7,'local');
                INSERT INTO requests VALUES
                    (1,3,1,1,200,NULL,NULL,7,3),
                    (2,4,1,1,200,NULL,NULL,7,3),
                    (3,5,1,2,200,NULL,NULL,7,3);
            ''')

    def read(self, force=False, reference='0000000000000001'):
        return smoke.shutdown_records(self.database, self.models, reference, force)

    def change(self, sql):
        with sqlite3.connect(self.database) as db:
            db.execute(sql)

    def test_drain_accepts_the_fixture_alias(self):
        rows = self.read()
        self.assertEqual([r['request_id'] for r in rows], ['0000000000000002', '0000000000000003'])
        self.assertEqual({r['account'] for r in rows}, {'acct_ba2c09ba'})

    def test_force_keeps_shutdown_cause_checks(self):
        self.change('UPDATE requests SET error=6, err_source=7, in_tok=NULL, out_tok=NULL WHERE id IN (2,3)')
        self.assertEqual(len(self.read(force=True)), 2)
        self.change('UPDATE requests SET err_source=NULL WHERE id=3')
        with self.assertRaisesRegex(RuntimeError, 'wrong Force cause'):
            self.read(force=True)

    def test_another_valid_alias_is_rejected(self):
        self.change('UPDATE requests SET account=2 WHERE id=3')
        with self.assertRaisesRegex(RuntimeError, 'wrong account'):
            self.read()

    def test_raw_account_storage_is_rejected(self):
        self.change("UPDATE labels SET name='fixture' WHERE id=1")
        with self.assertRaisesRegex(RuntimeError, 'not stored as an alias'):
            self.read()

    def test_missing_reference_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'reference request missing'):
            self.read(reference='0000000000000010')

    def test_missing_final_record_is_rejected(self):
        self.change('DELETE FROM requests WHERE id=3')
        with self.assertRaisesRegex(RuntimeError, 'flush lost records'):
            self.read()

    def test_incorrect_tokens_are_rejected(self):
        self.change('UPDATE requests SET out_tok=0 WHERE id=2')
        with self.assertRaisesRegex(RuntimeError, 'wrong completed usage'):
            self.read()


if __name__ == '__main__':
    unittest.main()
