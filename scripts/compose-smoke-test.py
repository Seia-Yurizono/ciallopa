#!/usr/bin/env python3
"""Check Compose smoke fixture configuration and persisted-account assertions locally."""

import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import tomllib
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('compose_smoke', Path(__file__).with_name('compose-smoke.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class FixtureConfigTests(unittest.TestCase):
    def test_current_shipped_template_has_no_duplicate_keys(self):
        template = (smoke.ROOT / 'config.example.toml').read_text()
        result = smoke.fixture_config(template, '127.0.0.1', 12345)
        config = tomllib.loads(result)
        self.assertEqual(config['upstream'], 'http://ciallopa-smoke.invalid/backend-api')
        self.assertEqual(config['proxy_url'], 'http://127.0.0.1:12345')
        self.assertTrue(config['proxy_enabled'])
        self.assertTrue(config['ui']['allow_config_write'])
        self.assertEqual(config['ui']['reset_credits_refresh_mins'], 0)
        self.assertEqual(config['usage']['prices_source'], '')
        self.assertEqual(config['usage']['flush_ms'], 20)
        self.assertFalse(config['upstream_opts']['compress_requests'])
        self.assertTrue(config['upstream_opts']['gzip_responses'])
        self.assertEqual(tomllib.loads(template)['upstream_opts']['compress_requests'], True)

    def test_older_template_with_implicit_compression_default(self):
        template = (smoke.ROOT / 'config.example.toml').read_text()
        template = template.replace('compress_requests = true\n', '')
        template = template.replace('gzip_responses = true\n', '')
        config = tomllib.loads(smoke.fixture_config(template, '127.0.0.1', 12345))
        self.assertFalse(config['upstream_opts']['compress_requests'])
        self.assertNotIn('gzip_responses', config['upstream_opts'])

    def test_existing_usage_defaults_are_replaced_and_generation_is_idempotent(self):
        template = (smoke.ROOT / 'config.example.toml').read_text()
        template = template.replace('[usage]\n', '[usage]\nflush_ms = 999\nprices_source = "fixture"\n')
        result = smoke.fixture_config(template, '127.0.0.1', 12345)
        self.assertEqual(result, smoke.fixture_config(result, '127.0.0.1', 12345))
        config = tomllib.loads(result)
        self.assertEqual(config['usage']['flush_ms'], 20)
        self.assertEqual(config['usage']['prices_source'], '')

    def test_section_scoping_keeps_other_tables_unchanged(self):
        text = '[usage]\nflush_ms = 100\n[other]\nflush_ms = 999\n'
        result = smoke.set_section_value(text, 'usage', 'flush_ms', '20')
        config = tomllib.loads(result)
        self.assertEqual(config['usage']['flush_ms'], 20)
        self.assertEqual(config['other']['flush_ms'], 999)

    def test_missing_or_duplicate_table_is_rejected(self):
        for text in ['', '[usage]\n[usage]\n']:
            with self.assertRaises(RuntimeError):
                smoke.set_section_value(text, 'usage', 'flush_ms', '20')


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
