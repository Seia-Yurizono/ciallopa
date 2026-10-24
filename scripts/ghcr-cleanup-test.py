#!/usr/bin/env python3
"""Regression checks for destructive retention decisions; no network or credentials."""
import datetime
import importlib.util
from pathlib import Path
import unittest
import sys

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location('cleanup', Path(__file__).with_name('ghcr-cleanup.py'))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


def digest(n):
    return 'sha256:' + f'{n:064x}'


def version(n, tags):
    return {'id': n, 'name': digest(n), 'created_at': f'2026-10-{n:02d}T00:00:00Z',
            'metadata': {'container': {'tags': tags}}}


NOW = datetime.datetime(2026, 11, 1, tzinfo=datetime.timezone.utc)


def plan(versions, manifest, now=NOW):
    return cleanup.plan(versions, manifest, now)


class RetentionTests(unittest.TestCase):
    def manifest(self, name):
        if name == digest(5):
            return {'manifests': [{'digest': digest(3)}, {'digest': digest(4)}]}
        return {'config': {}, 'layers': []}

    def test_aliases_count_as_two_tags_and_keep_runtime_and_attestation(self):
        versions = [version(1, []), version(2, ['0.1']), version(3, []),
                    version(4, []), version(5, ['0.1.0', 'latest'])]
        selected, protected, delete = plan(versions, self.manifest)
        self.assertEqual([t for t, _ in selected], ['latest', '0.1.0'])
        self.assertEqual(protected, {digest(3), digest(4), digest(5)})
        self.assertEqual({v['id'] for v in delete}, {1, 2})

    def test_two_tags_on_separate_images_preserve_shared_children(self):
        versions = [version(1, ['old']), version(3, ['previous']), version(4, []), version(5, ['new'])]
        _, protected, delete = plan(versions, self.manifest)
        self.assertEqual(protected, {digest(3), digest(4), digest(5)})
        self.assertEqual([v['id'] for v in delete], [1])

    def test_no_tag_refuses_entire_package_deletion(self):
        with self.assertRaises(RuntimeError):
            plan([version(1, [])], self.manifest)

    def test_missing_retained_manifest_aborts_before_deletion(self):
        def missing(_):
            raise RuntimeError('unavailable')
        with self.assertRaises(RuntimeError):
            plan([version(5, ['latest'])], missing)

    def multi_arch(self, name):
        # Merged release index: two runnable images and their two attestation manifests.
        if name == digest(9):
            return {'manifests': [{'digest': digest(d)} for d in (5, 6, 7, 8)]}
        return {'config': {}, 'layers': []}

    def test_multi_arch_index_keeps_every_child_and_drops_old_single_arch_image(self):
        versions = [version(1, []), version(2, []), version(3, []),
                    version(4, []), version(5, []), version(6, []), version(7, []), version(8, []),
                    version(9, ['0.1.2', '0.1', 'latest'])]
        # 1 and 2: the old amd64-only image and attestation; 3 and 4: per-arch upload indexes.
        selected, protected, delete = plan(versions, self.multi_arch)
        self.assertEqual([t for t, _ in selected], ['latest', '0.1'])
        self.assertEqual(protected, {digest(d) for d in (5, 6, 7, 8, 9)})
        self.assertEqual({v['id'] for v in delete}, {1, 2, 3, 4})

    def test_recent_untagged_uploads_wait_for_the_publishing_job(self):
        versions = [version(1, []), version(5, ['latest']), version(6, [])]
        now = datetime.datetime(2026, 10, 6, 3, tzinfo=datetime.timezone.utc)
        _, _, delete = cleanup.plan(versions, self.manifest, now)
        self.assertEqual([v['id'] for v in delete], [1])

    def test_single_tag_and_newer_untagged_version_past_grace(self):
        _, protected, delete = plan([version(1, ['only']), version(6, [])], self.manifest)
        self.assertEqual(protected, {digest(1)})
        self.assertEqual([v['id'] for v in delete], [6])


if __name__ == '__main__':
    unittest.main()
