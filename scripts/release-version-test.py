#!/usr/bin/env python3
"""Exercise release-reference updates against copies of the real install docs."""

import importlib.util
import shutil
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("release_version", ROOT / "scripts/release-version.py")
RELEASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RELEASE)


class ReleaseVersionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for name in (*RELEASE.REFERENCES, "docs/RELEASE_NOTES.md"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, path)
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "9.8.7-rc.2"\n')

    def snapshot(self):
        return {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}

    def test_check_reports_drift_without_writing(self):
        before = self.snapshot()
        self.assertEqual(set(RELEASE.synchronize(self.root, check=True)), set(RELEASE.REFERENCES))
        self.assertEqual(self.snapshot(), before)

    def test_prerelease_sync_keeps_history_and_is_idempotent(self):
        notes = (self.root / "docs/RELEASE_NOTES.md").read_bytes()
        RELEASE.synchronize(self.root, check=False)
        readme = (self.root / "README.md").read_text()
        self.assertIn("/releases/download/v9.8.7-rc.2/ciallopa_9.8.7-rc.2-1_amd64.deb", readme)
        self.assertIn("/ciallopa/v9.8.7-rc.2/packaging/docker/compose.yaml", readme)
        self.assertIn("ciallopa-9.8.7-rc.2-aarch64-unknown-linux-musl.tar.gz", readme)
        self.assertIn("cd ciallopa-9.8.7-rc.2-x86_64-unknown-linux-musl", readme)
        self.assertIn("image: ghcr.io/seia-yurizono/ciallopa:9.8.7-rc.2", (self.root / "packaging/docker/compose.yaml").read_text())
        self.assertEqual((self.root / "docs/RELEASE_NOTES.md").read_bytes(), notes)
        self.assertEqual(RELEASE.synchronize(self.root, check=True), [])
        self.assertEqual(RELEASE.synchronize(self.root, check=False), [])

    def test_missing_template_does_not_partially_update(self):
        (self.root / "packaging/docker/compose.yaml").write_text("services: {}\n")
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, "install reference missing"):
            RELEASE.synchronize(self.root, check=False)
        self.assertEqual(self.snapshot(), before)

    def test_invalid_version_does_not_write(self):
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "latest"\n')
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, "expected a release version"):
            RELEASE.synchronize(self.root, check=False)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
