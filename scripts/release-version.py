#!/usr/bin/env python3
"""Sync pinned install examples from workspace.package.version (Python 3.11+)."""

import argparse
import re
import sys
import tomllib
from pathlib import Path


# Only executable install references are generated. Historical versions in release
# notes and provider/dependency versions are deliberately outside this list.
REFERENCES = {
    "README.md": (
        r"(https://github\.com/Seia-Yurizono/ciallopa/releases/download/v)([^/\s]+)(/)",
        r"(https://raw\.githubusercontent\.com/Seia-Yurizono/ciallopa/v)([^/\s]+)(/)",
        r"(ciallopa_)([0-9][0-9A-Za-z.+-]*?)(-1_(?:amd64|arm64)\.deb)",
        r"(ciallopa-)([0-9][0-9A-Za-z.+-]*?)(-(?:x86_64|aarch64)-unknown-linux-musl)",
    ),
    "packaging/docker/compose.yaml": (
        r"(?m)^(\s*image: ghcr\.io/seia-yurizono/ciallopa:)(\S+)()",
    ),
}


def synchronize(root: Path, *, check: bool) -> list[str]:
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    version = manifest["workspace"]["package"]["version"]
    if not isinstance(version, str) or not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", version
    ):
        raise ValueError("expected a release version in workspace.package.version")

    changes = {}
    for name, patterns in REFERENCES.items():
        original = (root / name).read_text()
        updated = original
        for pattern in patterns:
            updated, count = re.subn(
                pattern, lambda m: m[1] + version + m[3], updated
            )
            if count == 0:
                raise ValueError(f"{name}: install reference missing; update REFERENCES")
        if original != updated:
            changes[name] = updated

    # Validate all templates before writing any file. --check never writes.
    if not check:
        for name, updated in changes.items():
            (root / name).write_text(updated)
    return list(changes)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on stale references without writing")
    args = parser.parse_args()
    try:
        changed = synchronize(Path(__file__).resolve().parent.parent, check=args.check)
    except (OSError, ValueError, KeyError) as error:
        print(f"release-version: {error}", file=sys.stderr)
        return 1
    for name in changed:
        print(f"{'stale' if args.check else 'updated'}: {name}")
    if args.check and changed:
        print("Run python3 scripts/release-version.py to sync install references.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
