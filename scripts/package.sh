#!/usr/bin/env bash
# Build the static x86_64 musl release binary and package it into dist/:
#   ciallopa-<version>-x86_64-unknown-linux-musl.tar.gz, ciallopa_<version>-1_amd64.deb, SHA256SUMS
# Needs: the rust-toolchain.toml toolchain with the x86_64-unknown-linux-musl target, musl-tools
# (musl-gcc), cargo-deb. Used as-is by .github/workflows/release.yml.
#   --no-build   package an already built binary
set -euo pipefail

cd "$(dirname "$0")/.."
TARGET=x86_64-unknown-linux-musl
DIST=${DIST:-dist}
TARGET_DIR=${CARGO_TARGET_DIR:-target}
BUILD=1
[[ "${1:-}" == "--no-build" ]] && BUILD=0

VERSION=$(cargo metadata --no-deps --format-version 1 --locked |
    python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="ciallopa"))')
NAME="ciallopa-${VERSION}-${TARGET}"
BIN="${TARGET_DIR}/${TARGET}/release/ciallopa"

if (( BUILD )); then
    export CC_x86_64_unknown_linux_musl=${CC_x86_64_unknown_linux_musl:-musl-gcc}
    cargo build --release --locked --target "$TARGET" -p ciallopa
fi

strip "$BIN"
info=$(file -b "$BIN")
echo "binary: $info"
if [[ "$info" != *"static"* ]] || ldd "$BIN" 2>&1 | grep -q '=>'; then
    echo "error: $BIN is not statically linked" >&2
    exit 1
fi
"$BIN" --version

rm -rf "$DIST"
mkdir -p "$DIST/$NAME"
install -m 755 "$BIN" "$DIST/$NAME/ciallopa"
install -m 644 config.example.toml README.md LICENSE \
    packaging/ciallopa.service "$DIST/$NAME/"
tar --owner=0 --group=0 --numeric-owner --sort=name -C "$DIST" -czf "$DIST/$NAME.tar.gz" "$NAME"
rm -rf "${DIST:?}/$NAME"

# Keep the SemVer filename: GitHub rewrites Debian's prerelease `~` to `.`, which
# would otherwise make the uploaded asset disagree with SHA256SUMS and download URLs.
# The package's internal Debian version still uses `~` for correct upgrade ordering.
cargo deb -p ciallopa --target "$TARGET" --no-build --no-strip --locked \
    -o "$DIST/ciallopa_${VERSION}-1_amd64.deb"

(cd "$DIST" && sha256sum -- *.tar.gz *.deb > SHA256SUMS && cat SHA256SUMS)
