#!/usr/bin/env bash
# Build the static musl release binary for one target and package it into dist/:
#   ciallopa-<version>-<target>.tar.gz, ciallopa_<version>-1_<deb arch>.deb, SHA256SUMS
# TARGET: x86_64-unknown-linux-musl (default, amd64) or aarch64-unknown-linux-musl (arm64).
# Needs: the rust-toolchain.toml toolchain with that target, a musl C compiler for it (musl-tools'
# musl-gcc on a native host; set CC_<target> when cross-compiling), cargo-deb.
# Used as-is by .github/workflows/release.yml (once per architecture, on native runners).
#   --no-build   package an already built binary
set -euo pipefail

cd "$(dirname "$0")/.."
TARGET=${TARGET:-x86_64-unknown-linux-musl}
case "$TARGET" in
    x86_64-unknown-linux-musl) DEB_ARCH=amd64 ;;
    aarch64-unknown-linux-musl)
        DEB_ARCH=arm64
        # jemalloc bakes the page size in at build time and aborts on kernels with larger
        # pages. 64 KiB (jemalloc's own aarch64 cross-compile default) also runs on 4 KiB and
        # 16 KiB kernels, so a native build on a 4 KiB runner matches a cross build.
        export JEMALLOC_SYS_WITH_LG_PAGE=${JEMALLOC_SYS_WITH_LG_PAGE:-16}
        # Ubuntu's GCC emits outline atomics by default; their libgcc helper needs glibc's
        # __getauxval, so linking against musl fails (jemalloc's configure then finds no
        # atomics). Inline atomics work on every ARMv8 CPU.
        export CFLAGS_aarch64_unknown_linux_musl="${CFLAGS_aarch64_unknown_linux_musl:+$CFLAGS_aarch64_unknown_linux_musl }-mno-outline-atomics" ;;
    *) echo "error: unsupported TARGET $TARGET" >&2; exit 1 ;;
esac
DIST=${DIST:-dist}
TARGET_DIR=${CARGO_TARGET_DIR:-target}
BUILD=1
[[ "${1:-}" == "--no-build" ]] && BUILD=0

VERSION=$(cargo metadata --no-deps --format-version 1 --locked |
    python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="ciallopa"))')
NAME="ciallopa-${VERSION}-${TARGET}"
BIN="${TARGET_DIR}/${TARGET}/release/ciallopa"

if (( BUILD )); then
    cc_var="CC_${TARGET//-/_}"
    export "$cc_var=${!cc_var:-musl-gcc}"
    cargo build --release --locked --target "$TARGET" -p ciallopa
fi

"${STRIP:-strip}" "$BIN"
info=$(file -b "$BIN")
echo "binary: $info"
if [[ "$info" != *"static"* ]] || ldd "$BIN" 2>&1 | grep -q '=>'; then
    echo "error: $BIN is not statically linked" >&2
    exit 1
fi
# A cross-built binary runs through binfmt/qemu-user when available; skip it otherwise.
if [[ "$TARGET" == "$(uname -m)-unknown-linux-musl" ]] || [[ -n "${RUN_CROSS:-}" ]]; then
    "$BIN" --version
fi

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
    -o "$DIST/ciallopa_${VERSION}-1_${DEB_ARCH}.deb"

(cd "$DIST" && sha256sum -- *.tar.gz *.deb > SHA256SUMS && cat SHA256SUMS)
