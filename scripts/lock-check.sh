#!/usr/bin/env bash
# Env: CODEX_LOCK = path to codex-rs Cargo.lock (required).
# Every third-party crate version in our Cargo.lock must also exist in codex-rs Cargo.lock.
set -euo pipefail
CODEX_LOCK=${CODEX_LOCK:?set to codex-rs Cargo.lock}
python3 - "$(dirname "$0")/../Cargo.lock" "$CODEX_LOCK" <<'PY'
import re, sys
def lock(p):
    d, cur = {}, None
    for l in open(p):
        m = re.match(r'name = "(.*)"', l)
        if m: cur = m.group(1); continue
        m = re.match(r'version = "(.*)"', l)
        if m and cur: d.setdefault(cur, set()).add(m.group(1)); cur = None
    return d
a, b = lock(sys.argv[1]), lock(sys.argv[2])
# Usage-log storage stack (crates/usage): not on the network path, cannot affect TLS/h2/WS
# fingerprints. sqlite-wasm-rs and rsqlite-vfs are wasm32-only (never compiled on native
# targets; no longer in the lock since rusqlite 0.41).
ALLOW = {"rusqlite", "fallible-iterator", "fallible-streaming-iterator", "rsqlite-vfs", "sqlite-wasm-rs"}
# Security updates ahead of codex-rs, pinned to the exact version so any other drift still fails.
# Drop an entry once codex-rs ships the same (or a newer) version.
SECURITY = {
    # RUSTSEC-2026-0459 (SQLite < 3.54.0, CVE-2026-119375); rusqlite 0.41 needs hashlink 0.12.
    "libsqlite3-sys": "0.39.1", "hashlink": "0.12.2",
    # RUSTSEC-2026-0391/0398/0403/0410 and the zstd-safe unsound notices. Same libzstd 1.5.7:
    # level-3 request bodies are byte-identical to the 0.13.3 / 7.2.4 / 2.0.16 build.
    "zstd": "0.13.4", "zstd-safe": "7.4.0", "zstd-sys": "2.1.2+zstd.1.5.7",
    # RUSTSEC-2026-0486; dev-dependency only (tempfile), not in the shipped binary.
    "rustix": "1.1.6",
}
def extra(k, v):
    return v - b.get(k, set()) - ({SECURITY[k]} if k in SECURITY else set())
bad = {k: extra(k, v) for k, v in a.items()
       if not k.startswith("ciallopa") and k not in ALLOW and extra(k, v)}
allowed = {k: sorted(a[k]) for k in sorted(ALLOW) if k in a}
print("allowed (non-network):", allowed)
ahead = {k: v for k, v in sorted(SECURITY.items()) if v in a.get(k, set()) and v not in b.get(k, set())}
print("security ahead of codex-rs:", ahead or "none")
for k in ["reqwest", "rustls", "hyper", "h2", "hyper-rustls", "aws-lc-rs", "tokio", "tokio-tungstenite", "tungstenite"]:
    print(f"{k:18} {sorted(a.get(k, []))}")
print("drift:", bad or "none")
sys.exit(1 if bad else 0)
PY
