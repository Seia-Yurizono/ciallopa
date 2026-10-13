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
# fingerprints. libsqlite3-sys / hashlink themselves still match codex-rs. sqlite-wasm-rs and
# rsqlite-vfs are wasm32-only (never compiled on native targets).
ALLOW = {"rusqlite", "fallible-iterator", "fallible-streaming-iterator", "rsqlite-vfs", "sqlite-wasm-rs"}
bad = {k: v - b.get(k, set()) for k, v in a.items()
       if not k.startswith("ciallopa") and k not in ALLOW and not v <= b.get(k, set())}
allowed = {k: sorted(a[k]) for k in sorted(ALLOW) if k in a}
print("allowed (non-network):", allowed)
for k in ["reqwest", "rustls", "hyper", "h2", "hyper-rustls", "aws-lc-rs", "tokio", "tokio-tungstenite", "tungstenite"]:
    print(f"{k:18} {sorted(a.get(k, []))}")
print("drift:", bad or "none")
sys.exit(1 if bad else 0)
PY
