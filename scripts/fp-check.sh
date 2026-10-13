#!/usr/bin/env bash
# Fingerprint check: cpa-lite upstream client vs Codex CLI 0.162.1 recorded in fp-survey.
# Points chatgpt.com at a local fpserver via /etc/hosts (restored on exit), runs
# fp-probe (same reqwest builder as the proxy), asserts JA4 + Akamai h2 fp.
set -euo pipefail
FP=${FP_SURVEY:-<local-path>}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WANT_JA4="t13d1011h2_61a7ad8aa9b6_0d308c48d2a3"
WANT_JA4_RESUMED="t13d1011h2_61a7ad8aa9b6_053248755fe4"  # PSK resumption (Codex records the same)
WANT_AKAMAI="2:0;4:2097152;5:16384;6:16384|5177345|0|m,s,a,p"
OUT=$(mktemp /tmp/cpa-lite-fp.XXXXXX.jsonl); rm -f "$OUT"
HOSTS_BAK=$(mktemp /tmp/hosts.bak.XXXXXX); sudo cp /etc/hosts "$HOSTS_BAK"
cleanup() {
  sudo pkill -x fpserver 2>/dev/null || true
  sudo cp "$HOSTS_BAK" /etc/hosts && rm -f "$HOSTS_BAK"
}
trap cleanup EXIT

export PATH=$HOME/.cargo/bin:$PATH
(cd "$ROOT" && cargo build -q --bin fp-probe)
echo "127.0.0.1 chatgpt.com" | sudo tee -a /etc/hosts >/dev/null
(cd "$FP/server" && sudo ./fpserver -addr 127.0.0.1:443 -ctl 127.0.0.1:9998 -out "$OUT" >/tmp/cpa-lite-fpserver.log 2>&1 &)
sleep 1
exec 3<>/dev/tcp/127.0.0.1/9998; printf "cpa-lite" >&3; cat <&3 >/dev/null; exec 3>&-

SSL_CERT_FILE="$FP/ca.pem" "$ROOT/target/debug/fp-probe" https://chatgpt.com/backend-api || true
sleep 0.5
sudo chmod a+r "$OUT"
python3 - "$OUT" "$WANT_JA4" "$WANT_JA4_RESUMED" "$WANT_AKAMAI" <<'PY'
import json, sys
out, want_ja4, want_res, want_ak = sys.argv[1:]
rows = [json.loads(l) for l in open(out) if l.strip()]
rows = [r for r in rows if r.get("label") == "cpa-lite"]
assert rows, "no captures recorded"
ok = True
for r in rows:
    ak = r["h2"].get("akamai", "")
    j = r["ja4"]
    good = j == (want_res if r.get("resumed") else want_ja4) and ak == want_ak
    ok &= good
    print(f"{'PASS' if good else 'FAIL'} path={r.get('path')} ja4={j} akamai={ak} resumed={r.get('resumed')} alpn={r.get('negotiated_alpn')} grease={r['hello']['HasGREASE']}")
print(f"expected ja4={want_ja4} (resumed {want_res}) akamai={want_ak}")
sys.exit(0 if ok else 1)
PY
