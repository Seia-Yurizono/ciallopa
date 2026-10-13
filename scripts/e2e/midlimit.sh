#!/usr/bin/env bash
# E2E: in-stream usage_limit after response.created (before any output) on acc-1/acc-2,
# real Codex CLI over WS and SSE must finish transparently on acc-3; limited accounts cool down.
# Prereq: mock_upstream on :18080 (MOCK_DIR=/tmp/cpa-e2e), 3 accounts in /tmp/cpa-e2e/auths,
# run.sh/lib.sh copied to /tmp/cpa-e2e, CODEX_HOME dirs home-ws / home-sse.
set -u
D=/tmp/cpa-e2e
sed 's/quota_avoid_threshold = .*/quota_avoid_threshold = 0.0/' "$(dirname "$0")/config.toml" > $D/config-mid.toml
source "$(dirname "$0")/lib.sh"
printf 'acc-1\nacc-2\n' > $D/midlimited
for m in sse ws; do
  restart config-mid.toml; : > $L
  for i in 1 2; do echo "== $m #$i"; run $m "" "hi"; show; done
done
printf 'acc-1\nacc-2\nacc-3\n' > $D/midlimited
for m in sse ws; do restart config-mid.toml; echo "== $m all limited (expect visible usage-limit error)"; run $m "" "hi"; : > $L; done
rm -f $D/midlimited
grep -E "switching account|replayed on another account" $D/server.log | tail -4
