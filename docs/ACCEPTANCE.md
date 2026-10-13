# cpa-lite M1 acceptance (2026-10-13, local only)

Setup (`scripts/e2e/`): `crates/server/examples/mock_upstream.rs` on 127.0.0.1:18080 emulating
chatgpt.com/backend-api (SSE + WS Responses, models, wham, quota headers, 429 usage_limit via
`/tmp/cpa-e2e/limited`, quota via `quota-<acct>`) and auth.openai.com device-code/token
(`CPA_LITE_OAUTH_ISSUER`, test-only override). cpa-lite on 127.0.0.1:18317. Client: **real Codex CLI
0.162.1** (`codex exec` / `codex exec resume --last`) with a custom provider
`base_url=http://127.0.0.1:18317/v1`, `env_key` = local API key, `supports_websockets` true/false.
3 fake accounts (acc-1..3) created through `cpa-lite login` (device code) against the mock.

| # | Check | Result |
|---|---|---|
| 1 | `login --method device` ×3 against mock → 3 auth files (0600, CPA names) | PASS |
| 2 | `login --method import` of a Codex `auth.json` | PASS |
| 3 | WS multi-turn (3 turns, exec + resume): one WS per process, permessage-deflate offered, same session-id, all on one account | PASS (acc-2 ×3) |
| 4 | HTTP SSE multi-turn (3 turns): same session, one account | PASS (acc-3 ×3) |
| 5 | WS session's account hits usage_limit (handshake 429) → reported, rebound, client continues on another account and stays there | PASS (acc-2 429 → acc-3, next turn acc-3) |
| 6 | SSE session's account hits usage_limit (429 before first byte) → transparent failover, client output normal | PASS (acc-3 429 → acc-1) |
| 7 | Cooldown respected: limited accounts receive no new sessions for resets_in_seconds | PASS (all new sessions → acc-1) |
| 8 | round-robin across new sessions (fresh server) | PASS (2/2/2 over 6) |
| 9 | quota avoid: acc-1 at 95% ≥ 0.9 threshold → no new sessions on acc-1 | PASS (acc-2/acc-3 only) |
| 10 | priority: acc-1 priority 5 → all new sessions on acc-1 | PASS (4/4) |
| 11 | weighted-round-robin weights 3/1/0 | PASS (6/2/0 over 8) |
| 12 | wrong client key → 401 (Codex shows `invalid api key`), `/healthz` open, unknown path 404, `wham/usage` passthrough 200 | PASS |
| 13 | fp-check: HTTP JA4 + Akamai h2 + Responses WS handshake JA4 (`t13d101000_61a7ad8aa9b6_0d308c48d2a3`) and header prefix order == Codex CLI | PASS |
| 14 | Unit + integration tests (SSE headers/zstd byte-identical, 5xx retry, 429 failover, 401 force-refresh, idle timeout, models cache, WS relay + mid-stream usage_limit close 1013, Images wrap/stream/direct/multipart) | PASS |

Not covered end-to-end: mid-connection WS usage_limit frame with real Codex (covered by integration
test; Codex shows the error and reconnects on the next turn), zstd request bodies (Codex only compresses
with ChatGPT auth, not a custom API-key provider; byte-identical passthrough covered by test), real
chatgpt.com / Cloudflare behaviour, paste/PKCE login interactively.
