# ciallopa M1 acceptance (2026-10-13, local only)

Setup (`scripts/e2e/`): `crates/server/examples/mock_upstream.rs` on 127.0.0.1:18080 emulating
chatgpt.com/backend-api (SSE + WS Responses, models, wham, quota headers, 429 usage_limit via
`/tmp/cpa-e2e/limited`, in-stream usage_limit after `response.created` via `/tmp/cpa-e2e/midlimited`, quota via `quota-<acct>`) and auth.openai.com device-code/token
(`CIALLOPA_OAUTH_ISSUER`, test-only override). ciallopa on 127.0.0.1:18317. Client: **real Codex CLI
0.162.1** (`codex exec` / `codex exec resume --last`) with a custom provider
`base_url=http://127.0.0.1:18317/v1`, `env_key` = local API key, `supports_websockets` true/false.
3 fake accounts (acc-1..3) created through `ciallopa login` (device code) against the mock.

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
| 9 | quota avoid (opt-in; default `quota_avoid_threshold = 0` = off): acc-1 at 95% ≥ 0.9 threshold → no new sessions on acc-1 | PASS (acc-2/acc-3 only) |
| 10 | priority: acc-1 priority 5 → all new sessions on acc-1 | PASS (4/4) |
| 11 | weighted-round-robin weights 3/1/0 | PASS (6/2/0 over 8) |
| 12 | wrong client key → 401 (Codex shows `invalid api key`), `/healthz` open, unknown path 404, `wham/usage` passthrough 200 | PASS |
| 13 | fp-check: HTTP JA4 + Akamai h2 + Responses WS handshake JA4 (`t13d101000_61a7ad8aa9b6_0d308c48d2a3`) and header prefix order == Codex CLI | PASS |
| 14 | Unit + integration tests (SSE headers/zstd byte-identical, upstream zstd compression + opt-out, 5xx retry, 429 failover, 401 force-refresh, idle timeout, models cache, WS relay + pre-output quota replay on another account + incremental-turn quota close 1012, SSE pre-output quota replay / post-output forward, Images wrap/stream/direct/multipart) | PASS |
| 15 | SSE: acc-1/acc-2 answer 200 + `response.created` then `error` usage_limit_reached (`scripts/e2e/midlimit.sh`) → proxy holds the preamble, cools the account, replays the same body on acc-3; Codex output normal, no error shown; next session skips the cooled account | PASS (acc-2→acc-3, then acc-1→acc-3) |
| 16 | WS: same in-stream usage_limit frame → not forwarded; proxy redials another account and replays the `response.create` (no `previous_response_id`, nothing sent yet); Codex output normal | PASS (acc-2→acc-3, then acc-1→acc-3) |
| 17 | All accounts limited → Codex shows "You've hit your usage limit" (WS: 1012 close → reconnect → handshake 429; SSE: 429), no hang | PASS |

Error-driven switching (2026-10-13): quota errors (`usage_limit_reached` / `insufficient_quota` /
`usage_not_included` / WS `error` status 429, also inside `response.failed`) seen before the first
meaningful event are never shown to the client: account cooled (resets_at / resets_in_seconds), same
request replayed on the next account (≤ pool size). After output started: SSE forwards the error +
cooldown; WS does not forward it, closes 1012 "upstream requires HTTP replay" — Codex treats a Close
before `response.completed` as a retryable stream error (codex-api responses_websocket.rs:839-842),
resets its WS session and resends the full input (core client.rs:474-485, 1656), landing on a new
account. Same 1012 path for incremental (`previous_response_id`) turns.

Not covered end-to-end: WS quota error on an incremental (`previous_response_id`) turn with real
Codex (needs a tool-call turn; covered by integration test + codex-rs reading), zstd request bodies against real chatgpt.com
(Codex only compresses with ChatGPT auth; ciallopa compresses plaintext `POST /responses` bodies itself
when `upstream_opts.compress_requests = true`, default; pre-encoded passthrough, level-3 output, retry
byte reuse, Accept-Encoding stripping and WS set-cookie filtering covered by mock tests), real
chatgpt.com / Cloudflare behaviour, paste/PKCE login interactively.
