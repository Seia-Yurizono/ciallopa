# ciallopa M1 acceptance (2026-10-13, local only)

Setup (`scripts/e2e/`): `crates/server/examples/mock_upstream.rs` on 127.0.0.1:18080 emulating
chatgpt.com/backend-api (SSE + WS Responses, models, wham, quota headers, 429 usage_limit via
`/tmp/cpa-e2e/limited`, in-stream usage_limit after `response.created` via `/tmp/cpa-e2e/midlimited`, `invalid_encrypted_content` via `encrypted` / `lateencrypted`, quota via `quota-<acct>`) and auth.openai.com device-code/token
(`CIALLOPA_OAUTH_ISSUER`, test-only override). ciallopa on 127.0.0.1:18317. Client: **real Codex CLI
0.163.0** (`codex exec` / `codex exec resume --last`) with a custom provider
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
| 18 | Codex 0.163.0, every account in `/tmp/cpa-e2e/encrypted` (200 + SSE `error` `invalid_encrypted_content`; WS frames `codex.rate_limits`, `codex.response.metadata`, then the error with `status: 400`) → SSE: HTTP 400, Codex prints the clear message once, 1 upstream request (before: 5 reconnects, then "stream closed before response.completed"); WS: frame passed through with the clear message, Codex stops at once; log `invalid_encrypted_content` / upstream, no switch or cooldown. `lateencrypted` (after output): `response.failed` with the clear message; Codex reconnects 5 times because the mock rejects every attempt after output (upstream rejects before output, so a real retry gets the 400) | PASS |

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

Note (live, 2026-10): chatgpt.com sends `/responses` SSE **without a `Content-Type` header**. ciallopa
treats a response as SSE when it is `text/event-stream`, or when the type is missing/unrecognised and
the client asked for a stream (`"stream": true` or `Accept: text/event-stream`); explicit JSON/text/HTML
types always win. Usage parsing (tokens/TTFT/TPS) and pre-output quota replay then work, and the
downstream response gets `Content-Type: text/event-stream` (Codex parses SSE regardless; other clients
need it). Covered by mock tests with the content type stripped.

## Isolated CLI smoke (2026-10-23, local only)

The earlier M1 table remains historical. The new opt-in `scripts/cli-smoke.py` runs four
small cases with real Codex CLI 0.163.0, independent temporary client state, fake accounts
and loopback-only mock/gateway processes. Invocation and configuration-isolation limits
are in [TESTING.md](TESTING.md#optional-real-codex-cli-smoke); this is not ordinary CI.

| Case | CLI result | Observed upstream chain | Final business usage |
| --- | --- | --- | --- |
| SSE success | exit 0, completed turn, `ok from acc-a` | SSE acc-a | completed, switches 0 |
| SSE pre-output quota | exit 0, completed turn, `ok from acc-b` | SSE acc-a → acc-b | completed, first acc-a, final acc-b, switches 1 |
| WS success | exit 0, completed turn, `ok from acc-a` | WS acc-a, prewarm then business frame | completed, switches 0 |
| WS pre-output quota | exit 0, completed turn, `ok from acc-b` | WS prewarm acc-a; business frames acc-a → acc-b | completed, first acc-a, final acc-b, switches 1 |

Each case had exactly one non-prewarm usage record, expected input/output token counts,
zero admission rejection, and zero inflight after completion. Protocol/account logs were
checked independently of the final message; WS fallback to SSE would fail the run.
The two failover cases inject `response.created` followed by an in-stream quota error
before output, so they verify transparent business-request replay rather than only an
HTTP 429 at the response header.

The initial run `/tmp/ciallopa-cli-smoke-7ltdn7mn` correctly **failed** the WS quota case:
the real CLI first sent `generate:false`, the old mock applied its quota fault to that
prewarm, and the business turn then succeeded on acc-b with switches 0. The runner rejected
this apparent success. An opt-in mock marker, `midlimit_business_only`, now leaves prewarm
alone and applies `midlimited` to the business turn; the default mock behavior is unchanged.
The mock also logs `generate`, and the runner checks the non-prewarm frame/account chain.
No production forwarding behavior was changed to make this fixture pass.

Final run: `/tmp/ciallopa-cli-smoke-3pt_m95h`, all four cases passed with the final driver
SHA256 `3cf9fd145cd1899123716a23e22e0047ee31d22948dbeede9b3ead02f6236f84`.
The run manifest records source HEAD `c671481d` separately from the actual tested debug
executables: ciallopa SHA256 `d422e11ab830…`, mock SHA256 `c2ed8cf2f12e…`, and Codex SHA256
`399ff69c431c…`. It also records uncommitted smoke/mock changes; HEAD alone does not identify
the executable build. Full hashes, raw client JSON/stderr, mock logs, usage results and
process exit codes remain in that directory. Executable hashes were unchanged across
the final run. A subsequent explicit
`cargo build --locked -p ciallopa --bin ciallopa --example mock_upstream` at `c671481d`
(including WS write-error fix `8cc0cd2`, with only the mock example modified) produced
the same hashes as the tested executables; `build-verification.json` records that
provenance check, so another CLI run was unnecessary.
All 13 owned subprocesses were reaped: four mock servers exited on TERM,
and the four gateways, four CLI sessions and version query exited 0.

This does not repeat the historical login, incremental-turn, real TLS/Cloudflare,
multi-turn, tool execution or fingerprint checks. Codex's warning about refusing PATH
helper aliases inside `/tmp` remains visible in stderr; these text-only sessions did
not need the aliases. No real upstream, user credential or persistent user configuration
was used, and no legacy PASS entry was reinterpreted as a result of this run.

## Caddy proxy-chain check (2026-10-23, local only)

The README's minimal `reverse_proxy 127.0.0.1:8317` example was checked with official
Caddy v2.11.7 and the `9111dd0` release gateway (`2c6142775ff4…`). The downloaded archive
matched the release API SHA256 and published SHA512 checksums. `caddy adapt` rejected the
old example's `encode off` as an unregistered encoder; both the minimal example and the old
example with only that invalid line removed parsed successfully.

A loopback HTTP/1.1 Caddy → gateway → fake upstream probe used admission limit 1 and no
explicit flush interval. It verified cancellation before headers, cancellation while the
upstream was silent after the first delta, and a subsequent successful byte-identical SSE
response. The delta arrived before the mock was allowed to send a terminal event. Both
cancellations closed the upstream and released the slot; usage contained exactly two
`client_closed/client` records and one success. The header-stage cancellation had no account
metadata yet. These are functional checks, not latency bounds or TLS/capacity coverage.

Evidence: `/tmp/ciallopa-caddy-review-vqqlnz33/`, including pinned source, adaptation results,
probe code/results and process exit codes. Both child services exited 0; the mock also stopped.
The official reverse-proxy page's statement about negative flush intervals and cancellation
does not match the inspected v2.11.7 path, so no cross-version claim is made about that option.
The minimal example relies on the verified default behavior.
