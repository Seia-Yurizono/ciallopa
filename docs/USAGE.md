# Usage log (per-request records)

Status: implemented on branch `usage-log` (crate `crates/usage`). Benchmarks with fake data:
`crates/usage/examples/usage_bench.rs`.

Current metric definitions, exact record lookup and pagination are described in [METRICS.md](METRICS.md).

## Goals / non-goals
- One row per finished request (HTTP/SSE) or per turn (WS `response.create` → terminal event).
- **No waiting for the recorder queue or SQLite on the forwarding path.** Parsing and
  observation still have a cost. Losing records under overload is acceptable; holding
  requests behind the recorder is not.
- Personal multi-account scale (≤ ~1M rows / 30 days). No multi-tenant billing, no exact
  accounting guarantees.

## Write path
```
handler ──Tracker (per request/turn)──▶ Drop ──try_send──▶ bounded std::sync::mpsc (queue_size)
                                                      full ⇒ drop + counters.dropped++
writer OS thread "usage-writer":  recv first msg → collect until batch_size or flush_ms
                                  → one INSERT OR IGNORE transaction → hourly purge chunk
```
- `Tracker` lives in `crates/forward/src/usage.rs`. The record is built when the tracker is
  dropped (stream end, client disconnect, idle timeout, error), so every exit path records.
- Enqueue path: one `Box<Record>` + nonblocking `try_send` per request. Responses SSE
  observations use complete events and the JSON object's top-level `type`, shared with
  the truncation check; model text or nested metadata cannot declare a terminal event or
  first output. Single-line `data:` borrows the observation buffer. Only created/terminal
  events build a full JSON value for usage metadata; WS output classification uses the same
  type rule.
- The SSE observer retains at most 8 MiB of an unfinished event, replacing the previous
  separate Tracker buffer. With usage enabled this replaces the former 8 MiB Tracker plus
  1 MiB quota observer; with usage disabled the observation allowance grows from 1 to 8 MiB.
  This is a per-stream inspection allowance, not a process memory limit.
  Beyond that observation limit, bodies still pass through. If the stream ends without a
  reliably observed terminal event, the row uses `observation_incomplete` / `local` and
  the new outcome `unknown`, rather than inferring success or client cancellation from
  byte substrings. A later normally sized terminal event restores its actual outcome;
  actual transport errors and client cancellation retain their own classifications.
  This is not strict validation of arbitrarily large events, and no missing tokens are reconstructed.
- SQLite via `rusqlite 0.39` (bundled SQLite, `libsqlite3-sys 0.37.0` = same as codex-rs).
  Not on the network path; `scripts/lock-check.sh` allowlists only `rusqlite` and its pure-Rust
  helpers, the TLS/h2/WS stack is unchanged.
- PRAGMAs: `journal_mode=WAL`, `synchronous=NORMAL` (no fsync per commit; a power loss can
  lose recent committed rows while SQLite maintains transaction consistency), `busy_timeout=5s`.
  File mode 0600.
- Graceful shutdown (Ctrl-C / SIGTERM) attempts to drain the queue (`Recorder::shutdown`)
  within the process's final flush budget; see the shutdown limits below.

### Images lifecycle and limits of its metrics

Images calls retain one tracker through response-body completion, failure or cancellation;
receiving HTTP headers alone is not success. Streaming paths use the existing producer-end
duration (upstream body read and channel sends), not a measurement of the client's final TCP
acknowledgment. Buffered wrapped JSON retains its tracker in the response Body. Ordinary
non-stream Responses likewise become complete only after upstream body EOF.

Images records keep the requested/default image model, chosen account, retries and attempt
chain. `transport` follows the actual client reply: SSE or HTTP. Their token fields and TTFT
are deliberately null: the wrapper's internal text-model usage, `tool_usage.image_gen` image
counts and image payload sizes are not interchangeable with priced text tokens or text first
output. Existing `usage` objects are still returned to the client unchanged when available;
they are not converted into token cost or generation speed in the log.

Direct `/codex/images/*` replies are byte passthrough with HTTP status, idle-timeout and body
transport-error accounting. Their successful SSE payload is **not** parsed as Responses:
an `image_generation.completed` stream is never given a fabricated `response.failed` suffix.
When usage is enabled, complete `event: error` or top-level JSON `type: "error"` events in an
explicit SSE reply record `upstream_error_event` / `stream`, preserving HTTP 200 and upstream
request metadata. They do not trigger replay/cooldown or finalize the record before the Body
ends. Observation keeps at most 64 KiB per event, skips oversized events to their real delimiter,
then resumes; bytes are forwarded immediately. Nested fields and literal error text do not count.
Unknown event schemas, oversized errors and non-SSE JSON remain outside this observation contract;
a complete transfer is not a claim that every Images event schema has been semantically validated.

The Responses wrapper already knows the events it consumes. It records explicit failure,
missing image output (`image_output_missing`) and premature EOF (`stream_truncated`), and emits
an error event for a truncated wrapped stream. Cancellation before headers keeps status 0
with `client_closed`; forced shutdown is `shutdown` / `local` (503 before headers, otherwise
the status already sent). These changes do not rewrite historical Images records that were
previously finalized at headers.

## Fields
| column | meaning |
|---|---|
| `id` | request id = `start_ms << 20 \| seq` — unique, time-ordered, the rowid. API shows it as 16 hex chars. Duplicate ids are ignored (`INSERT OR IGNORE`). |
| `account` / `first_account` | final selected account; first selected account only when failover changed it. HTTP and initial WS handshake cancellation before headers retain the selected candidate, even if credential acquisition or the network attempt has not completed; null when none was selected. Successful WS handshakes add no separate business row |
| `model`, `effort` | from body / `response.create` (`reasoning.effort`) |
| `transport` | 1 sse, 2 ws, 3 http (ordinary JSON/non-stream; Images follows its actual reply) |
| `endpoint` | `responses`, `compact`, `images`, `prewarm` (Codex WS `response.create` with `generate:false`: tokens and cost count; excluded from request counts, error rate and TTFT/duration percentiles; shown greyed with a 预热 badge in the panel) |
| `status` | recorded HTTP response status (WS turn: 200 or handshake status); 0 when no status was recorded, such as cancellation before response headers. UI shows “–” / “未记录 HTTP 状态” for 0 |
| `error` | error code, NULL = ok. Upstream HTTP errors keep the upstream's own `error.code` / `error.type` (e.g. `server_is_overloaded`, `invalid_prompt`, `usage_limit_reached`), else `http_<status>`. Local / network / stream codes: see [Error sources](#error-sources). |
| `err_source` | v9: `client`, `local`, `network`, `upstream` or `stream` (NULL = ok) |
| `up_status` | v9: last upstream HTTP status of the request; differs from `status` when the client got a local error (e.g. upstream 503 then no account left) |
| `retries` | pre-first-byte retries (transport / 408 / 5xx) |
| `switches` | account switches (401/402/403/404/429 failover, quota-before-output replay) |
| `ttft_ms` | Tracker start → first event carrying model output: any `*.delta` (output text, reasoning summary/text, tool-call arguments) or `response.output_item.done`. `response.created`, `in_progress`, `output_item.added`, `codex.*` / `responsesapi.*` metadata and rate-limit frames are not output. Same event rule for SSE and WS. Non-stream: → response headers |
| `duration_ms` | Tracker start → end of body / stream / turn |
| `in_tok`, `out_tok`, `cached_tok`, `reasoning_tok` | from `response.usage` |
| `cw_tok` | `input_tokens_details.cache_write_tokens` (part of `in_tok`) |
| `tier` | `service_tier` actually served: terminal event's `response.service_tier` (wins), else `response.created`'s, else the request body's; `auto` from a response is ignored (`priority` = Fast mode, `flex`, `default`/`auto` = standard). Pricing uses this. |
| `req_tier` | v5: request body's `service_tier` (`fast` stored as `priority`), kept even when downgraded |
| `resp_model`, `resp_effort` | v5: `response.model` / `response.reasoning.effort` (terminal event wins over `response.created`; SSE and WS) |
| `incomplete` | `response.incomplete` reason (`incomplete_details.reason`, else `unknown`). Passed through untouched, tokens recorded, **not** an error and never a cooldown (Codex treats it as a terminal turn and still counts its tokens) |
| `thread`, `turn` | Codex thread / turn id from the `thread-id` header, `x-codex-turn-metadata` (JSON `thread_id`, `turn_id`) or body `client_metadata` (WS `response.create`). Stored for a future authoritative cost lookup; nothing queries upstream yet. Values > 128 chars are ignored. |
| `key` | **label** of the client key that made the request (`[[api_keys]] label`, or `key-<sha256[..8]>` for plain keys). The raw key is never stored. |
| `instance` | `instance_id` of the ciallopa that wrote the row (default: hostname) |
| `plan`, `p_pct`, `p_reset`, `s_pct`, `s_reset` | upstream quota snapshot seen on this response: `x-codex-plan-type`, primary/secondary `used-percent` and `reset-at` (unix s). SSE: response headers; WS: handshake headers, updated by `codex.rate_limits` frames. A window without `window-minutes` whose reset is "now" (free plan's empty secondary) is treated as absent. |
Timing starts at different points: Responses excludes Body reading and the initial JSON
inspection; Images excludes Body extraction but includes subsequent parsing; WS starts at
the turn, while a failed WS handshake includes its credential/connect/retry wait. These
are not uniform full-ingress latency measurements. Legacy per-record `tps` is computed at query time as
`1000 × out_tok / (duration_ms − (ttft_ms or 0))`, in token/s; nonpositive output or generation
time gives null. The current UI uses the stricter `generation_tps` sample rules described in
[METRICS.md](METRICS.md), not an average of this legacy row value.

Not recorded: `/models`, `/wham/*` and other passthrough, WS handshakes that succeed (only turns).

## Error sources
| `err_source` | Meaning | Codes |
|---|---|---|
| `client` | the client went away **before** the terminal event | `client_closed` (not counted as an upstream failure). Closing after `response.completed` / `incomplete` / `failed` is a normal end (Codex hangs up as soon as it has the terminal event) and is not recorded as an error |
| `local` | ciallopa itself could not serve the request | `no_account`, `no_usable_account` (all disabled / excluded / need relogin), `all_cooling` (every account cooling, the first recovers later than the wait), `all_quota_exhausted` (every account out of usage quota), `first_quota_reset` (every account cooling, the first to recover is out of usage quota), `internal`; the record message gives the time to the first recovery |
| `network` | no usable upstream response | `connect`, `dns`, `tls`, `timeout`, `timeout_connect`, `header_timeout`, `preparation_timeout`, `error_body_timeout`, `error_body_read`, `models_body_timeout`, `models_body_read`, `transport`, `idle_timeout`, WS `upstream_closed` (transport); with an egress proxy: `proxy_connect` (proxy unreachable, auth or tunnel refused, proxy-side DNS failure), `proxy_timeout` (connect through the proxy timed out). Proxy errors fail the request at once (every account shares the proxy, so no account switching) and never cool an account; the panel labels them 代理. `cf_blocked` (a Cloudflare error page such as `error code: 1010`, browser signature banned, plain text, JSON or HTML) and `challenge` (a Cloudflare challenge page or `cf-mitigated`) on a 403: Cloudflare in front of the upstream blocks this host's egress, so the request fails at once with a neutral 502, no account switching and no cooldown (see [Cloudflare blocks](#cloudflare-blocks)) |
| `upstream` | upstream HTTP error, passed through verbatim | upstream `error.code` / `error.type`, else `http_<status>`; `invalid_encrypted_content` (also when the upstream reports it inside a 200 stream or WS turn, see below; panel label 加密推理被拒) |
| `stream` | error inside a 200 stream | `usage_limit`, `response_failed`, `upstream_error_event`, `stream_truncated`, Images `image_output_missing`, WS `upstream_closed` |

What the client receives:
- Complete upstream errors within the buffering bounds ⇒ the most meaningful one is returned verbatim (status, body and
  `retry-after`, `x-codex-*`, `x-request-id`, `cf-ray`, …). Priority: usage-limit 429 > other 429 >
  5xx/408 > 401/402 > 403 > 404; an upstream 5xx is never turned into a 429.
- Every account cooling down and the first recovers within `routing.cooldown_wait_secs`
  (default 20; WebSocket handshakes are held at most 10 s because Codex abandons a handshake
  after 15 s) ⇒ the request is held, then routed to that account. The hold ends when the client
  disconnects. A handshake whose recovery is within the wait but beyond 10 s gets 503
  `server_is_overloaded` with a matching short `Retry-After`, and Codex reconnects then.
- No upstream response ⇒ an upstream-shaped error with a standard code: 503
  `{"error":{"type":"server_error","code":"server_is_overloaded",…}}` without `Retry-After` for
  no account, no usable account, and every account cooling with the first recovery beyond the
  wait, so Codex stops after its short request retries (about 3 s; under 1 s on a WebSocket
  handshake). 502 / 504 for network failures. Codex retries these.
- Every account cooling and the first to recover is out of usage quota (all of them or only
  that one) ⇒ 429 `usage_limit_reached` with its `resets_at` / `resets_in_seconds`, no
  `plan_type` and no `Retry-After` (the only locally synthesized quota error). Codex stops and
  shows "You've hit your usage limit. Try again at <time>."
- `Retry-After` on any 429 / 5xx to the client is kept only as a delay of 1–20 s and dropped
  otherwise (0, longer, or an HTTP date): Codex sleeps for the full advice before each of its
  retries, about 30 per turn, with no upper bound.
- A stream that ends without a terminal event (upstream cut, stream error, idle timeout) ends with
  `event: response.failed` (`code: server_error`, "stream disconnected before completion") so Codex
  retries the turn.
- The upstream rejected the encrypted reasoning in the request history ⇒ an
  `invalid_encrypted_content` error with a clear message (see below).
- Responses carry no proxy-specific headers or wording; source and code exist only in the log.

### Preparation budgets and buffered response limits

`upstream_opts.response_header_timeout_secs` (default 300) bounds each HTTP attempt from sending
the request through receiving its response headers. It includes uploading the request, so
large inputs and non-streaming generation need a suitably generous value. A header timeout is
logged as `header_timeout` and uses the existing network failover rules without cooling an account.

`upstream_opts.preparation_timeout_secs` (default 600) is shared across all HTTP attempts and
account switches, including SSE replay. It charges credential acquisition/refresh, waiting for
headers, reading error bodies, cooldown holds and retry waits. Normal successful SSE bootstrap
and generation do not consume this budget, even before the first output. Once exhausted, no
new attempt starts: the client gets 504 and the record says `preparation_timeout`. Both values
are positive seconds (up to 86400) and apply to new HTTP requests after reload.

WS also uses `preparation_timeout_secs`: one budget covers the initial credential/handshake
sequence; each turn gets a fresh budget shared across its pre-output reconnects and retry
waits. Normal generation and frame writes are outside it. WS captures preparation and idle
limits before the initial handshake; reconnect to adopt changed values. Successful replay
restarts upstream idle. Exhaustion records `preparation_timeout` / `network`: a failed initial
handshake returns 504, while an active turn closes with 1012 for client replay. Closing an
abandoned upstream connection is bounded cleanup, not a prerequisite that can hold up redial.
The per-attempt response-header budget remains HTTP-only.

These gateway limits do not extend a client's own wait. Codex 0.164 defaults to a 15-second
WS connection timeout; its SSE idle clock waits for complete events, so comment heartbeats
can keep bytes moving without resetting that clock. Client retry, gateway attempt and account
switch counts are different scopes. The [versioned client research](research/CODEX_NETWORK.md)
and [recovery boundaries](RECOVERY.md) record the verified combinations; do not
multiply defaults into a universal per-turn attempt limit.

During a backpressured WS upstream write, the relay can observe a client Close/EOF and abandon
the upstream connection without flushing or replaying the partial frame. It may retain one
following downstream message in order, then stops reading until the write completes. Close
behind more queued messages remains subject to that backpressure. This adds neither a general
frame-write timeout nor a promise of immediate cancellation during every reconnect/downstream
write. Existing preparation and Force shutdown bounds still apply to those phases.

HTTP error bodies are limited to 1 MiB and an absolute 10-second read budget. A continuously
dripping error body cannot extend that deadline. An oversized, timed-out or interrupted body is
not forwarded as a complete upstream error: the client gets 502/504; the record keeps the
upstream status/request ID, an incomplete-body explanation and a redacted attempt prefix.
Codes are `error_body_too_large` (source `local`), `error_body_timeout` or `error_body_read`.

Successful `/models` responses are buffered under an 8 MiB / 10-second bound, then cached for
five minutes (at most 64 query variants and 16 MiB total body bytes). Incomplete catalogs return 502/504 and are never cached.
Normal response streams retain their idle timeout without an absolute generation time limit.

### Encrypted reasoning rejected (`invalid_encrypted_content`)
Codex sends the `encrypted_content` of earlier reasoning and compaction items back on every turn.
The upstream can only decrypt it under the account (organization) that produced it, and rejects
anything else with `invalid_encrypted_content` ("The encrypted content for item rs_… could not be
verified. Reason: …", e.g. `organization_id did not match the target organization`,
`item_id did not match the target item id`, `could not be decrypted or parsed`). No retry on any
account can fix the request, and nothing in the history is stripped or rewritten, the same as
official Codex. ciallopa only makes the error clear:

| where | upstream sends | client receives |
|---|---|---|
| HTTP, before any output | `200` + SSE `event: error` (Codex ignores it and reconnects the stream 5 times) | HTTP `400` `{"error":{"message":…,"type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"}}`; Codex shows it once and stops |
| HTTP `400` JSON | the same error object | the same `400`, clear message |
| HTTP, after output | SSE `error` event | `event: response.failed` with the same `code` and the clear message (Codex retries the turn; the retry is rejected before output and gets the `400`) |
| WebSocket | `error` frame with `status: 400` | the same frame and fields, clear message (`status: 400`, fatal for Codex) |

The message is: "The encrypted reasoning content in this conversation's history was rejected
upstream (it was most likely created under a different account or organization) and cannot be
reused. Start a new session to continue." The request log records code
`invalid_encrypted_content`, source `upstream` (panel: 加密推理被拒), and keeps the upstream message
and event in the error detail. It is a request-content problem: no retry, no account switch, no
cooldown, and it does not count toward any error streak or reset one. Equivalent rejections are
recognised too: the upstream wording under no code or under `thinking_signature_invalid`.

Upstream 408 / 5xx and transport errors, excluding recognized overload described below: the
request moves to another usable account after a short jitter (20–120 ms), at most `max_attempts`
times (default 1) per client request. These budgeted failovers do not retry the same account:
when no other account is usable, or the budget is spent, the
last upstream response (status, body, `Retry-After` within 1–20 s) goes back to the client at once, and the
client retries with its own backoff. This bounds the gateway's part of transient retries;
client retries can still multiply attempts (Codex retries 5xx 4 times per request and reconnects
the turn 5 times). Account errors and overload have separate rules, so `max_attempts` is not
a total upstream-attempt limit.
A WebSocket handshake is the exception: Codex does not retry a failed handshake itself, so
ciallopa retries it up to 4 times, on another usable account when there is one, otherwise on the
same account with backoff (or `Retry-After`, capped at 60 s). An account is cooled only after **3 consecutive** 5xx (any success resets the count), for
`transient_cooldown_secs` (default 15) or the upstream `Retry-After`, capped at 60 s. The last
usable account is never cooled; network errors never cool.

### Account errors (401 / 402 / 403 / 404 / 429)
These move the request to another usable account at once (HTTP requests and WS handshakes alike)
and do not use the `max_attempts` budget. The client sees nothing of the switch; when no account
is left, the most meaningful upstream response goes back verbatim (see above).

| upstream answer | cooldown | scope |
|---|---|---|
| 402 `{"detail":{"code":"deactivated_workspace"}}` (ChatGPT workspace deactivated) | `deactivated_cooldown_secs` (default 86400) | account |
| any other 402 (payment / billing) | `payment_required_cooldown_secs` (default 1800) | account |
| 404 whose body says the model is unsupported (`model_not_found`, `model_not_supported`, `unsupported_model`, `invalid_model`, or a message naming the model as not supported / not found / not available / not existing) | 12 h, or upstream `Retry-After` | that model on that account |
| any other 404 (e.g. `{"detail":"Not Found"}`) | `not_found_cooldown_secs` (default 600), or upstream `Retry-After` | that model on that account |
| 401 after one token refresh, 403 (not a Cloudflare page, see below) | 30 min | account |
| 429 | from the body (`resets_at` / `resets_in_seconds`) or `Retry-After` | account |

The three `*_cooldown_secs` keys live in `[upstream_opts]`, are hot-reloaded (next request) and
`0` means fail over without cooling. A usage-limit 429 never shortens a longer cooldown the
account already has for another reason (e.g. 402). The log line (`warn`) names the 402 kind and
the cooldown; the request log keeps the upstream code (`deactivated_workspace`, else `http_402`)
and the accounts page shows the reason (工作区已停用（402）, 上游 402 付费问题, 上游 404).

### Cloudflare blocks
A 403 whose body is a Cloudflare page rather than an upstream answer is not an account error:
a whole-body `error code: 1xxx` (e.g. 1010, browser signature banned; 1020, firewall rule), the
JSON form (`"cloudflare_error": true` or `"error_name": "browser_signature_banned"`), a Cloudflare
"you have been blocked" HTML page, or a challenge page / `cf-mitigated` header. Cloudflare blocks
the host's egress (IP, egress proxy, TLS or HTTP fingerprint), which every account shares, so:

- the request fails at once: no account switching, no retry, no account or model cooldown, the
  session binding stays;
- the client gets the same neutral `502 Bad gateway` body as other gateway failures, never the
  Cloudflare page; the request log keeps `err_source = network`, code `cf_blocked` / `challenge`,
  upstream status 403, the `cf-ray` and the Cloudflare code;
- a `warn` log line names the code and `cf-ray` (at most once a minute while it lasts, and again
  when the kind changes);
- `GET /usage/v1/quota` carries `egress_block` (`code`, `cf_code`, `upstream_status`, `cf_ray`,
  `account`, `first_at_ms`, `last_at_ms`, `count`, `active`; `null` if none since start) and the
  overview shows a red banner while `active`, i.e. until the next normal upstream response. The
  state is in memory only.

A real 403 from the upstream (JSON error body) keeps the 30 min account cooldown above.

### Successes that started before a cooldown
Each upstream attempt (HTTP request, WS handshake) records when it was sent. A success only
clears an account's cooldowns, error streaks and session binding when the attempt was sent after
the account's latest cooldown. A long request that was already in flight when another request
cooled the account (for example a usage limit) only updates the quota snapshot, so the cooled
account stays out of rotation. WS `codex.rate_limits` snapshots count as the handshake's attempt.

### Overloaded / "at capacity"
`server_is_overloaded`, `slow_down`, `service_unavailable_error`, or an error that only says
"…overloaded…" / "…at capacity…" with no other code. It usually arrives as HTTP 200 followed by
an in-stream `error` event and `response.failed`; it can also be an HTTP 503 / 502 / 400 / 429
body. It is account-level throttling, not quota and not a credential problem.
- **Before any output** (SSE: only envelope events have arrived — `response.created`,
  `response.in_progress`, `response.queued`, `response.metadata`, `codex.rate_limits`,
  keepalives — or empty announcements: a `response.output_item.added` message with no content
  or reasoning item with no summary, and a `response.content_part.added` /
  `reasoning_summary_part.added` with empty text, a `function_call` with empty arguments or a
  `custom_tool_call` with empty input. These are held until the first other event and dropped
  on a switch, so the client never sees them twice. WS: the same frames of the current turn): the request is replayed on another usable account, each account at
  most once per request, after a 20–120 ms jitter. This does not use `max_attempts`. Session
  affinity is unchanged: a session only moves when its account errors or cools down.
- **No other account usable** (one account, or every account already overloaded) and still
  nothing sent: ciallopa waits about `overload_retry_wait_secs` (default 3, jittered to 2–5 s; an
  upstream `Retry-After` is honored up to 10 s) and retries once on the same account, because
  Codex does not retry an in-stream overload itself. One such retry per HTTP request and per WS
  turn; `0` disables it.
- **After output** (text, reasoning, tool call), or when that retry is spent: the upstream error
  is passed through verbatim (the rest of the stream included), and the client decides. No 429
  is ever synthesized for overload.
  The request is logged as failed (`server_is_overloaded`, source `stream`), and the overload
  counts toward the account's streak instead of the HTTP 200 counting as a success; it never
  cools the account mid-stream on HTTP.
- `x-retry-metadata: NO_MORE_RETRY` forbids another try on the same account (it skips the
  wait-and-retry and ends the WS handshake retries on the last account); switching to another account is still allowed.
- Cooldown: after `overload_streak` (default 2) consecutive overloaded answers on one account it
  is cooled for `overload_cooldown_secs` (default 120), doubling on each further cooldown up to
  `overload_cooldown_max_secs` (default 600). Delivered output resets both the streak and the
  doubling. The last usable account is never cooled. The account page shows the reason
  `overloaded` and, per account, overloaded / upstream attempts over the last 24 h (memory only,
  cleared on restart and by "解除冷却"; nothing is written to disk for it).
- Log: source `stream` (in-stream) or `upstream` (HTTP body), error code as sent upstream (for
  example `server_is_overloaded`); the attempt chain records each switch with `kind: stream`.

## Error details
Stored for failed requests (status ≥ 400 or `error` set) and for successful requests that
retried or switched accounts (attempt chain only), in `error_detail`:
- `body`: upstream error body (HTTP error response, in-stream `error` / `response.failed` /
  `response.incomplete` event, WS error / quota frame, WS handshake failure, or the local
  JSON error such as `no_account`). Capture stops at 64 KiB; the stored text is redacted and
  then capped at **16 KiB** (`truncated = true`, `body_len` = original byte length).
- `message`: upstream `error.message` or a descriptive local message (`upstream stream idle
  for 300s (timeout)`, `no account available`, `upstream websocket closed mid-turn (code …)`,
  `upstream websocket disconnected (transport error: <cause>)`, …). The transport cause is also
  logged as a warning for read-side failures that interrupt a turn.
- `attempts` (v9): JSON array, at most 8 entries (`{"dropped": n}` marks omitted middle ones):
  `account`, `t_ms` (since request start), `kind` (`http` / `transport` / `refresh` / `stream`), `status`,
  `code`, `action` (`ok`, `retry(<ms>ms)`, `switch`, `cooldown(<s>s)+switch`, `cooldown+switch`, `refresh+retry`,
  `stop`) and `body` (first 512 bytes, redacted). Written by the same batched writer as the row.
- `up_req_id` (v9): upstream `x-request-id` / `x-oai-request-id` / `cf-ray`.
- `headers`: only `x-codex-*`, `cf-ray`, `retry-after`, `content-type`, `x-request-id`,
  `x-oai-request-id`, `openai-*request-id`/`openai-processing-ms`; never cookies or auth.
- Redaction (body, message, headers): `Bearer`/`Basic` credentials, JWT-like `eyJ…` tokens,
  `sk-`/`rt_`/`sess-` style tokens, `cookie`/`set-cookie`/`authorization`/`*token*`/`api_key`/
  `password`/`secret`/`session` values (JSON keys and `k=v` / `k: v` text) and e-mail
  addresses become `[REDACTED…]`.
- Purged with the same `retention_days` cutoff as `requests`.
- `/ui`: the error column shows a colored source badge (`客户端` / `ciallopa` / `网络` /
  `上游 <status>` / `流内`) and the code; the status column shows `502←上游 503` when the client
  status differs from the last upstream one. The status chip is green for a 2xx success, amber
  for 429, orange-red for other 4xx failures and red with an outline for 5xx; cancelled,
  unknown-outcome and prewarm rows stay neutral (the code and its tooltip carry the meaning,
  colour only reinforces it). Filters: account, source, code, errors only,
  retried/switched only. Clicking a row opens the detail panel with the attempt timeline,
  upstream request id, headers and body (copy button).
- `GET /usage/v1/records` filters: `source=upstream,network` (comma list), `error=<code>`,
  `retried=1`.

## Schema
```sql
CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);  -- interned strings
CREATE TABLE requests(
  id INTEGER PRIMARY KEY, account INTEGER, first_account INTEGER, model INTEGER, effort INTEGER,
  transport INTEGER NOT NULL, endpoint INTEGER, status INTEGER NOT NULL, error INTEGER,
  retries INTEGER NOT NULL, switches INTEGER NOT NULL, ttft_ms INTEGER, duration_ms INTEGER NOT NULL,
  in_tok INTEGER, out_tok INTEGER, cached_tok INTEGER, reasoning_tok INTEGER,
  key INTEGER, instance INTEGER, plan INTEGER,                -- labels.id
  p_pct REAL, p_reset INTEGER, s_pct REAL, s_reset INTEGER,
  tier INTEGER, cw_tok INTEGER, thread INTEGER, turn TEXT,    -- v3
  req_tier INTEGER, resp_model INTEGER, resp_effort INTEGER,  -- v5
  incomplete INTEGER,                                         -- v7
  err_source INTEGER, up_status INTEGER);                     -- v9
-- one row per *change* of an account's quota (id = observing request id)
CREATE TABLE quota_obs(id INTEGER PRIMARY KEY, account INTEGER NOT NULL, instance INTEGER,
  plan INTEGER, p_pct REAL, p_reset INTEGER, p_win INTEGER, s_pct REAL, s_reset INTEGER, s_win INTEGER);
CREATE INDEX quota_obs_account ON quota_obs(account, id);
-- failed requests, plus retried / switched successes; id = requests.id
CREATE TABLE error_detail(id INTEGER PRIMARY KEY, message TEXT, body TEXT,
  body_len INTEGER NOT NULL, truncated INTEGER NOT NULL, headers TEXT NOT NULL,
  up_req_id TEXT, attempts TEXT);                             -- v9
```
- Schema is at `user_version = 9`. Opening an older file upgrades it in place (`ALTER TABLE` /
  new tables); columns that did not exist yet stay NULL on old rows.
- `quota_obs` gets a row only when used % / plan changes or a reset time moves by > 120 s
  (upstream reset times jitter by a few seconds). Purge keeps each account's latest row.
- **No secondary index.** Time ranges are rowid range scans (`id BETWEEN from<<20 AND to<<20`),
  purge is a rowid range delete, "recent" is a reverse rowid scan. Strings (account e-mails,
  model names) are interned in `labels`, so a row is ~51 bytes on disk (1M rows ≈ 51 MB).

## Retention
- `retention_days` (default 90; `0` = keep forever, no purge at all). Error details follow the
  same cutoff. The writer purges at startup and then hourly, in
  chunks of 20 000 rows per transaction (~20 ms each) so a large backlog never stalls inserts.
- Freed pages are reused; the file does not shrink (no VACUUM). Steady-state size ≈
  rows-per-retention-window × 51 B.
- **No hourly rollup** (decision from the benchmark): a 30-day summary over 1M rows takes
  ~0.4 s, 24 h over 10M rows ~0.13 s. Revisit (hourly rollup with histogram buckets) only if
  real data exceeds ~5M rows per retention window.

## Failure modes
| failure | effect |
|---|---|
| queue full (writer slow / disk stall / DB locked) | records dropped, `dropped` counter, warn log at powers of two; no request latency change (measured) |
| DB locked > 5 s (external writer) | that batch fails, `write_errors += rows`, writer continues |
| DB cannot be opened at startup | warning, usage log disabled, proxy runs normally |
| kill -9 / crash | committed rows survive; WAL replayed on next open; `integrity_check` ok (20/20 runs) |
| client disconnects mid-stream | dropping the downstream Body cancels its producer, including while the upstream is silent; before a terminal event it records `client_closed` (source `client`), after it the completed result is retained |

## API (behind the same client key)
Filters on both list endpoints: `account=`, `model=`, `key=<label>`, `instance=`.
See [METRICS.md](METRICS.md) for the current outcome, sampling, cursor and bucket contracts.
- `GET /usage/v1/records?limit=100&before=<ms>&since=<ms>&errors=1&<filters>`
  → `{"records":[…],"total":…, "next_before_id":…}` newest first (limit ≤ 1000).
  Use `before_id=<16-hex-id>` for exclusive ID pagination; it takes precedence over legacy offset.
  `before`/`since` remain time filters. Rows default to all records including prewarm;
  `exclude_prewarm=1` limits both rows and total to business requests, matching `summary.count`
  within the same scope. Other request filters still intersect. Rows include
  `key`, `instance`, `plan`, `primary_used_pct`, `primary_reset_at`, `secondary_*`,
  `cache_write_tokens`, `service_tier`, `requested_service_tier`, `response_model`, `response_effort`, `incomplete`,
  `model_mismatch` (null = no response model; false = same ignoring case and a `-YYYY-MM-DD` snapshot
  suffix; true = different), `thread_id`, `turn_id`, `cost_usd`, `price_status`
  (`ok` | `unknown_price` | `no_tokens`), `has_detail`.
- `GET /usage/v1/records/{request_id}` → `{record: …}` for a preserved success, failure or prewarm.
  It does not require captured error detail. Returns 404 if the record is absent and 400 for an invalid ID.
- `GET /usage/v1/records/{request_id}/detail` → `{request_id, ts_ms, message, body, body_len,
  truncated, headers:{…}}` (404 if none, 400 if the id is not 16 hex chars). See *Error details*.
- `GET /usage/v1/summary?range=24h&group=key,instance&<filters>` (or `7d`, `30m`, …; or
  `from=<ms>&to=<ms>`). `group` = comma list of `account,model,key,instance,effort,transport,endpoint,tier,response_model`
  (default `account,model`; unknown → 400)
  → `{"summary":{"group_by":[…],"total":{…},"groups":[{group:{key:…,instance:…}, count, errors, error_rate,
  ttft_p50_ms, ttft_p90_ms, duration_p50_ms, duration_p90_ms, input_tokens, output_tokens,
  cached_tokens, reasoning_tokens, tps, cost_usd, unknown_price}]}, "unknown_price_models":[…]}, "recorder":{accepted, dropped, written,
  write_errors, purged}}`. Percentiles are exact (nearest rank).
- `GET /usage/v1/summary?…&bucket=day&tz=<minutes east of UTC>` (or `bucket=hour`) additionally returns
  `"buckets":[{day:"YYYY-MM-DD", start_ms, end_ms, count, errors, input_tokens, output_tokens,
  cached_tokens, reasoning_tokens, cost_usd, unknown_price}]` (ascending, empty days omitted;
  `tz` −840..840, default 0; unsupported `bucket` values → 400). `end_ms` is exclusive;
  partial first/last buckets are clipped to the shared scope by the `/ui` trend chart.
- `GET /usage/v1/quota` → `{"accounts":[{account, email, disabled, needs_relogin, quota:
  {plan, observed_at_ms, observed_by, primary:{used_pct, resets_at, window_minutes, estimate},
  secondary:{…}}}], "note": …}`. Every configured account is listed (`quota: null` if never
  observed); accounts seen in the log but no longer configured carry `configured: false`.
- 404 when `[usage] enabled = false`.
- `POST /admin/v1/accounts/{id}/quota-refresh` and `POST /admin/v1/accounts/quota-refresh` (all
  accounts, four at a time; admin key, JSON body, same-origin) ask upstream `GET /wham/usage`
  (5 s, Codex client headers, egress proxy) for the current windows. Read-only; one call per
  account per 30 s, a repeat inside that answers `rate_limited` without an upstream call.
  Result per account: `{account, ok:true, quota:{plan, primary, secondary}, observed_at_ms,
  usage_available_count, limit_reached}` or
  `{account, ok:false, error:{kind: rate_limited|http|network|timeout|credentials|token_invalid|decode,
  status?, message, retry_after_s?}}`; the batch call returns `{ok, skipped, failed, results}`
  (`skipped`: `rate_limited` accounts, `failed`: the rest). A successful answer is stored as a quota snapshot with `observed_by: "manual"` through the usage writer's
  batches (written even when unchanged, so its time shows the freshness).
  `usage_available_count` (`rate_limit_reset_credits.available_count`) and `limit_reached`
  (`rate_limit.limit_reached`) are `null` when upstream omits them. They are kept in memory on the
  account's reset-credit entry for display only (see below); nothing acts on them.

## Quota estimate
**The used % comes from upstream and includes usage by other clients (Codex app, other
proxies) and other ciallopa instances on the same account.** The estimate only sees tokens
this DB recorded, so it is accurate only when this instance is (nearly) the only user, and
when several instances share one DB path / account it under-counts their share unless they
write to the same file.

Per account and window (primary / secondary):
1. Take the first and last `quota_obs` row in the *current* window (same reset ± 120 s).
2. Δ% = last − first. Weighted tokens = Σ over this account's requests with
   `first ≤ id < last` (the snapshot on a response precedes that request's own tokens) of
   `input·(in − cached) + cached·cached + output·out` (`out` includes reasoning).
3. Δ% < `min_delta_pct` (default 5) → `status: "insufficient_data"` (1 % steps are too coarse).
   Otherwise `tokens_per_pct = weighted / Δ%`, `capacity_tokens = ×100`,
   `remaining_tokens = (100 − used) × tokens_per_pct`, `status: "ok"`.
4. Reset time already passed → `window_reset`; window not reported → `no_window`.

Weights (defaults: input 1, cached 0.1, output 4 — output/reasoning cost more, cache hits
less) are in `[usage.quota_weights]`.

## Cost (notional)
**For subscription (ChatGPT plan) traffic the cost is notional**: what the same tokens would
cost on the OpenAI API at list price, not what is billed. Nothing is stored: rows keep exact
token counts and `cost_usd` is computed at query time from the price table, so a price fix
re-prices history.

Price table: `[usage] prices` (default `~/.ciallopa/prices.toml`; missing ⇒ all costs null).
A new install has none: prices are pulled on request (see "Price updates"). Entries are
`[[model]]` with `model`, optional `tier` (`standard` | `flex` | `priority-fast` |
`ultrafast`), `in`, `cache` (cache read), `write` (cache write), `out` in USD per 1M tokens,
optional `long = {threshold, in_x, out_x}`, `flex_x`, `fast_x`, `ultra_x`,
`effective_from` / `effective_until` (`YYYY-MM-DD`, inclusive, UTC), `note`. The file can be
edited by hand; changes are picked up without a restart.

```
cost = (in − cached − cache_write)·P_in + cached·P_cache + cache_write·P_write + out·P_out
```
- `out` already includes reasoning tokens (not added again). Missing `cache` / `write` price ⇒ `in`.
- Long context: total input (incl. cached) > `long.threshold` ⇒ `P_in`, `P_cache`, `P_write`
  × `in_x`, `P_out` × `out_x` (OpenAI: 272K, ×2 / ×1.5).
- Tier: an entry with that tier wins; else the standard entry × `flex_x` (default 0.5),
  `fast_x` (default 2; per model, e.g. gpt-5.5 = 2.5) or `ultra_x` (no default: Ultrafast is
  unpriced without it). `[defaults]` sets the global multipliers.
- Dates: on the request's UTC date a dated entry wins over an undated one; with no matching
  entry the row is unpriced. Use dated entries for promotional prices.
- Unknown model / tier / date ⇒ `cost_usd: null`, `price_status: "unknown_price"`; summaries
  count those rows in `unknown_price` and list them in `unknown_price_models`. A group's
  `cost_usd` sums only priced rows (null if none) — never a fake 0; `unknown_price > 0` means
  it is a lower bound.
- Model names match exactly (case-insensitive); add aliases as extra entries.

### Price updates
Nothing is fetched in the background. An update downloads the dataset named by
`[usage] prices_source` through the configured egress (`proxy_url` when `proxy_enabled`; no
account data is sent), keeps the OpenAI text models (`gpt-*`, `codex*`, `o<n>*`, without
audio / realtime / image / search variants and dated snapshots) and diffs them against the local
table: `+` new, `~` changed, `!` the model has a dated/promo entry locally ⇒ never touched, `?`
local entry not in the dataset ⇒ kept. Applying writes the table only when something changes,
after copying the previous file to `prices.toml.bak` (comments are not preserved; per-model
multipliers the dataset does not give and `note`s are kept). New prices apply at once, history
included.

- Management page: settings → 「价格」 shows the number of priced models, the file time and the
  source. 「检查更新」 downloads and lists the changes; 「应用」 writes them (`POST
  /admin/v1/prices/check` → `POST /admin/v1/prices/apply {token}`; a check expires after 15
  minutes or when the file changes meanwhile). `GET /admin/v1/prices` also lists the models of
  the last 30 days without a price; the overview and settings pages show them as a hint.
  Summaries carry `price_models` (entries in the table; 0 = no table yet).
- Unpriced, ignored and hand-priced models: the same 「价格」 panel lists each unpriced model with
  「设置价格」 (input / cache / output, USD per 1M tokens; written as an undated standard entry
  with `custom = true`) and 「忽略」 (added to the table's top-level `ignore = [...]`). Ignored
  models cost exactly 0 (`price_status = "ignored"`): they are not counted in `unknown_price`,
  not listed as unpriced and do not turn totals into a lower bound; zero rather than excluded so
  request counts and token totals stay complete. 「取消忽略」 / 「删除」 undo either. Updates
  (`check` / `apply` / `prices sync`) never add, change or remove ignored or custom models
  (`skip_ignored` / `skip_custom`, shown with the promo skips). API: `POST
  /admin/v1/prices/models {model, action: ignore|unignore|set|unset, in, cache, out}` (model
  1–128 chars, no control characters, case-insensitive; prices 0–10000; unchanged ⇒ no write).
  Edits are written atomically at most once a second; the first edit of a hand-written file
  copies it to `prices.toml.bak` (an edit rewrites the file without its comments). Applied at
  once, history included; audited as `price <action>`.
- CLI: `ciallopa prices sync [--from litellm|models.dev|<url>|<file.json>] [--write]`, dry run
  without `--write`. On a `.deb` install run it as the service user
  (`sudo -u ciallopa HOME=/var/lib/ciallopa ciallopa -c /etc/ciallopa/config.toml prices sync
  --write`).

`prices_source` formats:
- LiteLLM `model_prices_and_context_window.json` (default,
  `https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json`,
  ~3 MB): standard, cache read / write, `*_above_272k_tokens` (long context) and
  `_priority` / `_flex` / `_ultrafast` prices. A tier whose input and output scale alike becomes
  a multiplier, otherwise an explicit tier entry.
- models.dev `api.json` (`https://models.dev/api.json`, ~6 MB): standard, cache, long context
  and the Fast mode price; no Flex / Ultrafast prices.

Manual export/import is intentionally not provided (the DB is a plain SQLite file).

## Client keys & instance
```toml
instance_id = "vps-1"               # default: hostname
# plain list still works (label = key-<sha256 prefix>)
# api_keys = ["sk-..."]
[[api_keys]]
label = "laptop"                    # 1–64 chars, unique
key = "sk-..."                      # ≥ 16 chars, unique
admin = true                        # optional; required for /admin/v1/* and reset-credit consume
```
Key matching is constant time across all keys; only the label reaches the forwarder / log.
Plain string keys and labeled keys without `admin = true` may only call the proxy and
`/usage/v1/*` reads. Management (`/admin/v1/*` and `POST /usage/v1/reset-credits/refresh`,
including config reload, settings writes, account management and reset-credit consume) needs
at least one admin key; if none is configured those routes answer 403 for every key.
When both `X-Api-Key` and `Authorization: Bearer` are sent, `X-Api-Key` wins — send only one.

## Config
```toml
[usage]
enabled = true                      # default true
path = "~/.ciallopa/usage.db"       # + -wal / -shm siblings
retention_days = 90                 # 0 = keep forever
queue_size = 8192                   # in-memory records between proxy and writer
batch_size = 256                    # rows per transaction (≈97% of max throughput)
flush_ms = 50                       # max wait before committing a partial batch

[usage.quota_weights]
input = 1.0                         # uncached input
cached = 0.1
output = 4.0                        # output incl. reasoning
min_delta_pct = 5.0                 # below → "insufficient_data"
```
`[usage] prices = "~/.ciallopa/prices.toml"` — price table; `prices_source = "<url>"` — dataset
for price updates (empty = updates off). Both apply without a restart (see "Cost").

## Web UI (`/ui`)
`[ui] enabled = true` (default). Static pages (`/ui`, `/ui/settings`, `/ui/accounts`), no data
without the key; every call goes to the endpoints above (and `/admin/v1/*`, see
[SETTINGS.md](SETTINGS.md)) with the key from `sessionStorage`. Read-only, except the opt-in reset-credit
consume action below (off unless `[ui] allow_reset_consume = true`) and settings / account changes
(off unless `[ui] allow_config_write = true`).
Headers: CSP `default-src 'none'; script-src 'sha256-…'; style-src 'sha256-…'; connect-src
'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`,
`Cache-Control: no-store`. Hashes are computed per page from the embedded files at startup, so
editing `crates/server/src/{ui,settings,accounts}.html` needs no manual CSP update.
`GET /favicon.ico` serves the tab icon (SVG).

Page behavior:
- Theme follows the system (`prefers-color-scheme`); the ◐ / ☀ / ☾ header button switches
  between system, light and dark and is remembered in `localStorage` (`ciallopa_theme`).
- `/ui` keeps the request filters and the time range / grouping / trend mode in the URL query
  (shareable, survives reload; `清除筛选` resets the request filters). Time range, grouping and
  trend mode are also remembered in `localStorage` as the defaults for a URL without them.
- Results of actions (refresh, enable / disable, save, copy) appear as toasts; load and save
  errors stay in a banner at the top of the page.
- Below 720 px the request and account tables switch to one card per row.
- Settings layout: a left nav groups the pages, each with its own hash link —
  转发 (`#routing` 账号调度, `#network` 网络, `#forward` 限流与容错, `#images` 图片接口),
  数据与账号 (`#usage` 用量与价格, `#prices` 模型价格, `#accounts` 账号与重置卡) and
  系统 (`#general` 常规, `#keys` 客户端密钥, `#audit` 变更记录). Old single-page anchors
  (`#g-base`, `#g-routing`, `#g-upstream_opts`, `#g-images`, `#g-log`, `#g-usage`, `#g-ui`,
  `#keysec`) still open the page that now holds that group. Below 760 px the nav becomes a list
  with a one-line summary per page that drills down (‹ 设置 goes back).
- Each page ends with a collapsed 「高级」 section; it opens by itself when it holds an unsaved
  change or a search hit. The search box searches every page (name, key, description, help);
  「只看已修改」 lists the settings that differ from their defaults.
- A setting whose value differs from the default has a blue left bar and 「默认 X · 恢复」.
  Settings that only apply when another one is on are indented under it and greyed with the
  reason while it is off; their values are kept. Numbers where 0 means off are a switch plus the
  value (turning the switch off keeps the number for next time). The 出错时冷却 table lists each
  upstream error with when it cools the account and for how long.
- Unsaved edits: the row is tinted and marked 未保存, a changed switch shows diagonal stripes in
  its new value's color (the saved value is in its tooltip), the nav marks the page 未保存, and
  a floating bar shows the count per page with 放弃并载入最新 / 检查并保存. Leaving the page
  with unsaved edits asks first. Irreversible confirmations carry a 不可逆 tag.
- 变更记录 refreshes when its page is opened, every 15 s while it is visible, and after price
  actions, so account changes and file reloads appear without reloading the page.
- Settings → 网络 → 允许 IPv6 出口: a switch that writes `ipv6` at once, like the proxy switch below.
- Settings → 网络 → 启用出口代理: the switch writes `proxy_enabled` at once (no review dialog); the
  address row shows the current egress and a 测试连接 button (`POST /admin/v1/proxy/test`) that
  probes the address as typed, saved or not, and shows exit IP, location, Cloudflare colo and
  latency, or the failure.
- Quota cards show **remaining** percent (bar width = remaining). Color follows remaining:
  green when ≥ 50%, yellow when < 50%, red when < 20%. The card top strip is yellow when
  the account's max used percent is over 50%, and red when over 80%.

## Reset credits (重置卡)
Manual only: nothing in ciallopa consumes a credit by itself (no failover / quota path calls it;
a test asserts the consume helper has a single caller, the admin handler).

Upstream (mirrors codex-rs `backend-client/src/client/rate_limit_resets.rs`), per account with
`Authorization: Bearer <access token>`, `ChatGPT-Account-Id`, `User-Agent: codex_cli_rs/<pinned>`,
`originator: codex_cli_rs`:
- `GET {upstream}/wham/rate-limit-reset-credits` (5 s) →
  `{available_count, credits: [{id, reset_type, status, granted_at, expires_at?, title?, description?}]}`
- `POST {upstream}/wham/rate-limit-reset-credits/consume` (10 s), body
  `{redeem_request_id, credit_id?}` → `{code: reset | nothing_to_reset | no_credit | already_redeemed, windows_reset}`

Config (`[ui]`):

| key | default | |
|---|---|---|
| `allow_reset_consume` | `false` | enable the consume API + page button |
| `reset_credits_refresh_mins` | `60` | background listing refresh (read-only); `0` = on demand only |

API (client key):
- `GET /usage/v1/reset-credits` → `{consume_enabled, can_consume, is_admin, refresh_mins, accounts: [{account, email,
  disabled, reset_credits: {fetched_ms, available_count, credits, nearest_expires_at, error,
  error_ms, usage_available_count, limit_reached, usage_observed_ms} | null}], attempts: [last 20
  consume attempts]}`. The `usage_*` fields and `limit_reached` come from the last 刷新额度
  (`/wham/usage`) answer; the listing fields stay authoritative for consuming. The 账号 page shows
  the newer of the two counts and 「已达额度上限」 until a newer quota observation replaces it. Cache is in memory (re-fetched
  after restart); `nearest_expires_at` = earliest expiry among `status == "available"`.
  `can_consume` = consume switch on **and** the calling key is admin (the page shows the button
  only then).
- `POST /usage/v1/reset-credits/refresh` — admin-only, JSON + same-origin (optional `{"account": "<id>"}`) → same body.
- `POST /admin/v1/accounts/{id}/reset-credits/consume` — `{"confirm": "<id>", "credit_id"?,
  "redeem_request_id"?}`. Guards: switch off → 403; key only via header (middleware), JSON
  content type (browser cross-origin forms cannot send it without a CORS preflight, which is
  never granted), a cross-site `Origin` / `Sec-Fetch-Site` → 403 (cookies such as Cloudflare
  Access's `CF_Authorization` are ignored, see [SETTINGS.md](SETTINGS.md)), `confirm` must equal the path id → 400, unknown
  account → 404, usage log disabled → 503. One consume per account at a time.
  Response: `{account, redeem_request_id, reused_redeem_request_id, attempt, status, code,
  windows_reset, reset_credits, rate_limit?}`; after `reset` the listing is re-fetched and
  `/wham/usage` `rate_limit` is returned (quota cards update on the next proxied response).
  After a `reset` the account's usage-limit and rate-limit cooldowns (account and per-model)
  are dropped at once, so it serves the next request; other cooldowns (401/403, 402, 404, 5xx,
  overload) stay. The response then carries `cooldown_cleared` (`true` if a live
  cooldown was dropped). Only this manual consume does it; nothing consumes a card by itself.

Idempotency: every attempt is a row in `reset_consume` (schema v6) with status `pending` →
`done` (any of the 4 codes) / `failed` (definitive 4xx) / `superseded`. Unknown outcome (timeout,
transport error, 5xx, 408/429, unparseable 2xx) stays `pending`; the next consume for the same
account + `credit_id` within 24 h reuses that `redeem_request_id` (upstream answers
`already_redeemed` if the first one went through). A new id is minted only after a definitive
answer.

## Client fingerprint for internal requests
ciallopa's own upstream calls (token refresh / code exchange / device login, `/wham/usage`,
reset-credit list / consume) send the `User-Agent` + `originator` of the **latest Codex client
seen** on an authenticated forwarded request (HTTP or WS handshake), instead of a fixed value.
Forwarded requests themselves are never changed.

- Accepted only as a matching pair, as built by codex-rs `get_codex_user_agent()`:
  `originator` ∈ {`codex_cli_rs`, `codex-tui`} and UA = `{originator}/{MAJOR.MINOR.PATCH[-pre]}…`
  (printable ASCII, ≤ 512 bytes, not `0.0.0`). curl, browsers, third-party clients,
  `codex_exec`, IDE/desktop originators and unauthenticated requests are ignored.
- Latest valid one wins (one global value). Persisted in the usage DB (`kv`, schema v8) or, with
  `[usage] enabled = false`, `client_fingerprint.json` next to the usage path; re-validated on
  restore. Fallback: `codex_cli_rs/0.164.0` + `codex_cli_rs`.
- `GET /usage/v1/client-fingerprint` → `{user_agent, originator, source: learned|default,
  learned_at_ms, key}` (key = client key label). Shown in the `/ui` header.

## Infrastructure cookies (`[upstream_opts] cloudflare_cookies`, default off)

Official Codex keeps a jar of Cloudflare service cookies plus the `__oailb` routing cookie for
ChatGPT hosts and replays them on HTTP and WebSocket requests. With `cloudflare_cookies = true`
ciallopa does the same, but with **one in-memory jar per account** (attached to the account's own
connection pool, dropped when the account disappears) so these cookies never link accounts.

- Stored names: `__cf_bm`, `__cflb`, `__cfruid`, `__cfseq`, `__cfwaitingroom`, `__oailb`,
  `_cfuvid`, `cf_clearance`, `cf_ob_info`, `cf_use_ob`, `cf_chl_*`. Everything else
  (session/auth/account cookies) is discarded.
- Only `https` ChatGPT hosts (`chatgpt.com`, `*.chatgpt.com`, `chat.openai.com`,
  `chatgpt-staging.com`, `*.chatgpt-staging.com`); `wss` shares the https scope. Domain, path,
  `Max-Age`/`Expires` are honoured (reqwest cookie jar). Nothing is persisted to disk.
- Sources: forwarded HTTP responses, WS handshake responses (including rejected upgrades), and
  internal requests (reset credits). Token refresh goes to `auth.openai.com` and never carries
  cookies.
- Always, regardless of the flag: the client's `Cookie` header is never forwarded upstream and
  upstream `Set-Cookie` is never returned to clients.

## Egress address family (`ipv6`, default off)
- `ipv6 = false`: direct connections (Codex HTTP and WebSocket traffic, token refresh, usage
  polling, login, `prices sync`, the proxy test) and connections to the proxy host use IPv4
  addresses only; no IPv6 connection is attempted. A host with only IPv6 addresses (or an IPv6
  literal upstream / proxy address) then fails to connect.
- `ipv6 = true`: IPv6 and IPv4 are both used, with Happy Eyeballs:
  - WebSocket dialer: addresses interleaved IPv6-first; a new attempt starts every 250 ms or as
    soon as the previous one fails; the first connection wins, the others are dropped; when all
    fail, one error lists every address.
  - HTTP (reqwest / hyper-util connector): the family of the first resolved address is tried
    first and the other family starts after 300 ms (hyper-util's fixed delay; reqwest does not
    expose it).
- With a proxy, the setting only affects reaching the proxy host; the proxy resolves the target
  names (`socks5h`, HTTP `CONNECT`) and picks the address family itself.

## Graceful shutdown
On SIGTERM / Ctrl-C the listener stops accepting and new forwarding admission closes.
HTTP/SSE and active WebSocket turns get up to 60 s to finish. An idle WebSocket closes normally
(1000); an active one closes normally after its current turn, allowing the client to reconnect.
At the deadline, or on a second stop signal, remaining work is cancelled. HTTP bodies may be
truncated; WebSockets attempt to close with 1001 (going away). Unfinished tracked work records
`shutdown` / `local`, rather than a client cancellation.

The process then allows up to 3 s for closing, 5 s for the final usage/fingerprint flush and
2 s for runtime shutdown. Queued records are best effort within those bounds; a blocked disk
write or forced kill can prevent persistence. The application budget is about 70 s, with a
75 s Docker/systemd stop timeout. See [DEPLOYMENT.md](DEPLOYMENT.md) for traffic draining and
replacement limits.

## Disk writes

ciallopa batches or rate-limits anything that can hit the disk under normal traffic (same idea as
Redis `save <seconds> <changes>`, Grafana settings debounce, or a single coalescing writer
thread):

| Path | Policy |
|---|---|
| Usage SQLite (`[usage]`) | Writer thread: batch up to `batch_size` rows or `flush_ms`, then one transaction. |
| Learned client fingerprint | One background writer; coalesces to the latest UA/originator; saves on change, at most once per 60 s, and at most hourly for an unchanged pair. Request path never blocks on I/O. Flushed on graceful shutdown. |
| Auth files (`auth_dir`) | Only on login / import / token refresh / admin account edits — never per forward. |
| Config write-back + `.bak` | Only on an explicit admin save. |
| `.audit.jsonl` | Append-only on admin actions (rare). |

Anything that would thrash the disk per request is a bug; prefer in-memory + a single coalescing
writer (see `ciallopa_core::fingerprint`).
