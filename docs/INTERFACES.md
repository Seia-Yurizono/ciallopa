# Interfaces and module boundaries

This is the current implementation map, not a historical worker assignment. Architecture
decisions and proposed changes live in [ARCHITECTURE.md](ARCHITECTURE.md). Public contracts in
`ciallopa-core` affect several crates: update all consumers and relevant contract tests together.

## Crates

| Crate | Responsibility | Boundary |
| --- | --- | --- |
| `core` | Config/schema, common account/routing types and traits, errors, redacting logs, validated client fingerprint | Shared contracts; currently includes adapter-specific types |
| `auth` | Account snapshots, OAuth/login/import, refresh locks, atomic credential writes | Implements `AccountStore`; uses `upstream` to construct compatible HTTP clients |
| `routing` | Session extraction/bindings, RR/WRR/fill-first, priorities, quota and cooldown policy | Implements synchronous `Router`; no network or disk I/O in selection |
| `upstream` | Per-account HTTP pools, WS dialer, proxy/TLS, cookies, retry timing, shared drain state | Owns transport configuration and compatibility-sensitive dependencies |
| `forward` | Client routes, protocol inspection, HTTP/SSE/WS/images forwarding, attempts and usage tracking | Coordinates account store, routing and transport; owns stream lifecycle |
| `usage` | Bounded record queue, SQLite writer/read queries, error detail, prices, reset-attempt persistence | No synchronous storage write in the forwarding path |
| `server` | CLI, assembly, key/admin enforcement, settings/accounts/usage APIs, embedded UI | Owns process lifecycle and management operations |

`auth`, `routing`, `upstream` and `usage` depend on `core`; `auth` also uses `upstream`.
`forward` uses their interfaces and implementations where protocol parsing requires them;
`server` composes the application. `auth` and `routing` do not depend on each other.

## Shared contracts

The source is authoritative; avoid maintaining a second copy of Rust signatures here:

- [`core/src/types.rs`](../crates/core/src/types.rs): `Account`, `Credentials`, `SessionInfo`,
  `RouteRequest`, `Selection`, `QuotaSnapshot`, `Outcome`, account patches and import results.
  `Credentials::Debug` redacts the access token. `RouteRequest::attempt_at` prevents an old
  in-flight success from clearing a newer cooldown. `Outcome::Transport` reports statusless
  failures without cooling an account or changing its bindings; HTTP 408/5xx use `ServerError`.
  `SessionInfo`'s subagent/fork flags determine parent inheritance and aliasing in both
  selection and result reporting.
- [`core/src/traits.rs`](../crates/core/src/traits.rs): `AccountStore` exposes synchronous,
  shared `accounts()` snapshots, `generation()` and `len()`, plus async credentials/refresh,
  reload and account management (`list()` copies the snapshot for admin callers). `Router`
  selects, receives attempt outcomes, updates config and exposes/clears routing state. Its
  methods remain synchronous and free of I/O; internal short critical sections are allowed.
  Each select/report captures one routing config snapshot.
- [`core/src/retry_after.rs`](../crates/core/src/retry_after.rs): shared Retry-After parsing
  retains explicit caller policies. HTTP uses integer seconds or HTTP-dates with a caller
  cap of 6 h; WS/overload retries use float seconds with their existing 60 s / 10 s caps.
- [`core/src/settings.rs`](../crates/core/src/settings.rs): `SharedConfig` uses atomically
  replaceable snapshots; `SCHEMA` declares hot/restart, editable and secret fields. New config
  fields must have schema coverage and reload behavior. Request timeouts retain their captured
  values; routing, egress and error policies can observe updates at their documented boundaries.
  This is not a transaction spanning all components; see [SETTINGS.md](SETTINGS.md).
- [`forward/src/lib.rs`](../crates/forward/src/lib.rs): `AppState` holds config, account store,
  router, upstream and optional usage recorder. `ClientKey` carries only a label and admin bit.
  Internal callers import attempts, HTTP pumps and classifiers from their owning modules.
  HTTP and WS consume `classify::Class` directly; the old `EventKind` view is test-only.
- [`usage/src/lib.rs`](../crates/usage/src/lib.rs): `Record` is a completed HTTP request or WS
  turn; `Recorder::record` uses non-blocking `try_send`. This is operational telemetry, not
  a durable billing transaction.
- [`usage/src/reset.rs`](../crates/usage/src/reset.rs): `ConsumeOutcome` and `reset_finish`
  require `ConsumeStatus`; persisted status strings and management JSON remain unchanged.
- [`usage/src/problem.rs`](../crates/usage/src/problem.rs): the single classification of a
  stored request into the list's problem buckets (`error` / `upstream` / `unfinished`), its cause,
  `rescued` and suggested `next` steps, in Rust per row and as SQL predicates for filters and
  `/usage/v1/records/counts`. The UI renders these fields and does not reclassify.

## Request lifecycle

1. Server key middleware authenticates, enforces admin access and attaches the key label.
2. Forwarding admission acquires a process lease before reading the request body. At capacity
   it returns 503 without queuing. It then maps the route, extracts session/model information
   and prepares the body once for reuse across permitted attempts.
3. Routing selects an eligible account; the account store obtains credentials, refreshing
   under per-account and cross-process locks when necessary.
4. Forwarding performs the upstream attempt and reports its classified outcome. Network
   failures, account failures, request errors and usage limits have different semantics.
5. Before output is committed, eligible failures may be replayed within the existing budgets.
   SSE preamble buffering is bounded. After output, the proxy does not splice another answer
   onto the stream. See [USAGE.md](USAGE.md#error-sources) for exact HTTP and WS behavior.
6. The response body owns the lease and streaming lifecycle. Its channel producer is cancelled when
   the downstream body is dropped, including while waiting for a silent upstream. The
   tracker emits a record on completion, failure or cancellation without waiting for SQLite.
   When a retry, account switch or WS replay happened, the record carries the attempt chain
   (captured detail) even if the request finally succeeded; a request with no retry stores none.
   It goes through the same batched writer, with no extra write per request and no schema change.

Every non-2xx HTTP response that has a usage record (including a refused WS handshake), and
every `type:error` WS frame of a recorded turn, carries `x-request-id: cp-<record_id>`; Codex
prints it as `request id: …`. An upstream `x-request-id` moves to `x-upstream-request-id`
(also in the frame's `headers`); `x-oai-request-id`, other headers and the body stay untouched.
Without a record (usage disabled, or rejected before one exists) the response is left as is.

WS uses HTTP/1.1 Upgrade, with optional `permessage-deflate`; HTTP/2 extended CONNECT is not
implemented. Each established WS is pinned to an account. A pre-output full-input turn can
reconnect/replay when permitted; an incremental turn or committed output cannot be silently
replayed on another account. The current protocol can close with 1012 to let the client replay
full history. Account-bound encrypted context is never stripped to make a failover succeed.

Models caching, passthrough and images have separate handlers. The current upstream adapter
is protocol-specific; the planned provider boundary is not implemented by these traits yet.
The Images wrapper returns HTTP 502 with `error.type=image_output_too_large` when an upstream
event, retained image bytes, image count, or serialized output exceeds its limit. In streaming
mode it ends the HTTP 200 body with a single `event: error` whose message identifies the limit.

## Invariants

- Preserve the pinned upstream network fingerprint. Dependency or client-builder changes
  require both lock comparison and wire fingerprint checks; do not tune HTTP/2/TLS casually.
- Use redacting logging. Never log raw credentials, client keys or authorization/cookie headers.
- Account writes preserve unknown fields, private modes and atomic rename. Refresh re-reads
  under the file lock and does not overwrite newer credentials written by another process.
- Bound sessions do not migrate because of quota-avoidance thresholds or priority changes.
- Retry/failover eligibility must account for already committed output and retained state.
- Record queues, captured errors, models bodies/cache and Responses preamble inspection have
  bounds. Forwarding admission counts operations, while HTTP preparation and shutdown have
  time budgets. These do not constitute a byte/RSS limit: Images collection and large request
  inspection still have separate costs documented in the architecture review.
- Management writes retain validation, revision checks and audit behavior. The UI cannot
  enable its own file-controlled write privilege.

## Verification

Use [TESTING.md](TESTING.md) for focused development commands, shared CI checks and checks
triggered by transport, UI or packaging changes. Local mock/client coverage is documented in
[RECOVERY.md](RECOVERY.md); deployment checks are described in [DEPLOYMENT.md](DEPLOYMENT.md).
