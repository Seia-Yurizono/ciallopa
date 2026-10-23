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
  in-flight success from clearing a newer cooldown.
- [`core/src/traits.rs`](../crates/core/src/traits.rs): `AccountStore` exposes snapshots,
  credentials/refresh, reload and account management. `Router` selects, receives attempt
  outcomes, updates config and exposes/clears routing state. Its methods remain synchronous
  and free of I/O; internal short critical sections are allowed.
- [`core/src/settings.rs`](../crates/core/src/settings.rs): `SharedConfig` uses atomically
  replaceable snapshots; `SCHEMA` declares hot/restart, editable and secret fields. New config
  fields must have schema coverage and reload behavior. Request timeouts retain their captured
  values; routing, egress and error policies can observe updates at their documented boundaries.
  This is not a transaction spanning all components; see [SETTINGS.md](SETTINGS.md).
- [`forward/src/lib.rs`](../crates/forward/src/lib.rs): `AppState` holds config, account store,
  router, upstream and optional usage recorder. `ClientKey` carries only a label and admin bit.
- [`usage/src/lib.rs`](../crates/usage/src/lib.rs): `Record` is a completed HTTP request or WS
  turn; `Recorder::record` uses non-blocking `try_send`. This is operational telemetry, not
  a durable billing transaction.

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

WS uses HTTP/1.1 Upgrade, with optional `permessage-deflate`; HTTP/2 extended CONNECT is not
implemented. Each established WS is pinned to an account. A pre-output full-input turn can
reconnect/replay when permitted; an incremental turn or committed output cannot be silently
replayed on another account. The current protocol can close with 1012 to let the client replay
full history. Account-bound encrypted context is never stripped to make a failover succeed.

Models caching, passthrough and images have separate handlers. The current upstream adapter
is protocol-specific; the planned provider boundary is not implemented by these traits yet.

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
[ACCEPTANCE.md](ACCEPTANCE.md); deployment checks are described in [DEPLOYMENT.md](DEPLOYMENT.md).
