# cpa-lite interfaces & module ownership (M0)

Design spec: `<local-path>` (v3). This file is the contract between
the three parallel workers. **Anything public in `cpa-lite-core` changes only via the lead.**
Workers may add private code / new pub items inside their own crate freely.

## Crates & owners
| Crate | Path | Owner | M1 scope |
|---|---|---|---|
| `cpa-lite-core` | crates/core | lead | `Config`, shared types (`types.rs`), traits (`traits.rs`), errors, redacting logging |
| `cpa-lite-auth` | crates/auth | **worker A (auth)** | `FileAccountStore: AccountStore` (refresh w/ per-account lock, atomic 0600 writes), `login::{device_code, paste_callback, pkce_localhost, import_codex_auth_json}`, `cpa-lite login` CLI |
| `cpa-lite-routing` | crates/routing | **worker B (routing)** | `CpaRouter: Router` (RR / WRR / fill-first, priority tiers, weights, session affinity + TTL + subagents + rebind-on, cooldown/Retry-After, quota-threshold avoidance for *new* sessions only), `extract_session()` full CPA order, quota header parser `parse_quota(&HeaderMap) -> Option<QuotaSnapshot>` |
| `cpa-lite-upstream` | crates/upstream | **worker C (forwarding)** | `build_reqwest` (**fp-sensitive, do not add h2/TLS tuning**), `UpstreamClient::ws_connect`, retries/backoff, idle timeout |
| `cpa-lite-forward` | crates/forward | **worker C (forwarding)** | `routes(AppState)`: SSE `/responses`, `/responses/compact`, models (5 min cache), WS `/responses`, catch-all `/backend-api/*`, `/v1/images/{generations,edits}` |
| `cpa-lite` (bin) | crates/server | lead | wiring, `/healthz`, static-key middleware (Bearer / x-api-key, constant-time), body limit, 404 fallback |

Dependency direction: `core` ← `auth`, `routing`, `upstream` ← `forward` ← `server`.
`auth` and `routing` never depend on each other or on reqwest/axum; `forward` talks to them only via traits.

## Shared types (crates/core/src/types.rs)
- `AccountId = String` — auth file stem.
- `Account { id, email, chatgpt_account_id, plan_type, priority: i32 (higher first, default 0), weight: i64 (default 1, ≤0 excluded, max 1e6), disabled }`
- `Credentials { account, access_token, chatgpt_account_id }` — Debug redacts the token.
- `SessionInfo { key: Option<String> ("codex:"/"pck:"/"conv:"/"derived:" prefixed), parent_key, is_subagent }`
- `RouteRequest { model, session: SessionInfo, exclude: Vec<AccountId> }`
- `Selection { account, from_binding }`
- `QuotaSnapshot { primary, secondary: Option<QuotaWindow{used_percent, window_minutes, resets_at}> }`
- `Outcome = Success{quota} | RateLimited{retry_after, usage_limit, quota} | AuthFailed | Transient{status} | ClientError{status}`
- Errors: `AuthError {NotFound, ReloginRequired, Refresh, Io, Invalid, Unimplemented}`, `RouteError {NoAccounts, AllCoolingDown{retry_after}, Unimplemented}`.

## Traits (crates/core/src/traits.rs)
```rust
#[async_trait] pub trait AccountStore: Send + Sync + 'static {
    async fn list(&self) -> Vec<Account>;
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError>; // refresh if exp < now+5m
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError>; // after upstream 401
    async fn reload(&self) -> Result<(), AuthError>;
}
pub trait Router: Send + Sync + 'static {               // sync, no I/O, internal Mutex ok
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError>;
    fn report(&self, req: &RouteRequest, account: &AccountId, outcome: &Outcome);
}
```
Routing also exports free fns (worker B): `extract_session(&HeaderMap, Option<&serde_json::Value>) -> SessionInfo`
and (to add) `parse_quota(&HeaderMap) -> Option<QuotaSnapshot>`.

## Request flow (forwarding, worker C)
```
key middleware (server) → handler
  session = routing::extract_session(headers, body)
  loop attempt ≤ max_attempts:
     sel   = router.select(&req, &accounts.list().await)?      // AllCoolingDown → 429 + Retry-After
     creds = accounts.credentials(&sel.account).await?         // ReloginRequired → report AuthFailed, exclude, continue
     resp  = upstream (replace only authorization + chatgpt-account-id; drop hop-by-hop, client key, X-Forwarded-*)
     401   → accounts.force_refresh() once, retry same account; still 401 → Outcome::AuthFailed
     router.report(&req, &sel.account, &outcome)
     retry (exclude account) only if nothing was sent downstream yet
```
WS: one downstream WS ↔ one upstream WS pinned to one account; on usage_limit/429/close → report, close downstream with same code.

## Invariants
- Fingerprint: never touch rustls/reqwest/h2 versions or builder knobs without `scripts/lock-check.sh` and `scripts/fp-check.sh` passing.
- No token / key ever logged unredacted; use `tracing` (output goes through `core::logging::redact`).
- Auth files: plain JSON, 0600, dir 0700, atomic tmp+rename (`auth::file::write_auth_file`).
- Bound sessions are never moved by quota-threshold avoidance or priority changes.
- No realtime/voice, no warm-up, no Chrome TLS.

## Gates (every worker PR)
`cargo build --all-targets && cargo clippy --all-targets -- -D warnings && cargo test && scripts/lock-check.sh`;
`scripts/fp-check.sh` (needs sudo; edits/restores /etc/hosts) for anything touching crates/upstream.

## M1 additive core changes (worker B, routing) — pending lead review
- `SessionInfo.is_fork: bool` (Codex `forked_from_thread_id`; fork inherits parent binding, not a subagent).
  `parent_key` = CPA "fallback id" (explicit parent / fork source / `conv:` alias for `pck:` / short message hash).
- `Outcome::ModelUnsupported { retry_after }` (404 / model unsupported → that model, Retry-After else 12h) and
  `Outcome::Challenge` (Cloudflare challenge → ≥10s exponential). `Transient{status:None}` = transport error:
  no cooldown, bindings untouched; `ClientError` likewise.
- routing exports for forwarding: `parse_quota`, `parse_quota_event_headers` (SSE/WS `codex.rate_limits` / `error` frames),
  `retry_after_for_429(headers, body, now)` (body resets_at/resets_in_seconds first, then Retry-After ≤6h),
  `is_usage_limit(body)`, `is_cloudflare_challenge(text)`; `CpaRouter::{quota_snapshot, cooldown_until, invalidate_account}`.
