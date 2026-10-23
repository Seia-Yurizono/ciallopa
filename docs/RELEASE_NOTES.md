ciallopa v0.1.1-rc.1 — pre-release.

This release brings the validated mainline improvements since v0.1.0 to the Linux x86_64
packages and Debian 13 container image. ciallopa remains a lightweight AI gateway: one Rust
binary, local SQLite records and an embedded UI. The implemented upstream adapter is still
the Codex backend; this release does not add other providers.

**Stream reliability and diagnostics**

- WebSocket handshake failures and early cancellation retain the actual selected account.
  Initial and per-turn preparation have cumulative budgets; normal generation retains its
  separate idle semantics. Blocked upstream writes can observe client cancellation through
  bounded prefetch without flushing or replaying a partially written frame.
- Oversized Retry-After and quota-reset values no longer panic or overflow the clock.
  Shutdown ends usage batch collection promptly, so a long flush interval does not prevent
  queued records from being written before the final flush returns.
- Abnormal SSE/WS termination reports bounded metadata: event type/count, observed terminal,
  activity/event gaps and available record/thread/turn/upstream IDs. WS replay-write failures
  use the new attempt's account and handshake ID. No new prompt or tool-result logging.
- Oversized unobservable SSE outcomes remain unknown instead of being counted as success or
  cancellation. Explicit direct Images stream errors are observed without changing forwarded bytes.

**Request investigation**

- Detail-load failures can be retried in place; expired details keep the request summary.
  Retrying details does not repeat a model request. Late responses cannot replace another
  record or clear a newer login.
- Active scope and request filters can be removed individually. Their different scope, URL
  state and keyboard focus are preserved. Detail navigation adds Alt+Up/Down and boundary focus recovery.
- Request rows reuse existing DOM nodes. Quota/egress snapshots handle failed or late reads,
  and trend buckets distinguish missing usage from reported zero.
- Documentation now follows the current architecture, recovery, deployment and UI contracts;
  obsolete development reports and manual e2e helpers have been removed.

**Validation and recovery limits**

The implementation passed 339 workspace tests, the shared checks, 34 official Codex 0.164.0
local-mock scenarios, six Caddy 2.11.7 reload scenarios, and desktop/375px browser checks.
Release packaging runs its own checks, .deb/systemd smoke, Docker smoke and Compose
recovery/active-stream shutdown before publication. No real model provider was used.

An unacknowledged tool can appear as `aborted` after client recovery even when its local side
effect already happened. A Caddy reload can cause another generation; a finite close delay
helps only if the turn finishes in time and retains old connection capacity meanwhile.
Neither behavior provides exactly-once tools, output or billing.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.1.1-rc.1` and `latest`, for `linux/amd64`.
  This prerelease does not advance the `0.1` alias. Change a pinned old Compose tag before
  pulling and recreating; pulling `latest` alone does not replace a running container.
- The existing config/data layout and SQLite schema are retained. Keep a data backup and the
  previous image locally. The configured cleanup policy retains only the newest two tag names,
  so `latest` and `0.1.1-rc.1` count as both retained tags; old registry versions may be deleted.
- Keep the supplied 75-second stop timeout. Remove new traffic and drain active work for
  planned replacement; normal drain has a 60-second application limit.
- Download the .deb or static musl tarball and verify it against the accompanying SHA256SUMS.
  No dependency or external service is needed for the management UI.

[Installation][install] · [Recovery boundaries][recovery] · [Deployment and backup][deploy] · [Documentation][docs]

Licensed under the [MIT License][license].

[install]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.1/README.md#quick-start-linux-x86_64
[recovery]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.1/docs/RECOVERY.md
[deploy]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.1/docs/DEPLOYMENT.md
[docs]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.1/docs/README.md
[license]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.1/LICENSE
