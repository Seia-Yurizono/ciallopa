## Unreleased

> Draft for the next release. `release.yml` publishes this whole file as the release body,
> so rename or remove this heading (and the rc.2 notes below) before tagging.

**Upstream reliability**

- Cloudflare block pages (e.g. `error code: 1010`) are treated as an egress block, not an
  account failure: the account is not cooled or rotated, the client gets a neutral 502, and
  the overview shows an egress banner with the Cloudflare error code and ray ID.
- Rescanning the auth directory keeps accounts whose file is temporarily unreadable or
  half-written, along with their cooldown state, instead of dropping and re-adding them.
- An upstream overload that arrives after empty item announcements but before any output
  now switches accounts as intended; the held announcements are not leaked to the client.
- Tracks Codex 0.164.0: client version, rustls 0.23.46 and refresh-reason labels. The TLS
  ClientHello offers X25519MLKEM768 first, matching Codex.
- The default `max_body_mb` is now 64 MiB, so long Codex histories with inline images are
  not rejected.
- New hot setting `max_buffered_body_mb` (default 256 MiB) caps all buffered HTTP request
  bodies together. When it is full, a request waits up to 1 s, then gets the usual 503 with
  `Retry-After: 1`. Ten concurrent 20 MiB requests now peak at about 330 MiB RSS instead of
  about 530 MiB, and a full 256 MiB budget peaks at about 0.5 GiB.
- Client WebSocket messages count against the same budget while they are held for replay.
  Without room, the client gets a 503 error frame and the connection stays open. This limits
  buffered message bytes, not peak memory: per-connection WebSocket buffers still dominate.
- A request whose declared size is over `max_body_mb` gets a JSON 413 before its body is read.
- `max_body_mb` now applies without a restart, on forwarding and management routes.

**Routing**

- Session bindings survive a restart. They are saved to `session_affinity.json` next to the
  usage database (mode 0600, at most every 30 s after a change and at shutdown). Expired
  bindings and removed accounts are dropped on load.

**Performance**

- The Linux builds use jemalloc (one arena). In local tests with 20 MiB bodies, CPU per
  request dropped by about 40% and small-request throughput rose from about 5,900 to 8,400
  requests per second. Peak memory rose by about 10%; idle memory is about 15 MiB.
- Client connections set `TCP_NODELAY`. Small requests under concurrency no longer wait
  about 40 ms for delayed ACKs (local p50 went from 44 ms to 4–5 ms at 32 connections).

**UI**

- Request status chips are coloured by outcome (2xx, 429, other 4xx, 5xx).
- On touch screens the header refresh-interval picker has a 44px hit area.
- Settings explain every option and routing strategy, and the auto-refresh interval can be
  selected.
- Single-sentence hints and empty states no longer end with 。.
- The accounts page shows each account's reset-card count and 「已达额度上限」 when the last
  刷新额度 reported it. Display only: cards are still spent only by the manual button.

**Build**

- GitHub Actions moved to their Node 24 major versions.

ciallopa v0.1.1-rc.2 — pre-release.

This release brings the validated mainline improvements since v0.1.0 to the Linux x86_64
packages and Debian 13 container image. ciallopa remains a lightweight AI gateway: one Rust
binary, local SQLite records and an embedded UI. The implemented upstream adapter is still
the Codex backend; this release does not add other providers.

This candidate corrects rc.1 packaging: the .deb asset now uses a GitHub-safe SemVer filename,
while its internal Debian version retains the proper prerelease ordering. SHA256SUMS and
installation links use the exact uploaded name. It also fixes a concurrent usage-query race
found during release validation: labels and records now share one read snapshot, so a newly
committed request cannot appear with missing account or model names.

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

The implementation passed 340 workspace tests, the shared checks, 34 official Codex 0.164.0
local-mock scenarios, six Caddy 2.11.7 reload scenarios, and desktop/375px browser checks.
Release packaging runs its own checks, .deb/systemd smoke, Docker smoke and Compose
recovery/active-stream shutdown before publication. No real model provider was used.

An unacknowledged tool can appear as `aborted` after client recovery even when its local side
effect already happened. A Caddy reload can cause another generation; a finite close delay
helps only if the turn finishes in time and retains old connection capacity meanwhile.
Neither behavior provides exactly-once tools, output or billing.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.1.1-rc.2` and `latest`, for `linux/amd64`.
  This prerelease does not advance the `0.1` alias. Change a pinned old Compose tag before
  pulling and recreating; pulling `latest` alone does not replace a running container.
- The existing config/data layout and SQLite schema are retained. Keep a data backup and the
  previous image locally. The configured cleanup policy retains only the newest two tag names,
  so `latest` and `0.1.1-rc.2` count as both retained tags; old registry versions may be deleted.
- Keep the supplied 75-second stop timeout. Remove new traffic and drain active work for
  planned replacement; normal drain has a 60-second application limit.
- Download the .deb or static musl tarball and verify it against the accompanying SHA256SUMS.
  No dependency or external service is needed for the management UI.

[Installation][install] · [Recovery boundaries][recovery] · [Deployment and backup][deploy] · [Documentation][docs]

Licensed under the [MIT License][license].

[install]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.2/README.md#quick-start-linux-x86_64
[recovery]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.2/docs/RECOVERY.md
[deploy]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.2/docs/DEPLOYMENT.md
[docs]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.2/docs/README.md
[license]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.1-rc.2/LICENSE
