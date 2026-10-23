ciallopa v0.1.0 — pre-release.

This rebuild replaces the earlier v0.1.0 release. Re-download the assets and verify their
new SHA256SUMS; the tag and container version now identify this build. The container is
published as `0.1.0` and `latest`; this prerelease leaves the `0.1` alias unchanged.
The `latest` container tag now follows successful tagged builds, including prereleases.

ciallopa is a lightweight, self-hosted AI gateway: one endpoint, streaming forwarding,
account routing and local request diagnostics. It remains one Rust binary with SQLite and
a built-in management UI, intended to run behind a TLS reverse proxy.

The direction is provider-independent; today's Codex backend adapter supports Responses
HTTP/SSE and WebSocket, model listing and images. Other providers are not yet supported.

**Request diagnostics**

- Overview, trends and requests share the same time range and account/model/key/instance
  filters. Clicking a count or time bucket opens the corresponding requests; the business
  request count and its list both exclude WebSocket prewarm records.
- Failures, client cancellations, incomplete answers and prewarm are distinct. Streaming
  first-output p50/p90 and generation-speed estimates show their sample counts; unknown
  usage or prices are not presented as zero. Costs remain estimates from the local price table.
- Successful requests also have an inspector: record ID, timing, usage, model/tier, account,
  retries and available attempt details. Browse adjacent requests, find a record by ID,
  copy JSON or share a link with a fixed query range. Pagination uses record-ID cursors.
- Selected HTTP/SSE timeout and truncation warnings include the existing record ID and
  available upstream request ID; active WebSocket transport/idle warnings include the turn's
  record ID. Recording must be enabled, and a queued or dropped record may not be retrievable.
- New metric fields preserve the older API fields. Historical records are not rewritten;
  older non-stream records may still carry their previous streaming/timing classification.

**Forwarding and limits**

- HTTP response-header waits and cumulative preparation across retries/account switches have
  separate configurable deadlines: 300 s and 600 s by default. Valid SSE bootstrap/generation uses the separate
  idle limit, with no new total generation deadline. These HTTP budgets do not apply to WS.
- Buffered upstream error bodies and model lists have byte/time bounds. HTTP timeout options
  stay with an in-flight request across reloads; connection errors use the egress actually
  used by that attempt, even when the proxy setting changes while it is running.
- `max_inflight_requests` defaults to 64. Each HTTP request holds its slot through its response
  body; each WebSocket holds one for its whole connection, including idle time. At capacity,
  new forwarding requests receive 503 `gateway_busy` and `Retry-After: 1` without a queue.
  Health and management do not consume forwarding slots. The limit is hot-reloadable; `0` disables rejection.
- Non-stream and Images records follow body completion or interruption. Responses SSE observes
  completion and first output from each event's top-level type, avoiding false matches in model text.
  Observation of events beyond 8 MiB remains best effort; bytes still pass through.

**Management**

- The overview puts request investigation near the trend, with current gateway occupancy
  separate from historical metrics. Fixed ranges and filters survive browsing and refreshes.
- Settings preserve drafts through refreshes and version conflicts, show the latest server
  differences before saving, and distinguish applied values from changes awaiting restart.
- Accounts retain the last successful quota snapshot during temporary failures and explain
  stale or unavailable data. Adding an account expands in place; device-code login and import
  remain accessible without filling the page. Late responses cannot replace a newer login's state.
- The UI stays self-contained, with keyboard-accessible controls and desktop/mobile layouts.
  No dashboard service or frontend build tool is required.

**Upgrade and deployment notes**

- `max_body_mb = 16` now means a 16 MiB HTTP request-body limit. The old implementation silently
  allowed at least 20 MiB. Set `max_body_mb = 20` and restart to retain that effective default.
  The limit measures incoming bytes, not decompressed JSON or WebSocket frames.
- Shutdown now drains HTTP/SSE work as well as active WebSocket turns for up to 60 s, then
  cancels remaining work and attempts a bounded final flush. A second signal skips the drain
  wait. Keep the supplied 75 s Docker/systemd stop timeout; queued-record persistence is best effort.
- The packaged unit supports `systemctl reload ciallopa`. It delivers SIGHUP; check the settings
  page's `last_reload` or the service log for the asynchronous validation/apply result.
- `apt purge` removes `/etc/ciallopa` and `/var/lib/ciallopa`, but now always keeps the system
  user/group so external data and backups retain their owner. Back up package and custom paths first.
- Compose requires pre-created, correctly owned config/data directories and retains a read-only
  root filesystem, non-root execution and bounded container logs. Image bases are pinned by digest;
  the release workflow compares the runnable image with its smoke-tested build before tagging it.
- The runtime base is now Distroless `static-debian13:nonroot`, replacing Debian 12 while retaining
  the static musl binary and the existing config/data mounts.
- Health checks measure local liveness, not upstream readiness. A saved `listen` change awaiting
  restart can make the probe target the wrong address. See deployment guidance before replacement.

**Installation and further reading**

Linux x86_64 artifacts: `.deb`, static musl tarball and `SHA256SUMS`; the container target is
`linux/amd64` at `ghcr.io/seia-yurizono/ciallopa`. Follow the [installation guide][install].
Use an exact release tag or image digest and retain a data backup when upgrading.

[Metrics][metrics] · [Live settings][settings] · [Deployment and backups][deploy] · [Architecture][architecture] · [Testing][testing]

Licensed under the [MIT License][license]. See the [usage notes][usage-notes]. Not affiliated with any AI service provider.

[install]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/README.md#quick-start-linux-x86_64
[metrics]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/docs/METRICS.md
[settings]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/docs/SETTINGS.md
[deploy]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/docs/DEPLOYMENT.md
[architecture]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/docs/ARCHITECTURE.md
[testing]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/docs/TESTING.md
[license]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/LICENSE
[usage-notes]: https://github.com/Seia-Yurizono/ciallopa/blob/HEAD/README.md#disclaimer--免责声明
