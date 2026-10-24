ciallopa v0.1.2

This release follows v0.1.1-rc.2. It makes accounts and memory behave better on small hosts,
keeps Codex sessions on their account across restarts, tracks Codex 0.164.0, and adds Linux
arm64 builds and a multi-arch image (including Docker Desktop on Apple Silicon). The
implemented upstream adapter is still the Codex backend.

**Platforms**

- Linux arm64: the container image is multi-arch (`linux/amd64` and `linux/arm64`), so the
  same tags and Compose file work on arm64 Linux hosts and under Docker Desktop on Apple
  Silicon. The release also ships `ciallopa-0.1.2-aarch64-unknown-linux-musl.tar.gz` and
  `ciallopa_0.1.2-1_arm64.deb`; `SHA256SUMS` covers every asset.
- Each architecture is built and smoke-tested on a native GitHub runner. The arm64 binary
  uses jemalloc's 64 KiB page setting, so it also runs on 16 KiB / 64 KiB page kernels. The
  TLS fingerprint (JA4, HTTP/2 settings, key shares) is the same on arm64.

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
- Registry cleanup keeps untagged uploads younger than six hours, so it cannot remove a
  per-architecture image before the release tags the merged index.

**Validation**

Checked with the workspace tests (365 passed), the UI state checks, a restart test for
session bindings, and local load tests on a 1 GiB budget. The arm64 binary was also checked
under emulation (health, SSE and WebSocket turns, management pages, TLS fingerprint). No real model provider or account
was used.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.1.2`, `0.1` and `latest`, for `linux/amd64` and
  `linux/arm64`.
  Change a pinned old Compose tag before pulling and recreating; pulling `latest` alone does
  not replace a running container.
- The config/data layout and SQLite schema are unchanged. The new `session_affinity.json`
  is created in the data directory on first use. Keep a data backup and the previous image
  locally: registry cleanup keeps only the newest two tag names, so `0.1.1-rc.2` may be deleted.
- The new `max_body_mb` default (64) applies only if the key is not set in `config.toml`.
- Keep the supplied 75-second stop timeout, so bindings and usage are flushed at shutdown.
- Download the .deb or static musl tarball and verify it against the accompanying SHA256SUMS.

[Installation][install] · [Recovery boundaries][recovery] · [Deployment and backup][deploy] · [Documentation][docs]

Licensed under the [MIT License][license].

[install]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.2/README.md#quick-start-linux-x86_64--arm64
[recovery]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.2/docs/RECOVERY.md
[deploy]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.2/docs/DEPLOYMENT.md
[docs]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.2/docs/README.md
[license]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.1.2/LICENSE
