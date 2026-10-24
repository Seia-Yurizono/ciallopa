# Deployment and operations

The release image is multi-arch, **linux/amd64** and **linux/arm64**, and contains the same
static musl binary as the release tarball of that architecture. Docker picks the matching
image, so the Compose example runs unchanged on x86_64 and arm64 Linux hosts and under Docker
Desktop on Apple Silicon. Other architectures are not built. The current deployment unit is
one process with local state.

The arm64 binary is built with jemalloc's 64 KiB page-size setting (`--with-lg-page=16`, set by
`scripts/package.sh`), which is also jemalloc's own default for aarch64 cross builds. A binary
built for 4 KiB pages aborts on kernels with 16 KiB or 64 KiB pages; this one runs on 4 KiB
kernels (Docker Desktop, most clouds) as well as on 16 KiB / 64 KiB ones. In a local
comparison the idle and peak memory matched a 4 KiB build within noise; memory retained after
a load burst was about 5 MiB higher.

## Container contract

Use the [Compose example](../packaging/docker/compose.yaml) and the first-run commands in the
[README](../README.md#docker). Create `config/` and `data/` with mode `0700`, owned by the UID:GID
in `user:`, before running Compose. Missing bind directories now fail immediately. The default
image user is `65532:65532`; the Compose example overrides it so files remain owned by your
host account. Run it as a non-root account.

- Mount the **config directory**, not just `config.toml`: settings saves use atomic rename,
  with a backup and audit file beside the config.
- Keep data on a local filesystem. Accounts, usage SQLite, prices and learned client metadata
  live under `data/` with the default configuration. Include any custom paths in backups.
- Keep the root filesystem read-only, drop capabilities and retain `no-new-privileges`.
  The binary is PID 1 and receives SIGTERM directly; a shell wrapper or init process is not
  required for the current single-process image.
- Publish on host loopback and terminate TLS at the reverse proxy. A containerized reverse
  proxy can instead reach `ciallopa:8317` on a shared private network without publishing the
  gateway port. Set streaming response buffering and idle timeouts deliberately, and support
  WebSocket upgrades; measure first-token latency through the complete proxy chain.
- The bounded `json-file` log rotation in Compose covers container stdout/stderr. It does not
  rotate the config audit file or bound the usage database. Configure usage retention and
  monitor the filesystem containing both mounts.

Use exact release tags or a registry digest for controlled upgrades. The `v0.2.0`
release uses image tag `0.2.0`. Update an older Compose image tag before pulling and recreating.
The `latest` alias follows successful tagged builds, including prereleases.
Only stable releases move the minor-version alias; this release moves `0.2` to `0.2.0`.
Keep the previous image locally, save it as an offline archive, and retain its image
ID/digest plus a pre-upgrade data backup for rollback. A digest does not keep a deleted
registry version pullable, and an image rollback alone does not reverse a data migration.

Registry maintenance keeps the newest two tag names and their referenced manifests;
other versions are deleted regardless of age. See [container cleanup](CONTAINER_CLEANUP.md)
for the retention semantics, manual preview, and optional token setup.

## Health, shutdown and traffic

`ciallopa healthcheck` has a three-second whole-probe bound and queries local `/healthz`.
This is a **process liveness** check: it does not call an upstream, verify an account can serve,
or prove the usage writer can persist records. A healthy empty installation is intentional.
Each probe reads `config.toml` again and uses its `listen` address. An invalid file or a saved
`listen` change awaiting restart can therefore fail the probe while the running process still
serves its previous address. Check the settings page's reload/pending-restart status and probe
the running address before treating that health failure as a stopped process.
Docker marks an unhealthy container but `restart: unless-stopped` only restarts a stopped
process; it does not restart one solely for failing health checks.

On SIGTERM/SIGINT the listener stops accepting new connections and forwarding admission
closes. HTTP/SSE requests and active WebSocket turns have up to 60 seconds to finish; idle
WebSockets close normally. Remaining work is cancelled, with up to three seconds for closing,
five seconds for the final usage/fingerprint flush and two seconds for runtime shutdown.
A second signal skips the normal drain wait. This gives an application budget of about
70 seconds within the Compose/systemd 75-second stop timeout. It does not guarantee that a
long generation completes or every queued record reaches disk. A stalled blocking write or
external SIGKILL can still lose in-memory records even if SQLite itself recovers normally.

Before planned replacement, remove the instance from new traffic at the reverse proxy, allow
active requests to finish, then stop it. Increasing only the external stop timeout does not
extend the application's 60-second normal drain. Avoid treating a
global upstream outage as a reason to repeatedly restart otherwise responsive gateway instances.

The image smoke test exercises initialization, health, permissions, a settings write and an
idle SIGTERM stop. The separate [Compose smoke runner](../scripts/compose-smoke.py) exercises
the shipped Compose file, container recreation and restoration from an offline image/data
archive; its optional `--upgrade-from` path creates state on an already local old image before
switching to the candidate and records both image IDs/revisions. Two builds with the same
version label provide build-to-build evidence, not cross-version migration proof. Invocation
and fixture isolation are in [TESTING.md](TESTING.md). The Compose runner additionally holds
active SSE and WS streams through normal stop and second-signal Force, checks the final
database before restart, and reads the same records after restart. These finite fixtures do
not promise uninterrupted rolling upgrades or completion of arbitrary long generations.
Run the candidate through the commands in [TESTING.md](TESTING.md); historical smoke results do not validate a new build.

The local Caddy 2.11.7 reload experiment confirms its default reload closes an existing WS.
Codex 0.164 can recover on a new connection, but this issued another generation request both
before and after output. In the after-output fixture, app-server received identical text deltas
under two message IDs, while the final completed turn retained only the second item. The mock
always returns that text; this does not establish real-model repetition, duplicate billing,
permanent UI duplication or repeated tool execution. Related public reports and source checks
are separated in [the CLIProxyAPI study](research/CLIPROXY_RECOVERY.md).
Do not describe this as a cost-free or exactly-once hot reload. For planned changes, stop new
traffic and drain active work before replacing the gateway or reloading its proxy routes.
The follow-up [local recovery experiment](RECOVERY.md) compares the default with
finite `stream_close_delay`: finishing within the delay keeps the original generation;
exceeding it still closes and recovers. An idle old WS also keeps an admission slot until it
closes. New HTTP requests can see the reloaded configuration while that old WS remains attached
to the old handler. The option remains opt-in; choose its duration together with connection
capacity and the acceptable delay in retiring old routes. It does not protect process exits.

`max_inflight_requests` defaults to 64 and is hot-reloadable. A request holds its slot through
the HTTP response body or entire WebSocket connection, including idle time. At capacity it
receives 503 `gateway_busy` and `Retry-After: 1` before its body is read. There is no queue;
health, admin and usage queries remain outside this limit. `0` disables rejection while
continuing to count occupancy. This is a conservative guard, not a measured capacity or memory
guarantee: body sizes, stream buffers and idle connections still matter. Observe current
occupancy in the overview, then measure representative traffic before increasing it or
choosing a container memory limit. See [PERFORMANCE.md](PERFORMANCE.md).

## Behind Cloudflare Tunnel and Access

ciallopa serves plain HTTP and authenticates every API call with a client key in a header
(`Authorization: Bearer` or `X-Api-Key`). Cloudflare Access is an extra gate in front of it, not
a replacement: the management page still asks for an admin key, and `/usage/v1/*` and
`/admin/v1/*` still answer 401 without one. ciallopa does not read the Access JWT or cookie.

Recommended layout:

| Paths | Who calls them | Cloudflare Access |
|---|---|---|
| `/ui`, `/ui/*`, `/admin/v1/*`, `/usage/v1/*`, `/favicon.ico` | Browser (management page) | An Access application with an **Allow** policy for your identity |
| `/v1/*`, `/backend-api/*`, `/api/codex/*`, `/healthz` | Codex and other clients | Not behind interactive login: a separate hostname, or a more specific path application with **Bypass**, or **Service Auth** with a service token |

- Simplest: two hostnames on the same tunnel, e.g. `cpa-admin.example.com` behind Access and
  `cpa.example.com` for clients. With one hostname, add path applications for the client paths;
  [the more specific path wins](https://developers.cloudflare.com/cloudflare-one/access-controls/policies/app-paths/).
  [Bypass](https://developers.cloudflare.com/cloudflare-one/access-controls/policies/) leaves only
  the client key in front of them (Bypass is also not logged by Access).
- To keep Access on the client paths, use a
  [service token](https://developers.cloudflare.com/cloudflare-one/access-controls/service-credentials/service-tokens/)
  with a **Service Auth** policy (an Allow policy would send Codex to the login page) and have
  Codex send it next to the key:
  ```toml
  [model_providers.cpa.env_http_headers]
  "CF-Access-Client-Id" = "CF_ACCESS_CLIENT_ID"          # names of environment variables
  "CF-Access-Client-Secret" = "CF_ACCESS_CLIENT_SECRET"
  ```
  ([Codex `env_http_headers`](https://learn.chatgpt.com/docs/config-file/config-reference)). The
  variables can go into `$CODEX_HOME/.env` like `CPA_KEY`. Check with
  `curl -H "CF-Access-Client-Id: …" -H "CF-Access-Client-Secret: …" -H "Authorization: Bearer $CPA_KEY" https://cpa.example.com/v1/models`.
  ciallopa strips `cf-*`, `True-Client-IP`, cookies and forwarding headers before anything goes upstream.

Management page behaviour:

- The page calls the API on its own origin with `credentials: "same-origin"`, so the browser
  sends the `CF_Authorization` cookie to Access. Pages, scripts and styles are inline in one
  document served from `/ui*`; there are no other assets, no EventSource and no WebSocket in
  the page. No CORS is needed or offered.
- When the Access session expires, Access answers an API call with a redirect to the sign-in page.
  The page fetches with `redirect: "manual"` and shows 「Cloudflare Access 登录已过期，请刷新页面重新登录」
  (also for an HTML answer or an Access 401/403) instead of a network error, and keeps the key.
  Reloading the page goes through the Access login. A real connection failure still shows
  「网络错误，连不上 ciallopa（…）」.
- Writes need JSON and must come from the same origin: when the browser sends `Sec-Fetch-Site` it
  must be `same-origin`, otherwise `Origin` must name the `Host` (or `X-Forwarded-Host`). Cookies
  are ignored. cloudflared keeps the public `Host` by default; if you set `httpHostHeader` to
  something else, writes from a browser that sends no `Sec-Fetch-Site` are rejected with 403.
- The pages send a strict CSP and `Cache-Control: no-store, no-transform`. `no-transform`
  [stops Rocket Loader, Email Obfuscation and Web Analytics injection](https://developers.cloudflare.com/cache/concepts/cache-control/)
  on them, which the CSP would block anyway. If you see CSP errors for `cdn-cgi` scripts, also
  turn these off for the admin host with a
  [Configuration Rule](https://developers.cloudflare.com/rules/configuration-rules/settings/)
  (Rocket Loader off, Email Obfuscation off, Disable RUM).
- ciallopa sets no cookies. Turn on **Always Use HTTPS** and, if you want, HSTS at Cloudflare;
  the origin behind the tunnel stays plain HTTP on loopback.

Streaming through Tunnel:

- cloudflared streams a response only when it is `Content-Type: text/event-stream`
  ([Tunnel troubleshooting](https://developers.cloudflare.com/tunnel/troubleshooting/)); ciallopa
  always labels SSE that way. WebSockets (`supports_websockets = true`) pass through Tunnel and
  Access unchanged.
- Cloudflare closes a request that sends no response bytes for
  [125 s (error 524)](https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-5xx-errors/error-524/).
  This applies between reads, so it hits a stream only if it stays silent that long; a slow
  non-streaming call such as a large `/v1/responses/compact` is the likely case. Codex's `remote_compaction = "v2"` streams, so prefer it.

Not needed for ciallopa: client-IP headers and trusted-proxy settings (ciallopa makes no decision
on the client address, so `CF-Connecting-IP` / `X-Forwarded-For` are neither trusted nor used),
a public-URL setting (no absolute URLs; account login callbacks go to the issuer or to Codex's
local callback, not to this host), and Access JWT validation (the key is the authentication).

How other self-hosted apps handle the same points, for comparison:

- Forwarded client addresses are trusted only after an explicit opt-in:
  [Home Assistant](https://www.home-assistant.io/integrations/http/) (Trust X-Forwarded-For + trusted proxies),
  [Nextcloud](https://docs.nextcloud.com/server/latest/admin_manual/configuration_server/reverse_proxy_configuration.html) (`trusted_proxies`),
  [Vaultwarden](https://github.com/dani-garcia/vaultwarden/blob/main/.env.template) (`IP_HEADER` + `IP_HEADER_TRUSTED_PROXIES`),
  [n8n](https://docs.n8n.io/hosting/configuration/environment-variables/deployment/) (`N8N_PROXY_HOPS`, default 0),
  [Uptime Kuma](https://github.com/louislam/uptime-kuma/wiki/Reverse-Proxy) (Trust Proxy). ciallopa keeps the default of not trusting them.
- Public URL / scheme comes from configuration when the app builds absolute links:
  Nextcloud `overwriteprotocol`, [Gitea `ROOT_URL`](https://docs.gitea.com/administration/reverse-proxies),
  Vaultwarden `DOMAIN`, n8n `N8N_EDITOR_BASE_URL`. ciallopa builds none.
- Origin checks behind proxies: [Portainer](https://docs.portainer.io/advanced/cli) added
  `--trusted-origins` after "Origin invalid" errors behind proxies
  ([#12748](https://github.com/portainer/portainer/issues/12748)); ciallopa compares `Origin` with
  `Host` / `X-Forwarded-Host` and prefers `Sec-Fetch-Site`, so no extra setting is needed for
  cloudflared.
- Identity from Access: [Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/jwt/)
  can log users in from `Cf-Access-Jwt-Assertion`, the header Cloudflare
  [recommends validating](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/validating-json/)
  instead of the cookie. ciallopa does not, by design.
- Cross-origin API calls to an Access-protected host need CORS settings at Access
  ([Access CORS](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/cors/));
  ciallopa's page only calls its own origin, so none are needed.

## Backup and restore

`config/` and `data/` form one recovery set. They contain API keys and account credentials;
store archives privately and encrypt off-host copies. Back up custom configured paths as well.
For the default layout, the simplest consistent backup is a brief maintenance stop after
traffic has drained. Save the running container's exact image before pulling/replacing it;
the registry's two-tag policy may already have removed its old tag or digest. Run these
commands as the owner of the bind directories, beside the Compose file:

```sh
umask 077
backup_dir="backups/ciallopa-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
container_id=$(docker compose ps -aq ciallopa)
test -n "$container_id" || exit 1
image_id=$(docker inspect --format '{{.Image}}' "$container_id")
printf '%s\n' "$image_id" > "$backup_dir/image-id"
docker image inspect "$image_id" > "$backup_dir/image.json"
docker image save -o "$backup_dir/image.tar" "$image_id"
cp compose.yaml "$backup_dir/compose.yaml"
docker compose stop ciallopa
# A forced kill (commonly exit 137) is not evidence of a completed drain and final flush.
test "$(docker inspect --format '{{.State.ExitCode}}' "$container_id")" = 0 || exit 1
tar -czpf "$backup_dir/state.tar.gz" config data
docker compose start ciallopa
```

The saved image metadata contains the version/revision labels and known registry digests.
Include custom Compose overrides and environment files if they affect the installation.
Keep the local old image and this backup until the upgraded instance has been checked;
do not run image pruning as part of that upgrade.

Do not copy a live `usage.db` by itself: SQLite WAL mode may keep committed transactions in
`usage.db-wal`. If downtime is unacceptable, use SQLite's online backup API or the SQLite CLI
`.backup` command from the host to take a consistent database snapshot. That snapshot does
not include queued usage records or make concurrent config/auth-file changes atomic; coordinate
those writes if the backup must represent one point in time. Do not delete or separate live
WAL/SHM files, and do not put a shared WAL database on NFS.

Rehearse restoration into empty directories while the service is stopped. Load the archived
image without relying on GHCR and give its recorded image ID a local recovery tag:

```sh
backup_dir=/absolute/path/to/your/backup
docker image load -i "$backup_dir/image.tar"
docker image tag "$(cat "$backup_dir/image-id")" ciallopa:restore
```

Copy the saved Compose file into the recovery directory, extract `state.tar.gz` there, and
restore ownership to its configured UID:GID and private permissions. Change its `image:` to
`ciallopa:restore`, then run `docker compose up -d --pull never --no-build ciallopa`.
Check health, accounts, settings and usage history before returning traffic. The restored data
must match the archived image; loading an older image does not downgrade a migrated database.
Keep the original directories until the restored instance has been checked. Do not restore
over a running instance or run two instances against the same bind directories.

## Build provenance and base updates

Only a tag push publishes images and a GitHub Release. A manual `workflow_dispatch` run remains
a dry run even when a tag is selected: it builds, smoke-tests and uploads workflow artifacts,
without logging in to GHCR, uploading registry images or changing release tags.

The [Dockerfile](../packaging/docker/Dockerfile) pins both BusyBox (directory preparation only)
and the Distroless `static-debian13:nonroot` runtime by manifest digest. Debian 13 follows the
current Distroless support baseline; retain digest updates and smoke tests when refreshing it.
Each architecture is compiled, packaged and smoke-tested (.deb under systemd, image,
Compose) on a native GitHub runner (`ubuntu-24.04` and `ubuntu-24.04-arm`); no emulation is
involved. The release workflow builds the image from the packaged
binary, smoke-tests locally, then rebuilds those same inputs for a push by digest. Both builds
use the source commit timestamp as `SOURCE_DATE_EPOCH`. Before attaching release tags, each
architecture's job resolves its single `linux/<arch>` manifest from the uploaded registry index
and compares its config digest with the smoke build metadata and the loaded smoke image. One
publish job then merges both uploaded indexes (runnable images and their attestations) into the
tagged multi-arch index with `docker buildx imagetools create` and checks that every tag holds
exactly the verified image for each platform. Missing or ambiguous
runtime manifests and mismatched digests fail the release. Attested exports need not include a
config digest in Buildx's output metadata. The config digest covers runtime settings and
uncompressed filesystem layer identities. Attestations change the registry index digest, so
that top-level digest is not compared with the local image ID. This fixes base-image drift and
checks the runnable image, without claiming bit-for-bit reproducibility across toolchains or
builder versions.

Published images include BuildKit minimal provenance and an SBOM. The local Docker smoke image
omits attestations for compatibility with Docker's classic image store. Image attestations
describe the image build; they do not attest the earlier Rust compilation or guarantee a full
inventory of dependencies statically linked into the executable. Use `Cargo.lock` and separate
dependency analysis for that part of the supply chain.

Digest pins need maintenance to receive base security fixes. Review upstream changes, inspect
the new index digests with `docker buildx imagetools inspect`, update both relevant `FROM` lines,
and run the manual release workflow before a release. Retain readable tags beside the pins.
Source-to-binary attestations and vulnerability checks can be added as release needs justify
them.

## Scaling boundaries

Connection pools, account cooldowns and session affinity are process-local. Usage is written by
one bounded background writer to local SQLite; account refresh and config changes are local as
well. Shared volumes are not a coordination mechanism. Multiple replicas need an explicit
ownership model for accounts/refresh, affinity and configuration, separate instance IDs, and
usage aggregation. Establish these semantics before adding a scheduler or Kubernetes manifests.

Size CPU, memory, file descriptors and connection limits from measured concurrent streams and
slow clients, not requests per second alone. Observe time to first token, completed-turn latency,
active HTTP/SSE/WS work, upstream errors, retries and recording-queue losses. Keep `/healthz`
cheap; use operational metrics and bounded diagnostics for these richer signals.

References: [Docker build practices](https://docs.docker.com/build/building/best-practices/),
[Compose service reference](https://docs.docker.com/reference/compose-file/services/),
[Build attestations](https://docs.docker.com/build/metadata/attestations/),
[Build metadata](https://docs.docker.com/reference/cli/docker/buildx/build/#metadata-file),
[Image exporters](https://docs.docker.com/build/exporters/image-registry/),
[SQLite online backup](https://sqlite.org/backup.html), [SQLite WAL](https://sqlite.org/wal.html).
