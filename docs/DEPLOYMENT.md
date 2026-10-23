# Deployment and operations

The supported release image is **linux/amd64** and contains the same static musl binary as
the release tarball. The base image supports other architectures; the packaged binary does
not. Do not publish an ARM manifest until an ARM binary has been built and smoke-tested on
that platform. The current deployment unit is one process with local state.

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

Use exact release tags or a registry digest for controlled upgrades. The `v0.1.1-rc.2`
pre-release uses image tag `0.1.1-rc.2`. Update an older Compose image tag before pulling and recreating.
The `latest` alias follows successful tagged builds, including prereleases.
Only stable releases move the minor-version alias; this prerelease leaves `0.1` unchanged.
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
The release workflow builds from the packaged
binary, smoke-tests locally, then rebuilds those same inputs for a push by digest. Both builds
use the source commit timestamp as `SOURCE_DATE_EPOCH`. Before attaching release tags, the
workflow resolves the single `linux/amd64` manifest from the uploaded registry index and compares
its config digest with the smoke build metadata and the loaded smoke image. Missing or ambiguous
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
Native ARM packaging, source-to-binary attestations and vulnerability checks can be added as
release needs justify them; adding a multi-platform label alone provides none of those guarantees.

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
