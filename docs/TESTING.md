# Development checks

Choose checks by the behavior being changed. Keep the edit/run loop small, then run the
shared checks once when the change is ready. A passing check only needs repeating after a
relevant edit, failure or new concern. Automated tests use synthetic data and local mocks.

## While editing

```sh
# A changed crate; substitute ciallopa-routing, ciallopa-forward, etc.
cargo test --locked -p ciallopa-usage

# A forwarding behavior; omit the final filter to run the integration file.
cargo test --locked -p ciallopa --test forward sse_disconnect

# Embedded UI query and asynchronous state behavior; no npm install needed.
node scripts/ui-state-check.cjs

# Settings drafts, reloads and revision conflicts.
node scripts/settings-state-check.cjs

# Account snapshots, partial failures and late reads.
node scripts/accounts-state-check.cjs
```

For a public `core` contract, include its consumers. For a fix, start with a regression
that exercises the failing behavior, then run the affected crate or integration target.
Documentation-only edits need link/content review, not a local Rust rebuild.

### Faults we can reproduce locally

The forwarding integration tests start their own loopback upstream. The CLI and deployment
runners use synthetic credentials and controlled fixtures; none needs a provider account.
Choose the layer whose behavior is in question rather than running every scenario on each edit.

| Fault or boundary | Reproduction | What it establishes |
| --- | --- | --- |
| 429/503, Retry-After, exhausted retries, delayed WS handshake | `cli-smoke.py --suite recovery` | Actual Codex attempts, gateway records, elapsed recovery and WS-to-SSE fallback. |
| Disconnect after a completed tool result reaches the following generation | `cli-smoke.py --suite tool-recovery` | Actual local callback count, original call/result in recovery input and client/gateway outcomes, on SSE and WS. |
| Pending tool disconnect/cancel and client crash before tool-result acknowledgement | `cli-smoke.py --suite tool-faults` | Local execution count versus persisted history and resumed request input; distinguishes an unexecuted tool from an executed but unacknowledged side effect. |
| SSE EOF, missing terminal event, silence, client cancellation | `crates/server/tests/forward.rs`; for example `cargo test --locked -p ciallopa --test forward idle_timeout_aborts_stream` | Forwarded result, failure classification and released request occupancy. |
| WS abrupt upstream close, preparation cancellation and blocked writes | Forwarding integration tests and focused socket probes described in [PERFORMANCE.md](PERFORMANCE.md) | Close/error behavior and bounded cancellation at the tested boundary; not cancellation of every possible blocked operation. |
| Proxy reload during an active WS | `caddy-smoke.py` | Real Codex recovery through a real Caddy process with a local upstream. |
| Process drain and forced shutdown with active SSE/WS | `compose-smoke.py` | Owned container signals, client results, final SQLite records and restart visibility. |

The runnable CLI commands below require already available binaries. Integration tests remain
the ordinary CI gate; real-client and deployment probes are opt-in compatibility checks.
Fixtures capture full synthetic requests only inside their private evidence directories. That
test capture is not a recommendation to log production prompts or tool results.

## Before merging

The shared [checks workflow](../.github/workflows/checks.yml) runs on pull requests and
pushes to `main`/`master`. The [release workflow](../.github/workflows/release.yml) calls
the same checks before packaging, including checking a version tag against the crate.
There is one Ubuntu job with the pinned Rust toolchain and Cargo cache:

```sh
cargo fmt --all --check
python3 scripts/ghcr-cleanup-test.py
node scripts/ui-state-check.cjs
node scripts/settings-state-check.cjs
node scripts/accounts-state-check.cjs
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test --locked -p ciallopa --example loadbench
```

Clippy compiles all targets, so a separate full debug build adds little here. The explicit
example test command runs loadbench's SSE parser and measurement regressions. These checks
do not install a browser, run capacity benchmarks or build release packages.

## Checks triggered by the change

| Change | Additional evidence |
| --- | --- |
| UI queries, interactions or layout | Run the UI state script and a local browser smoke using synthetic data. Check the affected flow at desktop and narrow widths, plus empty/error states when relevant. |
| Forwarding, retries or stream lifecycle | Local mock integration tests for the changed behavior. Use the real CLI against a local mock when client interpretation or compatibility is in question. |
| Usage queries, recording or admission | Run the relevant benchmark only if the change could affect cost or resource use; compare against the same workload and machine. |
| Upstream transport dependencies or client construction | Run lock comparison and wire fingerprint checks; see below. |
| Packaging or a release | Use the release workflow's manual dry run for deb/systemd and Docker smoke. A version tag also publishes the artifacts. |

For the shipped Compose configuration and recovery path, use an already built local image
on a local Linux Docker host (Python 3.12+, Compose v2, non-root user with Docker access):

```sh
python3 scripts/compose-smoke.py ciallopa:smoke
# Optional: both images must already be local; the runner does not pull or retag them.
python3 scripts/compose-smoke.py ciallopa:smoke --upgrade-from ghcr.io/seia-yurizono/ciallopa:0.1.0
```

The manual release dry run accepts the same optional check as a tag of this repository's
package: `gh workflow run release.yml --ref main -f upgrade_from=0.1.0`. The workflow pulls
that existing tag before invoking the runner. An empty input checks candidate recreation;
manual runs never publish an image or a release.

The runner copies the real Compose file, changing only its image, user and published port.
It checks missing bind directories, initialization, private ownership, a settings write and
one forwarded SSE request with a fake account. It then recreates the container, stops it for
an image/config/data archive, and restores into empty bind directories using `docker image
load` and the saved image ID, without pulling. Its local HTTP proxy serves only the fixture
and rejects CONNECT/other destinations; periodic reset-credit reads are disabled and the
price source is empty. It removes only its own random Compose project and retains the printed
private evidence directory. With `--upgrade-from`, init, settings and the fixture request run
on the old image before recreation switches to the candidate; the evidence records both exact
image IDs and version/revision labels, and recovery uses the candidate image ID. Replacing two
builds labelled `0.1.0` checks persistent-state compatibility between those builds, not a
cross-version data migration. The candidate then runs active SSE+WS normal drain and Force
scenarios through the real Compose stop path. The mock holds both streams behind a barrier;
the runner waits for the drain log before releasing completion or sending the second signal.
With a 60-second batch interval, it checks the stopped database before restarting and reading
the same rows through the API. It also checks close codes, upstream cancellation, bounded exit
and zero occupancy. This is a deployment check, not a per-PR Rust test.
The existing Docker smoke still covers first-run errors and refusing to
overwrite a config; the two runners exercise different user flows.

### Optional real Codex CLI smoke

Use an already installed Codex CLI; the script installs nothing and does not call a real
provider. Build the local executables once, after other Rust edits/builds have finished:

```sh
cargo build --locked -p ciallopa --bin ciallopa --example mock_upstream
PYTHONDONTWRITEBYTECODE=1 python3 scripts/cli-smoke.py \
  --codex-bin /path/to/codex \
  --binary target/debug/ciallopa \
  --mock target/debug/examples/mock_upstream
```

This runs four cases: SSE and WS, each with a normal answer and a quota error after
`response.created` but before output, followed by a transparent replay on another account.
Every case uses a fresh temporary directory, loopback ports, synthetic accounts and a
fake client key. WS prewarm is allowed to complete in the quota case so the assertion
checks the **business turn's** replay, not a switch that already happened during prewarm.
The mock's `midlimit_business_only` marker affects only this opt-in fixture; older mock
scenarios retain their behavior.

The runner checks CLI exit status and JSON events, the mock's actual protocol/account
chain (including non-prewarm WS frames), and one successful business usage record with
the expected original/final accounts and switch count. Silent WS-to-SSE fallback fails.
It polls readiness and recorder completion with bounds, retains stdout/stderr and JSON
evidence on failure, and stops only subprocess handles it created. All cases must pass;
exit 0 is a functional result, not a performance claim. Its manifest records CLI version,
executable hashes and source-tree state separately; concurrent binary replacement fails
the run. It is not a CI dependency or a replacement for the forwarding integration tests.

Use `--suite recovery` for client/gateway retry boundaries, `--suite conversation` for the
app-server tool/multi-turn/cancellation checks, `--suite tool-recovery` for disconnects after
tool-result submission, `--suite tool-faults` for pending/unacknowledged tools and crash/resume,
or `--suite all` for the combined opt-in run.
`--case NAME` selects a case within that suite. Recovery cases record actual upstream attempts
and elapsed time, including terminal failures; passing means the expected behavior was
observed, not that every CLI exited successfully. Conversation tests answer a synthetic dynamic
tool through app-server JSON-RPC and do not execute a model-generated shell command.
Native WS steering is a separate case with a temporary model catalog advertising
`use_responses_lite=true`; ordinary `turn/interrupt` may close the socket instead. The report
must preserve that capability condition rather than generalize it to all Codex installations.

The four tool-recovery cases cover SSE/WS before and after follow-up output. A dynamic-tool
callback updates a local counter and returns its execution ordinal. Only after the following
model request contains that result does the driver disconnect the held mock response. It checks
the callback count, call ID, recovered history, final turn, request records and released slots.
This tests an already submitted result, not exactly-once execution across every possible failure.
For example, run one case with `--suite tool-recovery --case ws-tool-recovery-after-output`.
Evidence and limits are in [RECOVERY.md](RECOVERY.md).

The tool-faults suite keeps the callback pending or runs only a fixed local counter worker.
Crash cases kill and restart the owned app-server, resume its persistent thread, and compare
the rollout history with the next actual upstream request. A synthesized `aborted` result is
not proof that the tool had no effect. No model-generated shell command is executed.

The optional Caddy reload check uses already available binaries and the same local fixtures:

```sh
python3 scripts/caddy-smoke.py --caddy-bin /path/to/caddy --codex-bin /path/to/codex \
  --binary target/debug/ciallopa --mock target/debug/examples/mock_upstream
```

It checks reload before and after actual client output, each with default closure, completion
within a finite `stream_close_delay`, and expiry of that delay. A revision endpoint proves the
new configuration serves new HTTP requests. The runner records close/recovery, distinct response
IDs, final usage and the idle old socket's retained admission slot until delay expiry. Use
`--case after_output-finish-within-grace` to select one case. It is a local HTTP/WS check, not TLS or a
promise that recovery avoids a second generation. Neither runner installs clients, uses real
provider credentials, or becomes a default CI dependency.

Only the child environment receives the official `CODEX_HOME` setting, pointing at a
fresh client directory; the script does not repurpose `HOME` or change the parent
environment. File credential storage plus a custom provider using only the fake named
environment key avoids loading cached OpenAI authentication. The child inherits only
PATH/locale/terminal settings, uses a fresh working directory and read-only, ephemeral
`codex exec` or app-server. Analytics, feedback, web search and plugins are disabled. This is configuration isolation,
not an OS sandbox against machine-managed Codex policy; such policy may prevent the test
from starting and should not be bypassed. Codex 0.163.0 may warn that PATH helper aliases
cannot be created under `/tmp`; these text-only cases do not execute tools or need them.

Official references: [configuration/state locations](https://learn.chatgpt.com/docs/config-file/config-advanced),
[credential storage and custom-provider auth](https://learn.chatgpt.com/docs/auth), and
[non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode).

### Transport compatibility

For transport changes:

```sh
CODEX_LOCK=/path/to/codex-rs/Cargo.lock scripts/lock-check.sh
scripts/fp-check.sh
```

`fp-check.sh` needs its documented reference tools and permission to temporarily modify
`/etc/hosts`. It requires both HTTP and WS captures, starts one owned capture server, and
removes only its uniquely marked hosts entry on exit. Existing `chatgpt.com` mappings are
refused; use a prepared isolated environment. Unrelated hosts edits are retained, although
the file lock only coordinates writers that also use it. Evidence stays in the printed
private temporary directory. Do not run overlapping capture servers on ports 443/9998.

When changing the helper, `python3 scripts/fp-check-test.py` checks the actual capture
judgement, hosts edits on a temporary fixture, and cleanup of an owned fake server while
another process stays alive. It uses no sudo, live hosts file, network or Rust build.
This is an opt-in tool regression, not a replacement for a real wire capture.

The transport dependency reference remains fixed separately from the client probe version.
Check the actual enabled package/feature graph; do not widen an allowlist to conceal drift.
Unrelated application edits do not require repeating wire fingerprint checks.

## Useful regressions

Prefer a small fixture with independently chosen expected results. Exercise public
behavior through the actual implementation; do not reproduce its formula, query or
state machine in the test to calculate the answer.

- Forwarding: a silent upstream, a bounded failed response, retry budget exhaustion,
  legitimate long generation, and downstream cancellation. Assert whether another
  attempt starts and what the client receives, rather than every internal transition.
- Usage: a mixed fixture with success, HTTP 200 stream failure, cancellation, incomplete
  output, prewarm and missing measurements. Preserve the documented distinction between
  unknown values and zero; verify weighted totals rather than averages of row averages.
- Queries: compare selected IDs, totals and the requested time window. Include multiple
  records in one millisecond and a record arriving between pages. A fixed timestamp
  cutoff with offset paging alone does not freeze late-finishing requests.
- UI: exercise the actual query/loading functions with controlled deferred responses.
  Cover changed filters, pagination and stale replies; use the browser for interaction
  and rendering that a minimal DOM stub cannot establish.

The Node scripts exercise real page functions with controlled asynchronous responses. Cover
state races in those scripts, then check the affected interaction in a browser. DOM stubs do
not establish layout, focus behavior or complete accessibility. Use the synthetic UI example:

```sh
cargo run --locked -p ciallopa --example ui_demo -- 127.0.0.1:8399
```

The demo creates a unique temporary directory by default and prints its location. An optional
second argument selects a directory that must not already exist. It uses generated local data;
do not use production credentials or accounts. Stop only the process you created.

Use a channel or notification to establish that a mock has reached the intended phase.
Use a generous timeout as a watchdog, not a tight wall-clock speed assertion. Wait for
the result or use the recorder's flush barrier instead of a fixed sleep. Keep one deadline
around polling; nested retry loops can turn a simple regression into a long hang. Existing
tests need not be rewritten wholesale when only one helper or behavior changes.

## Performance observations

`crates/server/examples/loadbench.rs` measures complete SSE requests against a synthetic
upstream. It is a fixed-concurrency, closed-loop tool: slower responses reduce offered
load. Its latency percentiles include successful requests only, so always report errors
and success rate alongside them. For an admission change, also hold requests open and
offer more work to verify timely rejection and control-plane availability; closed-loop
throughput alone cannot establish overload behavior.

`crates/usage/examples/usage_bench.rs` generates records and measures recording, querying
and retention. Use a disposable generated database: opening it starts a writer and may
perform retention maintenance. Neither benchmark should use real account traffic or a
live usage database.

Save commit, machine, build profile, workload, concurrency, usage on/off and repeat count
with results. Compare direct mock access with proxy access when measuring gateway overhead.
Establish a baseline before choosing latency or memory thresholds; one machine's RPS is
not a project-wide capacity promise.
