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

## Before merging

The shared [checks workflow](../.github/workflows/checks.yml) runs on pull requests and
pushes to `main`/`master`. The [release workflow](../.github/workflows/release.yml) calls
the same checks before packaging, including checking a version tag against the crate.
There is one Ubuntu job with the pinned Rust toolchain and Cargo cache:

```sh
cargo fmt --all --check
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

[ACCEPTANCE.md](ACCEPTANCE.md) records historical real-CLI/local-mock coverage and known
gaps; its PASS entries are not results for the current checkout. `scripts/e2e/` contains
manual helpers for that prepared setup, not a standalone test runner. They assume
`/tmp/cpa-e2e`, pre-created accounts and CLI homes. In particular, `lib.sh` restarts with
`pkill -x ciallopa`, and `run.sh` filters CLI output without reliably preserving its exit
status. Use them only in a disposable environment with no unrelated ciallopa process,
and inspect the client result and mock log. They are not part of ordinary CI. For the
small repeatable real-client check, use the isolated runner below.

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

Only the child environment receives the official `CODEX_HOME` setting, pointing at a
fresh client directory; the script does not repurpose `HOME` or change the parent
environment. File credential storage plus a custom provider using only the fake named
environment key avoids loading cached OpenAI authentication. The child inherits only
PATH/locale/terminal settings, uses a fresh working directory and read-only, ephemeral
`codex exec`. Analytics and web search are disabled. This is configuration isolation,
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
`/etc/hosts`. As of the 2026-10-23 review, lock comparison against official
`rust-v0.163.0` reports the existing `quinn-proto` 0.11.15/0.11.18 mismatch, while the
default enabled dependency graph does not include it. Keep that discrepancy visible as
a separate dependency maintenance item; do not expand the allowlist to conceal it or
claim the lock comparison passed. Unrelated application edits do not require repeating
wire fingerprint checks.

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

The settings script covers draft preservation across reloads and key-management refreshes,
revision-conflict previews against the current server values, a successful save followed by
a failed refresh, edits made while a request is in flight, and logout clearing a newly generated
key. Drafts belong to the open settings page, not to an API-key identity: switching keys keeps
typed work, clears the one-time secret display, and checks access on the server again.
Its DOM stub does not establish layout or keyboard behavior. For settings UI
changes, also check that an invalid numeric input survives a refresh, a 412 leaves the draft
available for another review, and explicitly discarding loads the current configuration.

The accounts script exercises independent quota/card snapshots, partial failure and recovery,
unavailable/permission responses, and reads that arrive after another load or login. Its browser
check uses temporary response interception: a failed quota read must retain the previous values,
show their age, and recover without reloading the page. The overview script also checks that a
late 401/403 from a previous key cannot clear the current login or change its admin state.
Device-code checks cover stopped starts/polls, a new flow after cancellation, logout clearing
codes and pasted credentials, and distinguishing disabled writes from a non-admin identity.
The browser covers the add-panel focus/empty state and restarting after a visible failure;
stopping the page's wait does not cancel authorization already running on the server.

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
