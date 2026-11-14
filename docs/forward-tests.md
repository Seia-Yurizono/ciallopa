# Forward integration tests

`crates/server/tests/it/main.rs` remains the single server integration-test binary.
`forward.rs` declares topic modules in `forward/`: admission, cookies, gzip, images,
models, retry, shutdown, SSE, usage, and WS. Shared forwarding fixtures live
in `forward/scaffold.rs`. `it/support.rs` supplies the fixed store, recording router,
listener, default HTTP client and AppState factory; the cooldown and compaction tests
reuse these helpers too.

The mock selects protocol fixtures with `x-test-mode` or WS turn tags. Unused modes
and the script interpreter were removed during WP8. Seeded usage API checks live in
`it/usage_api.rs` without a mock upstream; live refusal/occupancy sampling tests live
with admission, and forwarded record tests live with usage. UI page contracts use
the in-process app in `it/http.rs`.

WP8 reduced forwarding tests from 153 to 106, and the full workspace from 597 to 536.
Decision/classification variants stay in the pure tables, with representative e2e
tests proving transport, persistence and lifecycle wiring. The complete per-test
coverage ledger, per-file counts and three-run full-suite measurements are in the
[WP8 report](test-reduction-20261114.md).

## Clock and synchronization

`UpstreamOpts::time_unit` is skipped by serde and defaults to exactly one second.
It cannot be set through TOML or the settings API and is omitted from serialized
configuration. `forward::timing` scales preparation/header/idle limits, retry waits,
request-body budget waits, cooldown hold limits and shutdown flush bounds. Default
production durations and upstream retry formulas are unchanged.

The shared test factory uses 100 ms per configured second. Fixture delays and elapsed
assertions use `units`/`millis`; readiness watchdogs retain generous physical deadlines.
Budget tests establish the account clients' real HTTP connections with an unrecorded
404 request before measuring their deliberately small preparation windows. Policy tests
keep their original physical two-second preparation budget while scaling retry delays.

Compaction overlap uses a two-party barrier with a watchdog. Two waits intentionally
use physical time: natural cookie expiry (2.1 seconds), and silence diagnostics (200–300 ms)
whose message rounds seconds to one decimal place. The process-level signal/shutdown
test remains unchanged. Recorder polling intervals remain physical readiness checks.

## Historical WP4 timing method and results

These measurements predate WP8 and refer to the test inventory at that stage. See
the WP8 report above for the current full-workspace measurements.

Build with `cargo test -p ciallopa --test it --locked --no-run`, then run the emitted
binary directly, excluding compilation. Measure complete runs with the default test
parallelism; obtain names with `--list` and run each separately with `--exact NAME`.
Sum process durations measured by Python's `time.perf_counter`, including startup.
This serial sum is distinct from parallel wall time.

The baseline binary is built at `3f9b6d6`. For the final comparison, alternate baseline
and modified full runs three times, then alternate their matching individual tests.
The forward-only measurement uses the `forward::` filter on both binaries. There are
221 baseline tests (154 forward) and 220 modified tests (153 forward).

Measured on 2026-11-14 with the default 24 test threads:

| Measurement | Baseline | Modified |
| --- | ---: | ---: |
| Complete `it` wall, run 1 | 12.667 s | 10.917 s |
| Complete `it` wall, run 2 | 12.735 s | 11.047 s |
| Complete `it` wall, run 3 | 12.798 s | 11.039 s |
| Forward-only parallel wall | 10.890 s | 6.900 s |
| Complete `it` individual-run sum | 91.666 s | 47.138 s |
| Forward individual-run sum | 69.022 s | 30.675 s |

Every individual run passed on both binaries. The third modified full run exposed the
known compressed-WS connection reset (kept for the phase-2 production fix) and an
existing reset-credit assertion that assumed concurrent requests arrived in account
order. That assertion now selects the recorded request by account ID. The timing
binary predates this assertion-only correction. The complete binary remains about
11 seconds; forwarding alone is 6.9 seconds, with its serial sum reduced by 56%.

Final validation after the account-order correction: formatting and workspace clippy
with `-D warnings` passed; two complete `cargo test --workspace --all-targets --locked`
runs passed, including all 220 `it` tests (10.74 s and 10.98 s).
