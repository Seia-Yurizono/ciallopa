# Forward integration tests

`crates/server/tests/it/main.rs` remains the single server integration-test binary.
`forward.rs` declares topic modules in `forward/`: admission, cookies, gzip, images,
models, retry, shutdown, SSE, usage, usage API, and WS. Shared forwarding fixtures live
in `forward/scaffold.rs`. `it/support.rs` supplies the fixed store, recording router,
listener, default HTTP client and AppState factory; the cooldown and compaction tests
reuse these helpers too.

The mock accepts per-account `Script`s of `Reply::Status` or `Reply::Sse`, with data,
wait, stall and cut steps. Retry and basic SSE fault tests declare their scripts next
to the assertions. Specialized legacy modes remain for the larger protocol fixtures.
Only `sse_overload_after_output_is_forwarded` was removed: the retained
`sse_overload_after_output_is_recorded_as_failure` checks the same body and attempt
count, plus the usage record and router outcome. Other proposed clusters retain their
coverage until their pure policy replacements exist.

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
use physical time: natural cookie expiry (2.1 seconds), and silence diagnostics (300 ms)
whose message rounds seconds to one decimal place. The process-level signal/shutdown
test remains unchanged. Recorder polling intervals remain physical readiness checks.

## Timing method

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
