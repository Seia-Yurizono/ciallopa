# WP8 test reduction — 2026-11-14 integration base

Branch: `wp/slim-tests-20261114`. Base: `ce28b23`. Validation completed 2026-11-15 (Asia/Singapore). Test changes: `302564f`.

597 → 536 runnable Rust tests (−61), below the verified `origin/main` count of 566. Test code: 28,336 → 26,426 physical lines (−1,910, 6.7%).
Server integration tests: 220 → 167. Forwarding modules: 153 → 106 tests and 8,168 → 6,623 lines (−18.9%); three forwarding/sampling tests moved into their relevant modules, while the seeded API test moved out of forwarding.

Production behavior, dependencies, pinned network parameters and node scripts are unchanged. The only production-path files edited are `crates/forward/src/codes.rs` and `crates/server/src/ui.rs`, solely to remove their `#[cfg(test)]` source scans. Their production portions compare byte-for-byte with the base.

## Counts and test code by crate

| Crate | Tests before | Tests after | Test lines before | Test lines after |
| --- | ---: | ---: | ---: | ---: |
| auth | 26 | 25 | 1,050 | 1,034 |
| core | 34 | 34 | 649 | 649 |
| forward | 100 | 99 | 3,970 | 3,877 |
| routing | 53 | 52 | 2,113 | 2,108 |
| server | 278 | 222 | 15,952 | 14,266 |
| upstream | 20 | 20 | 509 | 509 |
| usage | 86 | 84 | 4,093 | 3,983 |

Full per-file inventory, including unchanged files and scaffolds: [test-reduction-20261114-files.csv](test-reduction-20261114-files.csv).
The line metric includes comments, blank lines and helpers. For files mixing production and tests, only `#[cfg(test)]` items count. Dedicated test files count in full. Counts include example targets; they reconcile with all successful cargo summaries.

## Full-suite wall time

Both sides use `cargo test --workspace --all-targets --locked --offline` with the default libtest parallelism, the same host/toolchain, and prebuilt test binaries. Wall time is `time.monotonic()` around the complete cargo command, including startup and all target binaries. Initial compilation is excluded; no nextest substitution or test-filter change is involved.

| Run | Base seconds | Reduced seconds |
| --- | ---: | ---: |
| 1 | 15.686 | 12.352 |
| 2 | 15.526 | 12.230 |
| 3 | 15.603 | 12.224 |
| Median | 15.603 | 12.230 |

Median wall time decreased by 21.6%. All six measured runs passed. Exact measurements: [test-reduction-20261114.json](test-reduction-20261114.json).

The default filesystem sandbox denied loopback socket creation (`Operation not permitted`), so the first sandbox attempt was unusable as a timing baseline. A tool permission request was approved and the baseline/final measurements ran with loopback access. Cargo dependencies were already cached; all builds ran offline. No external service was used.

## Coverage audit and retained feature contracts

Every baseline server/auth integration test (220 + 15 = 235) has an explicit retain/delete/merge decision and replacement in [test-reduction-20261114-coverage.csv](test-reduction-20261114-coverage.csv). Every removed or renamed test is mapped below; moves and reduced input matrices are included.

| User-visible family | Representative surviving integration coverage |
| --- | --- |
| HTTP 400/401/402/403/404/429/5xx and upstream passthrough | `account_error_families_fail_over`, `encrypted_rejections_preserve_transport_contracts`, `upstream_5xx_passed_through_with_provenance`, `forward_policy::d1_http_returns_ranked_error_ws_returns_last_error` |
| Egress/network/timeout and stream/decode/truncation/encoding errors | `cloudflare_block_is_egress_not_auth`, `websocket_handshake_cloudflare_block_is_egress`, HTTP/WS preparation tests, `stream_read_error_is_network_not_truncated`, gzip error tests |
| Retry, replay, failover and turn state | `retried_success_keeps_attempt_chain`, `usage_does_not_recount_retries_after_sse_replay`, `sse_empty_added_before_overload_switches`, `ws_overload_switch_records_attempt_chain`, `turn_state_not_carried_across_accounts`, `forward_policy` D1–D7 tests |
| WS turn lifecycle, incremental replay, prewarm and close | `usage_ws_turn_rows`, `ws_downstream_observation_per_turn`, `ws_client_close_records_upstream_silent_duration`, `ws_model_cooldown` tests |
| Shutdown, drain, admission and resource lifetimes | process `shutdown` test, all `forward::shutdown` tests, `idle_websockets_hold_no_inflight_slot_only_running_turns_do`, HTTP/WS/body-budget admission tests, cancellation records |
| Usage records, API, diagnostics and sampling | `usage_sse_rows`, `usage_ws_turn_rows`, `usage_error_detail_capture_and_api`, `usage_captures_response_model_effort_and_tier`, `seeded_records_summary_and_observation_contracts`, refusal/occupancy API tests |
| Settings writes/reloads, accounts, audit and prices | `write_api_switch_etag_comments_keys_and_audit`, reload/watch tests, `account_management_and_removal_cleanup`, price API tests, fingerprint test |
| Reset credits and auto reset | consume/cache/cooldown/audit, idempotent retry, pending spend lock, quota refresh/switch tests; `due_card_is_used_once_and_skips_disabled`, `pending_is_retried_with_the_same_id_after_restart`, relist/lock race tests |
| UI pages | `http::ui_pages_and_toggle` exercises all three served pages, public access, real CSP hashes, redirect and disabled routes; unchanged node checks exercise UI state |
| Images | all seven `forward::images` tests retain wrapper/direct, streaming, body lifetime, errors, incomplete results and truncation behavior |
| OAuth/auth storage | 14 mock OAuth tests retain all login modes, refresh wire/reason/persistence, single-flight, cross-process locks, disk rotation and relogin behavior |

Pure coverage remains in `attempt::policy`, HTTP prelude/finish tables, `classify`, `Tracker` drop tables, direct-stream gzip decoder tests, routing cooldown/session tables, usage outcome/migration fixtures, settings and reset/scheduler tables. Missing-vs-zero per-key usage samples and omission of empty hourly buckets were folded into the existing usage outcome matrix. Its fixture clock is now aligned to an hour to avoid a boundary-sensitive bucket count.

## Deleted, merged, moved, or reduced tests

Names below are source test names; replacements refer to their final names, including transitive merges. Full original paths remain in the coverage CSV.

| Original test or input matrix | Coverage after reduction | Reason |
| --- | --- | --- |
| `bootstrap_transport_error_reaches_client` | `stream_read_error_is_network_not_truncated`; `http::exchange::tests::prelude_read_preserves_the_bootstrap_exit_and_bytes` | Same prelude cut; retained test also checks the persisted source. |
| `bootstrap_idle_timeout_not_doubled` | `idle_timeout_aborts_stream` | Same scripted prelude stall; tightened the retained deadline to detect doubling. |
| `response_failed_instructions_mention_is_not_quota` | `classify::tests::frame_classification_table`; `usage::tests::terminal_detection`; `usage_sse_rows` | Classification of quoted error words is pure. |
| `sse_overload_before_output_switches` | `sse_empty_added_before_overload_switches`; `http::exchange::tests::prelude_verdict_covers_the_complete_replay_table` | Retained preamble replay case also verifies row and discarded announcements. |
| `sse_metadata_before_overload_still_switches` | `sse_empty_added_before_overload_switches`; `classify::tests::frame_classification_table` | Metadata and empty announcements are rows of the same preamble table. |
| `sse_metadata_before_encrypted_still_400` | `encrypted_rejections_preserve_transport_contracts`; `classify::tests::frame_classification_table` | Metadata is classified as preamble by the table. |
| `policy_capacity_unavailable_before_output_switches` | `sse_empty_added_before_overload_switches`; `ws_overload_switch_records_attempt_chain`; `classify::tests::frame_classification_table` | Overload alias is exhaustively classified without sockets. |
| `sse_overload_no_more_retry_skips_wait` | `sse_overload_last_account_waits_and_retries_once`; `attempt::policy::tests::overload_plan_table_covers_headers_waits_and_captured_ws_limit` | No-more-retry branch is a pure policy row. |
| `sse_overload_wait_retry_disabled` | `sse_overload_last_account_waits_and_retries_once`; `attempt::policy::tests::overload_plan_table_covers_headers_waits_and_captured_ws_limit` | Disabled-wait branch is a pure policy row. |
| `sse_close_after_completed_is_not_an_error` | `sse_downstream_observation_recorded` | Same slowtail request/client drop and TTFT assertions; observation test is stronger. |
| `read_error_after_completed_is_a_normal_end` | `gzip_completed_event_is_not_failed_by_late_trailer`; `sse_downstream_observation_recorded`; `http::pump::tests::finish_covers_every_observer_and_body_end` | Terminal plus read failure is covered by finish table and retained terminal/trailer wiring. |
| `idle_after_completed_is_a_normal_end` | `gzip_completed_event_is_not_failed_by_late_trailer`; `sse_downstream_observation_recorded`; `http::pump::tests::finish_covers_every_observer_and_body_end` | Terminal plus idle is a finish-table row. |
| `sse_overload_after_tool_call_arguments_is_a_failure` | `committed_errors_are_forwarded_and_recorded`; `classify::tests::frame_classification_table` | Tool arguments are content in the classifier table; same committed error path. |
| `ws_metadata_before_overload_still_switches` | `ws_overload_switch_records_attempt_chain`; `classify::tests::frame_classification_table` | Same metaover turn, retained test now checks discarded frames and router outcome too. |
| `ws_empty_added_before_overload_switches` | `ws_overload_switch_records_attempt_chain`; `classify::tests::frame_classification_table` | Empty announcements are pure preamble rows. |
| `ws_metadata_before_encrypted_clear_message` | `encrypted_rejections_preserve_transport_contracts`; `classify::tests::ws_bookkeeping_and_standalone_overload_keep_their_existing_view` | WS metadata commits by the pinned WS classification table. |
| `retries_5xx_then_succeeds` | `retried_success_keeps_attempt_chain`; `responses_compressed_like_codex_and_retries_reuse_bytes` | Same retry mock, stronger persisted-chain and byte-reuse tests retained. |
| `rate_limit_fails_over` | `account_error_families_fail_over`; `usage_sse_rows` | HTTP 429 wiring retained in the account error table and usage rows. |
| `unauthorized_forces_refresh_once` | `account_error_families_fail_over` | 401 row verifies refresh count and replayed success. |
| `http_400_overload_message_switches_despite_no_more_retry` | `account_error_families_fail_over`; `attempt::policy::tests::overload_plan_table_covers_headers_waits_and_captured_ws_limit` | 400 overload row kept in the shared e2e table. |
| `real_403_still_auth_failed_and_switches` | `account_error_families_fail_over` | 403 row kept with auth-failure outcome and next-account success. |
| `gzip_http_errors_keep_refresh_failover_and_classification` | `account_error_families_fail_over`; `response_body::tests::bad_crc_truncation_and_trailing_garbage_are_decode_errors` | Compressed 401/429/5xx/overload use the account-error e2e table. |
| `gzip_network_read_errors_remain_network_errors` | `stream_read_error_is_network_not_truncated`; `response_body::tests::network_read_failures_are_not_reported_as_gzip_corruption` | Pure decoder provenance plus plain network-read wiring cover the combination. |
| `gzip_sse_cancellation_and_force_release_held_decoder` | `sse_observation_on_client_disconnect`; `shutdown_cut_sse_flushes_queued_events_then_retryable_error_then_clean_end`; `response_body::tests::dropping_decoded_body_drops_the_owned_upstream_stream` | Decoder ownership/drop is tested directly; cancellation and force each retain e2e wiring. |
| `gzip_sse_idle_timeout_uses_decoded_progress` | `idle_timeout_aborts_stream`; `gzip_sse_flush_reaches_client_before_trailer_and_records_usage` | Decoded progress and idle wiring each retain their representative e2e. |
| `usage_counts_retries` | `retried_success_keeps_attempt_chain` | Same retry mock; added retry-count assertion to retained chain test. |
| `models_catalog_passthrough_no_null_service_tiers` | `models_cached_and_wham_passthrough` | Byte-for-byte catalog equality already proves no rewrite; retained test now covers both aliases and cache. |
| `rfc_helper` | `due_card_is_used_once_and_skips_disabled`; `pending_is_retried_with_the_same_id_after_restart`; `auto_reset::tests::retries_are_spaced_capped_and_stop`; `auto_reset::tests::clock_skew_bounds`; `auto_reset::tests::pending_decision_table` | Tests only a local test-fixture formatter. |
| `literal_codes_in_source_are_listed` | `attempt::policy::tests::classify_dial_and_refusal_table`; `classify::tests::frame_classification_table`; `error-family e2e tests` | Heuristic grepping of production source is an implementation-detail test. |
| `pages_have_no_replacement_char` | `ui_pages_and_toggle`; `scripts/*-check.cjs` | Static HTML/JavaScript source scans duplicate the node state checks and served-page e2e. |
| `self_contained_and_hashed` | `ui_pages_and_toggle`; `scripts/*-check.cjs` | Static HTML/JavaScript source scans duplicate the node state checks and served-page e2e. |
| `no_duplicate_function_names` | `ui_pages_and_toggle`; `scripts/*-check.cjs` | Static HTML/JavaScript source scans duplicate the node state checks and served-page e2e. |
| `payment_required_402_fails_over_and_cools` | `account_error_families_fail_over` | Merged input variants into one fixture. |
| `not_found_404_short_unless_model_unsupported` | `account_error_families_fail_over`; `classify::tests::classify_402_and_404` | 404 and model-unsupported rows share the account-error fixture; exact durations stay in pure tables. |
| `encrypted_rejection_sse_becomes_400` | `encrypted_rejections_preserve_transport_contracts` | Merged input variants into one fixture. |
| `encrypted_rejection_http_400_clear_message` | `encrypted_rejections_preserve_transport_contracts` | Four transport/phase rows share one environment and common error/detail assertions; WS reuse is preserved. |
| `encrypted_rejection_after_output_is_clear_failed_event` | `encrypted_rejections_preserve_transport_contracts` | Four transport/phase rows share one environment and common error/detail assertions; WS reuse is preserved. |
| `encrypted_rejection_ws_clear_message` | `encrypted_rejections_preserve_transport_contracts` | Four transport/phase rows share one environment and common error/detail assertions; WS reuse is preserved. |
| `broken_toml_file_error_hides_source_line` | `rejected_reload_leaves_running_config_unchanged` | One fixture checks validation failures, secret redaction, last-reload status, and unchanged running config. |
| `reload_rejects_the_example_key` | `rejected_reload_leaves_running_config_unchanged` | One fixture checks validation failures, secret redaction, last-reload status, and unchanged running config. |
| `sse_overload_after_output_is_recorded_as_failure` | `committed_errors_are_forwarded_and_recorded` | Merged input variants into one fixture. |
| `sse_quota_after_output_is_forwarded_and_cools` | `committed_errors_are_forwarded_and_recorded`; `classify::tests::frame_classification_table` | One shared fixture retains each committed error family; code/shape aliases stay in the classifier table. |
| `stream_request_error_records_its_code_without_account_penalty` | `committed_errors_are_forwarded_and_recorded`; `classify::tests::frame_classification_table` | One shared fixture retains each committed error family; code/shape aliases stay in the classifier table. |
| `stream_rate_limit_without_quota_code_is_not_usage_limit` | `committed_errors_are_forwarded_and_recorded`; `classify::tests::frame_classification_table` | One shared fixture retains each committed error family; code/shape aliases stay in the classifier table. |
| `sse_observation_uses_event_type_not_payload_strings` | `stream_read_error_is_network_not_truncated`; `usage::tests::terminal_detection`; `classify::tests::frame_classification_table` | Pure type-vs-payload rules and finish table cover the variants; retained SSE e2e distinguishes truncated EOF from network failure. |
| `sse_disconnect_while_upstream_idle_records_client_close_promptly` | `sse_observation_on_client_disconnect`; `ws_client_close_records_upstream_silent_duration` | Same idle_after_output disconnect; retained observation test now also checks the silent-duration detail. |
| `bucket_usage_samples_distinguish_missing_zero_and_prewarm` | `tests::summary::outcome_matrix_agrees_across_records_filters_groups_and_buckets` | The newer matrix covers missing/zero usage, prewarm exclusion, hourly buckets and summary totals together. |
| `day_incomplete_excludes_failures_cancellations_unknown_and_interrupted` | `tests::summary::outcome_matrix_agrees_across_records_filters_groups_and_buckets` | The outcome matrix already includes completed/interrupted, incomplete, failed, cancelled, unknown and prewarm rows. |
| `round_robin_cycles_deterministic` | `round_robin_resumes_rotation_across_retry_exclusions` | Retained test checks the same deterministic cycle before and after exclusions. |
| `error_labels_cover_codes_one_to_one` | `http::ui_pages_and_toggle`; `scripts/ui-state-check.cjs::failureClassesAndTrend` | Greps JavaScript source declarations rather than exercising UI behavior. |
| `new_codex_terminal_codes_have_chinese_labels` | `http::ui_pages_and_toggle`; `scripts/ui-state-check.cjs::failureClassesAndTrend` | Greps JavaScript source declarations rather than exercising UI behavior. |
| `key_compare` | `health_and_auth` | Exact match, shortened key, empty bearer and second-key matching are checked through the authentication middleware. |
| `ui_page_csp_and_toggle` | `http::ui_pages_and_toggle` | All three pages, CSP hashes, redirect, public access and disabled UI now use the in-process app with no upstream or usage DB. |
| `usage_key_label_instance_and_quota_api` | `forward::usage::usage_key_label_instance_and_quota_api` | Moved unchanged next to the forwarding/sampling behavior it exercises. |
| `refusals_are_stored_per_minute_and_served_by_the_usage_api` | `forward::admission::refusals_are_stored_per_minute_and_served_by_the_usage_api` | Moved unchanged next to the forwarding/sampling behavior it exercises. |
| `occupancy_samples_are_stored_and_served_by_the_usage_api` | `forward::admission::occupancy_samples_are_stored_and_served_by_the_usage_api` | Moved unchanged next to the forwarding/sampling behavior it exercises. |
| `summary_breaks_failures_down_by_class_error_code_and_source` | `usage_api::seeded_records_summary_and_observation_contracts` | A thin seeded API fixture keeps auth, JSON groups, records filters, previous-period comparison and observation errors; exhaustive classification/count arithmetic stays in usage tables. |
| `observation_api_rejects_bad_ids_and_old_rows` | `usage_api::seeded_records_summary_and_observation_contracts` | A thin seeded API fixture keeps auth, JSON groups, records filters, previous-period comparison and observation errors; exhaustive classification/count arithmetic stays in usage tables. |
| `foreign_write_during_refresh_is_not_clobbered` | `refresh_preserves_foreign_rotations_on_success_and_rejection` | Merged input variants into one fixture. |
| `rejected_but_rotated_elsewhere_adopts_disk` | `refresh_preserves_foreign_rotations_on_success_and_rejection` | Success and refresh-token-reused responses share the same foreign-write race fixture. |
| `d4_ws_server_cooldown_honors_retry_after_like_http [26 redundant rows]` | `d4_ws_server_cooldown_honors_retry_after_like_http`; `retry_after::tests::advice_formats`; `attempt::policy::tests::classify_dial_and_refusal_table`; `routing::tests::cooldown_table` | Kept one HTTP and one WS wiring case; date parsing, invalid/missing advice, caps, disabled cooldowns and status aliases stay in pure tables. |
| `gzip_response_encoding_contract_rejects_unknown_and_duplicate_without_replay` | `unsupported_response_encoding_is_rejected_without_replay`; `response_body::tests::encoding_contract_is_strict_but_bodyless_statuses_do_not_decode` | One e2e rejection; strict list/duplicate variants are decoder-table rows. |
| `gzip_completed_event_is_not_failed_by_late_trailer_or_client_close` | `gzip_completed_event_is_not_failed_by_late_trailer`; `sse_downstream_observation_recorded`; `http::pump::tests::finish_covers_every_observer_and_body_end` | One terminal/trailer e2e; terminal/client-close wiring and terminal finish variants are retained elsewhere. |
| `gzip_corruption_before_output_is_decode_error_without_replay [truncated/trailing rows]` | `gzip_corruption_before_output_is_decode_error_without_replay`; `response_body::tests::bad_crc_truncation_and_trailing_garbage_are_decode_errors` | One CRC e2e rejection; fault shapes stay in the direct decoder table. |
| `idle_websocket_is_a_session_without_an_inflight_slot` | `idle_websockets_hold_no_inflight_slot_only_running_turns_do`; `shutdown_cut_ws_turn_flushes_received_frames_then_retryable_error_then_1001`; `settings::shutdown_drain_closes_idle_ws_normally` | Idle occupancy, held turn, cut and released slots repeat the stronger lifecycle/admission and drain tests. |
| `ws_ttft_and_prewarm` | `ws_downstream_observation_per_turn` | Same prewarm and slow turns; merged the summary count/token assertions into the per-turn observation test. |
| `websocket_client_close_gets_close_frame` | `ws_client_close_records_upstream_silent_duration` | Retained close diagnostic test now verifies the peer close reply too. |
| `websocket_relay_and_limit_close` | `usage_ws_turn_rows` | Same three-turn replay lifecycle; retained test now checks headers, discarded quota frames, 1012 close and router outcome too. |
| `sse_quota_before_output_switches_transparently` | `usage_sse_rows`; `gzip_sse_without_content_type_keeps_usage_and_quota_failover` | Same midlimit replay; added identical-body and Retry-After assertions to the record/attempt-chain test. |
| `bounded_error_body_keeps_upstream_identity_without_forwarding_a_partial_body` | `gzip_http_error_limit_applies_after_decompression`; `limits::tests::cap_keeps_complete_bodies_and_marks_incomplete_errors` | Same oversized error body; retained compressed e2e now checks identity/detail and neutral response too. |
| `payment_required_402_on_every_account_passes_upstream_error` | `account_error_families_fail_over`; `forward_policy::d1_http_returns_ranked_error_ws_returns_last_error`; `attempt::policy::tests::refusal_decision_table` | 402 classification/failover and exhausted-account error selection already have representative wiring and pure policy coverage. |
| `usage_limit_text_in_output_is_not_a_quota_error` | `classify::tests::frame_classification_table`; `usage::tests::terminal_detection`; `usage_sse_rows` | Pure type-vs-payload classification; ordinary streaming usage retains the wiring test. |
| `nothing_to_reset_gives_up_silently` | `due_card_is_used_once_and_skips_disabled`; `auto_reset::tests::pending_decision_table`; `reset_credits::tests::consume_classification_table` | The pure tables cover nothing-to-reset completion and stopping future retries; automatic consume and persistence still have representative e2e coverage. |
| `retries_are_capped_spaced_and_end_at_expiry` | `pending_is_retried_with_the_same_id_after_restart`; `auto_reset::tests::retries_are_spaced_capped_and_stop`; `auto_reset::tests::clock_skew_bounds`; `auto_reset::tests::pending_decision_table` | Retry spacing/caps/expiry are pure scheduler decisions; persisted pending replay retains the retry and idempotency wiring. |

## Deliberately retained / skipped

- HTTP/WS preparation and shared-budget/reset-per-turn tests stay: the base has pure policy tables but does not have the in-memory paused-clock attempt/session tests proposed in review §5.2. Deleting these would lose real lifecycle coverage. Reload snapshot, cancellation, shutdown/drain, and admission lifetime tests likewise exercise more than a decision row.
- Core config/schema/alias/log-redaction tests and upstream network/fingerprint/pool/drain tests retain distinct contracts. No speculative deletion was made merely to reduce those crate totals. Auth disk adoption before a refresh is distinct from adoption during a refresh; both remain.
- Usage migrations retain the frozen historical fixtures, privacy rewrite, rewrite idempotency, and v15 data preservation. The generic historical-schema table does not replace those data-specific assertions.
- `sse_overload_after_output_is_forwarded` was already absent at `ce28b23`; its stronger successor was merged into `committed_errors_are_forwarded_and_recorded`.
- Obsolete HTTP mock branches and the now single-use script interpreter were removed. Live refusal/occupancy API tests still use an upstream because they verify real admission/lease sampling; only seeded read-only API tests use the upstream-free fixture.

## Validation

- `cargo fmt --all --check` — pass.
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` — pass.
- `cargo test --workspace --all-targets --locked --offline` — three baseline and three final runs pass; 536 tests per final run, zero ignored.
- Every `scripts/*-check.cjs` — pass, unchanged.
- `git diff --check` — pass.

No production API changes or behavior uncertainties remain. Coverage is an audit of retained assertions and replacement tables, not a claim of measured line/branch coverage. The 6.7% workspace line reduction is smaller than the 18.9% forwarding reduction because most tests outside these duplicate e2e clusters retain distinct contracts.
