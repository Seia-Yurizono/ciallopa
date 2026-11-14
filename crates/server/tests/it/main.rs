//! All server integration tests in one binary (one link step instead of one per file).
#![allow(clippy::unwrap_used)]
mod affinity_restart;
mod auto_reset;
mod compaction_concurrency;
mod cooling;
mod fingerprint;
mod forward;
mod forward_policy;
mod http;
mod prices;
mod reset_credits;
mod runtime_api;
mod settings;
mod shutdown;
mod support;
mod usage_api;
mod ws_model_cooldown;
