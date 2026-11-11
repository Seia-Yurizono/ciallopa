//! Official ChatGPT included-usage credit rates (Standard), plus Fast / Ultrafast multipliers.
//!
//! Credits are **not** API dollars. Published Standard rates are per 1M tokens; Fast included
//! usage is billed at 2.5× and Ultrafast at 8× those rates. Unknown models return `None`
//! (never 0). Cache-write tokens are treated as uncached input (they are part of `input`).
use crate::prices::{Tier, Usage};

/// Fast included-usage multiplier (ChatGPT plan, not the API Fast list-price multiplier).
pub const FAST_INCLUDED_X: f64 = 2.5;
/// Ultrafast included-usage multiplier (ChatGPT plan, not the API Ultrafast list-price multiplier).
pub const ULTRA_INCLUDED_X: f64 = 8.0;

/// Per-1M-token Standard credit rates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CreditRates {
    pub input: f64,
    pub cached: f64,
    pub output: f64,
}

/// Official Standard rates from ChatGPT pricing (learn.chatgpt.com / docs/pricing).
/// Names are stored lowercase; dated snapshot suffixes are stripped before lookup.
const RATES: &[(&str, CreditRates)] = &[
    (
        "gpt-6-astra",
        CreditRates {
            input: 250.0,
            cached: 25.0,
            output: 1250.0,
        },
    ),
    (
        "gpt-6.1-sol",
        CreditRates {
            input: 50.0,
            cached: 2.5,
            output: 250.0,
        },
    ),
    (
        "gpt-6.1-terra",
        CreditRates {
            input: 25.0,
            cached: 1.25,
            output: 125.0,
        },
    ),
    (
        "gpt-6-sol",
        CreditRates {
            input: 50.0,
            cached: 5.0,
            output: 250.0,
        },
    ),
    (
        "gpt-6-luna",
        CreditRates {
            input: 2.5,
            cached: 0.25,
            output: 12.5,
        },
    ),
    (
        "gpt-5.6-sol",
        CreditRates {
            input: 100.0,
            cached: 10.0,
            output: 500.0,
        },
    ),
    (
        "gpt-5.6-terra",
        CreditRates {
            input: 50.0,
            cached: 5.0,
            output: 300.0,
        },
    ),
    (
        "gpt-5.6-luna",
        CreditRates {
            input: 5.0,
            cached: 0.5,
            output: 30.0,
        },
    ),
    (
        "daybreak-blue",
        CreditRates {
            input: 100.0,
            cached: 10.0,
            output: 500.0,
        },
    ),
    (
        "daybreak-red",
        CreditRates {
            input: 312.5,
            cached: 31.25,
            output: 1875.0,
        },
    ),
    (
        "gpt-rosalind-research",
        CreditRates {
            input: 125.0,
            cached: 12.5,
            output: 625.0,
        },
    ),
    (
        "gpt-image-2",
        CreditRates {
            input: 125.0,
            cached: 31.25,
            output: 250.0,
        },
    ),
];

/// Extra lowercase aliases → canonical table key.
const ALIASES: &[(&str, &str)] = &[
    ("gpt-6.1-sol-codex", "gpt-6.1-sol"),
    ("gpt-5.6-sol-codex", "gpt-5.6-sol"),
];

/// Standard credit rates for `model`, if published.
pub fn rates(model: &str) -> Option<CreditRates> {
    let key = crate::strip_snapshot(model.trim()).to_ascii_lowercase();
    if key.is_empty() {
        return None;
    }
    let key = ALIASES
        .iter()
        .find(|(a, _)| *a == key)
        .map(|(_, k)| *k)
        .unwrap_or(key.as_str());
    RATES.iter().find(|(n, _)| *n == key).map(|(_, r)| *r)
}

/// Included-usage credits for one request. `None` = unknown model (never 0).
/// Flex (not an included-usage speed) uses the Standard rate.
pub fn credits(model: Option<&str>, tier: Tier, u: Usage) -> Option<f64> {
    let r = rates(model?)?;
    let x = match tier {
        Tier::Standard | Tier::Flex => 1.0,
        Tier::PriorityFast => FAST_INCLUDED_X,
        Tier::Ultrafast => ULTRA_INCLUDED_X,
    };
    let cached = u.cached.max(0) as f64;
    let fresh = (u.input - u.cached).max(0) as f64;
    let out = u.output.max(0) as f64;
    Some((fresh * r.input + cached * r.cached + out * r.output) * x / 1e6)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(input: i64, cached: i64, output: i64) -> Usage {
        Usage {
            input,
            cached,
            cache_write: 0,
            output,
        }
    }

    #[test]
    fn known_and_unknown() {
        assert!(rates("gpt-6.1-sol").is_some());
        assert!(rates("GPT-6.1-Sol-2026-10-01").is_some());
        assert!(rates("gpt-6.1-sol-codex").is_some());
        assert!(rates("mystery-model").is_none());
        assert!(credits(None, Tier::Standard, u(1000, 0, 10)).is_none());
        assert!(credits(Some("nope"), Tier::Standard, u(1000, 0, 10)).is_none());
    }

    #[test]
    fn standard_matches_published_per_million() {
        let c = credits(Some("gpt-6.1-sol"), Tier::Standard, u(1_000_000, 0, 0)).unwrap();
        assert!((c - 50.0).abs() < 1e-9);
        let c = credits(
            Some("gpt-6.1-sol"),
            Tier::Standard,
            u(1_000_000, 1_000_000, 0),
        )
        .unwrap();
        assert!((c - 2.5).abs() < 1e-9);
        let c = credits(Some("gpt-6-astra"), Tier::Standard, u(0, 0, 1_000_000)).unwrap();
        assert!((c - 1250.0).abs() < 1e-9);
    }

    #[test]
    fn fast_and_ultrafast_are_included_multipliers() {
        let std = credits(
            Some("gpt-6.1-sol"),
            Tier::Standard,
            u(100_000, 20_000, 10_000),
        )
        .unwrap();
        let fast = credits(
            Some("gpt-6.1-sol"),
            Tier::PriorityFast,
            u(100_000, 20_000, 10_000),
        )
        .unwrap();
        let ultra = credits(
            Some("gpt-6.1-sol"),
            Tier::Ultrafast,
            u(100_000, 20_000, 10_000),
        )
        .unwrap();
        assert!((fast - std * 2.5).abs() < 1e-9);
        assert!((ultra - std * 8.0).abs() < 1e-9);
        let flex = credits(Some("gpt-6.1-sol"), Tier::Flex, u(100_000, 20_000, 10_000)).unwrap();
        assert!((flex - std).abs() < 1e-9);
    }
}
