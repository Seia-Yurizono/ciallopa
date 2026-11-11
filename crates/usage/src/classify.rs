//! Per-window reset classification from consecutive quota snapshots.
//!
//! A drop in `used_percent` together with a moved `resets_at` is a reset. Card evidence
//! (a local `reset_consume` success, or a drop in available card count *and* a moved
//! `resets_at`) distinguishes manual / auto / external cards from a natural rollover or
//! an official/paid early reset (「提前重置」). `resets_at` moving alone is not a reset.
//! A percent drop without a `resets_at` change is a limit change, except the delayed
//! observation of a rollover whose new `resets_at` was already recorded while usage
//! still showed ~100%.
//!
//! A drop in available card count without a moved `resets_at` is not a reset: the card
//! expired unused (quota is not restored). Card `expires_at` is never a window
//! `resets_at` (see `usage_to_quota`).
use serde::Serialize;

/// Same jitter window as `quota_obs` (`RESET_TOLERANCE_S`).
pub const RESET_TOLERANCE_S: i64 = 120;

/// Minimum drop in used % counted as a real decrease (integer percent is coarse).
pub const DROP_PCT: f64 = 5.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowKind {
    Primary,
    Secondary,
}
impl WindowKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        }
    }
    pub fn from_index(i: usize) -> Self {
        if i == 0 {
            Self::Primary
        } else {
            Self::Secondary
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CardSource {
    Manual,
    Auto,
}
impl CardSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Auto => "auto",
        }
    }
    /// `reset_consume.key` written by the auto-use path is `"(auto)"`.
    pub fn from_key(key: Option<&str>) -> Self {
        if key == Some("(auto)") {
            Self::Auto
        } else {
            Self::Manual
        }
    }
}

/// What a snapshot-to-snapshot transition means for one window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Natural,
    ManualCard,
    AutoCard,
    ExternalCard,
    /// Official extra reset and paid instant reset, merged (UI: 「提前重置」).
    EarlyReset,
    /// Percent dropped, `resets_at` did not move: capacity/limit changed, not a reset.
    LimitChange,
    /// Only `resets_at` moved (idle slide / jitter beyond tolerance). Not a reset.
    IdleSlide,
    /// Nothing of interest.
    None,
}
impl Class {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Natural => "natural",
            Self::ManualCard => "manual_card",
            Self::AutoCard => "auto_card",
            Self::ExternalCard => "external_card",
            Self::EarlyReset => "early_reset",
            Self::LimitChange => "limit_change",
            Self::IdleSlide => "idle_slide",
            Self::None => "none",
        }
    }

    /// Closes the current estimate cycle (write an event + estimate row).
    pub fn closes_cycle(self) -> bool {
        matches!(
            self,
            Self::Natural
                | Self::ManualCard
                | Self::AutoCard
                | Self::ExternalCard
                | Self::EarlyReset
                | Self::LimitChange
        )
    }

    pub fn card_source(self) -> Option<CardSource> {
        match self {
            Self::ManualCard => Some(CardSource::Manual),
            Self::AutoCard => Some(CardSource::Auto),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Side {
    pub pct: Option<f64>,
    pub reset: Option<i64>,
    pub window_min: Option<i64>,
    pub cards: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct Transition {
    pub prev: Side,
    pub next: Side,
    /// Unix seconds of the new observation.
    pub now_s: i64,
    /// Successful local `reset_consume` (`code=reset`) near this observation.
    pub local_card: Option<CardSource>,
}

pub fn same_reset(a: Option<i64>, b: Option<i64>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => (x - y).abs() <= RESET_TOLERANCE_S,
        (None, None) => true,
        _ => false,
    }
}

fn dropped(prev: Option<f64>, next: Option<f64>) -> bool {
    let (Some(p), Some(n)) = (prev, next) else {
        return false;
    };
    if n >= p {
        return false;
    }
    let d = p - n;
    d >= DROP_PCT || (n <= 5.0 && d >= 0.5) || (p >= 10.0 && n <= p * 0.5)
}

/// Percent wiped from nearly-full to nearly-empty at the start of the (already recorded)
/// new window: the second half of a rollover whose `resets_at` jumped first.
fn delayed_rollover(t: &Transition) -> bool {
    let (Some(p), Some(n)) = (t.prev.pct, t.next.pct) else {
        return false;
    };
    if p < 95.0 || n > 5.0 {
        return false;
    }
    let reset = t.next.reset.or(t.prev.reset);
    let mins = t.next.window_min.or(t.prev.window_min);
    match (reset, mins) {
        (Some(r), Some(m)) if m > 0 => {
            let left = r - t.now_s;
            let win = m * 60;
            // Near the beginning of the window (most of it still ahead).
            left >= win - RESET_TOLERANCE_S * 4 || left > win * 8 / 10
        }
        _ => true,
    }
}

pub fn classify(t: &Transition) -> Class {
    let pct_dropped = dropped(t.prev.pct, t.next.pct);
    let reset_moved = !same_reset(t.prev.reset, t.next.reset);
    let cards_dropped = match (t.prev.cards, t.next.cards) {
        (Some(a), Some(b)) => b < a,
        _ => false,
    };
    let reset_due = t
        .prev
        .reset
        .is_some_and(|r| t.now_s + RESET_TOLERANCE_S >= r);

    if pct_dropped {
        if let Some(src) = t.local_card {
            return match src {
                CardSource::Manual => Class::ManualCard,
                CardSource::Auto => Class::AutoCard,
            };
        }
        // Redeeming a card starts a new window, so resets_at moves. A count drop
        // with the same resets_at is an unused card expiring (or a listing glitch),
        // not external_card — even if used% also dropped (that is a limit change).
        if cards_dropped && reset_moved {
            return Class::ExternalCard;
        }
        if !reset_moved {
            if delayed_rollover(t) {
                return Class::Natural;
            }
            return Class::LimitChange;
        }
        if reset_due {
            return Class::Natural;
        }
        return Class::EarlyReset;
    }
    if reset_moved {
        return Class::IdleSlide;
    }
    Class::None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(prev: Side, next: Side, now_s: i64, local: Option<CardSource>) -> Transition {
        Transition {
            prev,
            next,
            now_s,
            local_card: local,
        }
    }
    fn side(pct: f64, reset: i64) -> Side {
        Side {
            pct: Some(pct),
            reset: Some(reset),
            window_min: Some(300),
            cards: None,
        }
    }

    #[test]
    fn natural_at_scheduled_time() {
        let old = 1_700_000_000;
        let now = old + 10;
        let c = classify(&t(side(82.0, old), side(2.0, now + 300 * 60), now, None));
        assert_eq!(c, Class::Natural);
        assert!(c.closes_cycle());
    }

    #[test]
    fn manual_and_auto_card() {
        let old = 1_700_010_000;
        let now = old - 3_600;
        for (src, want) in [
            (CardSource::Manual, Class::ManualCard),
            (CardSource::Auto, Class::AutoCard),
        ] {
            let c = classify(&t(
                side(60.0, old),
                Side {
                    cards: Some(1),
                    ..side(1.0, now + 300 * 60)
                },
                now,
                Some(src),
            ));
            assert_eq!(c, want);
            assert_eq!(c.card_source(), Some(src));
        }
    }

    #[test]
    fn card_expiry_without_quota_restore_is_not_a_reset() {
        // openai/codex#60664: an unused card's expires_at is not a window reset.
        // After the card lapses the available count drops, but used% and resets_at
        // stay put (quota is not restored). That must not close a cycle.
        let reset = 1_700_010_000;
        let now = reset - 3_600;
        let prev = Side {
            pct: Some(55.0),
            reset: Some(reset),
            window_min: Some(300),
            cards: Some(2),
        };
        let expired = Side {
            cards: Some(1),
            ..prev
        };
        let c = classify(&t(prev, expired, now, None));
        assert_eq!(c, Class::None);
        assert!(!c.closes_cycle());

        // Continued usage after expiry (percent ticks up) is still not a reset.
        let used = Side {
            pct: Some(57.0),
            reset: Some(reset),
            window_min: Some(300),
            cards: Some(1),
        };
        let c = classify(&t(prev, used, now + 60, None));
        assert_eq!(c, Class::None);
        assert!(!c.closes_cycle());
    }

    #[test]
    fn expired_card_count_drop_is_not_external_card() {
        // A count drop without a moved resets_at is expiry (or a listing glitch),
        // not a card that was redeemed elsewhere. A coincident used% drop is a
        // limit change, not external_card.
        let reset = 1_700_010_000;
        let now = reset - 3_600;
        let c = classify(&t(
            Side {
                cards: Some(2),
                ..side(80.0, reset)
            },
            Side {
                cards: Some(1),
                ..side(40.0, reset)
            },
            now,
            None,
        ));
        assert_ne!(c, Class::ExternalCard);
        assert_eq!(c, Class::LimitChange);
    }

    #[test]
    fn external_card_when_count_drops() {
        let old = 1_700_010_000;
        let now = old - 3_600;
        let c = classify(&t(
            Side {
                cards: Some(3),
                ..side(55.0, old)
            },
            Side {
                cards: Some(2),
                ..side(1.0, now + 300 * 60)
            },
            now,
            None,
        ));
        assert_eq!(c, Class::ExternalCard);
    }

    #[test]
    fn early_reset_without_card() {
        let old = 1_700_010_000;
        let now = old - 3_600;
        let c = classify(&t(
            Side {
                cards: Some(2),
                ..side(40.0, old)
            },
            Side {
                cards: Some(2),
                ..side(1.0, now + 300 * 60)
            },
            now,
            None,
        ));
        assert_eq!(c, Class::EarlyReset);
    }

    #[test]
    fn idle_slide_is_not_a_reset() {
        let old = 1_700_000_000;
        let c = classify(&t(
            side(40.0, old),
            side(41.0, old + 400),
            old - 1_000,
            None,
        ));
        assert_eq!(c, Class::IdleSlide);
        assert!(!c.closes_cycle());
    }

    #[test]
    fn jitter_within_tolerance_is_nothing() {
        let old = 1_700_000_000;
        let c = classify(&t(side(40.0, old), side(40.0, old + 30), old - 1_000, None));
        assert_eq!(c, Class::None);
    }

    #[test]
    fn percent_drop_without_reset_move_is_limit_change() {
        let old = 1_700_010_000;
        let now = old - 3_600;
        let c = classify(&t(side(80.0, old), side(40.0, old + 20), now, None));
        assert_eq!(c, Class::LimitChange);
        assert!(c.closes_cycle());
    }

    #[test]
    fn delayed_rollover_after_idle_slide_is_natural() {
        // resets_at already jumped while used% was 100; later we see ~0% at the new reset.
        let new_reset = 1_700_000_000 + 300 * 60;
        let now = 1_700_000_000 + 30;
        let c = classify(&t(
            side(100.0, new_reset),
            side(1.0, new_reset + 10),
            now,
            None,
        ));
        assert_eq!(c, Class::Natural);
    }

    #[test]
    fn local_card_wins_over_delayed_rollover_shape() {
        let new_reset = 1_700_000_000 + 300 * 60;
        let now = 1_700_000_000 + 30;
        let c = classify(&t(
            side(100.0, new_reset),
            side(0.0, new_reset),
            now,
            Some(CardSource::Manual),
        ));
        assert_eq!(c, Class::ManualCard);
    }

    #[test]
    fn card_source_from_auto_label() {
        assert_eq!(CardSource::from_key(Some("(auto)")), CardSource::Auto);
        assert_eq!(CardSource::from_key(Some("laptop")), CardSource::Manual);
        assert_eq!(CardSource::from_key(None), CardSource::Manual);
    }

    #[test]
    fn each_window_independent() {
        assert_eq!(WindowKind::from_index(0).as_str(), "primary");
        assert_eq!(WindowKind::from_index(1).as_str(), "secondary");
    }
}
