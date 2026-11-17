//! A qualifying percent drop with an eligible local card receipt takes priority over reset shape.
//! Without that evidence, a moved reset time or delayed rollover shape identifies a reset;
//! other substantial drops in the same window are limit changes.
//! History matches receipts and limits each receipt to one attribution per window. This pure
//! classifier trusts the supplied evidence, including when time moves before percent drops.
//! Card expiry alone never restores quota.
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
    pub const ALL: [Self; 2] = [Self::Primary, Self::Secondary];
    pub const fn cols(self) -> (&'static str, &'static str) {
        match self {
            Self::Primary => ("p_pct", "p_reset"),
            Self::Secondary => ("s_pct", "s_reset"),
        }
    }
    pub fn side(self, q: Option<&crate::QuotaObs>) -> Side {
        let Some(q) = q else { return Side::default() };
        let (pct, reset, window_min) = match self {
            Self::Primary => (q.p_pct, q.p_reset, q.p_window_min),
            Self::Secondary => (q.s_pct, q.s_reset, q.s_window_min),
        };
        Side {
            pct,
            reset,
            window_min,
            cards: q.cards,
        }
    }
    pub const fn index(self) -> usize {
        match self {
            Self::Primary => 0,
            Self::Secondary => 1,
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
    /// Substantial drop without a moved reset, eligible local receipt or delayed rollover shape.
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

    /// Closes the current estimate cycle (write an event + estimate row). Cycles follow
    /// `resets_at`: a limit change keeps the window, so it is an event inside the cycle only
    /// (an estimate row for it would be a prefix of the cycle that goes on).
    pub fn closes_cycle(self) -> bool {
        matches!(
            self,
            Self::Natural
                | Self::ManualCard
                | Self::AutoCard
                | Self::ExternalCard
                | Self::EarlyReset
        )
    }

    /// Written to `quota_reset_events`.
    pub fn records_event(self) -> bool {
        self.closes_cycle() || self == Self::LimitChange
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
    /// Eligible successful local `reset_consume` (`code=reset`) near this observation.
    /// The caller must match its identity and prevent repeated attribution per window.
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

/// A drop large enough to mean something without a moved `resets_at`. The near-zero rule of
/// [`dropped`] only counts next to a moved reset: between two snapshots of one window a point or
/// so is rounding (headers are integers, `/wham/usage` may be fractional), not a limit change.
fn dropped_far(prev: Option<f64>, next: Option<f64>) -> bool {
    let (Some(p), Some(n)) = (prev, next) else {
        return false;
    };
    let d = p - n;
    d >= DROP_PCT || (p >= 10.0 && n <= p * 0.5)
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
        if !reset_moved && !dropped_far(t.prev.pct, t.next.pct) && !delayed_rollover(t) {
            // Same window, small dip: rounding or two sources a point apart.
            return Class::None;
        }
        if let Some(src) = t.local_card {
            return match src {
                CardSource::Manual => Class::ManualCard,
                CardSource::Auto => Class::AutoCard,
            };
        }
        // A card-count drop alone is ambiguous (expiry / listing changes). Only infer an
        // external card alongside a moved reset; direct local evidence above also supports
        // staged observations where the reset time arrived before the percentage drop.
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
        assert!(!c.closes_cycle() && c.records_event());
    }

    #[test]
    fn small_dip_in_the_same_window_is_nothing() {
        let old = 1_700_010_000;
        let now = old - 3_600;
        // Rounding or two sources a point apart; with or without a recent card.
        for (a, b) in [(3.0, 2.0), (4.6, 4.0), (6.0, 5.0), (40.0, 37.0)] {
            assert_eq!(
                classify(&t(side(a, old), side(b, old + 20), now, None)),
                Class::None,
                "{a}->{b}"
            );
            let local = Some(CardSource::Manual);
            assert_eq!(
                classify(&t(side(a, old), side(b, old), now, local)),
                Class::None,
                "card {a}->{b}"
            );
        }
        // A real reset to near zero still counts when resets_at moves.
        let c = classify(&t(side(4.0, old), side(0.0, old + 18_000), old + 10, None));
        assert!(c.closes_cycle(), "{c:?}");
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
        assert_eq!(WindowKind::Primary.as_str(), "primary");
        assert_eq!(WindowKind::Secondary.as_str(), "secondary");
    }

    /// Classifier behavior with supplied evidence, not receipt matching or its persistence.
    /// Generated transitions use the rules in docs/USAGE.md, not this module's arithmetic.
    /// Percentages are whole tenths; the seed is fixed and a failure prints the transition.
    mod invariants {
        use super::*;

        struct Gen(u64);
        impl Gen {
            fn next(&mut self) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0
            }
            fn below(&mut self, n: u64) -> i64 {
                (self.next() % n) as i64
            }
        }

        struct Case {
            t: Transition,
            /// Used percent in tenths (prev, next), when both are reported.
            tenths: Option<(i64, i64)>,
            same_window: bool,
        }

        fn case(g: &mut Gen) -> Case {
            let reset = 1_700_000_000 + g.below(86_400);
            let same_window = g.below(2) == 0;
            let next_reset = if same_window {
                reset + g.below(2 * RESET_TOLERANCE_S as u64 + 1) - RESET_TOLERANCE_S
            } else {
                reset + RESET_TOLERANCE_S + 1 + g.below(7 * 86_400)
            };
            let window_min = [None, Some(300), Some(10_080)][g.below(3) as usize];
            let mut cards = || [None, Some(0), Some(1), Some(2)][g.below(4) as usize];
            let (prev_cards, next_cards) = (cards(), cards());
            let tenths = (g.below(8) != 0).then(|| (g.below(1001), g.below(1001)));
            let pct = |v: i64| Some(v as f64 / 10.0);
            let local_card =
                [None, Some(CardSource::Manual), Some(CardSource::Auto)][g.below(3) as usize];
            Case {
                t: Transition {
                    prev: Side {
                        pct: tenths.and_then(|(p, _)| pct(p)),
                        reset: Some(reset),
                        window_min,
                        cards: prev_cards,
                    },
                    next: Side {
                        pct: tenths.and_then(|(_, n)| pct(n)),
                        reset: Some(next_reset),
                        window_min,
                        cards: next_cards,
                    },
                    // Anywhere from a week before the old reset to an hour after it.
                    now_s: reset - g.below(7 * 86_400) + g.below(3_600),
                    local_card,
                },
                tenths,
                same_window,
            }
        }

        fn each(check: impl Fn(&Case, Class)) {
            let mut g = Gen(0x005e_edc1_a55e_f1e5);
            for _ in 0..20_000 {
                let c = case(&mut g);
                check(&c, classify(&c.t));
            }
        }

        #[test]
        fn no_drop_records_no_event() {
            each(|c, class| {
                if c.tenths.is_none_or(|(p, n)| n >= p) {
                    assert!(!class.records_event(), "{class:?} for {:?}", c.t);
                }
            });
        }

        #[test]
        fn same_window_counts_drops_of_at_least_five_points() {
            // `limit_change`: at least 5 points (a halving from >= 10% is always that much);
            // a smaller dip in the same window is rounding and is not stored.
            each(|c, class| {
                let Some((p, n)) = c.tenths else { return };
                if c.same_window && n < p {
                    let want = p - n >= 50;
                    assert_eq!(class.records_event(), want, "{class:?} for {:?}", c.t);
                }
            });
        }

        #[test]
        fn a_moved_reset_with_a_real_drop_closes_the_cycle() {
            each(|c, class| {
                let Some((p, n)) = c.tenths else { return };
                if !c.same_window && p - n >= 50 {
                    assert!(class.closes_cycle(), "{class:?} for {:?}", c.t);
                }
                if !c.same_window {
                    assert_ne!(class, Class::LimitChange, "{:?}", c.t);
                }
            });
        }

        #[test]
        fn the_card_count_names_the_source_but_not_whether_there_was_a_reset() {
            // A falling available-card count without a moved reset is not enough evidence.
            each(|c, class| {
                let bare = Transition {
                    prev: Side {
                        cards: None,
                        ..c.t.prev
                    },
                    next: Side {
                        cards: None,
                        ..c.t.next
                    },
                    ..c.t
                };
                let without = classify(&bare);
                assert_eq!(
                    (class.records_event(), class.closes_cycle()),
                    (without.records_event(), without.closes_cycle()),
                    "{class:?} vs {without:?} for {:?}",
                    c.t
                );
            });
        }

        #[test]
        fn supplied_local_card_takes_priority_for_every_qualifying_drop() {
            // Characterize classifier precedence only. The database history tests verify
            // that a receipt is eligible and is not repeatedly supplied for the same window.
            each(|c, class| {
                if let (Some(src), true) = (c.t.local_card, class.records_event()) {
                    assert_eq!(class.card_source(), Some(src), "{class:?} for {:?}", c.t);
                }
            });
        }

        #[test]
        fn a_larger_drop_never_records_less() {
            each(|c, class| {
                let Some((_, n)) = c.tenths else { return };
                if !class.records_event() || n == 0 {
                    return;
                }
                // Any lower next value; derived from the case so the run stays deterministic.
                let lower = c.t.now_s.unsigned_abs() % n as u64;
                let deeper = Transition {
                    next: Side {
                        pct: Some(lower as f64 / 10.0),
                        ..c.t.next
                    },
                    ..c.t
                };
                let d = classify(&deeper);
                assert!(d.records_event(), "{class:?} -> {d:?} for {deeper:?}");
            });
        }
    }
}
