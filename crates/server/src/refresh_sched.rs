//! Randomized schedule for the background refreshes (`[ui] quota_refresh_*` per account,
//! `[ui] reset_credits_refresh_*` per round).
//!
//! Each key (an account, or one key for a whole round) gets its own next time, drawn
//! uniformly in `[min, max]` minutes after the previous run. A key seen for the first time
//! (server start, an account newly switched on, the timer just turned on) also waits a random
//! `[min, max]`, so keys never start in lockstep. State is in memory only — nothing is written
//! to disk; a restart simply draws new times.
//!
//! Time is plain milliseconds from any monotonic origin and the RNG is passed in, so the logic
//! is unit-tested without a runtime or a real clock.
use rand::Rng;
use std::collections::{BTreeSet, HashMap};

const MIN_MS: u64 = 60_000;

/// Uniform draw in `[min, max]` minutes, in milliseconds.
pub fn draw_ms<R: Rng + ?Sized>(rng: &mut R, (min, max): (u64, u64)) -> u64 {
    let (lo, hi) = (min.min(max) * MIN_MS, min.max(max) * MIN_MS);
    rng.random_range(lo..=hi)
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    /// When the current gap started (last run, or first seen).
    base: u64,
    next: u64,
}

/// Next-run times per key. `due` is the only entry point; call it on every tick.
#[derive(Debug, Default)]
pub struct Schedule {
    range: Option<(u64, u64)>,
    slots: HashMap<String, Slot>,
}

impl Schedule {
    /// Keys due at `now` among `keys` (the keys that should currently be refreshed). Due keys
    /// start a new gap at `now`. `range = None` (off) forgets everything and returns nothing;
    /// keys no longer listed are forgotten (switching one back on draws a fresh first time).
    /// A changed range redraws every pending time from the start of its gap.
    pub fn due<R: Rng + ?Sized>(
        &mut self,
        now: u64,
        range: Option<(u64, u64)>,
        keys: &[String],
        rng: &mut R,
    ) -> Vec<String> {
        let Some(r) = range else {
            self.range = None;
            self.slots.clear();
            return vec![];
        };
        if self.range != Some(r) {
            self.range = Some(r);
            for s in self.slots.values_mut() {
                s.next = s.base + draw_ms(rng, r);
            }
        }
        let want: BTreeSet<&str> = keys.iter().map(String::as_str).collect();
        self.slots.retain(|k, _| want.contains(k.as_str()));
        let mut out = vec![];
        for k in want {
            match self.slots.get_mut(k) {
                None => {
                    let next = now + draw_ms(rng, r);
                    self.slots.insert(k.to_string(), Slot { base: now, next });
                }
                Some(s) if s.next <= now => {
                    s.base = now;
                    s.next = now + draw_ms(rng, r);
                    out.push(k.to_string());
                }
                Some(_) => {}
            }
        }
        out
    }

    /// Earliest pending time (for sizing the next sleep).
    pub fn earliest(&self) -> Option<u64> {
        self.slots.values().map(|s| s.next).min()
    }

    /// Pending time of one key (tests and the list API).
    pub fn next_of(&self, key: &str) -> Option<u64> {
        self.slots.get(key).map(|s| s.next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    const M: u64 = MIN_MS;

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("acc{i}")).collect()
    }

    #[test]
    fn draws_stay_within_range() {
        let mut rng = StdRng::seed_from_u64(1);
        let (mut lo, mut hi) = (u64::MAX, 0);
        for _ in 0..10_000 {
            let d = draw_ms(&mut rng, (5, 10));
            assert!((5 * M..=10 * M).contains(&d), "{d}");
            lo = lo.min(d);
            hi = hi.max(d);
        }
        // spread over the whole range, not stuck at one end
        assert!(lo < 5 * M + 30_000 && hi > 10 * M - 30_000, "{lo} {hi}");
        // min == max is a fixed interval
        for _ in 0..100 {
            assert_eq!(draw_ms(&mut rng, (7, 7)), 7 * M);
        }
    }

    #[test]
    fn startup_offset_in_range_then_runs() {
        let mut rng = StdRng::seed_from_u64(2);
        let mut s = Schedule::default();
        let keys = ids(20);
        let t0 = 1_000;
        // first sight: nothing runs at once, every first time is within [min, max]
        assert!(s.due(t0, Some((5, 10)), &keys, &mut rng).is_empty());
        for k in &keys {
            let n = s.next_of(k).unwrap();
            assert!((t0 + 5 * M..=t0 + 10 * M).contains(&n), "{k} {n}");
        }
        assert!(s
            .due(t0 + 5 * M - 1, Some((5, 10)), &keys, &mut rng)
            .is_empty());
        // each key runs once when its time comes, then waits again [min, max]
        let mut ran = BTreeSet::new();
        let mut t = t0;
        while t <= t0 + 10 * M {
            for k in s.due(t, Some((5, 10)), &keys, &mut rng) {
                assert!(ran.insert(k.clone()), "{k} twice");
                let n = s.next_of(&k).unwrap();
                assert!((t + 5 * M..=t + 10 * M).contains(&n));
            }
            t += 1_000;
        }
        assert_eq!(ran.len(), keys.len());
    }

    #[test]
    fn accounts_desynchronize() {
        let mut rng = StdRng::seed_from_u64(3);
        let mut s = Schedule::default();
        let keys = ids(10);
        s.due(0, Some((5, 10)), &keys, &mut rng);
        let firsts: BTreeSet<u64> = keys.iter().map(|k| s.next_of(k).unwrap()).collect();
        assert!(firsts.len() >= 9, "first times collide: {firsts:?}");
        // run an hour in 5 s ticks: runs are spread out, not batched together
        let mut per_tick: Vec<usize> = vec![];
        let mut t = 0;
        while t <= 60 * M {
            per_tick.push(s.due(t, Some((5, 10)), &keys, &mut rng).len());
            t += 5_000;
        }
        let total: usize = per_tick.iter().sum();
        assert!((10 * 6..=10 * 12).contains(&total), "{total}");
        assert!(per_tick.iter().all(|&n| n <= 3), "{per_tick:?}");
        let busy = per_tick.iter().filter(|&&n| n > 0).count();
        assert!(busy > total * 3 / 4, "{busy} of {total}");
    }

    #[test]
    fn off_never_fires() {
        let mut rng = StdRng::seed_from_u64(4);
        let mut s = Schedule::default();
        let keys = ids(5);
        let mut t = 0;
        while t <= 3 * 60 * M {
            assert!(s.due(t, None, &keys, &mut rng).is_empty());
            t += 30_000;
        }
        assert_eq!(s.earliest(), None);
        // on → off drops pending times; back on draws a fresh first time
        s.due(t, Some((1, 1)), &keys, &mut rng);
        assert!(s.earliest().is_some());
        assert!(s.due(t + M, None, &keys, &mut rng).is_empty());
        assert_eq!(s.earliest(), None);
        assert!(s.due(t + 2 * M, Some((1, 1)), &keys, &mut rng).is_empty());
        assert_eq!(s.next_of("acc0"), Some(t + 3 * M));
    }

    #[test]
    fn new_keys_and_removed_keys() {
        let mut rng = StdRng::seed_from_u64(5);
        let mut s = Schedule::default();
        s.due(0, Some((2, 4)), &ids(1), &mut rng);
        // acc1 switched on later: its own offset from then, not acc0's time
        let t = 3 * M;
        s.due(t, Some((2, 4)), &ids(2), &mut rng);
        let n1 = s.next_of("acc1").unwrap();
        assert!((t + 2 * M..=t + 4 * M).contains(&n1));
        // switched off: forgotten
        s.due(t, Some((2, 4)), &ids(1), &mut rng);
        assert_eq!(s.next_of("acc1"), None);
    }

    #[test]
    fn range_change_redraws_from_gap_start() {
        let mut rng = StdRng::seed_from_u64(6);
        let mut s = Schedule::default();
        let keys = ids(8);
        s.due(0, Some((50, 60)), &keys, &mut rng);
        s.due(M, Some((1, 2)), &keys, &mut rng);
        for k in &keys {
            let n = s.next_of(k).unwrap();
            assert!((M..=2 * M).contains(&n), "{k} {n}");
        }
        let due = s.due(2 * M, Some((1, 2)), &keys, &mut rng);
        assert_eq!(due.len(), keys.len());
    }
}
