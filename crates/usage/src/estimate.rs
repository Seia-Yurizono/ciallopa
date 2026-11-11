//! Per-window capacity estimate: Δ%-weighted Theil–Sen of cumulative credits vs used_percent,
//! with integer-quantization bounds and coverage-gap fallbacks.
//!
//! Show nothing rather than 0. An empty intersection of segment bounds is treated as a
//! coverage gap (other clients / instances): fall back to the median of adjacent slopes,
//! or only a lower bound if even that is scattered.
use serde::Serialize;

/// Minimum |Δ%| between a pair before it votes in Theil–Sen (same default as `min_delta_pct`).
pub const MIN_PAIR_DELTA_PCT: f64 = 5.0;

/// Assumed rounding half-width of an integer `used_percent` (Codex client uses `.round()`).
const HALF_BIN: f64 = 0.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Ok,
    Fallback,
    LowerOnly,
    Insufficient,
}
impl Confidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Fallback => "fallback",
            Self::LowerOnly => "lower_only",
            Self::Insufficient => "insufficient",
        }
    }
}

/// One aligned observation: `used_percent` at this snapshot and credits accumulated since
/// the first snapshot of the cycle (`credits == 0` at the first point).
#[derive(Debug, Clone, Copy)]
pub struct Point {
    pub pct: f64,
    pub credits: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Capacity {
    /// Point estimate of full-window capacity in credits. `None` when we should show nothing
    /// (or only a lower bound).
    pub capacity: Option<f64>,
    pub lo: Option<f64>,
    pub hi: Option<f64>,
    pub delta_pct: Option<f64>,
    pub credits_used: f64,
    pub coverage_gap: bool,
    pub confidence: Confidence,
}

impl Capacity {
    fn insufficient(credits_used: f64, delta_pct: Option<f64>) -> Self {
        Self {
            capacity: None,
            lo: None,
            hi: None,
            delta_pct,
            credits_used,
            coverage_gap: false,
            confidence: Confidence::Insufficient,
        }
    }
}

/// Δ%-weighted Theil–Sen of credits vs percent, plus intersecting quantization bounds.
pub fn estimate(points: &[Point], min_delta: f64) -> Capacity {
    let min_delta = if min_delta > 0.0 {
        min_delta
    } else {
        MIN_PAIR_DELTA_PCT
    };
    if points.len() < 2 {
        return Capacity::insufficient(points.last().map(|p| p.credits).unwrap_or(0.0), None);
    }
    let first = points[0];
    let last = points[points.len() - 1];
    let delta_pct = Some(last.pct - first.pct);
    let credits_used = last.credits - first.credits;

    let mut slopes: Vec<(f64, f64)> = Vec::new();
    let mut bounds: Vec<(f64, Option<f64>)> = Vec::new();
    for i in 0..points.len() {
        for j in (i + 1)..points.len() {
            let dp = points[j].pct - points[i].pct;
            if dp < min_delta {
                continue;
            }
            let dc = points[j].credits - points[i].credits;
            if dc <= 0.0 {
                continue;
            }
            slopes.push((dc / dp * 100.0, dp));
            bounds.push(quant_bound(dc, dp));
        }
    }

    if slopes.is_empty() {
        return Capacity::insufficient(credits_used.max(0.0), delta_pct);
    }

    let ts = weighted_median(&mut slopes);
    match intersect(&bounds) {
        Some((lo, hi)) => {
            let cap = ts.clamp(lo, hi.unwrap_or(ts));
            Capacity {
                capacity: Some(cap),
                lo: Some(lo),
                hi,
                delta_pct,
                credits_used: credits_used.max(0.0),
                coverage_gap: false,
                confidence: Confidence::Ok,
            }
        }
        None => fallback(points, ts, credits_used.max(0.0), delta_pct),
    }
}

/// Adjacent-median fallback when quantization bounds do not intersect (coverage gap).
fn fallback(points: &[Point], ts: f64, credits_used: f64, delta_pct: Option<f64>) -> Capacity {
    let mut adj: Vec<(f64, f64)> = Vec::new();
    let mut lowers = Vec::new();
    for w in points.windows(2) {
        let dp = w[1].pct - w[0].pct;
        let dc = w[1].credits - w[0].credits;
        if dp <= 0.0 || dc <= 0.0 {
            continue;
        }
        adj.push((dc / dp * 100.0, dp));
        let (lo, _) = quant_bound(dc, dp);
        lowers.push(lo);
    }
    let lo = lowers
        .iter()
        .copied()
        .filter(|x| x.is_finite())
        .max_by(|a, b| a.total_cmp(b));
    if adj.len() >= 2 {
        let cap = weighted_median(&mut adj);
        return Capacity {
            capacity: Some(cap),
            lo,
            hi: None,
            delta_pct,
            credits_used,
            coverage_gap: true,
            confidence: Confidence::Fallback,
        };
    }
    if let Some(lo) = lo {
        // Scattered segments: only a lower bound. Do not invent a point estimate from Theil–Sen
        // of contradictory pairs (that number would look precise).
        let _ = ts;
        return Capacity {
            capacity: None,
            lo: Some(lo),
            hi: None,
            delta_pct,
            credits_used,
            coverage_gap: true,
            confidence: Confidence::LowerOnly,
        };
    }
    Capacity {
        capacity: None,
        lo: None,
        hi: None,
        delta_pct,
        credits_used,
        coverage_gap: true,
        confidence: Confidence::Insufficient,
    }
}

/// Capacity bounds implied by one pair, assuming each percent was rounded to nearest integer.
/// `dp` is the observed (already-rounded) percent difference.
/// capacity = credits / (true_Δp / 100) = credits * 100 / true_Δp
/// true_Δp ∈ [dp − 1, dp + 1].
fn quant_bound(credits: f64, dp: f64) -> (f64, Option<f64>) {
    let lo_dp = (dp - 2.0 * HALF_BIN).max(0.0);
    let hi_dp = dp + 2.0 * HALF_BIN;
    let cap_hi = if lo_dp > 1e-9 {
        Some(credits * 100.0 / lo_dp)
    } else {
        None
    };
    let cap_lo = credits * 100.0 / hi_dp;
    (cap_lo, cap_hi)
}

fn intersect(bounds: &[(f64, Option<f64>)]) -> Option<(f64, Option<f64>)> {
    if bounds.is_empty() {
        return None;
    }
    let mut lo = f64::NEG_INFINITY;
    let mut hi = None::<f64>;
    for (blo, bhi) in bounds {
        if blo.is_finite() {
            lo = lo.max(*blo);
        }
        if let Some(h) = *bhi {
            hi = Some(hi.map_or(h, |x| x.min(h)));
        }
    }
    if !lo.is_finite() {
        return None;
    }
    if let Some(h) = hi {
        if lo > h {
            return None;
        }
        return Some((lo, Some(h)));
    }
    Some((lo, None))
}

/// Weighted median of `(value, weight)` pairs. `pairs` is sorted in place by value.
fn weighted_median(pairs: &mut [(f64, f64)]) -> f64 {
    pairs.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total: f64 = pairs.iter().map(|(_, w)| *w).sum();
    let mut acc = 0.0;
    for (v, w) in pairs.iter() {
        acc += *w;
        if acc * 2.0 >= total {
            return *v;
        }
    }
    pairs.last().map(|(v, _)| *v).unwrap_or(0.0)
}

/// Build aligned `(percent, cumulative credits)` points.
/// `obs` is `(id, used_percent)` in id order; `reqs` is `(id, credits)` of requests whose
/// tokens landed after the snapshot they share an id with (so credits at snapshot `I` sum
/// requests with `first_id ≤ req.id < I`). The first snapshot is the origin (credits = 0).
pub fn points_from(obs: &[(i64, f64)], reqs: &[(i64, f64)]) -> Vec<Point> {
    if obs.is_empty() {
        return Vec::new();
    }
    let first_id = obs[0].0;
    let mut pts = Vec::with_capacity(obs.len());
    let mut ri = 0;
    let mut cum = 0.0;
    // Requests at/after the first snapshot and before the current one.
    for &(id, pct) in obs {
        while ri < reqs.len() && reqs[ri].0 < id {
            if reqs[ri].0 >= first_id {
                cum += reqs[ri].1;
            }
            ri += 1;
        }
        pts.push(Point { pct, credits: cum });
    }
    pts
}

/// Historical-median band used to flag a capacity change (no smoothing).
pub fn trend_flag(capacity: f64, historical_median: f64) -> Option<&'static str> {
    if historical_median <= 0.0 || !capacity.is_finite() {
        return None;
    }
    let r = capacity / historical_median;
    if r < 0.7 {
        Some("down")
    } else if r > 1.4 {
        Some("up")
    } else {
        Some("ok")
    }
}

/// Median of a slice of capacities (skips non-finite).
pub fn median(xs: &[f64]) -> Option<f64> {
    let mut xs: Vec<f64> = xs
        .iter()
        .copied()
        .filter(|x| x.is_finite() && *x > 0.0)
        .collect();
    if xs.is_empty() {
        return None;
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    let n = xs.len();
    if n % 2 == 1 {
        Some(xs[n / 2])
    } else {
        Some((xs[n / 2 - 1] + xs[n / 2]) / 2.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pts(pairs: &[(f64, f64)]) -> Vec<Point> {
        pairs
            .iter()
            .map(|&(pct, credits)| Point { pct, credits })
            .collect()
    }

    #[test]
    fn clean_linear_recovers_capacity() {
        // 10 credits per percent → capacity 1000.
        let p = pts(&[(10.0, 0.0), (20.0, 100.0), (40.0, 300.0), (70.0, 600.0)]);
        let e = estimate(&p, 5.0);
        assert_eq!(e.confidence, Confidence::Ok);
        assert!(!e.coverage_gap);
        let cap = e.capacity.unwrap();
        assert!((cap - 1000.0).abs() < 1.0, "{cap}");
        assert!(e.lo.unwrap() < cap && e.hi.unwrap() > cap);
        assert!((e.credits_used - 600.0).abs() < 1e-9);
    }

    #[test]
    fn integer_quantization_still_bounded() {
        // True capacity 1000; percents rounded to integers.
        let raw = [0.4, 10.4, 25.6, 50.4];
        let creds = [0.0, 100.0, 252.0, 500.0];
        let p: Vec<Point> = raw
            .iter()
            .zip(creds)
            .map(|(&pct, credits)| Point {
                pct: f64::round(pct),
                credits,
            })
            .collect();
        let e = estimate(&p, 5.0);
        assert_eq!(e.confidence, Confidence::Ok);
        let cap = e.capacity.unwrap();
        assert!(
            (cap - 1000.0).abs() / 1000.0 < 0.05,
            "cap={cap} lo={:?} hi={:?}",
            e.lo,
            e.hi
        );
        assert!(e.lo.unwrap() <= 1000.0 && e.hi.unwrap() >= 1000.0);
    }

    #[test]
    fn unseen_usage_empties_intersection_and_flags_gap() {
        // After 20% we "miss" 200 credits of other-client usage: next visible jump is too steep.
        let p = pts(&[
            (0.0, 0.0),
            (20.0, 200.0),
            // other client used 20% (=200 credits) unseen, then we add 100
            (50.0, 300.0),
            (70.0, 500.0),
        ]);
        let e = estimate(&p, 5.0);
        assert!(e.coverage_gap, "{e:?}");
        assert!(
            e.confidence == Confidence::Fallback || e.confidence == Confidence::LowerOnly,
            "{:?}",
            e.confidence
        );
        if let Some(cap) = e.capacity {
            assert!(cap > 0.0);
        } else {
            assert!(e.lo.unwrap() > 0.0);
        }
    }

    #[test]
    fn fast_and_ultrafast_credits_scale() {
        // Same tokens, Ultrafast costs 8/2.5 = 3.2× Fast credits; capacity should scale.
        let fast = pts(&[(0.0, 0.0), (20.0, 50.0), (40.0, 100.0)]);
        let ultra = pts(&[(0.0, 0.0), (20.0, 160.0), (40.0, 320.0)]);
        let ef = estimate(&fast, 5.0);
        let eu = estimate(&ultra, 5.0);
        assert_eq!(ef.confidence, Confidence::Ok);
        assert_eq!(eu.confidence, Confidence::Ok);
        assert!((eu.capacity.unwrap() / ef.capacity.unwrap() - 3.2).abs() < 0.05);
    }

    #[test]
    fn too_small_delta_shows_nothing() {
        let p = pts(&[(10.0, 0.0), (12.0, 20.0), (14.0, 40.0)]);
        let e = estimate(&p, 5.0);
        assert_eq!(e.confidence, Confidence::Insufficient);
        assert!(e.capacity.is_none());
    }

    #[test]
    fn show_nothing_rather_than_zero() {
        let p = pts(&[(10.0, 0.0), (30.0, 0.0)]);
        let e = estimate(&p, 5.0);
        assert!(e.capacity.is_none());
        assert_ne!(e.capacity, Some(0.0));
    }

    #[test]
    fn points_align_to_snapshot_ids() {
        let obs = [(100, 10.0), (200, 20.0), (300, 40.0)];
        let reqs = [(100, 5.0), (150, 10.0), (200, 15.0), (250, 20.0)];
        let p = points_from(&obs, &reqs);
        assert_eq!(p.len(), 3);
        assert!((p[0].credits - 0.0).abs() < 1e-9);
        assert!((p[1].credits - 15.0).abs() < 1e-9); // reqs 100 and 150
        assert!((p[2].credits - 50.0).abs() < 1e-9); // +200 +250
    }

    #[test]
    fn trend_band() {
        assert_eq!(trend_flag(100.0, 100.0), Some("ok"));
        assert_eq!(trend_flag(200.0, 100.0), Some("up"));
        assert_eq!(trend_flag(50.0, 100.0), Some("down"));
        assert_eq!(trend_flag(70.0, 100.0), Some("ok"));
        assert_eq!(trend_flag(140.0, 100.0), Some("ok"));
        assert_eq!(median(&[10.0, 30.0, 20.0]), Some(20.0));
        assert_eq!(median(&[10.0, 30.0]), Some(20.0));
    }
}
