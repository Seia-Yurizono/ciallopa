//! Process occupancy history: how full the local limits were, per minute. The proxy samples
//! its counters (cheap atomic reads) into an in-memory per-minute aggregate; the writer
//! thread stores each minute once it is complete, and everything on shutdown or an explicit
//! flush. Never blocks the proxy path.
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Proxy sampling interval.
pub const SAMPLE_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
/// Minutes kept in memory if the writer cannot store them (oldest dropped first).
const MAX_MINUTES: usize = 1440;

/// One sample: the current values, the exact peaks since the previous sample, and the
/// limits in effect (0 = unlimited).
#[derive(Debug, Clone, Copy, Default)]
pub struct Sample {
    pub ts_ms: i64,
    pub inflight: u64,
    pub ws: u64,
    pub buffered: u64,
    pub peak_inflight: u64,
    pub peak_ws: u64,
    pub peak_buffered: u64,
    pub limit_inflight: u64,
    pub limit_ws: u64,
    pub limit_buffered: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Agg {
    pub samples: u64,
    pub inflight_sum: u64,
    pub inflight_peak: u64,
    pub ws_sum: u64,
    pub ws_peak: u64,
    pub buffered_sum: u64,
    pub buffered_peak: u64,
    /// Latest limits seen in the minute.
    pub limit_inflight: u64,
    pub limit_ws: u64,
    pub limit_buffered: u64,
}
impl Agg {
    fn merge(&mut self, b: &Agg) {
        self.samples += b.samples;
        self.inflight_sum = self.inflight_sum.saturating_add(b.inflight_sum);
        self.ws_sum = self.ws_sum.saturating_add(b.ws_sum);
        self.buffered_sum = self.buffered_sum.saturating_add(b.buffered_sum);
        self.inflight_peak = self.inflight_peak.max(b.inflight_peak);
        self.ws_peak = self.ws_peak.max(b.ws_peak);
        self.buffered_peak = self.buffered_peak.max(b.buffered_peak);
        self.limit_inflight = b.limit_inflight;
        self.limit_ws = b.limit_ws;
        self.limit_buffered = b.limit_buffered;
    }
}

#[derive(Default)]
pub(crate) struct Aggregator {
    map: Mutex<BTreeMap<i64, Agg>>,
}

pub(crate) fn minute_of(ts_ms: i64) -> i64 {
    ts_ms - ts_ms.rem_euclid(60_000)
}

impl Aggregator {
    pub fn add(&self, s: Sample) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let a = map.entry(minute_of(s.ts_ms)).or_default();
        a.merge(&Agg {
            samples: 1,
            inflight_sum: s.inflight,
            inflight_peak: s.peak_inflight.max(s.inflight),
            ws_sum: s.ws,
            ws_peak: s.peak_ws.max(s.ws),
            buffered_sum: s.buffered,
            buffered_peak: s.peak_buffered.max(s.buffered),
            limit_inflight: s.limit_inflight,
            limit_ws: s.limit_ws,
            limit_buffered: s.limit_buffered,
        });
        while map.len() > MAX_MINUTES {
            map.pop_first();
        }
    }
    /// Minutes before `before_ms` (all with `None`).
    pub fn take(&self, before_ms: Option<i64>) -> BTreeMap<i64, Agg> {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        match before_ms {
            None => std::mem::take(&mut *map),
            Some(t) => {
                let keep = map.split_off(&minute_of(t));
                std::mem::replace(&mut *map, keep)
            }
        }
    }
    /// Put back minutes whose write failed, merged with samples added meanwhile.
    pub fn restore(&self, rows: BTreeMap<i64, Agg>) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        for (m, b) in rows {
            let a = map.entry(m).or_default();
            let newer = *a;
            *a = b;
            if newer.samples > 0 {
                a.merge(&newer);
            }
        }
        while map.len() > MAX_MINUTES {
            map.pop_first();
        }
    }
}

/// One bucket of `/usage/v1/occupancy` (one instance; a minute or `step` minutes).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct OccupancyRow {
    /// Bucket start (unix ms).
    pub minute_ms: i64,
    pub instance: Option<String>,
    /// Samples in the bucket (about one per second the process was running).
    pub samples: u64,
    /// Mean of the samples.
    pub inflight_avg: f64,
    /// Exact highest value in the bucket (not just at a sample).
    pub inflight_peak: u64,
    pub ws_avg: f64,
    pub ws_peak: u64,
    pub buffered_avg_bytes: f64,
    pub buffered_peak_bytes: u64,
    /// Limits in effect (the highest seen in the bucket; 0 = unlimited).
    pub max_inflight_requests: u64,
    pub ws_max_connections: u64,
    pub max_buffered_body_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(ts: i64, inflight: u64, peak: u64) -> Sample {
        Sample {
            ts_ms: ts,
            inflight,
            peak_inflight: peak,
            limit_inflight: 64,
            ..Default::default()
        }
    }

    #[test]
    fn aggregates_per_minute_and_takes_complete_minutes() {
        let a = Aggregator::default();
        a.add(s(60_000, 2, 5));
        a.add(s(61_000, 4, 4));
        a.add(s(120_500, 1, 0));
        let done = a.take(Some(121_000));
        assert_eq!(done.len(), 1);
        let m = done[&60_000];
        assert_eq!(
            (m.samples, m.inflight_sum, m.inflight_peak, m.limit_inflight),
            (2, 6, 5, 64)
        );
        let rest = a.take(None);
        assert_eq!(
            rest[&120_000].inflight_peak, 1,
            "peak is at least the sample"
        );
        a.restore(done);
        a.add(s(60_500, 9, 9));
        let m = a.take(None)[&60_000];
        assert_eq!((m.samples, m.inflight_sum, m.inflight_peak), (3, 15, 9));
    }

    #[test]
    fn memory_is_bounded() {
        let a = Aggregator::default();
        for i in 0..(MAX_MINUTES as i64 + 5) {
            a.add(s(i * 60_000, 1, 1));
        }
        let m = a.take(None);
        assert_eq!(m.len(), MAX_MINUTES);
        assert_eq!(*m.keys().next().unwrap(), 5 * 60_000, "oldest dropped");
    }
}
