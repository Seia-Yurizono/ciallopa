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
    /// Minutes in the bucket whose inflight or WebSocket peak reached its limit (0 = unlimited
    /// never counts).
    pub full_minutes: u64,
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

use crate::filter::{contains_filters, label_map};
use crate::write::WriterState;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};

/// Merge per-minute occupancy into its rows (one transaction).
pub(crate) fn upsert_occupancy(
    c: &mut Connection,
    ws: &mut WriterState,
    rows: &std::collections::BTreeMap<i64, crate::occupancy::Agg>,
) -> rusqlite::Result<()> {
    let tx = c.transaction()?;
    {
        let mut st = tx.prepare_cached(
            "INSERT INTO occupancy(minute,instance,samples,inflight_sum,inflight_peak,ws_sum,ws_peak,\
             buffered_sum,buffered_peak,lim_inflight,lim_ws,lim_buffered) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) \
             ON CONFLICT(minute,instance) DO UPDATE SET \
             samples = samples + excluded.samples, inflight_sum = inflight_sum + excluded.inflight_sum, \
             inflight_peak = max(inflight_peak, excluded.inflight_peak), ws_sum = ws_sum + excluded.ws_sum, \
             ws_peak = max(ws_peak, excluded.ws_peak), buffered_sum = buffered_sum + excluded.buffered_sum, \
             buffered_peak = max(buffered_peak, excluded.buffered_peak), lim_inflight = excluded.lim_inflight, \
             lim_ws = excluded.lim_ws, lim_buffered = excluded.lim_buffered",
        )?;
        let instance_name = ws.instance.clone();
        let instance = ws.label(&tx, Some(&instance_name))?.unwrap_or(0);
        let as_i = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        for (minute, a) in rows {
            st.execute(params![
                minute,
                instance,
                as_i(a.samples),
                as_i(a.inflight_sum),
                as_i(a.inflight_peak),
                as_i(a.ws_sum),
                as_i(a.ws_peak),
                as_i(a.buffered_sum),
                as_i(a.buffered_peak),
                as_i(a.limit_inflight),
                as_i(a.limit_ws),
                as_i(a.limit_buffered),
            ])?;
        }
    }
    tx.commit()
}

/// Range and bucket for occupancy reads.
#[derive(Debug, Clone, Default)]
pub struct OccupancyQuery {
    pub from_ms: i64,
    pub to_ms: i64,
    pub instance: Option<String>,
    /// Bucket width in minutes (at least 1).
    pub step_min: u32,
    /// UTC offset (minutes) that bucket boundaries align to, so hour / day buckets match
    /// the summary's local buckets.
    pub tz_min: i32,
}

/// Buckets per instance at most; a wider range gets a wider step.
pub const OCCUPANCY_MAX_BUCKETS: i64 = 1440;

/// The bucket width actually used for `q` (minutes).
pub fn occupancy_step(q: &OccupancyQuery) -> i64 {
    let span = (q.to_ms - q.from_ms).max(60_000) / 60_000;
    let min = (span + OCCUPANCY_MAX_BUCKETS - 1) / OCCUPANCY_MAX_BUCKETS;
    i64::from(q.step_min.max(1)).max(min)
}

pub(crate) fn occupancy_rows(
    c: &Connection,
    q: &OccupancyQuery,
) -> rusqlite::Result<Vec<crate::occupancy::OccupancyRow>> {
    let step = occupancy_step(q) * 60_000;
    // Non-negative offset modulo the step keeps the integer division floor-like.
    let off = (i64::from(q.tz_min) * 60_000).rem_euclid(step);
    let mut w = String::from(" WHERE minute > ? AND minute <= ?");
    let mut p = vec![(q.from_ms - 60_000).into(), q.to_ms.into()];
    if !contains_filters(c, &mut w, &mut p, &[("instance", &q.instance)])? {
        return Ok(vec![]);
    }
    let labels = label_map(c)?;
    let mut st = c.prepare(&format!(
        "SELECT ((minute + {off}) / {step}) * {step} - {off} AS b, instance, sum(samples), sum(inflight_sum), max(inflight_peak), \
         sum(ws_sum), max(ws_peak), sum(buffered_sum), max(buffered_peak), max(lim_inflight), max(lim_ws), \
         max(lim_buffered), \
         sum(CASE WHEN (lim_inflight > 0 AND inflight_peak >= lim_inflight) OR (lim_ws > 0 AND ws_peak >= lim_ws) THEN 1 ELSE 0 END) \
         FROM occupancy{w} GROUP BY b, instance ORDER BY b, instance"
    ))?;
    let rows = st.query_map(params_from_iter(p), |r| {
        let mut n = [0i64; 11];
        for (i, v) in n.iter_mut().enumerate() {
            *v = r.get(i + 2)?;
        }
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, n))
    })?;
    let u = |v: i64| v.max(0) as u64;
    let mut out = vec![];
    for row in rows {
        let (b, inst, n) = row?;
        let samples = u(n[0]);
        let avg = |sum: i64| {
            if samples == 0 {
                0.0
            } else {
                (u(sum) as f64 / samples as f64 * 100.0).round() / 100.0
            }
        };
        out.push(crate::occupancy::OccupancyRow {
            minute_ms: b,
            instance: (inst != 0).then(|| labels.get(&inst).cloned()).flatten(),
            samples,
            inflight_avg: avg(n[1]),
            inflight_peak: u(n[2]),
            ws_avg: avg(n[3]),
            ws_peak: u(n[4]),
            buffered_avg_bytes: avg(n[5]).round(),
            buffered_peak_bytes: u(n[6]),
            max_inflight_requests: u(n[7]),
            ws_max_connections: u(n[8]),
            max_buffered_body_bytes: u(n[9]),
            full_minutes: u(n[10]),
        });
    }
    Ok(out)
}

/// Delete occupancy minutes before `cutoff_ms`, at most a day per call. Returns true when
/// more may remain.
pub(crate) fn purge_occupancy_before(c: &Connection, cutoff_ms: i64) -> rusqlite::Result<bool> {
    let oldest: Option<i64> = c
        .query_row(
            "SELECT min(minute) FROM occupancy WHERE minute < ?1",
            [cutoff_ms],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(oldest) = oldest else {
        return Ok(false);
    };
    let until = cutoff_ms.min(oldest.saturating_add(86_400_000));
    c.execute("DELETE FROM occupancy WHERE minute < ?1", [until])?;
    Ok(until < cutoff_ms)
}
