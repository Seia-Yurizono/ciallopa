//! Sample drain counters and exact peaks in memory; the usage writer persists minute aggregates.
use crate::AppState;

/// Take peaks only when usage is enabled.
pub fn sample(st: &AppState) -> Option<ciallopa_usage::OccupancySample> {
    st.usage.as_ref()?;
    let d = &st.upstream.drain;
    let peaks = d.take_peaks();
    let c = st.config.load();
    Some(ciallopa_usage::OccupancySample {
        ts_ms: ciallopa_usage::now_ms(),
        inflight: d.inflight() as u64,
        ws: d.ws_connections() as u64,
        buffered: d.body_bytes(),
        peak_inflight: peaks.inflight as u64,
        peak_ws: peaks.ws as u64,
        peak_buffered: peaks.body_bytes,
        limit_inflight: c.max_inflight_requests as u64,
        limit_ws: c.ws_max_connections as u64,
        limit_buffered: c.max_buffered_body_mb.saturating_mul(1 << 20),
    })
}

pub fn record(st: &AppState) {
    if let (Some(s), Some(u)) = (sample(st), st.usage.as_ref()) {
        u.occupancy(s);
    }
}

/// Sample every [`ciallopa_usage::occupancy::SAMPLE_EVERY`] until the process exits. Does
/// nothing when the usage log is disabled.
pub fn spawn_sampler(st: AppState) -> Option<tokio::task::JoinHandle<()>> {
    st.usage.as_ref()?;
    Some(tokio::spawn(async move {
        let mut t = tokio::time::interval(ciallopa_usage::occupancy::SAMPLE_EVERY);
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            t.tick().await;
            record(&st);
        }
    }))
}
