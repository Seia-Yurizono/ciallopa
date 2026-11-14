use super::*;

#[test]
fn occupancy_buckets_align_to_the_requested_utc_offset() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "pod-a").unwrap();
    // A UTC hour boundary. Minute 20 reaches the inflight limit and minute 40 the WebSocket
    // limit; 0 = unlimited.
    let hour = 1_700_002_800_000;
    for (m, peak, ws) in [(0, 1, 0), (20, 2, 0), (40, 1, 3)] {
        r.occupancy(OccupancySample {
            ts_ms: hour + m * 60_000 + 1_000,
            inflight: 1,
            peak_inflight: peak,
            ws,
            peak_ws: ws,
            limit_inflight: 2,
            limit_ws: if m == 0 { 0 } else { 3 },
            ..Default::default()
        });
    }
    r.flush_blocking().unwrap();
    let c = store::open_ro(r.path()).unwrap();
    let q = |tz_min| OccupancyQuery {
        from_ms: hour,
        to_ms: hour + 3_600_000,
        step_min: 60,
        tz_min,
        ..Default::default()
    };
    let utc = store::occupancy_rows(&c, &q(0)).unwrap();
    assert_eq!(utc.len(), 1, "{utc:?}");
    assert_eq!(
        utc[0].full_minutes, 2,
        "minutes at the inflight or WS limit"
    );
    // UTC+5:30: local hours start at :30 UTC, splitting 00/20 from 40.
    let ist = store::occupancy_rows(&c, &q(330)).unwrap();
    let starts: Vec<i64> = ist.iter().map(|x| x.minute_ms).collect();
    assert_eq!(starts, [hour - 1_800_000, hour + 1_800_000], "{ist:?}");
    let full: Vec<u64> = ist.iter().map(|x| x.full_minutes).collect();
    assert_eq!(full, [1, 1]);
}

#[test]
fn occupancy_stores_minutes_merges_flushes_buckets_and_follows_retention() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "pod-a").unwrap();
    let minute = 1_700_000_100_000; // a five-minute boundary
    let mk = |ts_ms, inflight, peak, ws, buffered| OccupancySample {
        ts_ms,
        inflight,
        peak_inflight: peak,
        ws,
        peak_ws: ws,
        buffered,
        peak_buffered: buffered,
        limit_inflight: 8,
        limit_ws: 4,
        limit_buffered: 1 << 20,
    };
    r.occupancy(mk(minute + 1_000, 2, 6, 1, 100));
    r.flush_blocking().unwrap();
    r.occupancy(mk(minute + 2_000, 4, 4, 3, 300));
    r.occupancy(mk(minute + 61_000, 1, 1, 0, 0));
    r.occupancy(mk(minute + 3 * 60_000, 0, 0, 0, 0));
    r.shutdown();
    let c = store::open_ro(r.path()).unwrap();
    let q = OccupancyQuery {
        from_ms: minute,
        to_ms: minute + 10 * 60_000,
        ..Default::default()
    };
    let rows = store::occupancy_rows(&c, &q).unwrap();
    assert_eq!(rows.len(), 3, "{rows:?}");
    let m = &rows[0];
    assert_eq!(m.minute_ms, minute);
    assert_eq!(m.instance.as_deref(), Some("pod-a"));
    assert_eq!(m.samples, 2, "merged across flushes");
    assert_eq!(
        (m.inflight_avg, m.inflight_peak),
        (3.0, 6),
        "exact peak kept"
    );
    assert_eq!((m.ws_avg, m.ws_peak), (2.0, 3));
    assert_eq!((m.buffered_avg_bytes, m.buffered_peak_bytes), (200.0, 300));
    assert_eq!(
        (
            m.max_inflight_requests,
            m.ws_max_connections,
            m.max_buffered_body_bytes
        ),
        (8, 4, 1 << 20)
    );
    // Five-minute buckets merge the first two minutes.
    let b = store::occupancy_rows(
        &c,
        &OccupancyQuery {
            step_min: 5,
            ..q.clone()
        },
    )
    .unwrap();
    assert_eq!(b.len(), 1, "{b:?}");
    assert_eq!(
        (b[0].samples, b[0].inflight_peak, b[0].inflight_avg),
        (4, 6, 1.75)
    );
    assert_eq!(b[0].minute_ms % 300_000, 0);
    let other = OccupancyQuery {
        instance: Some("pod-b".into()),
        ..q.clone()
    };
    assert!(store::occupancy_rows(&c, &other).unwrap().is_empty());
    let mine = OccupancyQuery {
        instance: Some("pod-a".into()),
        ..q.clone()
    };
    assert_eq!(store::occupancy_rows(&c, &mine).unwrap().len(), 3);
    drop(c);
    purge_db(r.path(), minute + 60_000, 100).unwrap();
    let c = store::open_ro(r.path()).unwrap();
    let rows = store::occupancy_rows(&c, &q).unwrap();
    assert_eq!(rows.len(), 2, "older minute purged");
    assert_eq!(rows[0].minute_ms, minute + 60_000);
}

#[test]
fn occupancy_step_caps_buckets() {
    let q = |hours: i64, step_min| OccupancyQuery {
        from_ms: 0,
        to_ms: hours * 3_600_000,
        step_min,
        ..Default::default()
    };
    assert_eq!(occupancy_step(&q(1, 0)), 1);
    assert_eq!(occupancy_step(&q(24, 0)), 1);
    assert_eq!(occupancy_step(&q(24 * 7, 0)), 7);
    assert_eq!(occupancy_step(&q(24 * 7, 60)), 60);
}
