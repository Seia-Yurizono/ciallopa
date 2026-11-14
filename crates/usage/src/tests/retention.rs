use super::*;

#[test]
fn full_queue_drops_without_blocking() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path());
    c.queue_size = 4;
    c.batch_size = 1;
    let r = Recorder::open(&c).unwrap();
    let t = std::time::Instant::now();
    for _ in 0..20_000 {
        let id = r.ids.next();
        r.record(Record::new(id, Transport::Sse, "responses"));
    }
    assert!(t.elapsed() < std::time::Duration::from_secs(1));
    let s = r.stats();
    assert_eq!(s.accepted + s.dropped, 20_000);
    assert!(s.dropped > 0);
    r.flush_blocking().unwrap();
    assert_eq!(r.stats().written, r.stats().accepted);
    r.shutdown();
}

#[test]
fn retention_purge_runs_on_start() {
    let d = tempfile::tempdir().unwrap();
    let c = cfg(d.path());
    {
        let r = Recorder::open(&UsageConfig {
            retention_days: 0,
            ..c.clone()
        })
        .unwrap();
        let old = r.ids.next_at(now_ms() - 100 * 86_400_000);
        r.record(Record::new(old, Transport::Sse, "responses"));
        let new = r.ids.next();
        r.record(Record::new(new, Transport::Sse, "responses"));
        r.shutdown();
    }
    let r = Recorder::open(&c).unwrap();
    r.flush_blocking().unwrap();
    assert_eq!(r.stats().purged, 1);
    r.shutdown();
}

#[test]
fn shutdown_flushes_without_waiting_for_batch_deadline() {
    for count in [0, 2] {
        let d = tempfile::tempdir().unwrap();
        let mut c = cfg(d.path());
        c.flush_ms = 60_000;
        c.batch_size = 256;
        let r = Arc::new(Recorder::open(&c).unwrap());
        for _ in 0..count {
            assert!(r.record(Record::new(r.ids.next(), Transport::Sse, "responses")));
        }
        let (done, finished) = sync_channel(1);
        let writer = r.clone();
        let task = std::thread::spawn(move || {
            writer.shutdown();
            done.send(()).unwrap();
        });
        let stopped = finished
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok();
        task.join().unwrap();
        assert!(
            stopped,
            "shutdown waited for the batch deadline ({count} records)"
        );
        assert_eq!(r.stats().written, count);
        let conn = crate::schema::open_ro(r.path()).unwrap();
        let stored: i64 = conn
            .query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))
            .unwrap();
        assert_eq!(stored, count as i64);
    }
}

#[tokio::test]
async fn error_detail_stored_redacted_and_purged() {
    let d = tempfile::tempdir().unwrap();
    let c = UsageConfig {
        retention_days: 0,
        ..cfg(d.path())
    };
    let r = Recorder::open(&c).unwrap();
    let old_ms = now_ms() - 200 * 86_400_000;
    let mk = |ms: i64, status: u16, err: Option<&str>| {
        let mut x = Record::new(r.ids.next_at(ms), Transport::Sse, "responses");
        x.status = status;
        x.error = err.map(str::to_string);
        let mut det = ErrorDetail {
            message: Some("upstream 401 for me@corp.com".into()),
            ..Default::default()
        };
        det.set_body(
            br#"{"error":{"message":"bad token Bearer abc123"},"access_token":"eyJx.eyJy.zzz"}"#,
        );
        det.add_header("cf-ray", "ray-1");
        det.add_header("set-cookie", "s=1");
        x.detail = Some(Box::new(det));
        let id = x.id;
        assert!(r.record(x));
        id
    };
    let old = mk(old_ms, 500, Some("upstream_5xx")); // ids are monotonic: oldest first
    let ok = mk(now_ms(), 200, None); // detail ignored for successes
    let bad = mk(now_ms(), 401, Some("auth"));
    r.flush_blocking().unwrap();
    let rows = r
        .recent(RecentQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let has = |id: i64| {
        rows.iter()
            .find(|x| x.request_id == format!("{id:016x}"))
            .unwrap()
            .has_detail
    };
    assert!(!has(ok) && has(bad) && has(old));
    assert!(r.detail(ok).await.unwrap().is_none());
    let dt = r.detail(bad).await.unwrap().unwrap();
    assert_eq!(
        dt.message.as_deref(),
        Some("upstream 401 for [REDACTED_EMAIL]")
    );
    let body = dt.body.unwrap();
    assert!(!body.contains("abc123") && !body.contains("eyJx"), "{body}");
    assert_eq!(dt.headers["cf-ray"], "ray-1");
    assert!(dt.headers.get("set-cookie").is_none());
    assert!(!dt.truncated);
    r.shutdown();
    // retention 0 kept everything; 90 days purges the old row and its detail
    let r = Recorder::open(&UsageConfig {
        retention_days: 90,
        ..c.clone()
    })
    .unwrap();
    r.flush_blocking().unwrap();
    assert_eq!(r.stats().purged, 1);
    assert!(r.detail(old).await.unwrap().is_none());
    assert!(r.detail(bad).await.unwrap().is_some());
    r.shutdown();
}

#[tokio::test]
async fn observation_stored_nullable_and_purged() {
    let d = tempfile::tempdir().unwrap();
    let c = UsageConfig {
        retention_days: 0,
        ..cfg(d.path())
    };
    let r = Recorder::open(&c).unwrap();
    let mk = |ms: i64, obs: Option<Observation>| {
        let mut x = Record::new(r.ids.next_at(ms), Transport::Ws, "responses");
        x.status = 200;
        x.obs = obs.map(Box::new);
        let id = x.id;
        assert!(r.record(x));
        id
    };
    let full = Observation {
        up_first_text_ms: Some(0), // a real 0 stays 0
        up_terminal_ms: Some(42),
        up_bytes: Some(u64::MAX), // clamped, not wrapped
        response_id: Some("resp_1".into()),
        ws_sink_blocked_us: Some(7),
        down_incomplete: Some(false),
        ws_deflate_down: Some(true),
        ws_ext_down: Some("permessage-deflate".into()),
        ..Default::default()
    };
    let old = mk(now_ms() - 200 * 86_400_000, Some(full.clone()));
    let with = mk(now_ms(), Some(full.clone()));
    let none = mk(now_ms(), None);
    let empty = mk(now_ms(), Some(Observation::default()));
    r.flush_blocking().unwrap();
    let got = r.observation(with).await.unwrap().unwrap();
    assert_eq!(got.request_id, format!("{with:016x}"));
    assert_eq!(got.obs.up_first_text_ms, Some(0));
    assert_eq!(got.obs.up_bytes, Some(i64::MAX as u64));
    assert_eq!(got.obs.up_first_reasoning_ms, None);
    assert_eq!(got.obs.ws_deflate_up, None);
    assert_eq!(got.obs.down_incomplete, Some(false));
    let j = serde_json::to_value(&got).unwrap();
    assert!(j["sse_first_yield_ms"].is_null() && j["up_first_text_ms"] == 0);
    assert!(r.observation(none).await.unwrap().is_none());
    assert!(r.observation(empty).await.unwrap().is_none());
    r.shutdown();
    let r = Recorder::open(&UsageConfig {
        retention_days: 90,
        ..c.clone()
    })
    .unwrap();
    r.flush_blocking().unwrap();
    assert!(r.observation(old).await.unwrap().is_none());
    assert!(r.observation(with).await.unwrap().is_some());
    r.shutdown();
}

#[test]
fn retention_zero_keeps_forever_and_default_is_90() {
    assert_eq!(UsageConfig::default().retention_days, 90);
    let d = tempfile::tempdir().unwrap();
    let c = UsageConfig {
        retention_days: 0,
        ..cfg(d.path())
    };
    let r = Recorder::open(&c).unwrap();
    r.record(Record::new(
        r.ids.next_at(now_ms() - 3650 * 86_400_000),
        Transport::Sse,
        "responses",
    ));
    r.shutdown();
    let r = Recorder::open(&c).unwrap();
    r.flush_blocking().unwrap();
    assert_eq!(r.stats().purged, 0);
    let conn = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM requests", [], |x| x.get(0))
        .unwrap();
    assert_eq!(n, 1);
    r.shutdown();
}

#[tokio::test]
async fn purge_reclaims_threads_and_labels_then_writer_reloads_dictionary_ids() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&UsageConfig {
        retention_days: 0,
        ..cfg(d.path())
    })
    .unwrap();
    let mut row = Record::new(r.ids.next_at(now_ms() - 1000), Transport::Sse, "responses");
    row.status = 200;
    row.account = Some("account".into());
    row.model = Some("model".into());
    row.thread_id = Some("session-expired".into());
    assert!(r.record(row.clone()));
    r.flush_blocking().unwrap();
    purge_db(r.path(), now_ms(), 1).unwrap();
    let c = crate::schema::open_read(r.path()).unwrap();
    assert_eq!(
        c.query_row("SELECT count(*) FROM threads", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM labels", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    r.refuse(Refusal {
        ts_ms: now_ms(),
        reason: Some(RefusalReason::Inflight),
        route: "responses".into(),
        ua: Some("account".into()),
        ..Default::default()
    });
    r.flush_blocking().unwrap();
    let refusals = r
        .refusal_rows(RefusalQuery {
            to_ms: now_ms(),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(refusals[0].route.as_deref(), Some("responses"));
    assert_eq!(refusals[0].ua.as_deref(), Some("account"));
    row.id = r.ids.next();
    row.thread_id = Some("session-new".into());
    assert!(r.record(row));
    r.flush_blocking().unwrap();
    let rows = r
        .recent(RecentQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows[0].account.as_deref(), Some("account"));
    assert_eq!(rows[0].model.as_deref(), Some("model"));
    assert_eq!(rows[0].thread_id.as_deref(), Some("session-new"));
    r.shutdown();
}

#[tokio::test]
async fn failed_transaction_discards_rolled_back_dictionary_ids() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&cfg(d.path())).unwrap();
    let c = crate::schema::open_write(r.path()).unwrap();
    c.execute_batch("CREATE TRIGGER reject_request BEFORE INSERT ON requests BEGIN SELECT RAISE(ABORT,'injected'); END").unwrap();
    rec(&r, now_ms(), "account", "model", 1, 2, 3);
    r.flush_blocking().unwrap();
    assert_eq!(r.stats().write_errors, 1);
    c.execute_batch("DROP TRIGGER reject_request").unwrap();
    rec(&r, now_ms(), "account", "model", 1, 2, 3);
    r.flush_blocking().unwrap();
    let rows = r
        .recent(RecentQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].account.as_deref(), Some("account"));
    assert_eq!(rows[0].model.as_deref(), Some("model"));
    r.shutdown();
}
