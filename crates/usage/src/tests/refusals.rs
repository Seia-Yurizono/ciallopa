use super::*;

#[test]
fn refusals_merge_across_flushes_persist_on_shutdown_and_follow_retention() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&cfg(d.path())).unwrap();
    let minute = 1_700_000_040_000; // a minute boundary
    let mk = |ts_ms, inflight, key: &str| Refusal {
        ts_ms,
        reason: Some(crate::refusals::Reason::Inflight),
        key: Some(key.into()),
        route: "/v1/responses".into(),
        ua: Some("codex_cli_rs/1".into()),
        limit: 2,
        inflight,
        ..Default::default()
    };
    r.refuse(mk(minute + 1_000, 2, "a"));
    r.refuse(mk(minute + 2_000, 2, "a"));
    r.flush_blocking().unwrap();
    r.refuse(mk(minute + 30_000, 3, "a"));
    r.refuse(mk(minute + 61_000, 2, "b"));
    r.shutdown();
    let c = store::open_ro(r.path()).unwrap();
    let q = RefusalQuery {
        from_ms: minute,
        to_ms: minute + 120_000,
        limit: 10,
        ..Default::default()
    };
    let rows = store::refusal_rows(&c, &q).unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    let a = rows.iter().find(|x| x.key.as_deref() == Some("a")).unwrap();
    assert_eq!((a.minute_ms, a.count), (minute, 3), "merged across flushes");
    assert_eq!((a.first_ms, a.last_ms), (minute + 1_000, minute + 30_000));
    assert_eq!(a.max_inflight, 3);
    let t = store::refusal_totals(&c, &q).unwrap();
    assert_eq!((t.count, t.inflight), (4, 4));
    let k = store::refusal_totals(
        &c,
        &RefusalQuery {
            key: Some("b".into()),
            ..q.clone()
        },
    )
    .unwrap();
    assert_eq!(k.count, 1);
    drop(c);
    purge_db(r.path(), minute + 60_000, 100).unwrap();
    let c = store::open_ro(r.path()).unwrap();
    let rows = store::refusal_rows(&c, &q).unwrap();
    assert_eq!(rows.len(), 1, "older minute purged");
    assert_eq!(rows[0].key.as_deref(), Some("b"));
}
