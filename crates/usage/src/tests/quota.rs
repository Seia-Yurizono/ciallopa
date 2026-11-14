use super::*;

#[tokio::test]
async fn keys_instances_quota_and_cost() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    let t0 = now_ms() - 60_000;
    let reset = now_ms() / 1000 + 3600;
    // 10 requests, % unchanged for pairs (only changes create quota_obs rows); jitter in reset
    for i in 0..10 {
        let key = if i % 2 == 0 { "laptop" } else { "ci" };
        qrec(
            &r,
            t0 + i * 10,
            "a",
            key,
            100,
            10.0 + (i / 2) as f64 * 2.0,
            reset + (i % 3),
        );
    }
    // other account: tiny delta → insufficient data
    qrec(&r, t0, "b", "laptop", 10, 1.0, reset);
    qrec(&r, t0 + 1, "b", "laptop", 10, 2.0, reset);
    r.flush_blocking().unwrap();

    let c = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
    let obs: i64 = c
        .query_row("SELECT count(*) FROM quota_obs", [], |x| x.get(0))
        .unwrap();
    assert_eq!(obs, 5 + 2, "rows only on change (reset jitter tolerated)");

    let rows = r
        .recent(RecentQuery {
            limit: 100,
            filters: Filters {
                key: Some("ci".into()),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows
        .iter()
        .all(|x| x.key.as_deref() == Some("ci") && x.instance.as_deref() == Some("host-a")));
    assert_eq!(rows[0].primary_used_pct, Some(18.0));
    assert_eq!(rows[0].cost_usd, None, "no price table → no cost");

    let s = r
        .summary(SummaryQuery {
            from_ms: t0 - 1,
            to_ms: now_ms(),
            group_by: vec![Dim::Key, Dim::Instance],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(s.groups.len(), 2);
    let lap = s
        .groups
        .iter()
        .find(|g| g.group["key"].as_deref() == Some("laptop"))
        .unwrap();
    assert_eq!(lap.count, 7);
    assert_eq!(lap.group["instance"].as_deref(), Some("host-a"));

    // estimate: first obs at %10 (req 0) .. last at %18 (req 8): reqs 0..8 → 8 requests
    // weighted = 8 * (1.0*500 + 0.1*500 + 4.0*100) = 7600, Δ=8% → 950/%.
    let q = r.quota().await.unwrap();
    let a = q.iter().find(|x| x.account == "a").unwrap();
    assert_eq!(a.plan.as_deref(), Some("plus"));
    assert_eq!(a.primary.used_pct, Some(18.0));
    let e = &a.primary.estimate;
    assert_eq!(e.status, "ok");
    assert_eq!(e.requests, Some(8));
    assert!((e.tokens_per_pct.unwrap() - 950.0).abs() < 1e-6);
    assert!((e.capacity_tokens.unwrap() - 95_000.0).abs() < 1e-6);
    assert!((e.remaining_tokens.unwrap() - 82.0 * 950.0).abs() < 1e-6);
    assert_eq!(a.secondary.estimate.status, "no_window");
    let b = q.iter().find(|x| x.account == "b").unwrap();
    assert_eq!(b.primary.estimate.status, "insufficient_data");

    // cost hook
    let pt = PriceTable::parse("[[model]]\nmodel=\"m\"\nin=1.0\ncache=0.1\nout=10.0\n").unwrap();
    r.set_prices(pt);
    let rows = r
        .recent(RecentQuery {
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let want = (500.0 * 1.0 + 500.0 * 0.1 + 10.0 * 10.0) / 1e6;
    assert!((rows[0].cost_usd.unwrap() - want).abs() < 1e-12);
    let s = r
        .summary(SummaryQuery {
            from_ms: t0 - 1,
            to_ms: now_ms(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(s.total.cost_usd.unwrap() > 0.0);
    r.shutdown();
}
