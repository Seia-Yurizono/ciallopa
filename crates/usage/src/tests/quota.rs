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

/// Output-only requests: gpt-6.1-sol is 250 credits and 25 USD per 1M output tokens, so a
/// priced request costs exactly 0.1 USD per credit.
const RATE: f64 = 0.1;
const OUT_TOKENS: i64 = 40_000;
const PRICES: &str = "[[model]]\nmodel=\"gpt-6.1-sol\"\nin=5.0\ncache=0.5\nout=25.0\n\
     [[model]]\nmodel=\"no-credits\"\nin=1000.0\ncache=1000.0\nout=1000.0\n";

/// Ten 10-credit requests on a 100-credit window, preceded by one request per `extra` model.
async fn weekly_estimate(prices: Option<&str>, extra: &[&str]) -> Estimate {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    if let Some(p) = prices {
        r.set_prices(crate::prices::PriceTable::parse(p).unwrap());
    }
    let reset = now_ms() / 1000 + 3600;
    let t0 = now_ms() - 3_600_000;
    let send = |i: i64, model: &str, pct: f64| {
        let mut x = Record::new(r.ids.next_at(t0 + i * 1000), Transport::Ws, "responses");
        x.account = Some("a".into());
        x.model = Some(model.into());
        x.status = 200;
        x.output_tokens = Some(OUT_TOKENS);
        x.quota = Some(QuotaObs {
            plan: Some("pro".into()),
            p_pct: Some(pct),
            p_reset: Some(reset),
            p_window_min: Some(10080),
            ..Default::default()
        });
        assert!(r.record_blocking(x));
    };
    // The extras come first at 0%, so they sit inside the cycle without adding an observation.
    for (i, model) in extra.iter().enumerate() {
        send(i as i64 - 100, model, 0.0);
    }
    for i in 0..10 {
        send(i, "gpt-6.1-sol", i as f64 * 10.0);
    }
    r.flush_blocking().unwrap();
    let q = r.quota().await.unwrap();
    r.shutdown();
    q[0].primary.estimate.clone()
}

#[tokio::test]
async fn capacity_is_priced_at_the_cycle_average_rate() {
    let e = weekly_estimate(Some(PRICES), &[]).await;
    let cap = e.capacity_credits.expect("credit capacity");
    let usd = e.capacity_api_usd.expect("priced capacity");
    assert!((cap - 100.0).abs() < 5.0, "{e:?}");
    assert!((usd - cap * RATE).abs() < 1e-9, "{e:?}");
    assert_eq!(e.capacity_api_lo, None, "{e:?}");
}

#[tokio::test]
async fn requests_missing_credits_or_usd_do_not_skew_the_rate() {
    // `no-credits` is priced (1000 USD/1M) without credits; gpt-5.6-luna has credits but no price.
    let e = weekly_estimate(Some(PRICES), &["no-credits", "gpt-5.6-luna"]).await;
    let (cap, usd) = (e.capacity_credits.unwrap(), e.capacity_api_usd.unwrap());
    assert!((usd - cap * RATE).abs() < 1e-9, "{e:?}");
}

#[tokio::test]
async fn capacity_has_no_api_price_without_a_paired_request() {
    // No price table at all, and a table that prices only a model without credits.
    let only_no_credits = "[[model]]\nmodel=\"no-credits\"\nin=1.0\nout=1.0\n";
    for prices in [None, Some(only_no_credits)] {
        let e = weekly_estimate(prices, &["no-credits"]).await;
        assert!(e.capacity_credits.is_some(), "{e:?}");
        assert_eq!(
            (e.capacity_api_usd, e.capacity_api_lo),
            (None, None),
            "{e:?}"
        );
    }
}

#[test]
fn lower_bound_only_capacity_writes_capacity_api_lo() {
    let mut e = crate::quota::empty_estimate("ok");
    e.capacity_lo = Some(380.0);
    crate::quota::price_capacity(&mut e, 0.04);
    assert_eq!(e.capacity_api_lo, Some(380.0 * 0.04));
    assert_eq!(e.capacity_api_usd, None);

    e.capacity_credits = Some(420.0);
    crate::quota::price_capacity(&mut e, 0.04);
    assert_eq!(e.capacity_api_usd, Some(420.0 * 0.04));
}
