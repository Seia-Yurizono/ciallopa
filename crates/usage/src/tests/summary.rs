use super::*;

#[tokio::test]
async fn roundtrip_dedupe_summary_purge() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&cfg(d.path())).unwrap();
    let t0 = now_ms() - 10_000;
    let id = rec(&r, t0, "a", "m1", 100, 1100, 50);
    for i in 0..9 {
        rec(&r, t0 + i, "a", "m1", 200 + i as u32 * 10, 2000, 100);
    }
    rec(&r, t0, "b", "m2", 300, 1300, 0);
    // duplicate id is ignored
    let mut dup = Record::new(id, Transport::Ws, "responses");
    dup.status = 500;
    r.record(dup);
    let mut e = Record::new(r.ids.next_at(t0), Transport::Ws, "responses");
    e.account = Some("b".into());
    e.model = Some("m2".into());
    e.status = 200;
    e.error = Some("usage_limit".into());
    r.record(e);
    r.flush_blocking().unwrap();
    let st = r.stats();
    assert_eq!(st.written, 13);
    assert_eq!(st.dropped, 0);

    let rows = r
        .recent(RecentQuery {
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 12, "duplicate ignored");
    assert!(rows.windows(2).all(|w| w[0].ts_ms >= w[1].ts_ms));
    let first = rows
        .iter()
        .find(|x| x.request_id == format!("{id:016x}"))
        .unwrap();
    assert_eq!(first.status, 200);
    assert_eq!(first.reasoning_effort.as_deref(), Some("low"));
    assert_eq!(first.tps, Some(50.0)); // 50 tok / 1.0 s
    let errs = r
        .recent(RecentQuery {
            limit: 10,
            errors_only: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].error.as_deref(), Some("usage_limit"));
    let by = r
        .recent(RecentQuery {
            limit: 10,
            filters: Filters {
                account: Some("b".into()),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(by.len(), 2);
    assert!(r
        .recent(RecentQuery {
            limit: 10,
            filters: Filters {
                account: Some("nobody".into()),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap()
        .is_empty());

    let s = r
        .summary(SummaryQuery {
            from_ms: t0 - 1,
            to_ms: now_ms(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(s.total.count, 12);
    assert_eq!(s.total.errors, 1);
    let a = s
        .groups
        .iter()
        .find(|g| g.group["account"].as_deref() == Some("a"))
        .unwrap();
    assert_eq!(a.count, 10);
    assert_eq!(a.ttft_p50_ms, Some(230)); // 100,200..280 → 5th of 10
    assert_eq!(a.ttft_p90_ms, Some(270));
    assert_eq!(a.duration_p90_ms, Some(2000));
    assert_eq!(a.output_tokens, 950);
    assert_eq!(a.cached_tokens, 40);
    assert!(a.tps.unwrap() > 50.0);
    // time range excludes everything
    let none = r
        .summary(SummaryQuery {
            from_ms: 0,
            to_ms: t0 - 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(none.total.count, 0);

    r.shutdown();
    // purge: everything older than "now" (direct store call, chunked)
    let mut c = crate::schema::open_rw(&d.path().join("u.db")).unwrap();
    let mut n = 0;
    loop {
        let k = crate::write::purge_before(&mut c, now_ms(), 5).unwrap();
        n += k;
        if k < 5 {
            break;
        }
    }
    assert_eq!(n, 12);
}

#[tokio::test]
async fn cost_tier_cache_write_unknown_and_grouping() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h1").unwrap();
    r.set_prices(
        PriceTable::parse("[[model]]\nmodel=\"m\"\nin=2.0\ncache=0.2\nwrite=2.5\nout=12.0\n")
            .unwrap(),
    );
    let t0 = now_ms() - 1000;
    let mk = |model: &str, key: &str, tier: Option<&str>| {
        let mut x = Record::new(r.ids.next_at(t0), Transport::Sse, "responses");
        x.account = Some("a".into());
        x.model = Some(model.into());
        x.key = Some(key.into());
        x.status = 200;
        x.input_tokens = Some(1000);
        x.cached_tokens = Some(500);
        x.cache_write_tokens = Some(100);
        x.output_tokens = Some(100);
        x.reasoning_tokens = Some(60);
        x.service_tier = tier.map(str::to_string);
        x.thread_id = Some("th-1".into());
        x.turn_id = Some("tu-1".into());
        x
    };
    assert!(r.record(mk("m", "k1", None)));
    assert!(r.record(mk("m", "k2", Some("priority"))));
    assert!(r.record(mk("zz", "k2", Some("flex"))));
    let mut e = Record::new(r.ids.next_at(t0), Transport::Sse, "responses");
    e.model = Some("zz".into());
    e.status = 502;
    assert!(r.record(e)); // no tokens: neither priced nor unknown
    r.flush_blocking().unwrap();
    let base = (400.0 * 2.0 + 500.0 * 0.2 + 100.0 * 2.5 + 100.0 * 12.0) / 1e6;
    let rows = r
        .recent(RecentQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let by = |k: &str, m: &str| {
        rows.iter()
            .find(|x| x.key.as_deref() == Some(k) && x.model.as_deref() == Some(m))
            .unwrap()
    };
    let a = by("k1", "m");
    assert!((a.cost_usd.unwrap() - base).abs() < 1e-12);
    assert_eq!(
        (a.thread_id.as_deref(), a.turn_id.as_deref()),
        (Some("th-1"), Some("tu-1"))
    );
    assert_eq!(a.cache_write_tokens, Some(100));
    assert!(
        (by("k2", "m").cost_usd.unwrap() - base * 2.0).abs() < 1e-12,
        "fast ×2"
    );
    let z = by("k2", "zz");
    assert_eq!((z.cost_usd, z.price_status), (None, "unknown_price"));
    assert_eq!(
        rows.iter().find(|x| x.status == 502).unwrap().price_status,
        "no_tokens"
    );

    let s = r
        .summary(SummaryQuery {
            group_by: vec![Dim::Key],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(s.total.unknown_price, 1);
    assert!((s.total.cost_usd.unwrap() - base * 3.0).abs() < 1e-12);
    assert_eq!(s.unknown_price_models, vec!["zz (flex)".to_string()]);
    let k2 = s
        .groups
        .iter()
        .find(|g| g.group["key"].as_deref() == Some("k2"))
        .unwrap();
    assert_eq!(k2.unknown_price, 1);
    assert!((k2.cost_usd.unwrap() - base * 2.0).abs() < 1e-12);
    let s = r
        .summary(SummaryQuery {
            group_by: vec![Dim::Tier],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(s
        .groups
        .iter()
        .any(|g| g.group["service_tier"].as_deref() == Some("priority")));
    // ignored ⇒ exact 0 with status "ignored": totals stay exact (no ≥), hint gone
    let mut t = r.prices().as_ref().clone();
    t.edit("zz", &crate::prices::Edit::Ignore).unwrap();
    r.set_prices(t);
    let rows = r
        .recent(RecentQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let z = rows
        .iter()
        .find(|x| x.model.as_deref() == Some("zz") && x.status != 502)
        .unwrap();
    assert_eq!((z.cost_usd, z.price_status), (Some(0.0), "ignored"));
    let s = r
        .summary(SummaryQuery {
            group_by: vec![Dim::Key],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(s.total.unknown_price, 0);
    assert!(s.unknown_price_models.is_empty());
    assert!((s.total.cost_usd.unwrap() - base * 3.0).abs() < 1e-12);
    // nothing priced ⇒ null, never 0
    r.set_prices(PriceTable::default());
    let s = r.summary(SummaryQuery::default()).await.unwrap();
    assert_eq!((s.total.cost_usd, s.total.unknown_price), (None, 3));
    r.shutdown();
}

#[tokio::test]
async fn observed_metrics_separate_outcomes_and_missing_samples() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(
        &UsageConfig {
            retention_days: 0,
            ..cfg(d.path())
        },
        "h",
    )
    .unwrap();
    let base = now_ms();
    let mut rows = Vec::new();
    let samples = [
        (
            Transport::Sse,
            Some(100),
            1100,
            Some(100),
            Some(20),
            Some(50),
        ),
        (Transport::Sse, None, 1000, Some(200), Some(40), None),
        (
            Transport::Http,
            Some(50),
            1050,
            Some(100),
            Some(10),
            Some(0),
        ),
        (Transport::Sse, Some(200), 400, None, None, None),
        (Transport::Sse, Some(300), 500, None, None, None),
        (Transport::Ws, Some(400), 1400, Some(10), Some(2), Some(0)),
        (Transport::Ws, Some(10), 20, Some(1000), Some(0), Some(1000)),
        (
            Transport::Ws,
            Some(200),
            2200,
            Some(300),
            Some(60),
            Some(150),
        ),
    ];
    for (transport, ttft, dur, input, output, cached) in samples {
        let mut x = Record::new(r.ids.next_at(base), transport, "responses");
        x.status = 200;
        x.ttft_ms = ttft;
        x.duration_ms = dur;
        x.input_tokens = input;
        x.output_tokens = output;
        x.cached_tokens = cached;
        rows.push(x);
    }
    rows[3].error = Some("upstream_error_event".into());
    rows[3].err_source = Some("stream");
    rows[4].error = Some("client_closed".into());
    rows[4].err_source = Some("client");
    rows[5].incomplete = Some("max_output_tokens".into());
    rows[6].endpoint = PREWARM_ENDPOINT;
    for row in rows {
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    let summary = r
        .summary(SummaryQuery {
            from_ms: base,
            to_ms: base,
            bucket_day: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    let t = summary.total;
    assert_eq!(
        (
            t.count,
            t.failures,
            t.cancelled,
            t.incomplete,
            t.prewarm_count
        ),
        (7, 1, 1, 1, 1)
    );
    assert_eq!(t.errors, 2, "legacy errors still include cancellation");
    assert_eq!(t.failure_rate, Some(1.0 / 7.0));
    assert_eq!(t.cancel_rate, Some(1.0 / 7.0));
    assert_eq!(
        (t.stream_ttft_samples, t.duration_samples, t.tps_samples),
        (5, 7, 2)
    );
    assert_eq!(
        (t.stream_ttft_p50_ms, t.stream_ttft_p90_ms),
        (Some(200), Some(400))
    );
    assert!((t.generation_tps.unwrap() - 80.0 / 3.0).abs() < 1e-9);
    assert_eq!(
        (t.usage_samples, t.cache_samples, t.cache_input_tokens),
        (6, 5, 1510)
    );
    assert!((t.cache_ratio.unwrap() - 1200.0 / 1510.0).abs() < 1e-9);
    assert_eq!(t.input_tokens, 1710, "prewarm usage is included");
    let b = &summary.buckets.unwrap()[0];
    assert_eq!(
        (
            b.count,
            b.failures,
            b.cancelled,
            b.incomplete,
            b.prewarm_count
        ),
        (7, 1, 1, 1, 1)
    );
    assert_eq!(b.end_ms - b.start_ms, 86_400_000);
    for (outcome, expected) in [
        (Outcome::Completed, 4),
        (Outcome::Failed, 1),
        (Outcome::Cancelled, 1),
        (Outcome::Incomplete, 1),
        (Outcome::Prewarm, 1),
    ] {
        assert_eq!(
            r.recent_count(RecentQuery {
                outcome: Some(outcome),
                ..Default::default()
            })
            .await
            .unwrap(),
            expected
        );
    }
    let business = RecentQuery {
        limit: 100,
        exclude_prewarm: true,
        ..Default::default()
    };
    assert_eq!(r.recent_count(business.clone()).await.unwrap(), t.count);
    let business_rows = r.recent(business).await.unwrap();
    assert_eq!(business_rows.len() as u64, t.count);
    assert!(business_rows
        .iter()
        .all(|row| row.endpoint.as_deref() != Some(PREWARM_ENDPOINT)));
    assert_eq!(
        r.recent_count(RecentQuery::default()).await.unwrap(),
        t.count + t.prewarm_count,
        "default browsing includes prewarm"
    );
    assert_eq!(
        r.recent_count(RecentQuery {
            exclude_prewarm: true,
            outcome: Some(Outcome::Prewarm),
            ..Default::default()
        })
        .await
        .unwrap(),
        0,
        "request filters compose as AND"
    );
    let empty = r
        .summary(SummaryQuery {
            filters: Filters {
                model: Some("absent".into()),
                ..Default::default()
            },
            bucket_day: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        (
            empty.total.failure_rate,
            empty.total.cancel_rate,
            empty.total.cache_ratio,
            empty.total.generation_tps
        ),
        (None, None, None, None)
    );
    assert_eq!(empty.total.stream_ttft_samples, 0);
    assert_eq!(empty.total.stream_ttft_p50_ms, None);
    assert_eq!(empty.buckets, Some(vec![]));
    r.shutdown();
}

#[tokio::test]
async fn no_usage_counts_finished_business_rows_without_reported_usage() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
    let base = now_ms();
    // (endpoint, status, error, source, incomplete, input, output)
    type Row<'a> = (
        &'a str,
        u16,
        Option<&'a str>,
        Option<&'static str>,
        Option<&'a str>,
        Option<i64>,
        Option<i64>,
    );
    let rows: [Row; 9] = [
        ("responses", 200, None, None, None, None, None), // counted
        (
            "responses",
            200,
            None,
            None,
            Some("max_output_tokens"),
            None,
            None,
        ), // counted
        ("responses", 200, None, None, None, Some(0), Some(0)), // explicit zero is reported
        ("responses", 200, None, None, None, Some(5), None), // input alone is reported
        ("responses", 500, None, Some("upstream"), None, None, None), // failure
        (
            "responses",
            200,
            Some("client_closed"),
            Some("client"),
            None,
            None,
            None,
        ), // cancelled
        (
            "responses",
            200,
            Some("observation_incomplete"),
            Some("local"),
            None,
            None,
            None,
        ), // unknown
        (PREWARM_ENDPOINT, 200, None, None, None, None, None), // prewarm never counts
        ("responses", 200, None, None, None, None, None), // counted
    ];
    for (endpoint, status, error, source, incomplete, input, output) in rows {
        let mut row = Record::new(r.ids.next_at(base), Transport::Sse, endpoint);
        row.status = status;
        row.error = error.map(str::to_owned);
        row.err_source = source;
        row.incomplete = incomplete.map(str::to_owned);
        row.input_tokens = input;
        row.output_tokens = output;
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    for exclude_prewarm in [false, true] {
        let s = r
            .summary(SummaryQuery {
                from_ms: base,
                to_ms: base,
                exclude_prewarm,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(s.total.no_usage, 3, "exclude_prewarm={exclude_prewarm}");
        assert_eq!(s.groups.iter().map(|g| g.no_usage).sum::<u64>(), 3);
    }
    r.shutdown();
}

#[tokio::test]
async fn unobserved_outcome_is_separate_in_queries_groups_and_buckets() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
    let base = now_ms();
    for (endpoint, error, source, incomplete) in [
        ("responses", None, None, None),
        (
            "responses",
            Some("upstream_error_event"),
            Some("stream"),
            None,
        ),
        ("responses", Some("client_closed"), Some("client"), None),
        ("responses", None, None, Some("max_output_tokens")),
        (
            "responses",
            Some("observation_incomplete"),
            Some("local"),
            None,
        ),
        (
            PREWARM_ENDPOINT,
            Some("observation_incomplete"),
            Some("local"),
            None,
        ),
    ] {
        let mut row = Record::new(r.ids.next_at(base), Transport::Sse, endpoint);
        row.status = 200;
        row.error = error.map(str::to_owned);
        row.err_source = source;
        row.incomplete = incomplete.map(str::to_owned);
        row.input_tokens = Some(10);
        row.output_tokens = Some(100);
        row.ttft_ms = Some(100);
        row.duration_ms = 1100;
        assert!(r.record(row));
    }
    // This diagnostic name is not reserved in an upstream's error vocabulary, and
    // an observed failing HTTP status must remain a failure even for a local source.
    for (source, status) in [("upstream", 400), ("upstream", 200), ("local", 500)] {
        let mut row = Record::new(r.ids.next_at(base), Transport::Sse, "responses");
        row.status = status;
        row.error = Some("observation_incomplete".into());
        row.err_source = Some(source);
        row.input_tokens = Some(10);
        row.output_tokens = Some(100);
        row.ttft_ms = Some(100);
        row.duration_ms = 1100;
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    let summary = r
        .summary(SummaryQuery {
            from_ms: base,
            to_ms: base,
            bucket_day: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    let json = serde_json::to_value(&summary).unwrap();
    for stats in [&json["total"], &json["groups"][0], &json["buckets"][0]] {
        assert_eq!(stats["count"], 8);
        assert_eq!(stats["failures"], 4);
        assert_eq!(stats["cancelled"], 1);
        assert_eq!(stats["incomplete"], 1);
        assert_eq!(stats["unknown"], 1);
        assert_eq!(stats["prewarm_count"], 1);
        assert_eq!(
            stats["errors"], 6,
            "legacy errors retain the observation diagnostic"
        );
    }
    assert_eq!(summary.total.tps_samples, 1);
    assert_eq!(summary.total.generation_tps, Some(100.0));
    assert_eq!(
        summary.total.usage_samples, 9,
        "observed usage is still counted"
    );
    for value in [
        "completed",
        "failed",
        "cancelled",
        "incomplete",
        "unknown",
        "prewarm",
    ] {
        let query = RecentQuery {
            outcome: Some(Outcome::parse(value).unwrap()),
            limit: 20,
            ..Default::default()
        };
        assert_eq!(
            r.recent_count(query.clone()).await.unwrap(),
            if value == "failed" { 4 } else { 1 },
            "outcome={value}"
        );
        assert_eq!(
            r.recent(query).await.unwrap().len(),
            if value == "failed" { 4 } else { 1 },
            "outcome={value} records"
        );
    }
    r.shutdown();
}

/// Every recorded outcome combination: the SQL behind `problem`/`cause`, the chip counts
/// and each row's classification agree with [`problem::classify`] / [`problem::matches`].
#[tokio::test]
async fn day_buckets_with_tz() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(
        &UsageConfig {
            retention_days: 0,
            ..cfg(d.path())
        },
        "h",
    )
    .unwrap();
    r.set_prices(PriceTable::parse("[[model]]\nmodel=\"m\"\nin=1.0\nout=1.0\n").unwrap());
    let day = 86_400_000i64;
    let base = (now_ms() / day - 3) * day; // 00:00 UTC three days ago
                                           // 23:30 UTC (day 0), 00:30 UTC (day 1), 01:30 UTC (day 1)
    for (off, model) in [
        (23 * 3_600_000 + 1_800_000, "m"),
        (day + 1_800_000, "m"),
        (day + 5_400_000, "zz"),
    ] {
        let mut x = Record::new(r.ids.next_at(base + off), Transport::Sse, "responses");
        x.model = Some(model.into());
        x.status = 200;
        x.input_tokens = Some(1_000_000);
        x.output_tokens = Some(10);
        assert!(r.record(x));
    }
    r.flush_blocking().unwrap();
    let q = |tz| SummaryQuery {
        from_ms: base,
        to_ms: now_ms(),
        bucket_day: Some(tz),
        ..Default::default()
    };
    let b = r.summary(q(0)).await.unwrap().buckets.unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!((b[0].count, b[1].count), (1, 2));
    assert_eq!(b[0].start_ms, base);
    assert!((b[1].cost_usd.unwrap() - 1.00001).abs() < 1e-9);
    assert_eq!(b[1].unknown_price, 1);
    assert_eq!(b[0].day, store_day(base));
    // UTC+8: all three fall on the same local day
    let b8 = r.summary(q(480)).await.unwrap().buckets.unwrap();
    assert_eq!(b8.len(), 1);
    assert_eq!(b8[0].count, 3);
    assert_eq!(b8[0].start_ms, base + day - 480 * 60_000);
    assert!(r
        .summary(SummaryQuery::default())
        .await
        .unwrap()
        .buckets
        .is_none());
    r.shutdown();
}

#[test]
fn ops_figures_upstream_rate_bucket_latency_refusals_and_failure_class_filter() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&cfg(d.path())).unwrap();
    let day = 1_700_006_400_000; // 2023-11-15 00:00 UTC
    let mk =
        |ms: i64, status: u16, error: Option<&str>, src: Option<&'static str>, up: Option<u16>| {
            let mut x = Record::new(r.ids.next_at(ms), Transport::Sse, "responses");
            x.account = Some("a".into());
            x.model = Some("m".into());
            x.status = status;
            x.error = error.map(Into::into);
            x.err_source = src;
            x.upstream_status = up;
            x.ttft_ms = Some(100);
            x.duration_ms = 1000;
            x
        };
    // Hour 0: 4 rows with durations 1000..4000, one upstream quota 429, one retried.
    let mut a = mk(day + 1_000, 200, None, None, None);
    a.retries = 1;
    a.req_tier = Some("priority".into());
    a.service_tier = Some("default".into());
    r.record(a);
    let mut b = mk(
        day + 2_000,
        429,
        Some("usage_limit_reached"),
        Some("upstream"),
        Some(429),
    );
    b.duration_ms = 2000;
    b.switches = 1;
    r.record(b);
    let mut c = mk(day + 3_000, 200, None, None, None);
    c.duration_ms = 3000;
    c.req_tier = Some("priority".into());
    c.service_tier = Some("priority".into());
    r.record(c);
    let mut e = mk(
        day + 4_000,
        502,
        Some("connect_failed"),
        Some("network"),
        None,
    );
    e.duration_ms = 4000;
    e.ttft_ms = None;
    r.record(e);
    // Hour 1: local quota exhaustion (not upstream), a mid-stream drop and a cancel.
    r.record(mk(
        day + 3_600_000,
        503,
        Some("all_quota_exhausted"),
        Some("local"),
        None,
    ));
    r.record(mk(
        day + 3_601_000,
        200,
        Some("stream_closed"),
        Some("stream"),
        None,
    ));
    r.record(mk(
        day + 3_602_000,
        499,
        Some("client_closed"),
        Some("client"),
        None,
    ));
    r.record(mk(
        day + 3_603_000,
        500,
        Some("server_error"),
        Some("upstream"),
        Some(500),
    ));
    for (ms, reason) in [
        (day + 10_000, crate::refusals::Reason::Inflight),
        (day + 20_000, crate::refusals::Reason::Inflight),
        (day + 3_650_000, crate::refusals::Reason::Ws),
    ] {
        r.refuse(Refusal {
            ts_ms: ms,
            reason: Some(reason),
            route: "/v1/responses".into(),
            limit: 1,
            ..Default::default()
        });
    }
    r.flush_blocking().unwrap();
    let c = crate::schema::open_ro(r.path()).unwrap();
    let prices = PriceTable::default();
    let q = SummaryQuery {
        from_ms: day,
        to_ms: day + 7_200_000 - 1,
        group_by: vec![Dim::Account],
        bucket_day: Some(0),
        bucket_hour: true,
        bucket_refusals: true,
        ..Default::default()
    };
    let s = crate::summary::summary(&c, &q, &prices).unwrap();
    let t = &s.total;
    assert_eq!((t.count, t.failures, t.cancelled), (8, 5, 1), "{t:?}");
    assert_eq!(t.upstream_failures, 2, "upstream quota 429 + upstream 500");
    assert_eq!(t.upstream_failure_rate, Some(0.25));
    assert_eq!((t.retried, t.switched, t.downgraded), (2, 1, 1));
    assert_eq!(s.groups[0].upstream_failures, 2);
    let bs = s.buckets.as_ref().unwrap();
    assert_eq!(bs.len(), 2, "{bs:?}");
    let h0 = &bs[0];
    assert_eq!((h0.count, h0.upstream_failures, h0.refused), (4, 1, 2));
    assert_eq!(h0.refusals.get("inflight"), Some(&2));
    assert_eq!(
        (h0.duration_p50_ms, h0.duration_p90_ms, h0.duration_p99_ms),
        (Some(2000), Some(4000), Some(4000))
    );
    assert_eq!(h0.stream_ttft_p50_ms, Some(100));
    let h1 = &bs[1];
    assert_eq!((h1.upstream_failures, h1.refused), (1, 1));
    assert_eq!(h1.refusals.get("ws"), Some(&1));
    // An account filter drops refusals (they precede account choice).
    let scoped = SummaryQuery {
        filters: Filters {
            account: Some("a".into()),
            ..Default::default()
        },
        ..q.clone()
    };
    let s2 = crate::summary::summary(&c, &scoped, &prices).unwrap();
    assert!(s2.buckets.unwrap().iter().all(|b| b.refused == 0));
    // total_only skips groups.
    let s3 = crate::summary::summary(
        &c,
        &SummaryQuery {
            total_only: true,
            ..q.clone()
        },
        &prices,
    )
    .unwrap();
    assert!(s3.groups.is_empty());
    assert_eq!(s3.total.count, 8);

    let rows = |class: &str, outcome: Option<Outcome>| {
        crate::records::recent(
            &c,
            &RecentQuery {
                limit: 50,
                failure_class: Some(class.into()),
                outcome,
                ..Default::default()
            },
            &prices,
        )
        .unwrap()
        .into_iter()
        .map(|x| x.error.unwrap_or_default())
        .collect::<Vec<_>>()
    };
    let mut quota = rows("quota", None);
    quota.sort();
    assert_eq!(quota, ["all_quota_exhausted", "usage_limit_reached"]);
    assert_eq!(rows("network", None), ["connect_failed"]);
    assert_eq!(rows("stream", None), ["stream_closed"]);
    assert_eq!(rows("upstream", None), ["server_error"]);
    assert!(
        rows("local", None).is_empty(),
        "local quota is classed as quota"
    );
    assert!(rows("rate_limit", None).is_empty());
    assert_eq!(rows("network", Some(Outcome::Failed)), ["connect_failed"]);
    assert!(rows("network", Some(Outcome::Completed)).is_empty());
    assert!(rows("bogus", None).is_empty());
    let all = crate::records::recent(
        &c,
        &RecentQuery {
            limit: 50,
            ..Default::default()
        },
        &prices,
    )
    .unwrap();
    let cls = |e: &str| {
        all.iter()
            .find(|x| x.error.as_deref() == Some(e))
            .and_then(|x| x.failure_class)
    };
    assert_eq!(cls("usage_limit_reached"), Some("quota"));
    assert_eq!(cls("client_closed"), None, "cancels are not failures");
    assert_eq!(cls("server_error"), Some("upstream"));
}

#[tokio::test]
async fn incomplete_reasons_split_interrupted_content_filter_and_truncated() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
    let base = now_ms();
    for reason in ["interrupted", "content_filter", "max_output_tokens"] {
        let mut row = Record::new(r.ids.next_at(base), Transport::Sse, "responses");
        row.status = 200;
        row.ttft_ms = Some(100);
        row.duration_ms = 1100;
        row.input_tokens = Some(10);
        row.output_tokens = Some(100);
        row.incomplete = Some(reason.into());
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    let rows = r
        .recent(RecentQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let by = |reason: &str| {
        rows.iter()
            .find(|x| x.incomplete.as_deref() == Some(reason))
            .unwrap()
    };
    // Codex treats `interrupted` as a normal end: completed, not 没答完.
    let x = by("interrupted");
    assert_eq!(
        (x.outcome, x.problem, x.cause),
        (Outcome::Completed, None, None)
    );
    let x = by("content_filter");
    assert_eq!(
        (x.outcome, x.problem, x.cause),
        (
            Outcome::Incomplete,
            Some("unfinished"),
            Some("content_filter")
        )
    );
    let x = by("max_output_tokens");
    assert_eq!(
        (x.outcome, x.problem, x.cause),
        (Outcome::Incomplete, Some("unfinished"), Some("truncated"))
    );
    for (outcome, n) in [(Outcome::Completed, 1), (Outcome::Incomplete, 2)] {
        let got = r
            .recent(RecentQuery {
                outcome: Some(outcome),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(got.len(), n, "{outcome:?}");
    }
    let counts = r.problem_counts(RecentQuery::default()).await.unwrap();
    assert_eq!(counts.unfinished, 2);
    assert_eq!(counts.causes["unfinished"]["content_filter"], 1);
    assert_eq!(counts.causes["unfinished"]["truncated"], 1);
    let summary = r
        .summary(SummaryQuery {
            from_ms: base,
            to_ms: base,
            bucket_day: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    let json = serde_json::to_value(&summary).unwrap();
    for stats in [&json["total"], &json["groups"][0], &json["buckets"][0]] {
        assert_eq!(stats["incomplete"], 2, "{stats}");
        assert_eq!(stats["failures"], 0, "{stats}");
    }
    assert_eq!(
        summary.total.tps_samples, 1,
        "only the interrupted row is a clean sample"
    );
}
