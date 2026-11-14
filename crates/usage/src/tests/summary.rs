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
        let k = crate::write::purge_step(&mut c, now_ms(), 5)
            .unwrap()
            .removed;
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

#[tokio::test]
async fn day_incomplete_excludes_failures_cancellations_unknown_and_interrupted() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&cfg(d.path())).unwrap();
    for (status, error, source, reason, endpoint) in [
        (200, None, None, "max_output_tokens", "responses"),
        (200, None, None, "interrupted", "responses"),
        (
            200,
            Some("observation_incomplete"),
            Some("local"),
            "max_output_tokens",
            "responses",
        ),
        (
            200,
            Some("client_closed"),
            Some("client"),
            "max_output_tokens",
            "responses",
        ),
        (
            500,
            Some("upstream_5xx"),
            Some("upstream"),
            "max_output_tokens",
            "responses",
        ),
        (200, None, None, "max_output_tokens", "prewarm"),
    ] {
        let mut row = Record::new(r.ids.next(), Transport::Sse, endpoint);
        row.status = status;
        row.error = error.map(str::to_string);
        row.err_source = source;
        row.incomplete = Some(reason.into());
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    let s = r
        .summary(SummaryQuery {
            from_ms: 0,
            to_ms: now_ms(),
            bucket_day: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(s.total.unknown, 1);
    assert_eq!(s.total.incomplete, 1);
    assert_eq!(s.buckets.as_ref().unwrap()[0].incomplete, 1);
    r.shutdown();
}
#[tokio::test]
async fn outcome_matrix_agrees_across_records_filters_groups_and_buckets() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(
        &UsageConfig {
            retention_days: 0,
            ..cfg(d.path())
        },
        "h",
    )
    .unwrap();
    let base = now_ms() - 7_200_000;
    let mut cases = Vec::new();
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
    for (i, (transport, ttft, duration, input, output, cached)) in samples.into_iter().enumerate() {
        let mut row = Record::new(r.ids.next_at(base), transport, "responses");
        row.status = 200;
        row.ttft_ms = ttft;
        row.duration_ms = duration;
        row.input_tokens = input;
        row.output_tokens = output;
        row.cached_tokens = cached;
        let expected = match i {
            3 => {
                row.error = Some("upstream_error_event".into());
                row.err_source = Some("stream");
                (
                    Outcome::Failed,
                    Some("stream"),
                    Some("error"),
                    Some("stream"),
                )
            }
            4 => {
                row.error = Some("client_closed".into());
                row.err_source = Some("client");
                (
                    Outcome::Cancelled,
                    None,
                    Some("unfinished"),
                    Some("client_closed"),
                )
            }
            5 => {
                row.incomplete = Some("max_output_tokens".into());
                (
                    Outcome::Incomplete,
                    None,
                    Some("unfinished"),
                    Some("truncated"),
                )
            }
            6 => {
                row.endpoint = PREWARM_ENDPOINT;
                (Outcome::Prewarm, None, None, None)
            }
            _ => (Outcome::Completed, None, None, None),
        };
        cases.push((row, expected));
    }
    // A row for each additional outcome, missing/zero usage, source and tier boundary.
    macro_rules! case {
        ($outcome:ident,$class:expr,$problem:expr,$cause:expr; $($field:ident=$value:expr),* $(,)?) => {{
            let mut row=Record::new(r.ids.next_at(base+3_600_000+cases.len() as i64),Transport::Sse,"responses");
            row.status=200; row.ttft_ms=Some(100); row.duration_ms=1100; row.input_tokens=Some(10); row.output_tokens=Some(100);
            $(row.$field=$value;)*
            cases.push((row,(Outcome::$outcome,$class,$problem,$cause)));
        }};
    }
    case!(Completed,None,None,None; input_tokens=None,output_tokens=None);
    case!(Incomplete,None,Some("unfinished"),Some("truncated"); incomplete=Some("max_output_tokens".into()),input_tokens=None,output_tokens=None);
    case!(Completed,None,None,None; input_tokens=Some(0),output_tokens=Some(0));
    case!(Completed,None,None,None; input_tokens=Some(5),output_tokens=None);
    case!(Unknown,None,Some("unfinished"),Some("unconfirmed"); error=Some("observation_incomplete".into()),err_source=Some("local"),input_tokens=None,output_tokens=None);
    case!(Prewarm,None,None,None; endpoint=PREWARM_ENDPOINT,input_tokens=None,output_tokens=None);
    case!(Prewarm,None,None,None; endpoint=PREWARM_ENDPOINT,error=Some("observation_incomplete".into()),err_source=Some("local"));
    for status in [200, 400] {
        case!(Failed,Some("upstream"),Some("error"),Some("upstream"); status=status,error=Some("observation_incomplete".into()),err_source=Some("upstream"));
    }
    case!(Failed,Some("local"),Some("error"),Some("gateway"); status=500,error=Some("observation_incomplete".into()),err_source=Some("local"));
    case!(Completed,None,None,None; incomplete=Some("interrupted".into()));
    case!(Incomplete,None,Some("unfinished"),Some("content_filter"); incomplete=Some("content_filter".into()));
    case!(Failed,Some("quota"),Some("error"),Some("quota"); status=429,error=Some("usage_limit_reached".into()),err_source=Some("upstream"),upstream_status=Some(429),switches=1,duration_ms=2000);
    case!(Failed,Some("quota"),Some("error"),Some("quota"); status=503,error=Some("all_quota_exhausted".into()),err_source=Some("local"));
    case!(Failed,Some("network"),Some("error"),Some("network"); status=502,error=Some("connect_failed".into()),err_source=Some("network"),ttft_ms=None,duration_ms=4000);
    case!(Failed,Some("stream"),Some("error"),Some("stream"); error=Some("stream_closed".into()),err_source=Some("stream"));
    case!(Failed,Some("upstream"),Some("error"),Some("upstream"); status=500,error=Some("server_error".into()),err_source=Some("upstream"),upstream_status=Some(500));
    case!(Completed,None,None,None; retries=1,req_tier=Some("priority".into()),service_tier=Some("default".into()));
    case!(Completed,None,None,None; req_tier=Some("priority".into()),service_tier=Some("priority".into()),duration_ms=3000);
    for (i, (row, _)) in cases.iter_mut().enumerate() {
        row.account = Some("a".into());
        row.key = Some(format!("case-{i}"));
        assert!(r.record(row.clone()));
    }
    for (ms, reason) in [
        (base + 10_000, RefusalReason::Inflight),
        (base + 20_000, RefusalReason::Inflight),
        (base + 3_650_000, RefusalReason::Ws),
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
    // Preserve precise latency/cache/throughput arithmetic from the original eight samples.
    let metrics = r
        .summary(SummaryQuery {
            from_ms: base,
            to_ms: base,
            bucket_day: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    let t = &metrics.total;
    assert_eq!(
        (
            t.count,
            t.failures,
            t.cancelled,
            t.incomplete,
            t.prewarm_count,
            t.errors
        ),
        (7, 1, 1, 1, 1, 2)
    );
    assert_eq!(
        (t.failure_rate, t.cancel_rate),
        (Some(1.0 / 7.0), Some(1.0 / 7.0))
    );
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
        (
            t.usage_samples,
            t.cache_samples,
            t.cache_input_tokens,
            t.input_tokens
        ),
        (6, 5, 1510, 1710)
    );
    assert!((t.cache_ratio.unwrap() - 1200.0 / 1510.0).abs() < 1e-9);

    let query = SummaryQuery {
        from_ms: base,
        to_ms: now_ms(),
        group_by: vec![Dim::Key],
        bucket_day: Some(0),
        bucket_hour: true,
        bucket_refusals: true,
        ..Default::default()
    };
    let summary = r.summary(query.clone()).await.unwrap();
    let rows = r
        .recent(RecentQuery {
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    for (row, (outcome, class, problem, cause)) in &cases {
        let actual = rows
            .iter()
            .find(|x| x.request_id == format!("{:016x}", row.id))
            .unwrap();
        assert_eq!(
            (
                actual.outcome,
                actual.failure_class,
                actual.problem,
                actual.cause
            ),
            (*outcome, *class, *problem, *cause),
            "{row:?}"
        );
    }
    for outcome in [
        Outcome::Completed,
        Outcome::Failed,
        Outcome::Cancelled,
        Outcome::Incomplete,
        Outcome::Unknown,
        Outcome::Prewarm,
    ] {
        let expected = cases.iter().filter(|(_, e)| e.0 == outcome).count() as u64;
        let q = RecentQuery {
            outcome: Some(outcome),
            limit: 100,
            ..Default::default()
        };
        assert_eq!(
            r.recent_count(q.clone()).await.unwrap(),
            expected,
            "{outcome:?}"
        );
        assert_eq!(r.recent(q).await.unwrap().len() as u64, expected);
    }
    let business = cases
        .iter()
        .filter(|(_, e)| e.0 != Outcome::Prewarm)
        .count() as u64;
    assert_eq!(summary.total.count, business);
    assert_eq!(
        r.recent_count(RecentQuery {
            exclude_prewarm: true,
            ..Default::default()
        })
        .await
        .unwrap(),
        business
    );
    assert_eq!(
        r.recent_count(RecentQuery {
            exclude_prewarm: true,
            outcome: Some(Outcome::Prewarm),
            ..Default::default()
        })
        .await
        .unwrap(),
        0
    );
    let no_usage = cases
        .iter()
        .filter(|(r, e)| {
            matches!(e.0, Outcome::Completed | Outcome::Incomplete)
                && r.input_tokens.is_none()
                && r.output_tokens.is_none()
        })
        .count() as u64;
    assert_eq!(summary.total.no_usage, no_usage);
    let json = serde_json::to_value(&summary).unwrap();
    for (field, outcome) in [
        ("failures", Outcome::Failed),
        ("cancelled", Outcome::Cancelled),
        ("incomplete", Outcome::Incomplete),
        ("unknown", Outcome::Unknown),
        ("prewarm_count", Outcome::Prewarm),
    ] {
        let expected = cases.iter().filter(|(_, e)| e.0 == outcome).count() as u64;
        assert_eq!(json["total"][field], expected, "{field}");
    }

    for field in [
        "count",
        "errors",
        "failures",
        "upstream_failures",
        "cancelled",
        "incomplete",
        "unknown",
        "prewarm_count",
        "usage_samples",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "reasoning_tokens",
    ] {
        let sum = |items: &serde_json::Value| {
            items
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x[field].as_i64().unwrap())
                .sum::<i64>()
        };
        assert_eq!(
            sum(&json["groups"]),
            json["total"][field].as_i64().unwrap(),
            "group {field}"
        );
        assert_eq!(
            sum(&json["buckets"]),
            json["total"][field].as_i64().unwrap(),
            "bucket {field}"
        );
    }
    for field in [
        "retried",
        "switched",
        "downgraded",
        "no_usage",
        "cache_samples",
        "cache_input_tokens",
        "tps_samples",
    ] {
        let sum: i64 = json["groups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x[field].as_i64().unwrap())
            .sum();
        assert_eq!(sum, json["total"][field].as_i64().unwrap(), "{field}");
    }
    assert_eq!(
        summary.total.upstream_failure_rate,
        Some(summary.total.upstream_failures as f64 / business as f64)
    );
    let buckets = summary.buckets.as_ref().unwrap();
    assert_eq!(buckets.iter().map(|b| b.refused).sum::<u64>(), 3);
    assert_eq!(
        buckets
            .iter()
            .filter_map(|b| b.refusals.get("inflight"))
            .sum::<u64>(),
        2
    );
    assert_eq!(
        buckets
            .iter()
            .filter_map(|b| b.refusals.get("ws"))
            .sum::<u64>(),
        1
    );
    for bucket in buckets {
        let mut durations: Vec<_> = cases
            .iter()
            .filter(|(r, e)| {
                e.0 != Outcome::Prewarm && r.ts_ms() >= bucket.start_ms && r.ts_ms() < bucket.end_ms
            })
            .map(|(r, _)| r.duration_ms)
            .collect();
        durations.sort_unstable();
        for (pct, actual) in [
            (0.5, bucket.duration_p50_ms),
            (0.9, bucket.duration_p90_ms),
            (0.99, bucket.duration_p99_ms),
        ] {
            let expected = (!durations.is_empty())
                .then(|| durations[((durations.len() as f64 * pct).ceil() as usize) - 1]);
            assert_eq!(actual, expected);
        }
    }
    for class in FAILURE_CLASSES.iter().copied().chain(["bogus"]) {
        let expected = cases.iter().filter(|(_, e)| e.1 == Some(class)).count() as u64;
        assert_eq!(
            r.recent_count(RecentQuery {
                failure_class: Some(class.into()),
                ..Default::default()
            })
            .await
            .unwrap(),
            expected
        );
        assert_eq!(
            r.recent_count(RecentQuery {
                failure_class: Some(class.into()),
                outcome: Some(Outcome::Completed),
                ..Default::default()
            })
            .await
            .unwrap(),
            0
        );
    }
    for exclude_prewarm in [false, true] {
        let s = r
            .summary(SummaryQuery {
                exclude_prewarm,
                total_only: true,
                ..query.clone()
            })
            .await
            .unwrap();
        assert!(s.groups.is_empty());
        assert_eq!(s.total.no_usage, no_usage);
    }
    let scoped = r
        .summary(SummaryQuery {
            filters: Filters {
                account: Some("a".into()),
                ..Default::default()
            },
            ..query.clone()
        })
        .await
        .unwrap();
    assert!(scoped.buckets.unwrap().iter().all(|b| b.refused == 0));
    let empty = r
        .summary(SummaryQuery {
            filters: Filters {
                model: Some("absent".into()),
                ..Default::default()
            },
            ..query
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
    assert_eq!(empty.total.stream_ttft_p50_ms, None);
    assert_eq!(empty.buckets, Some(vec![]));
    r.shutdown();
}
