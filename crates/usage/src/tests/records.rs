use super::*;

/// RUSTSEC-2026-0459 / CVE-2026-119375: the bundled SQLite must be 3.54.0 or newer.
#[test]
fn bundled_sqlite_has_wal_page_size_fix() {
    let dir = tempfile::tempdir().unwrap();
    let c = crate::schema::open_rw(&dir.path().join("u.db")).unwrap();
    let v: String = c
        .query_row("SELECT sqlite_version()", [], |r| r.get(0))
        .unwrap();
    let n: Vec<u32> = v.split('.').map(|p| p.parse().unwrap()).collect();
    assert!(n[0] > 3 || (n[0] == 3 && n[1] >= 54), "SQLite {v} < 3.54.0");
    assert_eq!(v, rusqlite::version());
}

#[test]
fn broad_scope_contains_has_no_bind_variable_limit() {
    // A broad fragment can match more names than SQLite's variable limit.
    // Keep user text in bound data, not generated SQL placeholders or literals.
    let mut db = rusqlite::Connection::open_in_memory().unwrap();
    crate::schema::migrate(&mut db).unwrap();
    let tx = db.transaction().unwrap();
    {
        let mut label = tx
            .prepare("INSERT INTO labels(id,name) VALUES (?,?)")
            .unwrap();
        let mut row = tx.prepare("INSERT INTO requests(id,model,transport,status,retries,switches,duration_ms) VALUES (?,?,1,200,0,0,1)").unwrap();
        for i in 1..=33_000i64 {
            label
                .execute(rusqlite::params![i, format!("model-{i}")])
                .unwrap();
            row.execute(rusqlite::params![(1000 << SEQ_BITS) + i, i])
                .unwrap();
        }
    }
    tx.commit().unwrap();
    let filters = Filters {
        model: Some("MODEL-".into()),
        ..Default::default()
    };
    let q = RecentQuery {
        filters: filters.clone(),
        ..Default::default()
    };
    assert_eq!(crate::filter::recent_count(&db, &q).unwrap(), 33_000);
    let sum = crate::summary::summary(
        &db,
        &SummaryQuery {
            filters,
            total_only: true,
            ..Default::default()
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert_eq!(sum.total.count, 33_000);
    // Exact internal lookups (errors, sessions, prices etc.) were not broadened.
    assert_eq!(crate::filter::label_id(&db, "model-1").unwrap(), Some(1));
    assert_eq!(crate::filter::label_id(&db, "MODEL-1").unwrap(), None);
    assert_eq!(crate::filter::label_id(&db, "model-").unwrap(), None);
}

#[test]
fn ids_are_time_ordered_and_unique() {
    let g = RequestIds::default();
    let a = g.next_at(1000);
    let b = g.next_at(1000);
    let c = g.next_at(999); // clock went backwards: still increasing
    assert!(a < b && b < c);
    assert_eq!(a >> SEQ_BITS, 1000);
}

#[test]
fn read_snapshot_keeps_labels_and_rows_together_during_write() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open(&cfg(d.path())).unwrap();
    let reader = crate::schema::open_ro(r.path()).unwrap();
    // Pin the snapshot before the writer creates new dictionary ids and rows.
    let labels: i64 = reader
        .query_row("SELECT count(*) FROM labels", [], |row| row.get(0))
        .unwrap();
    assert_eq!(labels, 0);
    let now = now_ms();
    qrec(&r, now, "new-account", "new-key", 10, 25.0, now / 1000 + 60);
    r.flush_blocking().unwrap(); // Writer commits while the reader is still open.
    let prices = PriceTable::default();
    assert!(
        crate::records::recent(&reader, &RecentQuery::default(), &prices)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        crate::summary::summary(&reader, &SummaryQuery::default(), &prices)
            .unwrap()
            .total
            .count,
        0
    );
    assert!(
        crate::quota::quota(&reader, &QuotaWeights::default(), &prices, now)
            .unwrap()
            .is_empty()
    );
    drop(reader);

    let fresh = crate::schema::open_ro(r.path()).unwrap();
    let rows = crate::records::recent(&fresh, &RecentQuery::default(), &prices).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].account.as_deref(), Some("new-account"));
    assert_eq!(rows[0].key.as_deref(), Some("new-key"));
    assert_eq!(rows[0].model.as_deref(), Some("m"));
    assert_eq!(
        crate::summary::summary(&fresh, &SummaryQuery::default(), &prices)
            .unwrap()
            .total
            .count,
        1
    );
    assert_eq!(
        crate::quota::quota(&fresh, &QuotaWeights::default(), &prices, now)
            .unwrap()
            .len(),
        1
    );
    drop(fresh);
    r.shutdown();
}

#[test]
fn model_mismatch_rule() {
    assert_eq!(model_mismatch(Some("gpt-5.5"), None), None);
    assert_eq!(model_mismatch(Some("gpt-5.5"), Some("")), None);
    assert_eq!(
        model_mismatch(Some("gpt-5.5"), Some("gpt-5.5")),
        Some(false)
    );
    assert_eq!(
        model_mismatch(Some("GPT-5.5"), Some("gpt-5.5")),
        Some(false)
    );
    assert_eq!(
        model_mismatch(Some("gpt-5.5"), Some("gpt-5.5-2026-09-30")),
        Some(false)
    );
    assert_eq!(
        model_mismatch(Some("gpt-5.5"), Some("gpt-5.6-luna")),
        Some(true)
    );
    assert_eq!(model_mismatch(None, Some("gpt-5.5")), Some(true));
    assert_eq!(
        model_mismatch(Some("gpt-5.5"), Some("gpt-5.5-mini")),
        Some(true)
    );
}

#[tokio::test]
async fn models_since_lists_models_with_tokens() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
    let t0 = now_ms() - 1000;
    let mk = |model: &str, tokens: bool| {
        let mut x = Record::new(r.ids.next_at(t0), Transport::Sse, "responses");
        x.model = Some(model.into());
        x.status = 200;
        if tokens {
            x.input_tokens = Some(10);
            x.output_tokens = Some(1);
        }
        x
    };
    assert!(r.record(mk("Gpt-A", true)));
    assert!(r.record(mk("gpt-a", true)));
    assert!(r.record(mk("err-only", false)));
    assert!(r.record(mk("gpt-b", true)));
    r.flush_blocking().unwrap();
    assert_eq!(r.models_since(0).await.unwrap(), vec!["gpt-a", "gpt-b"]);
    assert!(r.models_since(now_ms() + 1000).await.unwrap().is_empty());
    r.shutdown();
}

#[tokio::test]
async fn thread_filter_resolves_fragments_counts_and_uses_index() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
    let t0 = now_ms() - 10_000;
    // UUIDv7-like: the two sessions share their leading (timestamp) chars.
    let a = "019a2b3c-4d5e-7f00-8a1b-aaaa11112222";
    let b = "019a2b3c-4d5e-7f00-8a1b-bbbb33334444";
    let mk = |thread: Option<&str>, status: u16, dt: i64| {
        let mut x = Record::new(r.ids.next_at(t0 + dt), Transport::Sse, "responses");
        x.status = status;
        x.account = Some("alice".into());
        x.thread_id = thread.map(str::to_string);
        x.turn_id = thread.map(|_| format!("turn-{dt}"));
        x
    };
    for (i, (t, st)) in [
        (Some(a), 200),
        (Some(a), 502),
        (Some(a), 200),
        (Some(b), 200),
        (None, 200),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(r.record(mk(t, st, i as i64)));
    }
    r.flush_blocking().unwrap();
    // Exact id, unique fragment (any position, case-insensitive, pasted short form).
    assert_eq!(
        r.resolve_thread(a.into()).await.unwrap(),
        ThreadMatch::Exact(a.into())
    );
    assert_eq!(
        r.resolve_thread("aaaa1111".into()).await.unwrap(),
        ThreadMatch::Exact(a.into())
    );
    assert_eq!(
        r.resolve_thread(" …BBBB3333 ".into()).await.unwrap(),
        ThreadMatch::Exact(b.into())
    );
    // Shared prefix is ambiguous; short, unknown or odd input matches nothing.
    assert_eq!(
        r.resolve_thread("019a2b3c".into()).await.unwrap(),
        ThreadMatch::Ambiguous
    );
    assert_eq!(
        r.resolve_thread("aaaa111".into()).await.unwrap(),
        ThreadMatch::None
    );
    assert_eq!(
        r.resolve_thread("cccc9999".into()).await.unwrap(),
        ThreadMatch::None
    );
    assert_eq!(
        r.resolve_thread("aaaa%111".into()).await.unwrap(),
        ThreadMatch::None
    );
    // A label that is not a thread (an account alias) is not a session.
    let acct = r
        .recent(RecentQuery {
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap()[0]
        .account
        .clone()
        .unwrap();
    assert_eq!(r.resolve_thread(acct).await.unwrap(), ThreadMatch::None);
    let q = |t: &str| RecentQuery {
        thread: Some(t.into()),
        limit: 10,
        ..Default::default()
    };
    let rows = r.recent(q(a)).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|x| x.thread_id.as_deref() == Some(a)));
    assert!(rows.windows(2).all(|w| w[0].request_id > w[1].request_id));
    assert_eq!(r.recent_count(q(a)).await.unwrap(), 3);
    assert_eq!(r.recent_count(q("nope")).await.unwrap(), 0);
    // Composes with the problem counts and other filters.
    let c = r.problem_counts(q(a)).await.unwrap();
    assert_eq!((c.all, c.error), (3, 1));
    let ok = RecentQuery {
        outcome: Some(Outcome::Completed),
        ..q(a)
    };
    assert_eq!(r.recent_count(ok).await.unwrap(), 2);
    // The plan uses the partial index rather than scanning the id range.
    let c = crate::schema::open_ro(&d.path().join("u.db")).unwrap();
    let tid = c
        .query_row("SELECT id FROM threads WHERE name=?1", [a], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();
    let plan: Vec<String> = c
        .prepare(
            "EXPLAIN QUERY PLAN SELECT id FROM requests \
             WHERE id BETWEEN 0 AND 9223372036854775807 AND thread = ?1 ORDER BY id DESC",
        )
        .unwrap()
        .query_map([tid], |x| x.get::<_, String>(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        plan.iter().any(|p| p.contains("requests_thread")),
        "{plan:?}"
    );
    r.shutdown();
}

#[tokio::test]
async fn bucket_usage_samples_distinguish_missing_zero_and_prewarm() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(
        &UsageConfig {
            retention_days: 0,
            ..cfg(d.path())
        },
        "h",
    )
    .unwrap();
    let hour = 3_600_000;
    let base = now_ms() / hour * hour;
    for (offset, endpoint, input, output) in [
        (1, "responses", None, None),
        (2, "responses", Some(0), Some(0)),
        (3, "responses", Some(20), Some(5)),
        (3, "responses", None, None),
        (3, PREWARM_ENDPOINT, Some(0), Some(0)),
        (4, PREWARM_ENDPOINT, None, None),
    ] {
        let mut row = Record::new(
            r.ids.next_at(base + offset * hour),
            Transport::Sse,
            endpoint,
        );
        row.status = 200;
        row.input_tokens = input;
        row.output_tokens = output;
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    let summary = r
        .summary(SummaryQuery {
            from_ms: base,
            to_ms: base + 5 * hour - 1,
            bucket_day: Some(0),
            bucket_hour: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let json = serde_json::to_value(&summary).unwrap();
    let buckets = json["buckets"].as_array().unwrap();
    assert_eq!(
        buckets.len(),
        4,
        "an empty bucket is omitted, not an unknown sample"
    );
    for (index, (count, prewarm, samples, input, output)) in [
        (1, 0, 0, 0, 0),
        (1, 0, 1, 0, 0),
        (2, 1, 2, 20, 5),
        (0, 1, 0, 0, 0),
    ]
    .into_iter()
    .enumerate()
    {
        let b = &buckets[index];
        assert_eq!(b["start_ms"], base + (index as i64 + 1) * hour);
        assert_eq!(b["count"], count);
        assert_eq!(b["prewarm_count"], prewarm);
        assert_eq!(
            b["usage_samples"], samples,
            "bucket {index} usage population"
        );
        assert_eq!(b["input_tokens"], input);
        assert_eq!(b["output_tokens"], output);
    }
    assert_eq!(summary.total.usage_samples, 3);
    r.shutdown();
}

#[tokio::test]
async fn problem_filters_counts_and_rows_agree_with_classify() {
    use problem::{classify, matches, Facts};
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "h").unwrap();
    // Before any label exists: everything zero, nothing matches.
    let empty = r.problem_counts(RecentQuery::default()).await.unwrap();
    assert_eq!(empty, ProblemCounts::zero());
    assert_eq!(empty.causes["error"]["gateway"], 0);
    let base = now_ms();
    let errors: [(Option<&str>, Option<&'static str>, Option<u16>); 20] = [
        (None, None, None),
        (Some("usage_limit_reached"), Some("upstream"), Some(429)),
        (Some("all_quota_exhausted"), Some("local"), None),
        (Some("rate_limited"), Some("upstream"), Some(429)),
        (Some("upstream_status"), Some("upstream"), Some(500)),
        (Some("upstream_status"), Some("upstream"), Some(401)),
        (
            Some("invalid_encrypted_content"),
            Some("upstream"),
            Some(400),
        ),
        (Some("connect"), Some("network"), None),
        (Some("proxy_connect"), Some("network"), None),
        (Some("upstream_error_event"), Some("stream"), None),
        (Some("rate_limit_exceeded"), Some("stream"), None),
        (Some("context_length_exceeded"), Some("stream"), None),
        (
            Some("websocket_connection_limit_reached"),
            Some("stream"),
            None,
        ),
        (Some("all_cooling"), Some("local"), None),
        (Some("bad_request"), Some("local"), None),
        (Some("mystery"), None, None),
        (Some("client_closed"), Some("client"), None),
        (Some("idle_timeout"), Some("client"), None),
        (Some("observation_incomplete"), Some("local"), None),
        (Some("observation_incomplete"), Some("upstream"), None),
    ];
    let mut facts = vec![];
    for endpoint in ["responses", PREWARM_ENDPOINT] {
        for &(error, source, up) in &errors {
            // Prewarm rows only need to stay out of business counts: one status and
            // two reasons keep the list under one page.
            let statuses: &[u16] = if endpoint == PREWARM_ENDPOINT {
                &[200, 502]
            } else {
                &[200, 429, 502]
            };
            for &status in statuses {
                let reasons: &[Option<&str>] = if endpoint == PREWARM_ENDPOINT {
                    &[None, Some("max_output_tokens")]
                } else {
                    &[
                        None,
                        Some("max_output_tokens"),
                        Some("interrupted"),
                        Some("content_filter"),
                    ]
                };
                for &incomplete in reasons {
                    for (retries, switches) in [(0u32, 0u32), (1, 0), (0, 1)] {
                        facts.push((
                            endpoint, status, error, source, up, incomplete, retries, switches,
                        ));
                    }
                }
            }
        }
    }
    let mut ids = std::collections::HashMap::new();
    for &(endpoint, status, error, source, up, incomplete, retries, switches) in &facts {
        let mut row = Record::new(r.ids.next_at(base), Transport::Sse, endpoint);
        row.status = status;
        row.error = error.map(str::to_owned);
        row.err_source = source;
        row.upstream_status = up;
        row.incomplete = incomplete.map(str::to_owned);
        row.retries = retries;
        row.switches = switches;
        let f = Facts {
            status,
            error,
            source,
            upstream_status: up,
            endpoint: Some(endpoint),
            incomplete,
            retries,
            switches,
        };
        ids.insert(format!("{:016x}", row.id), classify(&f));
        assert!(r.record(row));
    }
    r.flush_blocking().unwrap();
    let all = r
        .recent(RecentQuery {
            limit: 10_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(all.len(), facts.len());
    for row in &all {
        let c = &ids[&row.request_id];
        assert_eq!(
            (
                row.problem,
                row.cause,
                row.upstream_fault,
                row.rescued,
                &row.next
            ),
            (c.problem, c.cause, c.upstream_fault, c.rescued, &c.next),
            "row {row:?}"
        );
    }
    let mut pairs: Vec<(Problem, Option<&str>)> = vec![];
    for p in Problem::ALL {
        pairs.push((p, None));
        for c in p.causes() {
            pairs.push((p, Some(*c)));
        }
    }
    let counts = r.problem_counts(RecentQuery::default()).await.unwrap();
    assert_eq!(counts.all, facts.len() as u64);
    assert_eq!(counts.refused, None, "refusals are filled by the API");
    for (p, cause) in pairs {
        let want: std::collections::BTreeSet<_> = ids
            .iter()
            .filter(|(_, c)| matches(c, p, cause))
            .map(|(id, _)| id.clone())
            .collect();
        let q = RecentQuery {
            problem: Some(p),
            cause: cause.map(str::to_owned),
            limit: 10_000,
            ..Default::default()
        };
        let got: std::collections::BTreeSet<_> = r
            .recent(q.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|x| x.request_id)
            .collect();
        assert_eq!(got, want, "problem={} cause={cause:?}", p.name());
        assert_eq!(r.recent_count(q).await.unwrap(), want.len() as u64);
        let n = want.len() as u64;
        match (p, cause) {
            (Problem::Refused, _) => assert_eq!(n, 0),
            (Problem::All, None) => assert_eq!(counts.all, n),
            (Problem::Error, None) => assert_eq!(counts.error, n),
            (Problem::Upstream, None) => assert_eq!(counts.upstream, n),
            (Problem::Unfinished, None) => assert_eq!(counts.unfinished, n),
            (_, Some(c)) => assert_eq!(counts.causes[p.name()][c], n, "{} {c}", p.name()),
        }
        if p != Problem::Refused && p != Problem::All {
            assert!(n > 0, "problem={} cause={cause:?} has no sample", p.name());
        }
    }
    // The chips partition as the design says.
    let causes = |p: &str| counts.causes[p].values().sum::<u64>();
    assert_eq!(causes("error"), counts.error);
    assert_eq!(causes("upstream"), counts.upstream);
    assert_eq!(causes("unfinished"), counts.unfinished);
    // Counts follow the other filters; prewarm is excluded on request.
    let no_prewarm = r
        .problem_counts(RecentQuery {
            exclude_prewarm: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let business = facts.iter().filter(|f| f.0 != PREWARM_ENDPOINT).count();
    assert_eq!(no_prewarm.all, business as u64);
    assert_eq!(no_prewarm.error, counts.error);
    let only_retried = r
        .problem_counts(RecentQuery {
            retried: true,
            problem: Some(Problem::Error),
            cause: Some("quota".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        only_retried.all,
        facts.iter().filter(|f| f.6 > 0 || f.7 > 0).count() as u64,
        "problem/cause are ignored by the counts"
    );
    // Combined with the legacy failure_class, both apply.
    assert_eq!(
        r.recent_count(RecentQuery {
            failure_class: Some("quota".into()),
            problem: Some(Problem::Upstream),
            ..Default::default()
        })
        .await
        .unwrap(),
        ids.values()
            .filter(|c| c.cause == Some("quota") && c.upstream_fault)
            .count() as u64
    );
    r.shutdown();
}

#[tokio::test]
async fn keyset_preserves_same_millisecond_rows_and_ignores_late_higher_ids() {
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
    let mut rows: Vec<_> = (0..4)
        .map(|_| {
            let mut x = Record::new(r.ids.next_at(base), Transport::Sse, "responses");
            x.status = 200;
            x
        })
        .collect();
    let late = rows.remove(2);
    let expected: Vec<_> = rows
        .iter()
        .rev()
        .map(|r| format!("{:016x}", r.id))
        .collect();
    for x in rows {
        assert!(r.record(x));
    }
    r.flush_blocking().unwrap();
    let q = RecentQuery {
        limit: 2,
        since_ms: Some(base),
        before_ms: Some(base + 1),
        ..Default::default()
    };
    let first = r.recent(q.clone()).await.unwrap();
    assert_eq!(
        first.iter().map(|r| &r.request_id).collect::<Vec<_>>(),
        expected[..2].iter().collect::<Vec<_>>()
    );
    // No error/source labels exist yet: NULL must never match the missing-label sentinel.
    assert_eq!(
        r.recent_count(RecentQuery {
            outcome: Some(Outcome::Completed),
            ..q.clone()
        })
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        r.recent_count(RecentQuery {
            outcome: Some(Outcome::Cancelled),
            ..q.clone()
        })
        .await
        .unwrap(),
        0
    );
    let detail = r
        .record_by_id(i64::from_str_radix(&expected[0], 16).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(!detail.has_detail);
    assert_eq!(detail, first[0]);
    assert!(r.record_by_id(0).await.unwrap().is_none());
    assert!(r.record(late));
    let mut newer = Record::new(r.ids.next_at(base + 1), Transport::Sse, "responses");
    newer.status = 200;
    assert!(r.record(newer));
    r.flush_blocking().unwrap();
    let second = r
        .recent(RecentQuery {
            before_id: Some(i64::from_str_radix(&first[1].request_id, 16).unwrap()),
            ..q.clone()
        })
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].request_id, expected[2]);
    assert_eq!(
        r.recent_count(q).await.unwrap(),
        4,
        "late completions become visible on refresh; new starts stay outside the fixed window"
    );
    r.shutdown();
}

#[test]
fn stream_error_codes_classify_by_meaning() {
    use crate::problem::{classify, Facts};
    let c = |e: &str| {
        classify(&Facts {
            status: 200,
            error: Some(e),
            source: Some("stream"),
            endpoint: Some("responses"),
            ..Default::default()
        })
    };
    let rl = c("rate_limit_exceeded");
    assert_eq!(
        (rl.outcome, rl.cause),
        (Outcome::Failed, Some("rate_limit"))
    );
    assert_eq!(rl.next, vec!["cooldown"]);
    let trl = c("turn_rate_limit_exceeded");
    assert_eq!(
        (trl.outcome, trl.cause),
        (Outcome::Failed, Some("rate_limit"))
    );
    assert_eq!(
        failure_class(200, Some("turn_rate_limit_exceeded"), Some("stream"), None),
        "rate_limit"
    );
    let rq = c("context_length_exceeded");
    assert_eq!((rq.outcome, rq.cause), (Outcome::Failed, Some("request")));
    assert!(!rq.upstream_fault);
    assert_eq!(
        failure_class(200, Some("invalid_request_error"), Some("stream"), None),
        "request"
    );
    for e in LIFECYCLE_ERRORS {
        let l = c(e);
        assert_eq!(
            (l.outcome, l.cause),
            (Outcome::Cancelled, Some("reconnect")),
            "{e}"
        );
        assert!(!l.upstream_fault);
    }
    let q = c("usage_limit");
    assert_eq!(q.cause, Some("quota"));
    assert!(q.upstream_fault);
}
