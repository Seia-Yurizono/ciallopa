use super::scaffold::*;
use super::*;

#[tokio::test]
async fn usage_key_label_instance_and_quota_api() {
    let d = tempfile::tempdir().unwrap();
    let recorder = usage_recorder(d.path());
    recorder.set_prices(
        ciallopa_usage::PriceTable::parse(
            "[[model]]\nmodel=\"gpt-5.5\"\nin=5.0\ncache=0.5\nout=30.0\nfast_x=2.5\n",
        )
        .unwrap(),
    );
    let e = env_usage(300, Some(recorder)).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "usage")
        .header("thread-id", "th-abc")
        .header(
            "x-codex-turn-metadata",
            r#"{"turn_id":"turn-1","thread_id":"th-abc"}"#,
        )
        .body(r#"{"model":"gpt-5.5","service_tier":"priority","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap();
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    let label = ciallopa_core::config::default_key_label(KEY);
    assert_eq!(u["key"], label.as_str());
    assert_eq!(u["instance"], ciallopa_core::config::hostname().as_str());
    assert_eq!(u["plan"], "plus");
    assert_eq!(u["primary_used_pct"], 12.0);
    assert!(u["primary_reset_at"].is_i64());
    assert_eq!(u["thread_id"], "th-abc");
    assert_eq!(u["turn_id"], "turn-1");
    assert_eq!(u["service_tier"], "priority");
    assert_eq!(u["cache_write_tokens"], 10);
    // (120-100-10)·5 + 100·0.5 + 10·5 (no write price ⇒ input) + 30·30, fast ×2.5
    let want = (10.0 * 5.0 + 100.0 * 0.5 + 10.0 * 5.0 + 30.0 * 30.0) * 2.5 / 1e6;
    assert!(
        (u["cost_usd"].as_f64().unwrap() - want).abs() < 1e-12,
        "{u}"
    );
    assert_eq!(u["price_status"], "ok");
    let get = |path: String| {
        let h = e.http.clone();
        async move {
            h.get(path)
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    // raw key never stored or returned
    assert!(!std::fs::read(d.path().join("usage.db"))
        .unwrap()
        .windows(KEY.len())
        .any(|w| w == KEY.as_bytes()));
    let q = get(format!("http://{}/usage/v1/quota", e.proxy)).await;
    assert!(q["note"].as_str().unwrap().contains("other clients"));
    let accts = q["accounts"].as_array().unwrap();
    assert_eq!(accts.len(), 2, "{q}");
    let a = accts.iter().find(|x| x["account"] == "a").unwrap();
    assert_eq!(a["needs_relogin"], false);
    assert_eq!(a["quota"]["primary"]["used_pct"], 12.0);
    assert_eq!(
        a["quota"]["primary"]["estimate"]["status"],
        "insufficient_data"
    );
    let b = accts.iter().find(|x| x["account"] == "b").unwrap();
    assert!(b["quota"].is_null());
}

#[tokio::test]
async fn ui_page_csp_and_toggle() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // static page needs no key and contains no data
    let r = e
        .http
        .get(format!("http://{}/ui", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().contains_key("content-security-policy"));
    assert_eq!(r.headers()["x-content-type-options"], "nosniff");
    assert_eq!(r.headers()["cache-control"], "no-store, no-transform");
    let body = r.text().await.unwrap();
    assert!(body.contains("管理面板") && !body.contains(KEY));
    let r = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .get(format!("http://{}/", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["location"], "/ui");
    // APIs the page calls stay behind the key
    let r = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    let off = env_cfg(300, None, "[ui]\nenabled = false\n").await;
    let r = off
        .http
        .get(format!("http://{}/ui", off.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn refusals_are_stored_per_minute_and_served_by_the_usage_api() {
    use tower::ServiceExt;
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    let held = held_response(&e).await;
    assert_eq!(held.status(), 200, "the first turn holds the only slot");
    for _ in 0..3 {
        let r = e
            .app
            .clone()
            .oneshot(
                http::Request::post("/v1/responses?trace=abc")
                    .header("authorization", format!("Bearer {KEY}"))
                    .header("user-agent", "codex_cli_rs/0.99 test")
                    .body(Body::from(r#"{"stream":true,"input":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 503);
    }
    assert_eq!(e.drain.rejected_by(), [3, 0, 0, 0]);
    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let get = |path: String| {
        let h = e.http.clone();
        async move {
            h.get(path)
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    let v = get(format!("http://{}/usage/v1/refusals?range=1h", e.proxy)).await;
    assert_eq!(v["totals"]["count"], 3, "{v}");
    assert_eq!(v["totals"]["inflight"], 3);
    let rows = v["rows"].as_array().unwrap();
    // One group per minute (two if the requests straddle a minute boundary).
    assert!(matches!(rows.len(), 1 | 2), "{v}");
    let n: u64 = rows.iter().map(|r| r["count"].as_u64().unwrap()).sum();
    assert_eq!(n, 3);
    let row = &rows[0];
    assert_eq!(row["reason"], "inflight");
    assert_eq!(row["transport"], "http");
    assert_eq!(row["route"], "/v1/responses", "query stripped");
    assert_eq!(row["ua"], "codex_cli_rs/0.99 test");
    assert_eq!(row["limit"], 1);
    assert_eq!(row["max_inflight"], 1);
    assert!(row["key"].is_string());
    let label = row["key"].as_str().unwrap().to_string();
    let none = get(format!(
        "http://{}/usage/v1/refusals?range=1h&reason=ws",
        e.proxy
    ))
    .await;
    assert_eq!(none["totals"]["count"], 0);
    let other = get(format!(
        "http://{}/usage/v1/refusals?range=1h&key=nobody",
        e.proxy
    ))
    .await;
    assert_eq!(other["rows"].as_array().unwrap().len(), 0);
    let s = get(format!(
        "http://{}/usage/v1/summary?range=1h&key={label}",
        e.proxy
    ))
    .await;
    assert_eq!(s["refusals"]["count"], 3, "{s}");
    let s = get(format!(
        "http://{}/usage/v1/summary?range=1h&account=x",
        e.proxy
    ))
    .await;
    assert!(
        s["refusals"].is_null(),
        "no refusal count under an account filter"
    );
    // The request list's chip counts carry the same refusals (本地拒绝) and their reasons.
    let c = get(format!(
        "http://{}/usage/v1/records/counts?range=1h&key={label}",
        e.proxy
    ))
    .await;
    assert_eq!(c["refused"], 3, "{c}");
    assert_eq!(c["causes"]["refused"]["inflight"], 3, "{c}");
    assert_eq!(c["causes"]["refused"]["ws"], 0, "{c}");
    let c = get(format!(
        "http://{}/usage/v1/records/counts?range=1h&account=x",
        e.proxy
    ))
    .await;
    assert!(c["refused"].is_null(), "{c}");
    assert!(c["causes"].get("refused").is_none(), "{c}");
    drop(held);
}

/// The sampler stores current occupancy, exact peaks between samples and the limits in
/// effect; `/usage/v1/occupancy` serves them per minute and instance.
#[tokio::test]
async fn occupancy_samples_are_stored_and_served_by_the_usage_api() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 4;
    cfg.ws_max_connections = 3;
    e.config.store(cfg);
    let (mut ws, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(r.status(), 101);
    ws.send(M::Ping(Bytes::from_static(b"ready")))
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
    // A burst between samples: two turns start and one ends before the sample.
    let a = held_response(&e).await;
    let b = held_response(&e).await;
    assert_eq!((a.status().as_u16(), b.status().as_u16()), (200, 200));
    drop(b);
    wait_inflight(&e, 1).await;
    let s = ciallopa_forward::occupancy::sample(&e.state).unwrap();
    assert_eq!(
        (s.inflight, s.peak_inflight),
        (1, 2),
        "the burst peak is kept"
    );
    assert_eq!((s.ws, s.peak_ws), (1, 1));
    assert_eq!((s.limit_inflight, s.limit_ws), (4, 3));
    rec.occupancy(s);
    ciallopa_forward::occupancy::record(&e.state);
    drop(a);
    drop(ws);
    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let get = |q: &str| {
        e.http
            .get(format!("http://{}/usage/v1/occupancy?{q}", e.proxy))
            .bearer_auth(KEY)
            .send()
    };
    let v = get("range=1h")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["step_min"], 1, "{v}");
    let rows = v["rows"].as_array().unwrap();
    // One minute (two if the samples straddle a boundary).
    assert!(matches!(rows.len(), 1 | 2), "{v}");
    let n: u64 = rows.iter().map(|r| r["samples"].as_u64().unwrap()).sum();
    assert_eq!(n, 2);
    let peak = rows
        .iter()
        .map(|r| r["inflight_peak"].as_u64().unwrap())
        .max();
    assert_eq!(peak, Some(2), "{v}");
    let row = &rows[0];
    assert_eq!(row["ws_peak"], 1);
    assert_eq!(row["max_inflight_requests"], 4);
    assert_eq!(row["ws_max_connections"], 3);
    assert!(row["instance"].is_string(), "{v}");
    let v = get("range=1h&instance=nowhere")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["rows"].as_array().unwrap().len(), 0);
    let v = get("range=7d&step=1m")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["step_min"], 7, "a week is at most 1440 buckets");
    assert_eq!(get("step=90s").await.unwrap().status(), 400);
    assert_eq!(get("range=bogus").await.unwrap().status(), 400);
}

/// 429 quota errors count as failures, and the summary tells their reason apart from other
/// upstream, network/stream and local failures (`failure_classes`), with `group=error` /
/// `group=err_source` for the codes behind them. Cancellations are not failures.
#[tokio::test]
async fn summary_breaks_failures_down_by_class_error_code_and_source() {
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let now = ciallopa_usage::now_ms() - 1_000;
    let row = |status: u16, error: Option<&str>, src: Option<&'static str>, up: Option<u16>| {
        let mut r = ciallopa_usage::Record::new(
            rec.ids.next_at(now),
            ciallopa_usage::Transport::Sse,
            "responses",
        );
        r.account = Some("a".into());
        r.model = Some("m".into());
        r.status = status;
        r.error = error.map(Into::into);
        r.err_source = src;
        r.upstream_status = up;
        assert!(rec.record(r));
    };
    // One row in the previous hour (ids are time-ordered, so it goes first).
    let mut old = ciallopa_usage::Record::new(
        rec.ids.next_at(now - 5_400_000),
        ciallopa_usage::Transport::Sse,
        "responses",
    );
    old.status = 500;
    old.error = Some("server_error".into());
    old.err_source = Some("upstream");
    old.upstream_status = Some(500);
    assert!(rec.record(old));
    row(200, None, None, None);
    row(
        429,
        Some("usage_limit_reached"),
        Some("upstream"),
        Some(429),
    );
    row(200, Some("usage_limit"), Some("stream"), None); // mid-stream quota
    row(429, Some("all_quota_exhausted"), Some("local"), None);
    row(
        429,
        Some("rate_limit_exceeded"),
        Some("upstream"),
        Some(429),
    );
    row(500, Some("server_error"), Some("upstream"), Some(500));
    row(502, Some("timeout_connect"), Some("network"), None);
    row(200, Some("stream_truncated"), Some("stream"), None);
    row(503, Some("all_cooling"), Some("local"), None);
    row(499, Some("client_closed"), Some("client"), None);
    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let get = |q: &str| {
        e.http
            .get(format!("http://{}/usage/v1/summary?range=1h&{q}", e.proxy))
            .bearer_auth(KEY)
            .send()
    };
    let r = get("group=error,err_source").await.unwrap();
    assert_eq!(r.status(), 200, "error / err_source are summary groups");
    let v = r.json::<serde_json::Value>().await.unwrap();
    let t = &v["summary"]["total"];
    assert_eq!(
        (
            t["count"].as_u64(),
            t["failures"].as_u64(),
            t["cancelled"].as_u64()
        ),
        (Some(10), Some(8), Some(1)),
        "{t}"
    );
    assert_eq!(
        t["failure_classes"],
        serde_json::json!({"quota": 3, "rate_limit": 1, "upstream": 1, "network": 1, "stream": 1, "local": 1}),
        "{t}"
    );
    let groups = v["summary"]["groups"].as_array().unwrap();
    let g = groups
        .iter()
        .find(|g| g["group"]["error"] == "all_quota_exhausted")
        .unwrap();
    assert_eq!(g["group"]["err_source"], "local");
    assert_eq!(g["failure_classes"], serde_json::json!({"quota": 1}));
    let ok = groups
        .iter()
        .find(|g| g["group"]["error"].is_null())
        .unwrap();
    assert_eq!(
        (ok["count"].as_u64(), ok["failures"].as_u64()),
        (Some(1), Some(0))
    );
    let v = get("group=err_source")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let up = v["summary"]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["group"]["err_source"] == "upstream")
        .unwrap()
        .clone();
    assert_eq!(up["failures"], 3, "{up}");
    assert_eq!(
        up["failure_classes"],
        serde_json::json!({"quota": 1, "rate_limit": 1, "upstream": 1})
    );
    // Upstream failure rate: upstream-sourced failures (incl. upstream 429s) / requests.
    assert_eq!(t["upstream_failures"], 3, "{t}");
    assert_eq!(t["upstream_failure_rate"].as_f64(), Some(0.3));

    // Previous period: same length right before `from`, total only.
    let v = get("compare=1&bucket=hour&tz=0")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let p = &v["previous"];
    assert_eq!(p["total"]["count"], 1, "{p}");
    assert_eq!(p["total"]["upstream_failures"], 1);
    assert_eq!(
        p["to"].as_i64().unwrap() - p["from"].as_i64().unwrap(),
        3_600_000
    );
    assert!(p["buckets"].is_array(), "{p}");
    assert!(v["summary"]["buckets"][0]["duration_p90_ms"].is_number());
    let v = get("")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(v["previous"].is_null(), "compare is opt-in");

    // Records: failure_class filter and per-row class.
    let rec_get = |q: &str| {
        e.http
            .get(format!("http://{}/usage/v1/records?range=1h&{q}", e.proxy))
            .bearer_auth(KEY)
            .send()
    };
    let v = rec_get("failure_class=quota")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["total"], 3, "{v}");
    let rows = v["records"].as_array().unwrap();
    assert!(rows.iter().all(|r| r["failure_class"] == "quota"));
    let r = rec_get("failure_class=bogus").await.unwrap();
    assert_eq!(r.status(), 400);
    let v = rec_get("failure_class=network&outcome=completed")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["total"], 0, "a conflicting outcome matches nothing");
}

#[tokio::test]
async fn observation_api_rejects_bad_ids_and_old_rows() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    assert_eq!(obs_of(&e, "xyz").await.0, 400);
    assert_eq!(obs_of(&e, "0000000000000001").await.0, 404);
    let r = e
        .http
        .get(format!(
            "http://{}/usage/v1/records/0000000000000001/observation",
            e.proxy
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}
