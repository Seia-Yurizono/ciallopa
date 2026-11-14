use super::scaffold::*;
use super::*;

#[tokio::test]
async fn cancel_before_headers_keeps_selected_account() {
    use tower::ServiceExt;
    for (path, mode, body) in [
        (
            "/v1/images/generations",
            "image_header_stall",
            r#"{"model":"gpt-image-2","prompt":"cat"}"#,
        ),
        (
            "/v1/responses",
            "header_stall_a",
            r#"{"model":"gpt-5.5","stream":true,"input":[]}"#,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let req = http::Request::post(path)
            .header("authorization", format!("Bearer {KEY}"))
            .header("x-test-mode", mode)
            .body(Body::from(body))
            .unwrap();
        let call = tokio::spawn(e.app.clone().oneshot(req));
        tokio::time::timeout(Duration::from_secs(3), e.mock.ready.notified())
            .await
            .unwrap();
        assert_eq!(rec.stats().accepted, 0);
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 1))
            .await
            .unwrap();
        assert_eq!(rows[0]["error"], "client_closed");
        assert_eq!(rows[0]["err_source"], "client");
        assert_eq!(rows[0]["status"], 0);
        assert_eq!(rows[0]["account"], "a");
        assert_eq!(rows[0]["switches"], 0);
        assert!(rows[0]["first_account"].is_null());
        let detail: serde_json::Value =
            detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
                .await
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
        assert!(
            detail["attempts"].is_null(),
            "no completed HTTP attempt: {detail}"
        );
        assert_eq!(e.drain.inflight(), 0);
        e.mock.release.notify_one();
        rec.shutdown();
    }
}

#[tokio::test]
async fn no_account_record_has_no_selected_account() {
    use tower::ServiceExt;
    let dir = tempfile::tempdir().unwrap();
    let usage = usage_recorder(dir.path());
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://127.0.0.1:9/backend-api\"\n"
    ))
    .unwrap();
    let app = ciallopa::build_app(AppState {
        accounts: Arc::new(Store::rotating(&[])),
        router: Arc::new(Rec::default()),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        config: Arc::new(cfg.into()),
        usage: Some(usage.clone()),
        egress: Default::default(),
    });
    let response = app
        .oneshot(
            http::Request::post("/v1/responses")
                .header("authorization", format!("Bearer {KEY}"))
                .body(Body::from(r#"{"stream":true,"input":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    usage.shutdown();
    let rows = usage
        .recent(ciallopa_usage::RecentQuery {
            limit: 5,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].error.as_deref(), Some("no_account"));
    assert!(rows[0].account.is_none());
    assert!(rows[0].first_account.is_none());
    assert_eq!(rows[0].switches, 0);
}

#[tokio::test]
async fn nonstream_http_body_cancel_and_force_are_not_successes() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for force in [false, true] {
        let r = e
            .post("/v1/responses")
            .header("x-test-mode", "json_body_stall")
            .json(&serde_json::json!({"model":"gpt-5.5","stream":false,"input":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        if force {
            e.drain.force();
        }
        drop(r);
        let n = if force { 2 } else { 1 };
        let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, n))
            .await
            .unwrap();
        let row = rows
            .iter()
            .find(|r| r["error"] == if force { "shutdown" } else { "client_closed" })
            .unwrap();
        assert_eq!(row["transport"], "http");
        assert_eq!(row["status"], 200);
        assert_eq!(row["err_source"], if force { "local" } else { "client" });
    }
    rec.shutdown();
}

#[tokio::test]
async fn usage_sse_rows() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "usage")
        .body(r#"{"model":"gpt-5.5","reasoning":{"effort":"low"},"input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("r-u"));
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    assert_eq!(u["account"], "a");
    assert_eq!(u["model"], "gpt-5.5");
    assert_eq!(u["reasoning_effort"], "low");
    assert_eq!(u["transport"], "sse");
    assert_eq!(u["endpoint"], "responses");
    assert_eq!(u["status"], 200);
    assert!(u["error"].is_null(), "{u}");
    assert_eq!(u["input_tokens"], 120);
    assert_eq!(u["cached_tokens"], 100);
    assert_eq!(u["output_tokens"], 30);
    assert_eq!(u["reasoning_tokens"], 12);
    assert!(u["ttft_ms"].is_u64());
    assert_eq!(u["switches"], 0);

    // quota before output → transparent switch a→b
    sse_text(&e, "midlimit").await;
    // quota after output → recorded as usage_limit
    sse_text(&e, "latelimit").await;
    // 429 on a → failover to b
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "limit")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap();
    let rows = wait_rows(&e, 4).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let (limit, late, mid) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(mid["first_account"], "a");
    assert_eq!(mid["account"], "b");
    assert_eq!(mid["switches"], 1);
    assert!(mid["error"].is_null(), "{mid}");
    assert_eq!(late["error"], "usage_limit");
    assert_eq!(limit["account"], "b");
    assert_eq!(limit["switches"], 1);
    // Rescued requests carry their attempt chain; a clean one stores no detail.
    assert_eq!(u["has_detail"], false, "{u}");
    assert_eq!(u["rescued"], false);
    assert!(u["problem"].is_null(), "{u}");
    assert_eq!(mid["rescued"], true, "{mid}");
    assert_eq!(mid["upstream_fault"], true);
    assert_eq!(mid["next"], serde_json::json!(["attempts"]));
    assert_eq!(mid["has_detail"], true, "{mid}");
    let v: serde_json::Value = detail_of(&e, mid["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 3, "{v}");
    assert_eq!(at[1]["account"], "a");
    assert_eq!(at[1]["kind"], "stream");
    assert_eq!(at[1]["code"], "usage_limit");
    assert!(at[1]["action"].as_str().unwrap().ends_with("switch"), "{v}");
    assert_eq!(at[2]["account"], "b");
    assert_eq!(at[2]["action"], "ok");
    assert_eq!(late["problem"], "error");
    assert_eq!(late["cause"], "quota");
    assert_eq!(
        late["next"],
        serde_json::json!(["refresh_quota", "account"])
    );

    let s: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/summary?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["summary"]["total"]["count"], 4);
    assert_eq!(s["summary"]["total"]["errors"], 1);
    assert_eq!(s["recorder"]["dropped"], 0);
    // API is behind the key
    let r = e
        .http
        .get(format!("http://{}/usage/v1/summary", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn usage_counts_retries() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "retry").await; // one 503, then ok on another account
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["retries"], 1, "{rows:?}");
    assert!(rows[0]["error"].is_null());
}

#[tokio::test]
async fn usage_does_not_recount_retries_after_sse_replay() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // HTTP 503 on a, SSE overload on b, then the one permitted overload replay on b.
    let text = sse_text(&e, "retry_then_overload").await;
    assert!(text.contains("response.completed"), "{text}");
    assert!(!text.contains("server_is_overloaded"), "{text}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["retries"], 1, "{rows:?}");
    assert_eq!(rows[0]["switches"], 1, "{rows:?}");
    assert_eq!(rows[0]["account"], "b", "{rows:?}");
    assert_eq!(rows[0]["first_account"], "a", "{rows:?}");
    assert!(rows[0]["error"].is_null(), "{rows:?}");
    assert_eq!(rows[0]["rescued"], true, "{rows:?}");
    let v: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let actions: Vec<_> = v["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["action"].as_str().unwrap().to_string())
        .collect();
    assert!(actions.iter().any(|a| a.ends_with("wait+retry")), "{v}");
    assert_eq!(actions.last().unwrap(), "ok", "{v}");
}

#[tokio::test]
async fn usage_ws_turn_rows() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"high"}}"#,
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"limit"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    ws.send(M::text(
        r#"{"type":"response.create","previous_response_id":"r-b","tag":"prevlimit"}"#,
    ))
    .await
    .unwrap();
    let _ = ws.next().await; // 1012 close
    let rows = wait_rows(&e, 3).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (t3, t2, t1) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(t1["transport"], "ws");
    assert_eq!(t1["reasoning_effort"], "high");
    assert_eq!(t1["account"], "a");
    assert!(t1["error"].is_null(), "{t1}");
    assert_eq!(t2["first_account"], "a");
    assert_eq!(t2["account"], "b");
    assert_eq!(t2["switches"], 1);
    assert!(t2["error"].is_null(), "{t2}");
    assert!(t2["ttft_ms"].is_u64());
    assert_eq!(t3["error"], "usage_limit");
    // A WS turn replayed on another account records its chain; a plain turn records none.
    assert_eq!(t1["has_detail"], false, "{t1}");
    assert_eq!(t2["rescued"], true, "{t2}");
    assert_eq!(t2["has_detail"], true, "{t2}");
    let v: serde_json::Value = detail_of(&e, t2["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        v["attempts"],
        serde_json::json!([
            {"account": "a", "t_ms": v["attempts"][0]["t_ms"], "kind": "stream", "code": "usage_limit", "action": "switch"},
            {"account": "b", "t_ms": v["attempts"][1]["t_ms"], "kind": "stream", "action": "ok"},
        ]),
        "{v}"
    );
}

#[tokio::test]
async fn usage_error_detail_capture_and_api() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "errbody")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let t = r.text().await.unwrap();
    assert!(t.contains("bad input"), "{t}"); // client still sees the raw upstream body
    sse_text(&e, "latelimit").await; // in-stream response.failed
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    let t = r.text().await.unwrap(); // idle timeout (1s) ends the stream with response.failed
    assert!(t.contains("event: response.failed"), "{t}");
    sse_text(&e, "usage").await; // success: no detail
    let rows = wait_rows(&e, 4).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let (ok, idle, late, bad) = (&rows[0], &rows[1], &rows[2], &rows[3]);
    assert_eq!(ok["has_detail"], false);
    assert_eq!(bad["status"], 400);
    for r in [idle, late, bad] {
        assert_eq!(r["has_detail"], true, "{r}");
    }
    let id = |r: &serde_json::Value| r["request_id"].as_str().unwrap().to_string();

    // auth required; malformed / unknown ids
    assert_eq!(detail_of(&e, &id(bad), false).await.status(), 401);
    assert_eq!(detail_of(&e, "nothex", true).await.status(), 400);
    assert_eq!(detail_of(&e, &id(ok), true).await.status(), 404);

    let v: serde_json::Value = detail_of(&e, &id(bad), true).await.json().await.unwrap();
    let raw = v.to_string();
    for secret in ["SECRETSECRET", "alice@example.com", "supersecret"] {
        assert!(!raw.contains(secret), "{secret} leaked: {raw}");
    }
    assert_eq!(v["headers"]["cf-ray"], "ray-123");
    assert_eq!(v["headers"]["x-request-id"], "req_abc");
    assert_eq!(v["headers"]["x-codex-primary-used-percent"], "40");
    assert!(v["headers"].get("set-cookie").is_none());
    assert_eq!(v["truncated"], false);
    let body: serde_json::Value = serde_json::from_str(v["body"].as_str().unwrap()).unwrap();
    // Content allowlist drops non-envelope keys (stronger than [REDACTED]).
    assert!(
        body.get("refresh_token").is_none() || body["refresh_token"] == "[REDACTED]",
        "refresh_token must be absent or redacted: {body}"
    );
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("<img src=x"),
        "stored as data, escaped by the UI"
    );

    let v: serde_json::Value = detail_of(&e, &id(late), true).await.json().await.unwrap();
    assert!(
        v["body"].as_str().unwrap().contains("usage_limit_reached"),
        "{v}"
    );
    let v: serde_json::Value = detail_of(&e, &id(idle), true).await.json().await.unwrap();
    assert!(v["message"].as_str().unwrap().contains("idle"), "{v}");
}

#[tokio::test]
async fn usage_captures_response_model_effort_and_tier() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // SSE: Fast requested (legacy "fast"), downgraded; no request effort; dated snapshot.
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "meta")
        .body(r#"{"model":"gpt-5.5","service_tier":"fast","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    r.text().await.unwrap();
    // created-only stream: auto never becomes the served tier
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "meta_created_only")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    r.text().await.unwrap();
    // WS turn: real mismatch + flex
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","service_tier":"flex","reasoning":{"effort":"high"},"tag":"meta"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    drop(ws);
    let rows = wait_rows(&e, 3).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (w, created, sse) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(sse["requested_service_tier"], "priority");
    assert_eq!(sse["service_tier"], "default");
    assert_eq!(sse["response_model"], "gpt-5.5-2026-09-30");
    assert_eq!(
        sse["response_effort"], "medium",
        "completed wins over created"
    );
    assert!(sse["reasoning_effort"].is_null());
    assert_eq!(sse["model_mismatch"], false);
    assert_eq!(created["response_model"], "gpt-5.5");
    assert_eq!(created["response_effort"], "high");
    assert!(created["service_tier"].is_null(), "auto ignored: {created}");
    assert!(created["requested_service_tier"].is_null());
    assert_eq!(w["transport"], "ws");
    assert_eq!(w["requested_service_tier"], "flex");
    assert_eq!(w["service_tier"], "flex");
    assert_eq!(w["response_model"], "gpt-5.6-luna");
    assert_eq!(w["model_mismatch"], true);
    // summary grouped by response model
    let v: serde_json::Value = e
        .http
        .get(format!(
            "http://{}/usage/v1/summary?range=1h&group=response_model",
            e.proxy
        ))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let gs = v["summary"]["groups"].as_array().unwrap();
    assert!(
        gs.iter()
            .any(|g| g["group"]["response_model"] == "gpt-5.6-luna"),
        "{v}"
    );
}

/// Item 3: response.incomplete passes through, tokens recorded, flagged, not an error, no cooldown.
#[tokio::test]
async fn incomplete_passthrough_recorded_no_cooldown() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let body = sse_text(&e, "incomplete").await;
    assert!(body.contains("response.incomplete") && body.contains("max_output_tokens"));
    let rows = wait_rows(&e, 1).await;
    let r = &rows[0];
    assert_eq!(r["incomplete"], "max_output_tokens");
    assert!(r["error"].is_null(), "{r}");
    assert_eq!(r["input_tokens"], 40);
    assert_eq!(r["output_tokens"], 16);
    assert_eq!(r["reasoning_tokens"], 4);
    let o = e.outcomes();
    assert!(
        o.iter().all(|(_, o)| matches!(o, Outcome::Success { .. })),
        "{o:?}"
    );
    // a still serves the next request (no cooldown / switch)
    let _ = sse_text(&e, "").await;
    assert!(e.outcomes().iter().all(|(a, _)| a == "a"));
}

#[tokio::test]
async fn usage_limit_text_in_output_is_not_a_quota_error() {
    // H1 / L3: only `error` / `response.failed` events count, never raw bytes.
    let e = env(300).await;
    let t = sse_text(&e, "textlimit").await;
    assert!(t.contains("usage_limit_reached") && t.contains("response.completed"));
    let o = e.outcomes();
    assert!(
        !o.iter()
            .any(|(_, o)| matches!(o, Outcome::RateLimited { .. })),
        "{o:?}"
    );
}
