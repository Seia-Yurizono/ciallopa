use super::scaffold::*;
use super::*;

#[tokio::test]
async fn configured_body_limit_has_no_hidden_twenty_mib_floor() {
    use tower::ServiceExt;
    let e = env(300).await;
    assert_eq!(e.config.load().max_body_mb, 64);
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .body(Body::from(vec![b' '; 64 * 1024 * 1024 + 1]))
        .unwrap();
    let response = e.app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), 413);
    drop(response);
    assert_eq!(e.drain.inflight(), 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn buffered_body_budget_waits_briefly_then_refuses_without_upstream() {
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_buffered_body_mb = 1;
    e.config.store(cfg);
    let mib = 1024 * 1024u64;
    // Another request's body occupies 900 KiB of the 1 MiB budget.
    let mut other = e.drain.body_permit();
    e.drain
        .reserve_body(&mut other, 900 * 1024, mib, Duration::ZERO)
        .await
        .unwrap();

    let t = std::time::Instant::now();
    let r = e
        .app
        .clone()
        .oneshot(sized_post(200 * 1024, false))
        .await
        .unwrap();
    let waited = t.elapsed();
    let json = busy_json(r).await;
    assert_eq!(json["error"]["type"], "gateway_busy");
    assert_eq!(json["error"]["message"], "Gateway is busy; retry shortly.");
    assert!(
        waited >= Duration::from_millis(900),
        "bounded wait first: {waited:?}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
    assert_eq!(e.drain.inflight(), 0);

    // A chunked body is charged as it arrives: 1.2 MiB can never fit in 1 MiB.
    drop(other);
    let r = e
        .app
        .clone()
        .oneshot(sized_post(1200 * 1024, true))
        .await
        .unwrap();
    assert_eq!(busy_json(r).await["error"]["type"], "gateway_busy");
    assert_eq!(
        e.drain.body_bytes(),
        0,
        "a refused body keeps nothing reserved"
    );

    // Room freed during the wait admits the request; its bytes are returned afterwards.
    let mut other = e.drain.body_permit();
    e.drain
        .reserve_body(&mut other, 900 * 1024, mib, Duration::ZERO)
        .await
        .unwrap();
    let app = e.app.clone();
    let pending = tokio::spawn(async move { app.oneshot(sized_post(200 * 1024, false)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(other);
    let r = pending.await.unwrap().unwrap();
    assert_eq!(r.status(), 200);
    let _ = axum::body::to_bytes(r.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(e.drain.body_bytes(), 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);

    // Chunked bodies within budget pass, and 0 = unlimited.
    let r = e
        .app
        .clone()
        .oneshot(sized_post(300 * 1024, true))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mut cfg = (*e.config.load()).clone();
    cfg.max_buffered_body_mb = 0;
    e.config.store(cfg);
    let r = e
        .app
        .clone()
        .oneshot(sized_post(1200 * 1024, false))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn declared_oversized_body_gets_neutral_json_413() {
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_body_mb = 1;
    e.config.store(cfg);
    let r = e
        .app
        .clone()
        .oneshot(sized_post(1024 * 1024 + 100, false))
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    let json: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(r.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(json["error"]["type"], "invalid_request_error");
    assert_eq!(json["error"]["message"], "Request body is too large.");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
    assert_eq!(e.drain.body_bytes(), 0);
}

#[tokio::test]
async fn admission_holds_http_body_and_hot_limit_keeps_existing_occupancy() {
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    cfg.api_keys = vec![ciallopa_core::config::ApiKeyEntry::Labeled {
        label: "admin".into(),
        key: KEY.into(),
        admin: true,
    }];
    e.config.store(cfg.clone());
    let first = held_response(&e).await;
    assert_eq!(first.status(), 200);
    assert_eq!(e.drain.inflight(), 1, "headers do not release the slot");

    let unread = Body::from_stream(futures::stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
            panic!("an overloaded request must be rejected before its body is polled")
        },
    ));
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .body(unread)
        .unwrap();
    let busy = e.app.clone().oneshot(req).await.unwrap();
    assert_eq!(busy.status(), 503);
    assert_eq!(busy.headers()["retry-after"], "3");
    assert_eq!(busy.headers()["retry-after-ms"], "3000");
    let json: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(busy.into_body(), 4096).await.unwrap())
            .unwrap();
    assert!(json.to_string().contains("gateway_busy"));
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);

    for path in ["/healthz", "/admin/v1/config", "/admin/v1/memory"] {
        let r = e
            .app
            .clone()
            .oneshot(
                http::Request::get(path)
                    .header("authorization", format!("Bearer {KEY}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{path} remains available at capacity");
        let body = axum::body::to_bytes(r.into_body(), 100_000).await.unwrap();
        if path.ends_with("config") {
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["runtime"]["inflight"], 1);
            assert_eq!(value["runtime"]["rejected"], 1);
        }
        if path.ends_with("memory") {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["buffered_body_limit_bytes"], 256u64 << 20);
            assert_eq!(v["ws_max_connections"], 24);
            assert_eq!(v["ws_max_message_bytes"], 16u64 << 20);
            assert_eq!(v["now"]["ws_connections"], 0);
            assert_eq!(v["jemalloc"], false, "tests use the system allocator");
            assert!(v["history"].is_array());
            if cfg!(target_os = "linux") {
                assert!(v["now"]["rss_bytes"].as_u64().unwrap() > 0);
            }
        }
    }
    cfg.max_inflight_requests = 0;
    e.config.store(cfg.clone());
    let second = held_response(&e).await;
    assert_eq!(second.status(), 200);
    assert_eq!(e.drain.inflight(), 2, "unlimited still counts");
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    assert_eq!(held_response(&e).await.status(), 503);
    drop(first);
    assert_eq!(
        e.drain.inflight(),
        1,
        "hot lowering did not terminate either body"
    );
    assert_eq!(held_response(&e).await.status(), 503);
    drop(second);
    assert_eq!(e.drain.inflight(), 0);
    let again = held_response(&e).await;
    assert_eq!(again.status(), 200);
    drop(again);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
}

#[tokio::test]
async fn idle_websocket_is_a_session_without_an_inflight_slot() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, response) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(response.status(), 101);
    // Ping/pong proves the detached relay has started; no turn is active.
    ws.send(M::Ping(Bytes::from_static(b"ready")))
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap(),
        Some(Ok(M::Pong(_)))
    ));
    assert_eq!(e.drain.inflight(), 0, "an idle socket holds no slot");
    assert_eq!(
        e.drain.active(),
        1,
        "but shutdown still waits for its session"
    );
    assert_eq!(held_response(&e).await.status(), 200);
    wait_inflight(&e, 0).await;
    ws.send(M::Close(None)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            // Both the WebSocket implementation and relay can answer the readiness
            // ping. A second pong may already be queued ahead of the close reply.
            match ws.next().await.unwrap().unwrap() {
                M::Close(_) => break,
                M::Pong(_) | M::Ping(_) => continue,
                other => panic!("unexpected frame while closing: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    drop(ws);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(3)).await, 0);
    assert_eq!(e.drain.inflight(), 0);
    assert_eq!(held_response(&e).await.status(), 200);

    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","input":"held"}"#))
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(output
        .to_text()
        .unwrap()
        .contains("response.output_text.delta"));
    assert_eq!(e.drain.inflight(), 1, "the running turn holds the slot");
    let drain = e.drain.clone();
    let stopped = tokio::spawn(async move {
        drain
            .shutdown(Duration::from_millis(30), Duration::from_secs(2))
            .await
    });
    // Cut mid-turn: a retryable response.failed (Codex replays after retry-after), then 1001.
    let close = loop {
        let m = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match m {
            M::Text(t) if t.contains("response.output_text.delta") => continue,
            M::Text(t) => {
                assert!(
                    t.contains("\"response.failed\"") && t.contains("gateway_restarting"),
                    "{t}"
                );
                break tokio::time::timeout(Duration::from_secs(2), ws.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }
            other => break other,
        }
    };
    assert!(matches!(close, M::Close(Some(frame)) if u16::from(frame.code) == 1001));
    drop(ws);
    assert!(stopped.await.unwrap() > 0);
    assert_eq!(e.drain.inflight(), 0);
}

#[tokio::test]
async fn ws_connection_cap_refuses_handshakes_until_one_closes() {
    use tokio_tungstenite::tungstenite::Message as M;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.ws_max_connections = 1;
    e.config.store(cfg.clone());
    let (mut first, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(r.status(), 101);
    assert_eq!(e.drain.ws_connections(), 1);
    // Over the cap: the admission 503 wording and headers, no upstream dial.
    match tokio_tungstenite::connect_async(ws_req(&e)).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => {
            assert_eq!(r.status(), 503);
            assert_eq!(r.headers()["retry-after"], "3");
            assert_eq!(r.headers()["retry-after-ms"], "3000");
            let body: serde_json::Value =
                serde_json::from_slice(r.body().as_deref().unwrap()).unwrap();
            assert_eq!(body["error"]["type"], "gateway_busy");
            assert_eq!(body["error"]["message"], "Gateway is busy; retry shortly.");
        }
        other => panic!("expected 503, got {other:?}"),
    }
    assert_eq!(e.drain.rejected(), 1);
    // HTTP is not limited by the WebSocket cap.
    assert_eq!(held_response(&e).await.status(), 200);
    // 0 = unlimited (hot, next handshake).
    cfg.ws_max_connections = 0;
    e.config.store(cfg.clone());
    let (second, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(e.drain.ws_connections(), 2);
    drop(second);
    cfg.ws_max_connections = 1;
    e.config.store(cfg);
    first.send(M::Close(None)).await.unwrap();
    while let Some(Ok(m)) = first.next().await {
        if matches!(m, M::Close(_)) {
            break;
        }
    }
    drop(first);
    tokio::time::timeout(Duration::from_secs(3), async {
        while e.drain.ws_connections() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (_third, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(r.status(), 101);
}

#[tokio::test]
async fn ws_message_over_the_limit_closes_1009_even_when_compressed() {
    use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode, Message as M};
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.ws_max_message_mb = 1;
    e.config.store(cfg);
    let msg = |len: usize| {
        let head = r#"{"type":"response.create","model":"gpt-5.5","pad":""#;
        M::text(format!("{head}{}\"}}", "x".repeat(len - head.len() - 2)))
    };
    async fn closed_with<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Option<CloseCode>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .unwrap()
            {
                Some(Ok(M::Close(f))) => return f.map(|f| f.code),
                Some(Ok(_)) => continue,
                _ => return None,
            }
        }
    }
    // Plain: just under the limit passes, just over closes with 1009.
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(msg(1024 * 1024 - 64)).await.unwrap();
    let got = ws.next().await.unwrap().unwrap();
    assert!(
        got.to_text().unwrap().contains("response.completed"),
        "{got:?}"
    );
    ws.send(msg(1024 * 1024 + 1)).await.unwrap();
    assert_eq!(closed_with(&mut ws).await, Some(CloseCode::Size));
    // Deflated: a 4 MiB message that compresses to a few KiB is still refused by size.
    let (mut ws, _) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    ws.send(msg(4 * 1024 * 1024)).await.unwrap();
    assert_eq!(closed_with(&mut ws).await, Some(CloseCode::Size));
    tokio::time::timeout(Duration::from_secs(3), async {
        while e.drain.ws_connections() > 0 || e.drain.body_bytes() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn admission_force_cancels_headers_and_unpolled_sse_with_shutdown_records() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let held = held_response(&e).await;
    assert_eq!(held.status(), 200);
    let headers = tokio::spawn(
        e.post("/v1/responses")
            .header("x-test-mode", "header_stall_a")
            .body(r#"{"stream":true,"input":[]}"#)
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    assert_eq!(e.drain.inflight(), 2);
    let drain = e.drain.clone();
    let stopped = tokio::spawn(async move {
        drain
            .shutdown(Duration::from_millis(30), Duration::from_millis(100))
            .await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), headers)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        503
    );
    assert!(stopped.await.unwrap() > 0);
    // No poll of held's body: force must cancel its detached producer on its own.
    let rows = tokio::time::timeout(Duration::from_secs(3), wait_rows(&e, 2))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row["error"], "shutdown", "{row}");
        assert_eq!(row["err_source"], "local", "{row}");
        assert_eq!(row["account"], "a", "{row}");
    }
    drop(held);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
    e.mock.release.notify_one();
    rec.shutdown();
}

#[tokio::test]
async fn websocket_messages_share_the_buffered_body_budget() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_buffered_body_mb = 1;
    e.config.store(cfg);
    let mib = 1024 * 1024u64;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let msg = |tag: &str, len: usize| {
        let head = format!(r#"{{"type":"response.create","tag":"{tag}","pad":""#);
        M::text(format!("{head}{}\"}}", "x".repeat(len - head.len() - 2)))
    };
    async fn text(
        ws: &mut (impl futures::Stream<
            Item = Result<
                tokio_tungstenite::tungstenite::Message,
                tokio_tungstenite::tungstenite::Error,
            >,
        > + Unpin),
    ) -> String {
        loop {
            match ws.next().await.unwrap().unwrap() {
                M::Text(t) => return t.as_str().to_string(),
                M::Ping(_) | M::Pong(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    // Another request holds 900 KiB of the 1 MiB budget: a 200 KiB message waits about 1 s,
    // then gets the HTTP busy wording as a wrapped 503 error event; it is not forwarded.
    let mut other = e.drain.body_permit();
    e.drain
        .reserve_body(&mut other, 900 * 1024, mib, Duration::ZERO)
        .await
        .unwrap();
    let t = std::time::Instant::now();
    ws.send(msg("refused", 200 * 1024)).await.unwrap();
    let busy: serde_json::Value = serde_json::from_str(&text(&mut ws).await).unwrap();
    assert!(
        t.elapsed() >= Duration::from_millis(900),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(
        busy,
        serde_json::json!({
            "type": "error",
            "status": 503,
            "error": {"type": "gateway_busy", "message": "Gateway is busy; retry shortly."},
            "headers": {"retry-after": "3", "retry-after-ms": "3000"},
        })
    );
    assert_eq!(e.drain.body_bytes(), 900 * 1024);
    assert_eq!(e.drain.ws_bytes(), 0, "the refused message holds nothing");

    // Room again: the connection is still usable and the next reply is for this message.
    drop(other);
    ws.send(msg("next", 200 * 1024)).await.unwrap();
    let echo = text(&mut ws).await;
    assert!(echo.contains("\"tag\":\"next\"") && !echo.contains("refused"));
    // Released once the turn completes.
    for _ in 0..50 {
        if e.drain.body_bytes() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(e.drain.body_bytes(), 0);

    // Held for the running turn (its replay copy stays in memory).
    ws.send(M::text(r#"{"type":"response.create","tag":"held"}"#))
        .await
        .unwrap();
    assert!(text(&mut ws).await.contains("held"));
    assert_eq!(
        e.drain.body_bytes(),
        r#"{"type":"response.create","tag":"held"}"#.len() as u64
    );
    assert_eq!(e.drain.ws_bytes(), e.drain.body_bytes());

    // A message larger than the whole budget is not refused forever: it waits for all of it.
    ws.send(msg("whole", (3 * mib / 2) as usize)).await.unwrap();
    let echo = text(&mut ws).await;
    assert!(echo.contains("\"tag\":\"whole\""), "{}", &echo[..80]);
    ws.close(None).await.unwrap();
    for _ in 0..100 {
        if e.drain.body_bytes() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(e.drain.body_bytes(), 0);
    assert_eq!(e.drain.ws_bytes(), 0);
}

/// Running WS turns fill `max_inflight_requests`; new turns are then refused, but model
/// lists and quota polls are exempt.
#[tokio::test]
async fn running_turns_fill_inflight_but_models_and_quota_polls_are_exempt() {
    use tokio_tungstenite::tungstenite::Message as M;
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 2;
    e.config.store(cfg);
    let mut idle = Vec::new();
    for _ in 0..2 {
        let (mut ws, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
        assert_eq!(r.status(), 101);
        ws.send(M::text(r#"{"type":"response.create","input":"held"}"#))
            .await
            .unwrap();
        assert!(ws_text(&mut ws)
            .await
            .contains("response.output_text.delta"));
        idle.push(ws);
    }
    assert_eq!(e.drain.inflight(), 2, "two running turns hold both slots");
    assert_eq!(
        held_response(&e).await.status(),
        503,
        "a new turn is refused"
    );
    let before = e.drain.rejected();
    for path in [
        "/v1/models",
        "/backend-api/codex/models",
        "/backend-api/wham/usage",
        "/api/codex/usage",
    ] {
        let r = e
            .app
            .clone()
            .oneshot(
                http::Request::get(path)
                    .header("authorization", format!("Bearer {KEY}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{path} is exempt from the inflight cap");
        let _ = axum::body::to_bytes(r.into_body(), 1 << 20).await;
    }
    assert_eq!(e.drain.rejected(), before, "exempt polls are never refused");
    assert_eq!(e.drain.inflight(), 2, "exempt polls take no slot");
    drop(idle);
}

/// `max_inflight_requests` counts running work only: an idle WebSocket holds no slot (it is
/// bounded by `ws_max_connections`), a WS turn holds one from its client request to its
/// terminal event, and a turn that finds the cap full gets the `gateway_busy` frame and is
/// recorded as an inflight refusal over ws.
#[tokio::test]
async fn idle_websockets_hold_no_inflight_slot_only_running_turns_do() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    let mut idle = Vec::new();
    for _ in 0..2 {
        let (mut ws, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
        assert_eq!(r.status(), 101);
        ws.send(M::Ping(Bytes::from_static(b"ready")))
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
        idle.push(ws);
    }
    wait_inflight(&e, 0).await;
    assert_eq!(e.drain.ws_connections(), 2);
    assert_eq!(
        e.drain.active(),
        2,
        "idle sessions are still waited for on shutdown"
    );
    let held = held_response(&e).await;
    assert_eq!(held.status(), 200, "idle sockets leave the slot free");
    assert_eq!(e.drain.inflight(), 1);

    // The cap is full: a WS turn is refused with the busy frame, the socket stays open.
    let ws = &mut idle[0];
    ws.send(M::text(r#"{"type":"response.create","input":"plain"}"#))
        .await
        .unwrap();
    let busy = ws_text(ws).await;
    assert!(busy.contains("gateway_busy"), "{busy}");
    assert_eq!(e.drain.rejected_by(), [1, 0, 0, 0]);
    drop(held);
    wait_inflight(&e, 0).await;

    // A completed turn releases its slot at the terminal event.
    ws.send(M::text(r#"{"type":"response.create","input":"plain"}"#))
        .await
        .unwrap();
    assert!(ws_text(ws).await.contains("response.completed"));
    wait_inflight(&e, 0).await;

    // A running turn holds one slot and keeps HTTP out.
    ws.send(M::text(r#"{"type":"response.create","input":"held"}"#))
        .await
        .unwrap();
    assert!(ws_text(ws).await.contains("response.output_text.delta"));
    assert_eq!(e.drain.inflight(), 1, "a running WS turn holds a slot");
    assert_eq!(held_response(&e).await.status(), 503);
    // Closing mid-turn releases it.
    let mut ws = idle.remove(0);
    ws.send(M::Close(None)).await.unwrap();
    drop(ws);
    wait_inflight(&e, 0).await;
    assert_eq!(held_response(&e).await.status(), 200);
    drop(idle);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(3)).await, 0);

    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let v = e
        .http
        .get(format!("http://{}/usage/v1/refusals?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["totals"]["inflight"], 2, "{v}");
    let rows = v["rows"].as_array().unwrap();
    let by = |t: &str| -> u64 {
        rows.iter()
            .filter(|r| r["reason"] == "inflight" && r["transport"] == t)
            .map(|r| r["count"].as_u64().unwrap())
            .sum()
    };
    assert_eq!((by("ws"), by("http")), (1, 1), "{v}");
}
