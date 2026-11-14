use super::scaffold::*;
use super::*;

#[tokio::test]
async fn shutdown_cut_ws_turn_flushes_received_frames_then_retryable_error_then_1001() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","input":"burst"}"#))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    // Upstream writes 50 frames; cut the turn while they are still on their way to the client.
    e.mock.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    let drain = e.drain.clone();
    let stopped =
        tokio::spawn(async move { drain.shutdown(Duration::ZERO, Duration::from_secs(2)).await });
    let mut deltas = Vec::new();
    let failed = loop {
        let m = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let M::Text(t) = m else {
            panic!("expected text before close, got {m:?}")
        };
        let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
        if v["type"] == "response.output_text.delta" {
            deltas.push(v["delta"].as_str().unwrap().to_string());
        } else {
            break t;
        }
    };
    let want: Vec<String> = (0..=50).map(|i| format!("b{i}")).collect();
    assert_eq!(
        deltas, want,
        "every frame upstream already sent, in order, before the error"
    );
    assert_shutdown_event(failed.as_str());
    let close = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(close, M::Close(Some(ref f)) if u16::from(f.code) == 1001),
        "{close:?}"
    );
    drop(ws);
    assert!(stopped.await.unwrap() > 0);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
}

#[tokio::test]
async fn shutdown_cut_sse_flushes_queued_events_then_retryable_error_then_clean_end() {
    use tower::ServiceExt;
    let e = env(300).await;
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .header("x-test-mode", "burst_held")
        .body(Body::from(r#"{"stream":true,"input":[]}"#))
        .unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(3), e.app.clone().oneshot(req))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), 200);
    // The producer queues all three events; nothing has been read by the client yet.
    tokio::time::sleep(millis(200)).await;
    let drain = e.drain.clone();
    let stopped =
        tokio::spawn(async move { drain.shutdown(Duration::ZERO, Duration::from_secs(2)).await });
    let body = tokio::time::timeout(
        Duration::from_secs(3),
        axum::body::to_bytes(resp.into_body(), 1 << 20),
    )
    .await
    .unwrap()
    .expect("a cut Responses stream ends cleanly, not with a body error");
    let text = String::from_utf8(body.to_vec()).unwrap();
    let datas: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    assert_eq!(datas.len(), 4, "{text}");
    for (i, d) in datas[..3].iter().enumerate() {
        assert!(d.contains(&format!("\"d{}\"", i + 1)), "{text}");
    }
    assert_shutdown_event(datas[3]);
    assert!(text.contains("event: response.failed\n"), "{text}");
    assert!(stopped.await.unwrap() > 0);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
}

#[tokio::test]
async fn ws_turn_sent_while_draining_gets_retryable_error_then_1001() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    // Another request keeps the drain open, so this idle session is the one being refused.
    let held = held_response(&e).await;
    e.drain.start();
    ws.send(M::text(r#"{"type":"response.create","input":"x"}"#))
        .await
        .unwrap();
    let mut texts = Vec::new();
    let close = loop {
        let m = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match m {
            M::Text(t) => texts.push(t.to_string()),
            M::Ping(_) | M::Pong(_) => {}
            other => break other,
        }
    };
    // Draining closes an idle session between turns; a turn that raced it gets the event.
    if let Some(last) = texts.last() {
        assert_shutdown_event(last);
        assert!(
            matches!(close, M::Close(Some(ref f)) if u16::from(f.code) == 1001),
            "{close:?}"
        );
    } else {
        assert!(
            matches!(close, M::Close(Some(ref f)) if u16::from(f.code) == 1000),
            "{close:?}"
        );
    }
    drop(held);
}
