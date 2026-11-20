//! Non-2xx responses and WS error frames carry the gateway record ID as `x-request-id`
//! (`cp-<record_id>`), which Codex prints as "request id: …".
use super::scaffold::*;
use super::*;
use tokio_tungstenite::tungstenite::Message as M;

#[tokio::test]
async fn upstream_error_gets_the_record_id_and_keeps_upstream_ids_and_body() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "reqid_400")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let rows = wait_rows(&e, 1).await;
    let id = rows[0]["request_id"].as_str().unwrap().to_string();
    assert_eq!(r.headers()["x-request-id"], format!("cp-{id}").as_str());
    assert_eq!(r.headers()["x-upstream-request-id"], "up-req");
    assert_eq!(r.headers()["x-oai-request-id"], "up-oai");
    assert_eq!(r.bytes().await.unwrap(), REQID_BODY.as_bytes());
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    assert_eq!(v["upstream_request_id"], "up-req");
}

#[tokio::test]
async fn local_rejection_gets_the_record_id_only_when_a_record_exists() {
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(Store::rotating(&[]));
    let e = env_cfg_accounts(30, Some(usage_recorder(d.path())), "", Some(accounts)).await;
    let r = e
        .post("/backend-api/codex/responses")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let rows = wait_rows(&e, 1).await;
    let id = rows[0]["request_id"].as_str().unwrap();
    assert_eq!(r.headers()["x-request-id"], format!("cp-{id}").as_str());

    // Without usage recording there is no record to point at: the response is left alone.
    let accounts = Arc::new(Store::rotating(&[]));
    let e = env_cfg_accounts(30, None, "", Some(accounts)).await;
    let r = e
        .post("/backend-api/codex/responses")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    assert!(r.headers().get("x-request-id").is_none());
}

#[tokio::test]
async fn successful_responses_keep_the_upstream_request_id() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "diag_success").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-request-id"], "diag-upstream-id");
    assert!(r.headers().get("x-upstream-request-id").is_none());
    r.bytes().await.unwrap();
}

#[tokio::test]
async fn ws_error_frame_gets_the_turn_record_id_and_keeps_its_headers() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"stamp_ws"}"#,
    ))
    .await
    .unwrap();
    let frame = loop {
        match tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap()
        {
            Some(Ok(M::Text(t))) if t.as_str().contains("\"error\"") => break t.to_string(),
            Some(Ok(_)) => {}
            other => panic!("{other:?}"),
        }
    };
    let v: serde_json::Value = serde_json::from_str(&frame).unwrap();
    let rows = wait_rows(&e, 1).await;
    let id = rows[0]["request_id"].as_str().unwrap();
    let want: serde_json::Value = serde_json::from_str(WS_ERROR_FRAME).unwrap();
    assert_eq!(v["error"], want["error"]);
    assert_eq!(v["status"], 400);
    assert_eq!(v["headers"]["retry-after"], "7");
    assert_eq!(v["headers"]["x-upstream-request-id"], "up-ws-req");
    assert_eq!(v["headers"]["x-request-id"], format!("cp-{id}").as_str());
    let _ = ws.close(None).await;
}

#[tokio::test]
async fn ws_handshake_rejection_gets_the_record_id() {
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(Store::rotating(&[]));
    let e = env_cfg_accounts(30, Some(usage_recorder(d.path())), "", Some(accounts)).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(r)) =
        tokio_tungstenite::connect_async(ws_req(&e)).await
    else {
        panic!("handshake must be refused over HTTP");
    };
    let rows = wait_rows(&e, 1).await;
    let id = rows[0]["request_id"].as_str().unwrap();
    assert_eq!(r.headers()["x-request-id"], format!("cp-{id}").as_str());
}
