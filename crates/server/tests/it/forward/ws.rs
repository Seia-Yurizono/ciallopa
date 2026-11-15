use super::scaffold::*;
use super::*;

#[tokio::test]
async fn ws_deflate_negotiated_with_codex_offer() {
    let e = env(300).await;
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(ext(&resp).as_deref(), Some("permessage-deflate"));
    // No ciallopa-specific headers on the handshake.
    assert!(resp
        .headers()
        .keys()
        .all(|k| !k.as_str().contains("ciallopa")));
    ws_roundtrips(&mut ws, 3).await;
}

#[tokio::test]
async fn ws_deflate_params() {
    use tokio_tungstenite::tungstenite::protocol::Role;
    let e = env(300).await;
    let cfg = deflate_client(|d| {
        d.set_no_context_takeover(Role::Server, true)
            .set_no_context_takeover(Role::Client, true)
            .set_max_window_bits(Role::Server, 10)
            .unwrap()
            .set_max_window_bits(Role::Client, 12)
            .unwrap()
    });
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(ws_req(&e), Some(cfg), false)
        .await
        .unwrap();
    assert_eq!(
        ext(&resp).as_deref(),
        Some("permessage-deflate; server_no_context_takeover; client_no_context_takeover; server_max_window_bits=10; client_max_window_bits=12")
    );
    ws_roundtrips(&mut ws, 3).await;
    // Context takeover on, smaller windows only.
    let cfg = deflate_client(|d| d.set_max_window_bits(Role::Server, 9).unwrap());
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(ws_req(&e), Some(cfg), false)
        .await
        .unwrap();
    assert_eq!(
        ext(&resp).as_deref(),
        Some("permessage-deflate; server_max_window_bits=9")
    );
    ws_roundtrips(&mut ws, 2).await;
}

#[tokio::test]
async fn ws_plain_without_offer_or_when_disabled() {
    let e = env(300).await;
    // No offer → plain.
    let (mut ws, resp) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(ext(&resp), None);
    ws_roundtrips(&mut ws, 1).await;
    // Unacceptable offer → plain.
    let mut req = ws_req(&e);
    req.headers_mut().insert(
        "sec-websocket-extensions",
        "permessage-deflate; server_max_window_bits=8, x-webkit-deflate-frame"
            .parse()
            .unwrap(),
    );
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(ext(&resp), None);
    ws_roundtrips(&mut ws, 1).await;
    // ws_compression = false applies to the next handshake (hot).
    let mut c = (*e.config.load()).clone();
    c.ws_compression = false;
    e.config.store(c.clone());
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(ext(&resp), None);
    ws_roundtrips(&mut ws, 1).await;
    c.ws_compression = true;
    e.config.store(c);
    let (_ws, resp) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(ext(&resp).as_deref(), Some("permessage-deflate"));
}

/// The server really compresses (RSV1) once negotiated, and accepts an uncompressed client
/// frame on a deflate connection (RFC 7692 §6).
#[tokio::test]
async fn ws_deflate_wire_frames_compressed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let e = env(300).await;
    let mut s = tokio::net::TcpStream::connect(&e.proxy).await.unwrap();
    s.write_all(format!(
        "GET /v1/responses HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\nAuthorization: Bearer {KEY}\r\n\r\n",
        e.proxy
    ).as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut b = [0u8; 1024];
        let n = s.read(&mut b).await.unwrap();
        assert!(n > 0);
        buf.extend_from_slice(&b[..n]);
    }
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 101"), "{head}");
    assert!(
        head.contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="),
        "{head}"
    );
    assert!(
        head.contains("sec-websocket-extensions: permessage-deflate\r\n"),
        "{head}"
    );
    // Masked, uncompressed text frame.
    let payload: String = format!(
        r#"{{"type":"response.create","input":"{}"}}"#,
        "abcabcabc ".repeat(200)
    );
    let key = [1u8, 2, 3, 4];
    let mut f = vec![0x81u8, 0x80 | 126];
    f.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    f.extend_from_slice(&key);
    f.extend(payload.bytes().enumerate().map(|(i, b)| b ^ key[i % 4]));
    s.write_all(&f).await.unwrap();
    let mut rest = buf[end..].to_vec();
    while rest.len() < 4 {
        let mut b = [0u8; 4096];
        let n = s.read(&mut b).await.unwrap();
        assert!(n > 0);
        rest.extend_from_slice(&b[..n]);
    }
    assert_eq!(rest[0] & 0x0f, 1, "text frame");
    assert_eq!(rest[0] & 0x40, 0x40, "RSV1 (compressed) set");
    let len = match rest[1] & 0x7f {
        126 => u16::from_be_bytes([rest[2], rest[3]]) as usize,
        n => n as usize,
    };
    assert!(
        len < payload.len() / 4,
        "compressed {len} vs {}",
        payload.len()
    );
}

#[tokio::test]
async fn ws_preparation_budget_cancels_a_credential_lock_wait() {
    use ciallopa_auth::file::{write_auth_file, FileLock};
    use tokio_tungstenite::tungstenite::Error;

    let d = tempfile::tempdir().unwrap();
    let auth_dir = d.path().join("auths");
    write_auth_file(
        &auth_dir,
        "acct",
        &serde_json::json!({
            "type":"codex", "access_token":"old", "refresh_token":"fixture",
            "expired":"2000-01-01T00:00:00Z"
        }),
    )
    .unwrap();
    let accounts = Arc::new(
        ciallopa_auth::FileAccountStore::open_with(
            auth_dir.clone(),
            ciallopa_auth::OAuthClient::with_issuer("http://127.0.0.1:1", reqwest::Client::new()),
        )
        .await
        .unwrap(),
    );
    let lock = FileLock::acquire(&auth_dir, "acct").unwrap();
    let e = env_cfg_accounts(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
        Some(accounts),
    )
    .await;
    let mut request = tokio::spawn(tokio_tungstenite::connect_async(ws_req(&e)));
    let result = tokio::time::timeout(Duration::from_secs(2), &mut request).await;
    if result.is_err() {
        request.abort();
    }
    assert!(
        result.is_ok(),
        "WS preparation must cancel the credential lock wait"
    );
    let Err(Error::Http(response)) = result.unwrap().unwrap() else {
        panic!("expected an HTTP preparation timeout");
    };
    assert_eq!(response.status(), 504);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "local", "{rows:?}");
    assert!(rows[0]["duration_ms"].as_u64().unwrap() >= millis(1000).as_millis() as u64);
    assert_eq!(rows[0]["account"], "acct", "{rows:?}");
    assert_eq!(rows[0]["switches"], 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
    assert!(
        e.outcomes().is_empty(),
        "budget exhaustion does not cool an account"
    );
    assert_eq!(e.drain.wait_idle(Duration::from_secs(1)).await, 0);
    assert_eq!(e.drain.inflight(), 0);

    // The timed-out refresh must release its in-process lock too. A later request can
    // re-read the fresh token another process wrote, without reaching the OAuth endpoint.
    write_auth_file(
        &auth_dir,
        "acct",
        &serde_json::json!({
            "type":"codex", "access_token":"tok-a-1", "refresh_token":"fixture",
            "expired":"2999-01-01T00:00:00Z"
        }),
    )
    .unwrap();
    drop(lock);
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_tungstenite::connect_async(ws_req(&e)),
    )
    .await
    .unwrap()
    .unwrap();
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_preparation_budget_is_shared_across_handshake_retries() {
    use tokio_tungstenite::tungstenite::Error;
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\nconnect_timeout_secs = 5\n",
    )
    .await;
    let mut req = ws_req(&e);
    req.headers_mut()
        .insert("x-test-mode", "ws_retry_delay_fail".parse().unwrap());
    let mut request = tokio::spawn(tokio_tungstenite::connect_async(req));
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    tokio::time::sleep(millis(650)).await;
    e.mock.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    // A fresh one-second budget for the second attempt would miss this bound.
    let result = tokio::time::timeout(millis(700), &mut request).await;
    e.mock.release.notify_waiters();
    if result.is_err() {
        request.abort();
    }
    assert!(
        result.is_ok(),
        "the second handshake must use only the remaining budget"
    );
    let Err(Error::Http(response)) = result.unwrap().unwrap() else {
        panic!("expected a preparation timeout");
    };
    assert_eq!(response.status(), 504);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "local", "{rows:?}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert_eq!(rows[0]["account"], "b", "{rows:?}");
    assert_eq!(rows[0]["first_account"], "a", "{rows:?}");
    assert_eq!(rows[0]["switches"], 1);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(1)).await, 0);
}

#[tokio::test]
async fn ws_preparation_timeout_keeps_the_candidate_waiting_for_credentials() {
    struct WaitingCredentials {
        store: Store,
        selected: tokio::sync::Notify,
    }
    #[async_trait]
    impl AccountStore for WaitingCredentials {
        fn accounts(&self) -> Arc<[Account]> {
            self.store.accounts()
        }
        fn generation(&self) -> u64 {
            self.store.generation()
        }
        async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
            if id == "b" {
                self.selected.notify_one();
                std::future::pending().await
            } else {
                self.store.credentials(id).await
            }
        }
        async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
            self.credentials(id).await
        }
        async fn manual_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
            self.force_refresh(id).await
        }
        async fn reload(&self) -> Result<(), AuthError> {
            Ok(())
        }
    }
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(WaitingCredentials {
        store: Store::rotating(&["a", "b"]),
        selected: tokio::sync::Notify::new(),
    });
    let e = env_cfg_accounts(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
        Some(accounts.clone()),
    )
    .await;
    let mut req = ws_req(&e);
    req.headers_mut()
        .insert("x-test-mode", "ws_retry_delay_fail".parse().unwrap());
    let request = tokio::spawn(tokio_tungstenite::connect_async(req));
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    e.mock.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), accounts.selected.notified())
        .await
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout");
    assert_eq!(rows[0]["account"], "b");
    assert_eq!(rows[0]["first_account"], "a");
    assert_eq!(rows[0]["switches"], 1);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_eq!(e.drain.inflight(), 0);
}

#[tokio::test]
async fn ws_preparation_budget_is_shared_across_turn_replays() {
    use tokio_tungstenite::tungstenite::Message as M;
    for tag in ["budget_quota", "budget_overload"] {
        let d = tempfile::tempdir().unwrap();
        let accounts = Arc::new(Store::rotating(&["a", "b", "c"]));
        let e = env_cfg_accounts(
            30,
            Some(usage_recorder(d.path())),
            "preparation_timeout_secs = 1\nconnect_timeout_secs = 5\n",
            Some(accounts),
        )
        .await;
        let mut req = ws_req(&e);
        req.headers_mut()
            .insert("x-test-mode", "ws_budget_replay".parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        // Existing connections retain the preparation policy captured at their handshake.
        let mut cfg = (*e.config.load()).clone();
        cfg.upstream_opts.preparation_timeout_secs = 30;
        e.config.store(cfg);
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"{tag}"}}"#
        )))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
            .await
            .unwrap();
        tokio::time::sleep(millis(650)).await;
        e.mock.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
            .await
            .unwrap();
        let result = tokio::time::timeout(millis(700), ws.next()).await;
        e.mock.release.notify_waiters();
        assert!(
            result.is_ok(),
            "{tag}: every redial must share the turn's remaining budget"
        );
        assert!(
            matches!(result.unwrap(), Some(Ok(M::Close(Some(frame)))) if u16::from(frame.code) == 1012)
        );
        drop(ws);
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows.len(), 1, "{tag}: {rows:?}");
        assert_eq!(rows[0]["error"], "preparation_timeout", "{tag}: {rows:?}");
        assert_eq!(rows[0]["err_source"], "local", "{tag}: {rows:?}");
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
        assert_eq!(e.drain.wait_idle(Duration::from_secs(3)).await, 0);
    }
}

#[tokio::test]
async fn ws_preparation_budget_resets_each_turn_and_excludes_generation() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(Store::rotating(&["a", "b", "c"]));
    let e = env_cfg_accounts(
        1,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
        Some(accounts),
    )
    .await;
    let mut req = ws_req(&e);
    req.headers_mut()
        .insert("x-test-mode", "ws_budget_turns".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    for tag in ["budget_long", "budget_next"] {
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"{tag}"}}"#
        )))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            let mut deltas = 0;
            loop {
                let message = ws.next().await.unwrap().unwrap();
                let text = message.to_text().expect("a valid long turn must not close");
                if text.contains("response.output_text.delta") {
                    deltas += 1;
                }
                if text.contains("response.completed") {
                    break;
                }
            }
            assert_eq!(deltas, 3);
        })
        .await
        .unwrap();
    }
    let rows = wait_rows(&e, 2).await;
    assert_eq!(rows.len(), 2, "successful handshakes are not business rows");
    for row in rows {
        assert!(row["error"].is_null(), "{row}");
        assert!(
            row["duration_ms"].as_u64().unwrap() > millis(1900).as_millis() as u64,
            "{row}"
        );
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_failed_handshake_records_the_wait_before_failure() {
    for (mode, attempts) in [("ws_delay_fail", 1), ("ws_retry_delay_fail", 2)] {
        tokio::time::timeout(Duration::from_secs(5), async {
            let d = tempfile::tempdir().unwrap();
            let rec = usage_recorder(d.path());
            let e = env_usage(300, Some(rec.clone())).await;
            let mut req = ws_req(&e);
            req.headers_mut()
                .insert("x-test-mode", mode.parse().unwrap());
            let before_ms = ciallopa_usage::now_ms();
            let request = tokio::spawn(tokio_tungstenite::connect_async(req));
            let mut first_ready_ms = None;
            for _ in 0..attempts {
                e.mock.ready.notified().await;
                first_ready_ms.get_or_insert_with(ciallopa_usage::now_ms);
                tokio::time::sleep(millis(200)).await;
                e.mock.release.notify_one();
            }
            assert!(request.await.unwrap().is_err());
            let rows = wait_rows(&e, 1).await;
            assert_eq!(rows.len(), 1, "{mode}: {rows:?}");
            let row = &rows[0];
            assert_eq!(row["status"], 400, "{mode}: {row}");
            assert_eq!(row["transport"], "ws");
            assert_eq!(row["error"], "handshake_fixture");
            assert_eq!(row["account"], if attempts == 1 { "a" } else { "b" });
            assert_eq!(row["switches"], attempts - 1);
            if attempts == 1 {
                assert!(row["first_account"].is_null());
            } else {
                assert_eq!(row["first_account"], "a");
            }
            assert!(
                row["duration_ms"].as_u64().unwrap() >= millis(200).as_millis() as u64 * attempts,
                "handshake preparation/retries must count: {mode}: {row}"
            );
            let started = row["ts_ms"].as_i64().unwrap();
            assert!(
                (before_ms..=first_ready_ms.unwrap()).contains(&started),
                "failed handshake belongs to its start window: {mode}: {row}"
            );
            assert_eq!(e.mock.hits.load(Ordering::SeqCst), attempts as usize);
            rec.shutdown();
        })
        .await
        .expect("handshake fixture must finish");
    }
}

#[tokio::test]
async fn ws_cancelled_handshake_keeps_selected_account() {
    use tokio::io::AsyncWriteExt;
    for (mode, attempts) in [("ws_delay_fail", 1), ("ws_retry_delay_fail", 2)] {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let mut client = tokio::net::TcpStream::connect(&e.proxy).await.unwrap();
        client.write_all(format!(
            "GET /v1/responses HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer {KEY}\r\nx-test-mode: {mode}\r\n\r\n",
            e.proxy
        ).as_bytes()).await.unwrap();
        for attempt in 0..attempts {
            tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
                .await
                .expect("upstream handshake started");
            if attempt + 1 < attempts {
                e.mock.release.notify_one();
            }
        }
        assert_eq!(rec.stats().accepted, 0);
        client.shutdown().await.unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(3), wait_rows(&e, 1))
            .await
            .expect("a real client FIN cancels a pending handshake");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], 0);
        assert_eq!(rows[0]["error"], "client_closed");
        assert_eq!(rows[0]["err_source"], "client");
        assert_eq!(rows[0]["account"], if attempts == 1 { "a" } else { "b" });
        assert_eq!(rows[0]["switches"], attempts - 1);
        if attempts == 2 {
            assert_eq!(rows[0]["first_account"], "a");
        }
        assert_eq!(e.drain.inflight(), 0);
        e.mock.release.notify_one();
        rec.shutdown();
    }
}

#[tokio::test]
async fn ws_unroutable_handshake_has_no_candidate_account() {
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(Store::rotating(&[]));
    let e = env_cfg_accounts(30, Some(usage_recorder(d.path())), "", Some(accounts)).await;
    assert!(tokio_tungstenite::connect_async(ws_req(&e)).await.is_err());
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["error"], "no_account");
    assert!(rows[0]["account"].is_null());
    assert!(rows[0]["first_account"].is_null());
    assert_eq!(rows[0]["switches"], 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ws_handshake_rejections_unchanged() {
    let e = env(300).await;
    let r = e
        .http
        .get(format!("http://{}/v1/responses", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.text().await.unwrap(),
        "Connection header did not include 'upgrade'"
    );
}

#[tokio::test]
async fn ws_upstream_transport_error_mid_turn_recorded_with_cause() {
    use tokio_tungstenite::tungstenite::Message as M;
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"drop","client_metadata":{"thread_id":"diag-ws-thread","x-codex-turn-metadata":"{\"turn_id\":\"diag-ws-turn\"}"}}"#,
    ))
    .await
    .unwrap();
    let mut close = None;
    while let Some(Ok(m)) = ws.next().await {
        if let M::Close(f) = m {
            close = f;
            break;
        }
    }
    let f = close.expect("close frame");
    assert_eq!(u16::from(f.code), 1011);
    let rows = wait_rows(&e, 1).await;
    let r = &rows[0];
    // TCP gone without a close frame: an EOF, not a close frame or a read error.
    assert_eq!(r["error"], "upstream_ws_eof", "{r}");
    assert_eq!(r["err_source"], "network", "{r}");
    let id = r["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let msg = v["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("upstream websocket ended without a close frame (") && msg.len() > 50,
        "{v}"
    );
    let logs = warnings.0.lock().unwrap().clone();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = &logs[0];
    assert_eq!(log["transport"], "ws");
    assert_eq!(log["error_class"], "eof");
    assert_eq!(log["last_event"], "response.created");
    assert_eq!(log["event_count"], 1);
    assert_eq!(log["terminal_received"], false);
    assert_eq!(log["record_id"], id);
    assert_eq!(log["upstream_request_id"], "ws-diagnostic-upstream-id");
    assert_eq!(log["thread_id"], "diag-ws-thread");
    assert_eq!(log["turn_id"], "diag-ws-turn");
    assert!(
        log.get("error").is_none(),
        "no raw transport error in process log"
    );
}

#[tokio::test]
async fn ws_replayed_attempt_diagnostics_use_current_handshake_and_fresh_progress() {
    use tokio_tungstenite::tungstenite::Message as M;
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    tokio::time::timeout(Duration::from_secs(8), async {
        for tag in ["limit", "metaover"] {
            warnings.0.lock().unwrap().clear();
            let d = tempfile::tempdir().unwrap();
            let e = env_usage(30, Some(usage_recorder(d.path()))).await;
            let mut req = ws_req(&e);
            req.headers_mut().insert("x-test-mode", "ws_replay_drop".parse().unwrap());
            let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
            ws.send(M::text(format!(r#"{{"type":"response.create","tag":"{tag}","client_metadata":{{"thread_id":"replay-thread","x-codex-turn-metadata":"{{\"turn_id\":\"replay-turn\"}}"}}}}"#))).await.unwrap();
            assert!(matches!(ws.next().await, Some(Ok(M::Close(Some(f)))) if u16::from(f.code) == 1011));
            drop(ws);
            let rows = wait_rows(&e, 1).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["first_account"], "a", "{tag}: {rows:?}");
            assert_eq!(rows[0]["account"], "b");
            assert_eq!(rows[0]["switches"], 1);
            assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
            let logs = warnings.0.lock().unwrap().clone();
            assert_eq!(logs.len(), 1, "{tag}: {logs:?}");
            let log = &logs[0];
            assert_eq!(log["transport"], "ws");
            assert_eq!(log["error_class"], "eof");
            assert_eq!(log["account"], "b");
            assert_eq!(log["upstream_request_id"], "ws-replay-upstream-id");
            assert_eq!(log["record_id"], rows[0]["request_id"]);
            assert_eq!(log["thread_id"], "replay-thread");
            assert_eq!(log["turn_id"], "replay-turn");
            assert_eq!(log["event_count"], 0);
            assert_eq!(log["terminal_received"], false);
            assert_eq!(log["activity_received"], false);
            assert!(log["last_event"].is_null());
        }
    }).await.expect("both replay diagnostic fixtures must finish");
}

#[tokio::test]
async fn ws_stream_diagnostics_count_ping_as_activity_and_hide_unknown_event_types() {
    use tokio_tungstenite::tungstenite::Message as M;
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    let e = env(1).await;
    for mode in ["diagnostic_close", "diagnostic_complete"] {
        warnings.0.lock().unwrap().clear();
        let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"{mode}"}}"#
        )))
        .await
        .unwrap();
        while let Some(Ok(m)) = ws.next().await {
            if matches!(m, M::Close(_)) {
                break;
            }
            if let M::Text(t) = m {
                if t.contains("response.completed") {
                    ws.close(None).await.unwrap();
                    break;
                }
            }
        }
        let logs = warnings.0.lock().unwrap().clone();
        if mode == "diagnostic_complete" {
            assert!(logs.is_empty(), "completed WS turn is quiet: {logs:?}");
            continue;
        }
        assert_eq!(logs.len(), 1, "{logs:?}");
        let log = &logs[0];
        assert_eq!(log["error_class"], "close_frame");
        assert_eq!(log["last_event"], "unknown");
        assert_eq!(log["event_count"], 1);
        assert_eq!(log["activity_received"], true);
        assert_eq!(log["terminal_received"], false);
        assert!(
            log["event_idle_ms"].as_u64().unwrap()
                >= log["activity_idle_ms"].as_u64().unwrap() + millis(80).as_millis() as u64,
            "{log}"
        );
        assert!(!log.to_string().contains("private-"), "{log}");
    }
}

#[tokio::test]
async fn ws_upstream_close_frame_mid_turn_is_upstream_ws_close() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","tag":"diagnostic_close"}"#,
    ))
    .await
    .unwrap();
    let mut code = None;
    while let Some(Ok(m)) = ws.next().await {
        if let M::Close(f) = m {
            code = f.map(|f| u16::from(f.code));
            break;
        }
    }
    // The upstream close frame is relayed as is.
    assert_eq!(code, Some(1000));
    let rows = wait_rows(&e, 1).await;
    let r = &rows[0];
    assert_eq!(r["error"], "upstream_ws_close", "{r}");
    assert_eq!(r["err_source"], "stream", "{r}");
    let id = r["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let msg = v["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("upstream websocket closed mid-turn (code 1000)"),
        "{v}"
    );
}

/// Codex gives up on a silent stream after its own idle timeout; the client_closed detail
/// carries how long the upstream had been silent so that case is visible.
#[tokio::test]
async fn ws_client_close_records_upstream_silent_duration() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","tag":"held"}"#))
        .await
        .unwrap();
    loop {
        match ws.next().await {
            Some(Ok(M::Text(t))) if t.as_str().contains("held") => break,
            Some(Ok(_)) => {}
            other => panic!("{other:?}"),
        }
    }
    // The diagnostic rounds physical silence to tenths of a second.
    tokio::time::sleep(Duration::from_millis(300)).await;
    ws.send(M::Close(None)).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .unwrap();
    assert!(matches!(reply, Some(Ok(M::Close(_)))), "{reply:?}");
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
    let id = rows[0]["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let msg = v["message"].as_str().unwrap_or_default();
    let secs: f64 = msg
        .split("upstream silent for ")
        .nth(1)
        .and_then(|r| r.split('s').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no silent duration: {v}"));
    assert!((0.2..5.0).contains(&secs), "{v}");
}

/// A status-less error frame with an unknown code does not end the turn (Codex ignores it);
/// it is forwarded and the turn completes normally.
#[tokio::test]
async fn ws_soft_error_frame_does_not_end_turn() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","tag":"soft_error"}"#))
        .await
        .unwrap();
    let mut seen = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap()
        {
            Some(Ok(M::Text(t))) => {
                let done = t.as_str().contains("response.completed");
                seen.push(t.as_str().to_string());
                if done {
                    break;
                }
            }
            Some(Ok(_)) => {}
            other => panic!("{other:?} after {seen:?}"),
        }
    }
    assert!(seen.iter().any(|t| t.contains("some_notice")), "{seen:?}");
    ws.close(None).await.unwrap();
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{rows:?}");
    assert_eq!(rows[0]["input_tokens"], 5, "{rows:?}");
    assert!(e
        .outcomes()
        .iter()
        .all(|(_, o)| !matches!(o, Outcome::Transport)));
}

#[tokio::test]
async fn websocket_handshake_402_fails_over() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req.headers_mut()
        .insert("x-test-mode", "deact".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","n":1}"#))
        .await
        .unwrap();
    let m = ws.next().await.unwrap().unwrap();
    assert!(m.to_text().unwrap().contains("\"echo\""));
    let o = e.outcomes();
    assert_eq!(o[0].0, "a");
    assert!(matches!(
        o[0].1,
        Outcome::PaymentRequired {
            deactivated: true,
            ..
        }
    ));
    let auths: Vec<String> = e
        .mock
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(h, _)| {
            h.iter()
                .find(|(k, _)| k == "authorization")
                .map(|(_, v)| v.clone())
        })
        .collect();
    assert_eq!(auths, ["Bearer tok-a-1", "Bearer tok-b-1"]);
}

/// WS: an overload replayed on another account is recorded as rescued with its chain.
#[tokio::test]
async fn ws_overload_switch_records_attempt_chain() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"metaover"}"#,
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
        assert!(
            !t.contains("overloaded") && !t.contains("openai_chatgpt_moderation_metadata"),
            "{t}"
        );
        if t.contains("response.completed") {
            assert!(t.contains("r-b"), "{t}");
            break;
        }
    }
    drop(ws);
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
    let rows = wait_rows(&e, 1).await;
    let row = &rows[0];
    assert!(row["error"].is_null(), "{row}");
    assert_eq!(row["rescued"], true, "{row}");
    assert_eq!(row["upstream_fault"], true, "{row}");
    let v: serde_json::Value = detail_of(&e, row["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 2, "{v}");
    assert_eq!(at[0]["account"], "a");
    assert_eq!(at[0]["code"], "server_is_overloaded");
    assert_eq!(at[0]["action"], "switch");
    assert_eq!(at[1]["account"], row["account"]);
    assert_eq!(at[1]["action"], "ok");
}

#[tokio::test]
async fn websocket_handshake_cloudflare_block_is_egress() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req.headers_mut()
        .insert("x-test-mode", "repro_cf1010_text".parse().unwrap());
    let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(r) => {
            assert_eq!(r.status(), 502);
            let b = String::from_utf8_lossy(r.body().as_deref().unwrap_or_default())
                .to_ascii_lowercase();
            assert!(!b.contains("1010") && !b.contains("cloudflare"), "{b}");
        }
        other => panic!("{other:?}"),
    }
    let o = e.outcomes();
    assert_eq!(o.len(), 1, "{o:?}");
    assert!(matches!(o[0].1, Outcome::EgressBlocked));
}

#[tokio::test]
async fn ws_rate_limit_and_lifecycle_errors_keep_their_codes() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    ws_turn(&e, "late_ratelimit_ws").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "rate_limit_exceeded", "{}", rows[0]);
    assert!(
        e.outcomes().iter().all(|(_, o)| !matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                ..
            }
        )),
        "{:?}",
        e.outcomes()
    );
    ws_turn(&e, "lifecycle_ws").await;
    let rows = wait_rows(&e, 2).await;
    assert_eq!(
        rows[0]["error"], "websocket_connection_limit_reached",
        "{}",
        rows[0]
    );
    assert_eq!(rows[0]["outcome"], "cancelled", "{}", rows[0]);
}

/// WS: per-turn send completion, deflate negotiation per leg, prewarm turn has its own row.
#[tokio::test]
async fn ws_downstream_observation_per_turn() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    for body in [
        r#"{"type":"response.create","model":"gpt-5.5","generate":false}"#,
        r#"{"type":"response.create","model":"gpt-5.5","tag":"slow"}"#,
        r#"{"type":"response.create","model":"gpt-5.5","tag":"meta"}"#,
    ] {
        ws.send(M::text(body)).await.unwrap();
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
    }
    drop(ws);
    let rows = wait_rows(&e, 3).await;
    let slow = &rows[1];
    assert!(
        slow["ttft_ms"].as_u64().unwrap() >= millis(250).as_millis() as u64,
        "{slow}"
    );
    let summary: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/summary?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(summary["summary"]["total"]["count"], 2);
    assert_eq!(summary["summary"]["total"]["input_tokens"], 18);
    let (turn, warm) = (&rows[0], &rows[2]);
    assert_eq!(warm["endpoint"], "prewarm", "{warm}");
    let (s, o) = obs_of(&e, turn["request_id"].as_str().unwrap()).await;
    assert_eq!(s, 200, "{o}");
    let term = ms(&o, "up_terminal_ms");
    assert_eq!(
        turn["ttft_ms"].as_u64(),
        Some(ms(&o, "up_first_text_ms")),
        "{turn} {o}"
    );
    // The terminal frame's send is attributed to its own turn.
    assert!(ms(&o, "ws_last_send_ms") >= term, "{o}");
    assert!(
        ms(&o, "ws_first_send_ms") <= ms(&o, "ws_last_send_ms"),
        "{o}"
    );
    assert!(
        ms(&o, "ws_send_msgs") >= 2 && ms(&o, "ws_send_bytes") > 0,
        "{o}"
    );
    assert!(
        ms(&o, "ws_sink_blocked_max_us") <= ms(&o, "ws_sink_blocked_us"),
        "{o}"
    );
    assert_eq!(o["down_incomplete"], false, "{o}");
    // The test client offers no permessage-deflate.
    assert_eq!(o["ws_deflate_down"], false, "{o}");
    assert!(o["ws_ext_down"].is_null(), "{o}");
    assert!(o["ws_deflate_up"].is_boolean(), "{o}");
    for k in ["sse_first_queue_ms", "sse_first_yield_ms"] {
        assert!(o[k].is_null(), "{k}: {o}");
    }
    // Old duration semantics: fixed when the terminal frame arrived (before its send).
    assert!(turn["duration_ms"].as_u64().unwrap() >= term, "{turn} {o}");
    let (_, reasoning) = obs_of(&e, rows[1]["request_id"].as_str().unwrap()).await;
    assert_eq!(
        rows[1]["ttft_ms"].as_u64(),
        Some(ms(&reasoning, "up_first_reasoning_ms")),
        "{} {reasoning}",
        rows[1]
    );
    let (s, w) = obs_of(&e, warm["request_id"].as_str().unwrap()).await;
    assert_eq!(s, 200, "{w}");
    assert!(
        w["response_id"].is_string() || w["response_id"].is_null(),
        "{w}"
    );
}

/// Wait for a close frame from the proxy and return its code (None: no close frame).
async fn ws_close_code<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>, secs: u64) -> Option<u16>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message as M;
    loop {
        match tokio::time::timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("close within the bound")
        {
            Some(Ok(M::Close(f))) => return f.map(|f| u16::from(f.code)),
            Some(Ok(_)) => continue,
            _ => return None,
        }
    }
}

/// A `response.create` turn of exactly `len` bytes.
fn sized_turn(len: usize) -> String {
    let head = r#"{"type":"response.create","model":"gpt-5.5","pad":""#;
    format!("{head}{}\"}}", "x".repeat(len - head.len() - 2))
}

/// The 101 advertises the smaller of the upstream's limit and `ws_max_message_mb`
/// (parsing edge cases are unit tests in `ws/mod.rs`).
#[tokio::test]
async fn ws_handshake_advertises_max_message_bytes_min_of_upstream_and_local() {
    const MIB: u64 = 1 << 20;
    let e = env(300).await;
    // (upstream 101 values, local ws_max_message_mb, advertised to the client)
    for (up, local, want) in [
        (None, 16, 16 * MIB),
        (Some("1048576"), 16, MIB),
        (Some("33554432"), 16, 16 * MIB),
        (Some("bogus"), 4, 4 * MIB),
        (Some("2097152|8388608"), 16, 2 * MIB),
    ] {
        let mut cfg = (*e.config.load()).clone();
        cfg.ws_max_message_mb = local;
        e.config.store(cfg);
        let mut req = ws_req(&e);
        if let Some(v) = up {
            req.headers_mut()
                .insert("x-test-mode", format!("ws_maxmsg:{v}").parse().unwrap());
        }
        let (mut ws, r) = tokio_tungstenite::connect_async(req).await.unwrap();
        assert_eq!(r.status(), 101);
        let got: Vec<_> = r
            .headers()
            .get_all("x-codex-websocket-max-message-bytes")
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        assert_eq!(
            got,
            vec![want.to_string()],
            "upstream {up:?}, local {local} MiB"
        );
        let _ = ws.close(None).await;
    }
}

/// An upstream 1009 is about the message, not the account: forwarded as 1009, recorded as
/// `upstream_ws_too_big`, no cooldown and no switch, whether the upstream closes after
/// reading the message or while ciallopa is still writing it.
#[tokio::test]
async fn ws_upstream_1009_closes_1009_without_cooling() {
    for (mode, len, local_mb, detail) in [
        ("ws_close_1009", 4096, None, "upstream closed with 1009"),
        // Above the upstream limit (1 MiB in this fixture, 16 MiB for real) and far more than
        // the socket buffers hold: the upstream stops reading, so the write fails first.
        ("ws_up_limit", 24 << 20, Some(32), "after a failed send"),
    ] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        if let Some(mb) = local_mb {
            let mut cfg = (*e.config.load()).clone();
            cfg.ws_max_message_mb = mb;
            e.config.store(cfg);
        }
        let mut req = ws_req(&e);
        req.headers_mut()
            .insert("x-test-mode", mode.parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        let hits = e.mock.hits.load(Ordering::SeqCst);
        ws.send(tokio_tungstenite::tungstenite::Message::text(sized_turn(
            len,
        )))
        .await
        .unwrap();
        assert_eq!(ws_close_code(&mut ws, 30).await, Some(1009), "{mode}");
        assert_eq!(
            e.mock.hits.load(Ordering::SeqCst),
            hits,
            "{mode}: no redial"
        );
        let outcomes = e.outcomes();
        assert!(
            outcomes
                .iter()
                .all(|(_, o)| matches!(o, Outcome::Success { .. })),
            "{mode}: {outcomes:?}"
        );
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], "upstream_ws_too_big", "{mode}: {rows:?}");
        assert_eq!(rows[0]["account"], "a", "{mode}: {rows:?}");
        assert_eq!(rows[0]["switches"], 0, "{mode}: {rows:?}");
        let v: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
            .await
            .json()
            .await
            .unwrap();
        assert!(v.to_string().contains(detail), "{mode}: {v}");
    }
}

/// Client-to-server frames after the HTTP handshake: (fin, rsv1, opcode, unmasked payload).
fn wire_frames(raw: &[u8]) -> Vec<(bool, bool, u8, Vec<u8>)> {
    let start = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let mut b = &raw[start..];
    let mut out = Vec::new();
    while b.len() >= 2 {
        let (fin, rsv1, op) = (b[0] & 0x80 != 0, b[0] & 0x40 != 0, b[0] & 0x0f);
        let masked = b[1] & 0x80 != 0;
        let (mut len, mut i) = ((b[1] & 0x7f) as usize, 2);
        if len == 126 {
            len = u16::from_be_bytes([b[2], b[3]]) as usize;
            i = 4;
        } else if len == 127 {
            len = u64::from_be_bytes(b[2..10].try_into().unwrap()) as usize;
            i = 10;
        }
        let mask = masked.then(|| {
            let m = [b[i], b[i + 1], b[i + 2], b[i + 3]];
            i += 4;
            m
        });
        if b.len() < i + len {
            break;
        }
        let mut p = b[i..i + len].to_vec();
        if let Some(m) = mask {
            for (j, x) in p.iter_mut().enumerate() {
                *x ^= m[j % 4];
            }
        }
        out.push((fin, rsv1, op, p));
        b = &b[i + len..];
    }
    out
}

#[tokio::test]
async fn ws_upstream_turn_is_fragmented_at_1_mib_like_codex_0_169() {
    use tokio_tungstenite::tungstenite::Message as M;
    const MIB: usize = 1 << 20;
    let mut tap = None;
    let e = env_inner(300, None, "", None, Some(&mut tap)).await;
    let tap = tap.unwrap();
    // A 4-byte character straddles the first 1 MiB boundary, so the first two fragments
    // are not valid UTF-8 on their own.
    let head = r#"{"type":"response.create","model":"gpt-5.5","pad":""#;
    let mut big = String::from(head);
    big.push_str(&"a".repeat(MIB - head.len() - 2));
    big.push('🌱');
    while big.len() < 3 * MIB + 5 {
        big.push('b');
    }
    big.push_str("\"}");
    assert_eq!(big.len(), 3 * MIB + 7);
    let exact = sized_turn(MIB);
    let small = sized_turn(300);
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    for msg in [&small, &exact, &big] {
        ws.send(M::text(msg.as_str())).await.unwrap();
        let got = ws.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(got.to_text().unwrap()).unwrap();
        let sent: serde_json::Value = serde_json::from_str(msg).unwrap();
        assert!(v["echo"] == sent, "upstream got the message intact");
    }
    let _ = ws.close(None).await;
    let conns = tap.lock().unwrap().clone();
    let raw = conns
        .iter()
        .map(|c| c.lock().unwrap().clone())
        .find(|b| {
            b.starts_with(b"GET ")
                && String::from_utf8_lossy(&b[..b.len().min(4096)])
                    .to_ascii_lowercase()
                    .contains("upgrade: websocket")
        })
        .expect("websocket connection through the tap");
    let data: Vec<_> = wire_frames(&raw).into_iter().filter(|f| f.2 <= 2).collect();
    let shape: Vec<_> = data
        .iter()
        .map(|(fin, rsv1, op, p)| (*fin, *rsv1, *op, p.len()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (true, false, 1, small.len()),
            // exactly 1 MiB is still one frame (Codex: `text.len() <= FRAGMENT_SIZE`)
            (true, false, 1, MIB),
            (false, false, 1, MIB),
            (false, false, 0, MIB),
            (false, false, 0, MIB),
            (true, false, 0, 7),
        ]
    );
    let joined: Vec<u8> = data[2..].iter().flat_map(|f| f.3.clone()).collect();
    assert_eq!(joined, big.as_bytes());
    assert!(std::str::from_utf8(&data[2].3).is_err() && std::str::from_utf8(&data[3].3).is_err());
}

/// Codex 0.169.0 ends the turn on status-less terminal error codes
/// (`parse_terminal_provider_error`; the full list is a unit test in `classify.rs`): the relay
/// settles the turn on the frame instead of holding it until the idle timeout, and the next
/// turn on the same socket goes through.
#[tokio::test]
async fn ws_codex_terminal_error_codes_end_the_turn() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    let codes = [
        "model_not_found",
        "audio_too_large",
        "inference_hook_denied",
    ];
    for (i, code) in codes.iter().enumerate() {
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"terminal_code:{code}"}}"#
        )))
        .await
        .unwrap();
        // Before any output a non-terminal error frame is held with the preamble, so the
        // client would not see it until the idle timeout.
        loop {
            match tokio::time::timeout(Duration::from_secs(3), ws.next())
                .await
                .unwrap_or_else(|_| panic!("{code}: turn held until the idle timeout"))
            {
                Some(Ok(M::Text(t))) if t.as_str().contains("\"type\":\"error\"") => {
                    assert!(t.as_str().contains(code), "{t}");
                    break;
                }
                Some(Ok(_)) => {}
                other => panic!("{code}: {other:?}"),
            }
        }
        // Settled on the error frame: the row is written while the socket stays open.
        let rows = wait_rows(&e, i + 1).await;
        assert_eq!(rows.len(), i + 1, "{code}: turn still open: {rows:?}");
        assert!(rows.iter().any(|r| r["error"] == *code), "{code}: {rows:?}");
    }
    ws.send(M::text(r#"{"type":"response.create","tag":"soft_error"}"#))
        .await
        .unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap()
        {
            Some(Ok(M::Text(t))) if t.as_str().contains("response.completed") => break,
            Some(Ok(_)) => {}
            other => panic!("next turn: {other:?}"),
        }
    }
    ws.close(None).await.unwrap();
    assert_eq!(wait_rows(&e, codes.len() + 1).await.len(), codes.len() + 1);
    assert!(e
        .outcomes()
        .iter()
        .all(|(_, o)| !matches!(o, Outcome::Transport)));
}
