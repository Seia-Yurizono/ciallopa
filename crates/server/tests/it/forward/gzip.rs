use super::scaffold::*;
use super::*;

#[tokio::test]
async fn responses_compressed_like_codex_and_retries_reuse_bytes() {
    let e = env(300).await;
    let body = r#"{"model":"gpt-5.5","prompt_cache_key":"pk","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello hello hello"}]}]}"#;
    // 503, then 200 on another account: both attempts carry the same compressed bytes.
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "retry")
        .header("accept-encoding", "gzip, br")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().get("content-encoding").is_none());
    assert!(r.text().await.unwrap().contains("response.completed"));
    let seen = e.mock.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for (h, b) in &seen {
        assert_eq!(hdr(h, "content-encoding"), Some("zstd"));
        assert_eq!(hdr(h, "accept-encoding"), Some("gzip"));
        assert_eq!(hdr(h, "content-length"), Some(b.len().to_string().as_str()));
        assert_eq!(zstd::decode_all(b.as_ref()).unwrap(), body.as_bytes());
        assert_eq!(b, &seen[0].1, "retries reuse the same bytes");
    }
    assert_eq!(
        seen[0].1.as_ref(),
        zstd::encode_all(body.as_bytes(), 3).unwrap().as_slice(),
        "zstd level 3"
    );
    // Session routing still sees the plaintext body.
    assert!(matches!(
        &e.outcomes().last().unwrap().1,
        Outcome::Success { .. }
    ));
}

#[tokio::test]
async fn compression_skips_compact_models_and_preencoded() {
    let e = env(300).await;
    let r = e
        .post("/v1/responses/compact")
        .header("accept-encoding", "gzip")
        .body(r#"{"model":"m"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("compact-ok"));
    let pre = zstd::encode_all(&b"{\"model\":\"m\"}"[..], 19).unwrap();
    e.post("/backend-api/codex/responses")
        .header("content-encoding", "zstd")
        .body(pre.clone())
        .send()
        .await
        .unwrap();
    e.http
        .get(format!("http://{}/v1/models", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    let seen = e.mock.seen.lock().unwrap().clone();
    let (compact_headers, compact_body) = &seen[0];
    assert_eq!(compact_body.as_ref(), br#"{"model":"m"}"#);
    assert!(hdr(compact_headers, "content-encoding").is_none());
    assert!(
        hdr(compact_headers, "accept-encoding").is_none(),
        "compact retains its previous header policy"
    );
    let (h, b) = &seen[1];
    assert_eq!(
        b.as_ref(),
        pre.as_slice(),
        "pre-encoded body byte-identical"
    );
    assert_eq!(hdr(h, "content-encoding"), Some("zstd"));
    let (h, _) = &seen[2];
    assert!(
        hdr(h, "content-encoding").is_none(),
        "models never compressed"
    );
}

#[tokio::test]
async fn compression_can_be_disabled() {
    let e = env_cfg(300, None, "compress_requests = false").await;
    let body = r#"{"model":"gpt-5.5","input":[]}"#;
    e.post("/v1/responses")
        .header("accept-encoding", "gzip")
        .body(body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let seen = e.mock.seen.lock().unwrap().clone();
    let (h, b) = &seen[0];
    assert_eq!(b.as_ref(), body.as_bytes());
    assert!(hdr(h, "content-encoding").is_none());
    assert_eq!(
        hdr(h, "accept-encoding"),
        Some("gzip"),
        "upload compression is independent of response negotiation"
    );
}

#[tokio::test]
async fn gzip_responses_negotiate_independently_and_preserve_plain_downstream() {
    for path in [
        "/v1/responses?fixture=1",
        "/backend-api/codex/responses?fixture=2",
    ] {
        let e = env(300).await;
        let response = e
            .post(path)
            .header("accept-encoding", "br, zstd")
            .header("x-test-mode", "gzip_json")
            .header("x-test-gzip", "1")
            .body(r#"{"model":"gpt-5.5","stream":false,"input":[]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.headers().get("content-encoding").is_none());
        // A new length for a buffered decoded body is fine; the compressed length is not.
        let length = response.headers().get("content-length").cloned();
        let body = response.bytes().await.unwrap();
        assert_eq!(
            body.as_ref(),
            br#"{"id":"plain-json","status":"completed"}"#
        );
        if let Some(length) = length {
            assert_eq!(
                length.to_str().unwrap().parse::<usize>().unwrap(),
                body.len()
            );
        }
        let seen = e.mock.seen.lock().unwrap();
        assert_eq!(hdr(&seen[0].0, "accept-encoding"), Some("gzip"));
    }

    let e = env_cfg(300, None, "gzip_responses = false\n").await;
    let response = e
        .post("/v1/responses")
        .header("accept-encoding", "gzip, br")
        .header("x-test-mode", "gzip_json")
        .header("x-test-gzip", "1")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert!(response.headers().get("content-encoding").is_none());
    assert!(response.text().await.unwrap().contains("plain-json"));
    assert_eq!(
        hdr(&e.mock.seen.lock().unwrap()[0].0, "accept-encoding"),
        Some("identity")
    );
}

#[tokio::test]
async fn gzip_sse_flush_reaches_client_before_trailer_and_records_usage() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut response = tokio::time::timeout(
        Duration::from_secs(3),
        e.post("/v1/responses")
            .header("accept-encoding", "identity")
            .header("x-test-mode", "gzip_flush_held")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send(),
    )
    .await
    .expect("gzip output must not wait for the terminal event or trailer")
    .unwrap();
    assert_eq!(response.status(), 200);
    for name in ["content-encoding", "content-length", "etag", "content-md5"] {
        assert!(response.headers().get(name).is_none(), "stale {name}");
    }
    let mut body = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let chunk = response
                .chunk()
                .await
                .unwrap()
                .expect("held stream stays open");
            body.extend_from_slice(&chunk);
            if String::from_utf8_lossy(&body).contains("你好") {
                break;
            }
        }
    })
    .await
    .expect("flushed UTF-8 SSE output must arrive while trailer is held");
    assert!(!String::from_utf8_lossy(&body).contains("response.completed"));
    e.mock.release.notify_one();
    body.extend_from_slice(&response.bytes().await.unwrap());
    assert!(String::from_utf8_lossy(&body).contains("response.completed"));
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{rows:?}");
    assert!(rows[0]["ttft_ms"].is_u64(), "{rows:?}");
    assert_eq!(rows[0]["input_tokens"], 7);
    assert_eq!(rows[0]["output_tokens"], 2);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
    rec.shutdown();
}

#[tokio::test]
async fn gzip_sse_without_content_type_keeps_usage_and_quota_failover() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for mode in ["usage", "midlimit"] {
        let response = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .header("x-test-noct", "1")
            .header("x-test-gzip", "1")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        assert!(response.headers().get("content-encoding").is_none());
        let body = response.text().await.unwrap();
        assert!(body.contains("response.completed"), "{mode}: {body}");
        assert!(!body.contains("usage_limit_reached"), "{mode}: {body}");
    }
    let rows = wait_rows(&e, 2).await;
    let usage = rows.iter().find(|row| row["input_tokens"] == 120).unwrap();
    assert_eq!(usage["output_tokens"], 30);
    assert!(usage["ttft_ms"].is_u64());
    let rescued = rows.iter().find(|row| row["switches"] == 1).unwrap();
    assert_eq!(rescued["account"], "b");
    assert!(rescued["error"].is_null());
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    rec.shutdown();
}

#[tokio::test]
async fn gzip_http_errors_keep_refresh_failover_and_classification() {
    for mode in ["unauth", "limit", "retry", "over400"] {
        let e = env_cfg(300, None, "overload_retry_wait_secs = 0\n").await;
        let response = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .header("x-test-gzip", "1")
            .body(r#"{"model":"gpt-5.5","input":[]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "{mode}");
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("response.completed"),
            "{mode}"
        );
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2, "{mode}");
        if mode == "unauth" {
            assert_eq!(e.store.refreshed.load(Ordering::SeqCst), 1);
        }
        if mode == "limit" {
            assert!(e.outcomes().iter().any(|(_, out)| matches!(
                out,
                Outcome::RateLimited {
                    usage_limit: true,
                    ..
                }
            )));
        }
    }
}

#[tokio::test]
async fn gzip_response_encoding_contract_rejects_unknown_and_duplicate_without_replay() {
    for encoding in ["br", "gzip, gzip", "duplicate"] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let response = e
            .post("/v1/responses")
            .header("x-test-response-encoding", encoding)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 502, "{encoding}");
        let body = response.text().await.unwrap();
        assert!(body.contains("server_error"), "{encoding}: {body}");
        assert_eq!(
            e.mock.hits.load(Ordering::SeqCst),
            1,
            "no replay for {encoding}"
        );
        assert!(
            e.outcomes()
                .iter()
                .all(|(_, out)| matches!(out, Outcome::Success { .. })),
            "no account penalty: {:?}",
            e.outcomes()
        );
        let rows = wait_rows(&e, 1).await;
        assert_eq!(
            rows[0]["error"], "unsupported_response_encoding",
            "{rows:?}"
        );
        assert_eq!(rows[0]["err_source"], "upstream", "{rows:?}");
        rec.shutdown();
    }
}

#[tokio::test]
async fn gzip_corruption_before_output_is_decode_error_without_replay() {
    for fault in ["bad_crc", "truncated", "trailing"] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let response = e
            .post("/v1/responses")
            .header("x-test-mode", "gzip_bootstrap_only")
            .header("x-test-gzip", fault)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        // Preserve the gateway's existing SSE read-failure contract: a neutral
        // failure event on the already selected upstream status, with detail in usage.
        assert_eq!(response.status(), 200, "{fault}");
        let body = response.text().await.unwrap();
        assert!(body.contains("response.failed"), "{fault}: {body}");
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
        assert!(
            e.outcomes()
                .iter()
                .all(|(_, out)| matches!(out, Outcome::Success { .. })),
            "no account penalty: {:?}",
            e.outcomes()
        );
        let rows = wait_rows(&e, 1).await;
        assert_eq!(
            rows[0]["error"], "response_decode_error",
            "{fault}: {rows:?}"
        );
        assert_eq!(rows[0]["err_source"], "stream", "{fault}: {rows:?}");
        assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
        rec.shutdown();
    }
}

#[tokio::test]
async fn gzip_truncation_after_flushed_output_is_decode_error_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut response = e
        .post("/v1/responses")
        .header("x-test-mode", "gzip_late_truncated")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut first = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !String::from_utf8_lossy(&first).contains("response.output_text.delta") {
            first.extend_from_slice(&response.chunk().await.unwrap().unwrap());
        }
    })
    .await
    .expect("flushed output arrives before the truncated trailer");
    e.mock.release.notify_one();
    let body = response.text().await.unwrap();
    assert!(body.contains("response.failed"), "{body}");
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "response_decode_error", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "stream", "{rows:?}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
    rec.shutdown();
}

#[tokio::test]
async fn gzip_network_read_errors_remain_network_errors() {
    for mode in ["delta_then_reset", "reset_first"] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let response = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .header("x-test-gzip", "stream")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        let _ = response.bytes().await;
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], "stream_read_error", "{mode}: {rows:?}");
        assert_eq!(rows[0]["err_source"], "network", "{mode}: {rows:?}");
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
        rec.shutdown();
    }
}

#[tokio::test]
async fn gzip_completed_event_is_not_failed_by_late_trailer_or_client_close() {
    for mode in [
        "gzip_terminal_bad_crc",
        "gzip_terminal_truncated",
        "gzip_terminal_drop",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let mut response = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        let mut prefix = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !String::from_utf8_lossy(&prefix).contains("response.completed") {
                prefix.extend_from_slice(&response.chunk().await.unwrap().unwrap());
            }
        })
        .await
        .expect("a completed answer must arrive before the gzip trailer");
        if mode == "gzip_terminal_drop" {
            drop(response);
        } else {
            e.mock.release.notify_one();
            let rest = response.text().await.unwrap();
            assert!(!rest.contains("response.failed"), "{mode}: {rest}");
        }
        let rows = wait_rows(&e, 1).await;
        assert!(rows[0]["error"].is_null(), "{mode}: {rows:?}");
        assert_eq!(rows[0]["output_tokens"], 2, "{mode}: {rows:?}");
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
        assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
        e.mock.release.notify_one();
        rec.shutdown();
    }
}

#[tokio::test]
async fn gzip_sse_cancellation_and_force_release_held_decoder() {
    for force in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let mut response = e
            .post("/v1/responses")
            .header("x-test-mode", "gzip_flush_held")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        assert!(response.chunk().await.unwrap().is_some());
        if force {
            e.drain.force();
        }
        drop(response);
        let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 1))
            .await
            .unwrap();
        assert_eq!(
            rows[0]["error"],
            if force { "shutdown" } else { "client_closed" },
            "{rows:?}"
        );
        assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
        e.mock.release.notify_one();
        rec.shutdown();
    }
}

#[tokio::test]
async fn gzip_sse_idle_timeout_uses_decoded_progress() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(1, Some(rec.clone())).await;
    let response = e
        .post("/v1/responses")
        .header("x-test-mode", "gzip_flush_held")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(4), response.text())
        .await
        .unwrap()
        .unwrap();
    assert!(body.contains("response.failed"), "{body}");
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "idle_timeout", "{rows:?}");
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
    e.mock.release.notify_one();
    rec.shutdown();
}

#[tokio::test]
async fn gzip_http_error_limit_applies_after_decompression() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let response = e
        .post("/v1/responses")
        .header("x-test-mode", "error_too_large")
        .header("x-test-gzip", "1")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert!(response.bytes().await.unwrap().len() < 1024);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "error_body_too_large", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "local", "{rows:?}");
    assert_eq!(rows[0]["upstream_status"], 503, "{rows:?}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
    rec.shutdown();
}

#[tokio::test]
async fn gzip_response_policy_reload_only_changes_new_requests_and_retries_keep_snapshot() {
    let e = env(300).await;
    let request = e
        .post("/v1/responses")
        .header("x-test-mode", "gzip_retry_reload")
        .body(r#"{"model":"gpt-5.5","input":[]}"#);
    let response = tokio::spawn(async move { request.send().await.unwrap().text().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(3), e.mock.ready.notified())
        .await
        .unwrap();
    let mut cfg = (*e.config.load()).clone();
    cfg.upstream_opts.gzip_responses = false;
    e.config.store(cfg);
    e.mock.release.notify_one();
    assert!(response.await.unwrap().contains("response.completed"));
    let next = e.post("/v1/responses").body("{}").send().await.unwrap();
    let _ = next.bytes().await.unwrap();
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert_eq!(hdr(&seen[0].0, "accept-encoding"), Some("gzip"));
    assert_eq!(hdr(&seen[1].0, "accept-encoding"), Some("gzip"));
    assert_eq!(
        seen[0].1, seen[1].1,
        "retry keeps the compressed upload snapshot too"
    );
    assert_eq!(hdr(&seen[2].0, "accept-encoding"), Some("identity"));
}
