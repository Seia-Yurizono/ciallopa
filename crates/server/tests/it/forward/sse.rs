use super::scaffold::*;
use super::*;

#[tokio::test]
async fn sse_passthrough_headers_and_zstd() {
    let e = env(300).await;
    let raw = br#"{"model":"gpt-5.5","prompt_cache_key":"pk","input":[]}"#;
    let z = zstd::encode_all(&raw[..], 3).unwrap();
    let r = e
        .post("/backend-api/codex/responses")
        .header("originator", "codex_cli_rs")
        .header("chatgpt-account-id", "client-acct")
        .header("x-forwarded-for", "9.9.9.9")
        .header("True-Client-IP", "203.0.113.7")
        .header("CF-Connecting-IP", "203.0.113.7")
        .header("X-Real-IP", "203.0.113.7")
        .header("content-encoding", "zstd")
        .header("session_id", "s1")
        .body(z.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");
    assert!(r.text().await.unwrap().contains("response.completed"));
    let seen = e.mock.seen.lock().unwrap();
    let (h, b) = &seen[0];
    assert_eq!(
        b.as_ref(),
        z.as_slice(),
        "zstd body forwarded byte-identical"
    );
    let names: Vec<&str> = h.iter().map(|(k, _)| k.as_str()).collect();
    let get = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    assert_eq!(get("authorization"), Some("Bearer tok-a-1"));
    assert_eq!(get("chatgpt-account-id"), Some("acct-a"));
    assert_eq!(get("content-encoding"), Some("zstd"));
    for k in [
        "x-forwarded-for",
        "true-client-ip",
        "cf-connecting-ip",
        "x-real-ip",
    ] {
        assert!(get(k).is_none(), "{k} forwarded upstream");
    }
    let pos = |k: &str| names.iter().position(|n| *n == k).unwrap();
    assert!(
        pos("originator") < pos("chatgpt-account-id")
            && pos("chatgpt-account-id") < pos("content-encoding")
    );
    assert!(matches!(
        &e.outcomes()[0].1,
        Outcome::Success { quota: Some(_) }
    ));
}

#[tokio::test]
async fn idle_timeout_aborts_stream() {
    let e = env(1).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let t0 = std::time::Instant::now();
    // A cut-off stream ends with a synthetic `response.failed` so the client retries.
    let b = r.text().await.unwrap();
    assert!(b.contains("event: response.failed"), "{b}");
    assert!(!b.contains("ciallopa"));
    assert!(t0.elapsed() < Duration::from_secs(4));
    assert!(e.outcomes().iter().any(|(_, o)| *o == Outcome::Transport));
}

#[tokio::test]
async fn stream_diagnostics_distinguish_heartbeat_activity_from_events_without_payloads() {
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    for (mode, error, last, count) in [
        ("diag_boot_eof", "eof", "response.created", 1),
        ("diag_boot_idle", "idle_timeout", "response.created", 1),
        ("diag_stream_eof", "eof", "response.output_text.delta", 2),
        ("diag_unknown", "eof", "unknown", 1),
    ] {
        warnings.0.lock().unwrap().clear();
        let r = e
            .post("/backend-api/codex/responses")
            .header("x-test-mode", mode)
            .header("thread-id", "diag-thread")
            .header("x-codex-turn-metadata", r#"{"turn_id":"diag-turn"}"#)
            .body(r#"{"input":"private-prompt"}"#)
            .send()
            .await
            .unwrap();
        assert!(r.text().await.unwrap().contains("response.failed"));
        let logs = warnings.0.lock().unwrap().clone();
        assert_eq!(logs.len(), 1, "{mode}: {logs:?}");
        let log = &logs[0];
        assert_eq!(log["transport"], "sse");
        assert_eq!(log["error_class"], error);
        assert_eq!(log["last_event"], last);
        assert_eq!(log["event_count"], count);
        assert_eq!(log["terminal_received"], false);
        assert_eq!(log["observation_limited"], false);
        assert_eq!(log["activity_received"], true);
        assert_eq!(log["account"], "a");
        assert_eq!(log["upstream_request_id"], "diag-upstream-id");
        assert_eq!(log["thread_id"], "diag-thread");
        assert_eq!(log["turn_id"], "diag-turn");
        assert_eq!(log["record_id"].as_str().unwrap().len(), 16);
        assert!(
            log["event_idle_ms"].as_u64().unwrap()
                >= log["activity_idle_ms"].as_u64().unwrap() + 80,
            "{log}"
        );
        assert!(!log.to_string().contains("private-"), "{log}");
    }
    warnings.0.lock().unwrap().clear();
    let text = sse_text(&e, "diag_success").await;
    assert!(text.contains("response.completed"));
    assert!(
        warnings.0.lock().unwrap().is_empty(),
        "successful stream is quiet"
    );

    // The operational warning remains available even if request persistence is disabled.
    let e = env(1).await;
    assert!(sse_text(&e, "diag_boot_eof")
        .await
        .contains("response.failed"));
    let logs = warnings.0.lock().unwrap().clone();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["error_class"], "eof");
    assert!(logs[0].get("record_id").is_none());
}

#[tokio::test]
async fn sse_quota_before_output_switches_transparently() {
    let e = env(300).await;
    let t = sse_text(&e, "midlimit").await;
    assert!(!t.contains("usage_limit_reached"), "{t}");
    assert!(t.contains("r-b") && t.contains("response.completed"), "{t}");
    assert!(!t.contains("r-a"), "{t}");
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].1, seen[1].1, "same body replayed");
    drop(seen);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a" && matches!(
        o,
        Outcome::RateLimited { usage_limit: true, retry_after: Some(d), .. } if *d == Duration::from_secs(45)
    )));
}

#[tokio::test]
async fn sse_quota_after_output_is_forwarded_and_cools() {
    let e = env(300).await;
    let t = sse_text(&e, "latelimit").await;
    assert!(
        t.contains("delta") && t.contains("usage_limit_reached"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a"
        && matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                ..
            }
        )));
}

#[tokio::test]
async fn oversized_sse_event_keeps_its_existing_passthrough_contract() {
    for mode in [
        "event_large_completed",
        "event_large_literal",
        "event_large_plain",
        "event_large_delta_completed",
    ] {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let text = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if mode == "event_large_completed" {
            assert!(text.starts_with("data: {\"type\":\"response.completed\""));
            assert!(text.ends_with("\"output_tokens\":0}}}\n\n"));
        } else {
            assert!(text.starts_with("data: {\"type\":\"response.output_text.delta\""));
        }
        assert_eq!(
            text.bytes().filter(|&b| b == b'x').count(),
            9 * 1024 * 1024 + usize::from(mode != "event_large_completed") // output_text
        );
        assert!(!text.contains("stream disconnected before completion"));
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["status"], 200);
        assert_eq!(rows[0]["transport"], "sse");
        if mode == "event_large_delta_completed" {
            // Observation can resume at the next complete event after the oversized one.
            assert!(rows[0]["error"].is_null(), "{rows:?}");
            assert_eq!(rows[0]["input_tokens"], 7);
        } else {
            assert_eq!(
                rows[0]["error"], "observation_incomplete",
                "{mode}: {rows:?}"
            );
            assert_eq!(rows[0]["err_source"], "local");
            assert!(rows[0]["input_tokens"].is_null());
            assert!(rows[0]["output_tokens"].is_null());
        }
        rec.shutdown();
    }
}

#[tokio::test]
async fn oversized_sse_observation_keeps_real_client_cancellation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let response = e
            .post("/v1/responses")
            .header("x-test-mode", "event_large_held")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        let mut body = response.bytes_stream();
        let mut received = 0;
        while received < 9 * 1024 * 1024 {
            received += body.next().await.unwrap().unwrap().len();
        }
        drop(body);
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
        assert_eq!(rows[0]["err_source"], "client");
        rec.shutdown();
    })
    .await
    .expect("cancelled large stream must release its tracker");
}

#[tokio::test]
async fn sse_observation_uses_event_type_not_payload_strings() {
    for (mode, error, output) in [
        ("event_literal_complete", None, Some(3)),
        ("event_literal_truncated", Some("stream_truncated"), None),
        ("event_metadata_only", None, Some(0)),
        ("event_done", None, Some(0)),
        // The upstream code is kept, like an HTTP error body.
        ("event_spaced_error", Some("bad_request"), None),
    ] {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let text = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            text.contains("stream disconnected before completion"),
            mode == "event_literal_truncated",
            "{mode}: {text}"
        );
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"].as_str(), error, "{mode}: {}", rows[0]);
        assert_eq!(
            rows[0]["output_tokens"].as_i64(),
            output,
            "{mode}: {}",
            rows[0]
        );
        if matches!(mode, "event_metadata_only" | "event_done") {
            assert!(rows[0]["ttft_ms"].is_null(), "{mode}: {}", rows[0]);
        }
        rec.shutdown();
    }
}

#[tokio::test]
async fn sse_without_content_type_usage_and_failover() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let stream = r#"{"model":"gpt-5.5","stream":true,"input":[]}"#;
    // usage tokens: stream:true hint (v1) and Accept hint (backend-api, no stream field)
    let (ct, t) = noct_post(&e, "/v1/responses", "usage", stream, false).await;
    assert_eq!(ct, "text/event-stream", "content-type added downstream");
    assert!(t.contains("r-u"));
    let (_, t) = noct_post(
        &e,
        "/backend-api/codex/responses",
        "usage",
        r#"{"model":"gpt-5.5","input":[]}"#,
        true,
    )
    .await;
    assert!(t.contains("r-u"));
    let rows = wait_rows(&e, 2).await;
    for u in &rows {
        assert_eq!(u["input_tokens"], 120, "{u}");
        assert_eq!(u["output_tokens"], 30, "{u}");
        assert!(u["ttft_ms"].is_u64(), "{u}");
    }
    // pre-output usage_limit_reached replays on the next account, both paths
    for path in ["/v1/responses", "/backend-api/codex/responses"] {
        let before = e.mock.seen.lock().unwrap().len();
        let (_, t) = noct_post(&e, path, "midlimit", stream, false).await;
        assert!(!t.contains("usage_limit_reached"), "{path}: {t}");
        assert!(t.contains("r-b") && !t.contains("r-a"), "{path}: {t}");
        assert_eq!(e.mock.seen.lock().unwrap().len(), before + 2, "{path}");
    }
    // compaction turn without content-type
    let body = r#"{"model":"gpt-5.5","stream":true,"input":[{"type":"compaction_trigger"}]}"#;
    let (ct, t) = noct_post(&e, "/v1/responses", "compact", body, false).await;
    assert_eq!(ct, "text/event-stream");
    assert!(t.contains("\"encrypted_content\":\"gAAA\""), "{t}");
}

#[tokio::test]
async fn no_content_type_without_stream_hint_stays_passthrough() {
    let e = env(300).await;
    let (ct, t) = noct_post(
        &e,
        "/v1/responses",
        "midlimit",
        r#"{"model":"gpt-5.5","input":[]}"#,
        false,
    )
    .await;
    assert!(ct.is_empty(), "no hint: headers untouched");
    assert!(t.contains("usage_limit_reached"), "{t}");
}

#[tokio::test]
async fn bootstrap_transport_error_reaches_client() {
    // M2: an upstream error after `response.created` must not look like a clean EOF.
    let e = env(300).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "booterr")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let b = r.text().await.unwrap();
    assert!(
        b.contains("event: response.failed"),
        "client must see the stream error: {b}"
    );
    assert!(e.outcomes().iter().any(|(_, o)| *o == Outcome::Transport));
}

#[tokio::test]
async fn bootstrap_idle_timeout_not_doubled() {
    // L1: idle=2s must fail after ~2s, not ~4s.
    let e = env(2).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body("{}")
        .send()
        .await
        .unwrap();
    let t0 = std::time::Instant::now();
    assert!(r.text().await.unwrap().contains("response.failed"));
    let el = t0.elapsed();
    assert!(el < Duration::from_millis(3500), "took {el:?}");
}

#[tokio::test]
async fn response_failed_instructions_mention_is_not_quota() {
    let e = env(300).await;
    let t = sse_text(&e, "instrlimit").await;
    assert!(
        t.contains("server_error") && t.contains("usage_limit_reached"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1, "must not failover");
    assert!(
        !e.outcomes()
            .iter()
            .any(|(_, o)| matches!(o, Outcome::RateLimited { .. })),
        "{:?}",
        e.outcomes()
    );
}

#[tokio::test]
async fn sse_overload_before_output_switches() {
    let e = env(300).await;
    let t = sse_text(&e, "midover").await;
    assert!(!t.contains("overloaded"), "{t}");
    assert!(
        t.contains("r-b") && t.contains("response.completed") && !t.contains("r-a"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let o = e.outcomes();
    assert!(o
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { cool: true, .. })));
    assert!(!o
        .iter()
        .any(|(_, o)| matches!(o, Outcome::RateLimited { .. } | Outcome::AuthFailed)));
}

#[tokio::test]
async fn sse_overload_everywhere_passes_through_verbatim() {
    let e = env(300).await;
    let t = sse_text(&e, "allover").await;
    assert!(
        t.contains("server_is_overloaded") && t.contains("response.failed"),
        "{t}"
    );
    assert!(t.contains("\"r-a\""), "{t}");
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        3,
        "each account once, plus one wait-and-retry"
    );
    let o = e.outcomes();
    // The last usable account is never cooled.
    assert!(o
        .iter()
        .any(|(a, o)| a == "b" && matches!(o, Outcome::Overloaded { cool: false, .. })));
    // Once per attempt on b: the passed-through event is not counted again by the stream.
    assert_eq!(
        o.iter()
            .filter(|(a, o)| a == "b" && matches!(o, Outcome::Overloaded { .. }))
            .count(),
        2,
        "{o:?}"
    );
}

#[tokio::test]
async fn sse_overload_after_output_is_forwarded() {
    let e = env(300).await;
    let t = sse_text(&e, "lateover").await;
    assert!(
        t.contains("delta") && t.contains("server_is_overloaded"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sse_overload_last_account_waits_and_retries_once() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 1\n").await;
    let t0 = std::time::Instant::now();
    let t = sse_text(&e, "overonce").await;
    assert!(t.contains("r-b") && !t.contains("overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    assert!(t0.elapsed() >= std::time::Duration::from_millis(600));
}

#[tokio::test]
async fn sse_overload_no_more_retry_skips_wait() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 1\n").await;
    let t = sse_text(&e, "overnmr").await;
    assert!(t.contains("server_is_overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn sse_overload_wait_retry_disabled() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 0\n").await;
    let t = sse_text(&e, "allover").await;
    assert!(t.contains("server_is_overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

/// Codex drops the connection as soon as it has `response.completed`; the upstream body still
/// has a tail. That is a normal end, not `client_closed`. TTFT is the first output event.
#[tokio::test]
async fn sse_close_after_completed_is_not_an_error() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut r = e
        .post("/v1/responses")
        .header("x-test-mode", "slowtail")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    let mut got = Vec::new();
    while let Some(c) = r.chunk().await.unwrap() {
        got.extend_from_slice(&c);
        if String::from_utf8_lossy(&got).contains("response.completed") {
            break;
        }
    }
    drop(r);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    assert!(u["error"].is_null(), "{u}");
    assert!(u["err_source"].is_null(), "{u}");
    assert!(u["ttft_ms"].as_u64().unwrap() >= 250, "{u}");
}

#[tokio::test]
async fn sse_disconnect_while_upstream_idle_records_client_close_promptly() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut response = e
        .post("/v1/responses")
        .header("x-test-mode", "idle_after_output")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.chunk().await.unwrap().is_some());
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(response);

    // The upstream never yields another chunk; client closure must finish the tracker
    // well before the configured 300 s upstream idle timeout.
    let rows = tokio::time::timeout(Duration::from_secs(5), usage_rows(&e))
        .await
        .expect("client disconnect releases the tracker promptly");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "client", "{rows:?}");
    // The detail records how long the upstream had been silent when the client left,
    // which tells a Codex idle-timeout disconnect apart from a user cancel.
    let v: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let msg = v["message"].as_str().unwrap_or_default();
    let secs: f64 = msg
        .split("upstream silent for ")
        .nth(1)
        .and_then(|r| r.split('s').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no silent duration: {v}"));
    assert!((0.2..5.0).contains(&secs), "{v}");
}

#[tokio::test]
async fn http_idle_snapshot_survives_reload_before_headers() {
    let e = env(10).await;
    for existing in [true, false] {
        let request = e
            .post("/v1/responses")
            .header("x-test-mode", "reload_idle")
            .body(r#"{"stream":true,"input":[]}"#);
        let response =
            tokio::spawn(async move { request.send().await.unwrap().text().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(3), e.mock.ready.notified())
            .await
            .expect("upstream reached the header barrier");
        if existing {
            let mut cfg = (*e.config.load()).clone();
            cfg.upstream_opts.stream_idle_timeout_secs = 1;
            e.config.store(cfg);
        }
        e.mock.release.notify_one();
        let body = tokio::time::timeout(Duration::from_secs(5), response)
            .await
            .unwrap()
            .unwrap();
        if existing {
            assert!(
                body.contains("response.completed"),
                "in-flight request changed its timeout: {body}"
            );
            assert!(!body.contains("response.failed"), "{body}");
        } else {
            assert!(
                body.contains("response.failed"),
                "next request did not use the new timeout: {body}"
            );
            assert!(!body.contains("response.completed"), "{body}");
        }
    }
}

/// HTTP: a 200 + SSE `error` rejection before output becomes an upstream-shaped 400 (Codex
/// shows it once instead of retrying the stream), with no retry, switch or cooldown.
#[tokio::test]
async fn encrypted_rejection_sse_becomes_400() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "enc").await;
    assert_eq!(r.status(), 400);
    assert_eq!(r.headers()["content-type"], "application/json");
    let v: serde_json::Value = r.json().await.unwrap();
    let err = &v["error"];
    assert_eq!(err["code"], "invalid_encrypted_content");
    assert_eq!(err["type"], "invalid_request_error");
    assert_eq!(err["param"], "input");
    let msg = err["message"].as_str().unwrap();
    assert!(
        msg.contains("encrypted reasoning content") && msg.contains(ENC_HINT),
        "{msg}"
    );
    assert!(
        !msg.to_ascii_lowercase().contains("ciallopa")
            && !msg.to_ascii_lowercase().contains("proxy")
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1, "no retry / switch");
    assert_no_account_outcome(&e);
    // The account keeps serving.
    let _ = sse_text(&e, "").await;
    assert!(e.mock.seen.lock().unwrap()[1]
        .0
        .iter()
        .any(|(k, v)| k == "authorization" && v.contains("tok-a")));
    let rows = wait_rows(&e, 2).await;
    let row = &rows[1];
    assert_eq!(row["status"], 400, "{row}");
    assert_eq!(row["error"], "invalid_encrypted_content", "{row}");
    assert_eq!(row["err_source"], "upstream", "{row}");
    assert_eq!(row["switches"], 0, "{row}");
    let dv: serde_json::Value = detail_of(&e, row["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert!(
        dv["message"]
            .as_str()
            .unwrap()
            .contains("organization_id did not match"),
        "{dv}"
    );
    assert!(dv["body"].as_str().unwrap().contains("rs_0abc"), "{dv}");
}

/// HTTP: a plain upstream 400 rejection gets the same clear message.
#[tokio::test]
async fn encrypted_rejection_http_400_clear_message() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "enc400").await;
    assert_eq!(r.status(), 400);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["error"]["code"], "invalid_encrypted_content");
    assert!(v["error"]["message"].as_str().unwrap().contains(ENC_HINT));
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_no_account_outcome(&e);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "invalid_encrypted_content");
    assert_eq!(rows[0]["err_source"], "upstream");
    // Other 400 bodies still pass through verbatim.
    let r = post_mode(&e, "errbody").await;
    assert_eq!(r.status(), 400);
    assert!(r.text().await.unwrap().contains("bad input"));
}

/// HTTP: after output the rejection ends the stream as `response.failed` with the clear message
/// (the upstream wording stays out of the client stream, but in the log detail).
#[tokio::test]
async fn encrypted_rejection_after_output_is_clear_failed_event() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "lateenc").await;
    assert!(t.contains("\"delta\":\"hi\""), "{t}");
    assert!(t.contains("event: response.failed"), "{t}");
    assert!(
        t.contains("invalid_encrypted_content") && t.contains(ENC_HINT),
        "{t}"
    );
    assert!(
        !t.contains("could not be verified") && !t.contains("event: error"),
        "{t}"
    );
    assert!(t.ends_with("\n\n"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_no_account_outcome(&e);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["status"], 200);
    assert_eq!(rows[0]["error"], "invalid_encrypted_content");
    assert_eq!(rows[0]["err_source"], "upstream");
    let dv: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert!(
        dv["message"]
            .as_str()
            .unwrap()
            .contains("could not be verified"),
        "{dv}"
    );
}

/// Bookkeeping events (`response.queued` / `response.metadata` / `keepalive`) before an
/// in-stream overload must not commit output: switch account and succeed (sub2api #8139).
#[tokio::test]
async fn sse_metadata_before_overload_still_switches() {
    let e = env(300).await;
    let t = sse_text(&e, "metaover").await;
    assert!(
        !t.contains("overloaded") && !t.contains("openai_chatgpt_moderation_metadata"),
        "first-account preamble+error must be discarded on switch: {t}"
    );
    assert!(
        t.contains("r-b") && t.contains("response.completed") && !t.contains("r-a"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { cool: true, .. })));
}

/// Same bookkeeping before `invalid_encrypted_content` must still become HTTP 400 (not
/// `response.failed` after a false "output started").
#[tokio::test]
async fn sse_metadata_before_encrypted_still_400() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "metaenc").await;
    assert_eq!(r.status(), 400, "expected HTTP 400, not a 200 stream");
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["error"]["code"], "invalid_encrypted_content");
    assert!(
        v["error"]["message"].as_str().unwrap().contains(ENC_HINT),
        "{v}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_no_account_outcome(&e);
}

/// After we commit, the same bookkeeping events are forwarded to the client unchanged.
#[tokio::test]
async fn sse_metadata_forwarded_once_output_commits() {
    let e = env(300).await;
    let t = sse_text(&e, "metaok").await;
    assert!(t.contains("response.queued"), "{t}");
    assert!(t.contains("openai_chatgpt_moderation_metadata"), "{t}");
    assert!(t.contains("\"type\":\"keepalive\""), "{t}");
    assert!(
        t.contains("\"delta\":\"hi\"") && t.contains("response.completed"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
}

/// Regression: chatgpt announces an empty message item / text part before an overload.
/// Those announcements are not output: the attempt must switch with nothing duplicated.
#[tokio::test]
async fn sse_empty_added_before_overload_switches() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "emptyadded_over").await;
    assert!(!t.contains("overloaded"), "{t}");
    assert!(
        !t.contains("r-a") && !t.contains("msg_a") && !t.contains("rs_a"),
        "{t}"
    );
    assert_eq!(t.matches("response.created").count(), 1, "{t}");
    assert!(t.contains("r-b") && t.contains("response.completed"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let o = e.outcomes();
    assert!(
        o.iter()
            .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { cool: true, .. })),
        "{o:?}"
    );
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert_eq!(rows[0]["account"], "b", "{}", rows[0]);
}

/// Streamed tool-call arguments are real output: the overload after them is forwarded, never
/// replayed, and the request is recorded as a failure (not a success) on the account.
#[tokio::test]
async fn sse_overload_after_tool_call_arguments_is_a_failure() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "tooladded_over").await;
    assert!(
        t.contains("fc_a") && t.contains("server_is_overloaded"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "server_is_overloaded", "{}", rows[0]);
    assert_eq!(rows[0]["err_source"], "stream", "{}", rows[0]);
    let o = e.outcomes();
    assert!(
        o.iter()
            .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })),
        "a late overload still counts against the account: {o:?}"
    );
}

/// Late overload (after real text) is recorded as a failure as well.
#[tokio::test]
async fn sse_overload_after_output_is_recorded_as_failure() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "lateover").await;
    assert!(
        t.contains("delta") && t.contains("server_is_overloaded"),
        "{t}"
    );
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "server_is_overloaded", "{}", rows[0]);
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
}

#[tokio::test]
async fn read_error_after_completed_is_a_normal_end() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "completed_then_reset").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert!(rows[0]["err_source"].is_null(), "{}", rows[0]);
    assert_eq!(rows[0]["output_tokens"], 1);
    assert!(
        e.outcomes()
            .iter()
            .all(|(_, o)| matches!(o, Outcome::Success { .. })),
        "{:?}",
        e.outcomes()
    );
}

#[tokio::test]
async fn idle_after_completed_is_a_normal_end() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    read_all(&e, "completed_then_idle").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert!(rows[0]["err_source"].is_null(), "{}", rows[0]);
    assert!(
        e.outcomes()
            .iter()
            .all(|(_, o)| matches!(o, Outcome::Success { .. })),
        "{:?}",
        e.outcomes()
    );
}

#[tokio::test]
async fn stream_request_error_records_its_code_without_account_penalty() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "late_request_error").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "context_length_exceeded", "{}", rows[0]);
    assert_eq!(rows[0]["err_source"], "stream", "{}", rows[0]);
    assert_eq!(rows[0]["cause"], "request", "{}", rows[0]);
    assert_eq!(rows[0]["upstream_fault"], false, "{}", rows[0]);
    no_account_penalty(&e);
}

#[tokio::test]
async fn stream_rate_limit_without_quota_code_is_not_usage_limit() {
    for (mode, code) in [
        ("late_rate_limit", "rate_limit_exceeded"),
        ("late_429_nocode", "rate_limit_exceeded"),
        ("late_failed_rate_limit", "rate_limit_exceeded"),
    ] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        read_all(&e, mode).await;
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], code, "{mode}: {}", rows[0]);
        assert_eq!(
            rows[0]["failure_class"], "rate_limit",
            "{mode}: {}",
            rows[0]
        );
        assert!(
            e.outcomes().iter().all(|(_, o)| !matches!(
                o,
                Outcome::RateLimited {
                    usage_limit: true,
                    ..
                }
            )),
            "{mode}: {:?}",
            e.outcomes()
        );
        // The model is cooled (rate limit), not left untouched.
        assert!(
            e.outcomes().iter().any(|(_, o)| matches!(
                o,
                Outcome::RateLimited {
                    usage_limit: false,
                    ..
                }
            )),
            "{mode}: {:?}",
            e.outcomes()
        );
    }
}

#[tokio::test]
async fn stream_read_error_is_network_not_truncated() {
    for mode in ["delta_then_reset", "reset_first"] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        read_all(&e, mode).await;
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], "stream_read_error", "{mode}: {}", rows[0]);
        assert_eq!(rows[0]["err_source"], "network", "{mode}: {}", rows[0]);
        assert_eq!(rows[0]["failure_class"], "network", "{mode}: {}", rows[0]);
    }
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "event_literal_truncated").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "stream_truncated", "{}", rows[0]);
    assert_eq!(rows[0]["err_source"], "stream", "{}", rows[0]);
}

/// Codex 0.166 `policy_capacity_unavailable` before any output is an overload: switch the
/// account instead of failing the turn (SSE `response.failed` and a code-only WS frame).
#[tokio::test]
async fn policy_capacity_unavailable_before_output_switches() {
    let e = env(300).await;
    let t = sse_text(&e, "pre_policycap").await;
    assert!(!t.contains("policy_capacity_unavailable"), "{t}");
    assert!(
        t.contains("r-b") && t.contains("response.completed") && !t.contains("r-a"),
        "{t}"
    );
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
    let e = env(300).await;
    let got = ws_frames(&e, "pre_policycap_ws").await;
    assert!(
        got.iter()
            .all(|t| !t.contains("policy_capacity_unavailable")),
        "{got:?}"
    );
    assert!(
        got.iter()
            .any(|t| t.contains("r-b") && t.contains("response.completed")),
        "{got:?}"
    );
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
}

/// HTTP 429 `turn_rate_limit_exceeded`: a model-level rate limit that honours Retry-After, not
/// a usage limit. Recorded with its own code and classed as a rate limit, not quota.
#[tokio::test]
async fn http_turn_rate_limit_is_a_rate_limit_not_quota() {
    let e = env(300).await;
    let t = sse_text(&e, "http_turnrl").await;
    assert!(t.contains("r-b") && t.contains("response.completed"), "{t}");
    assert!(e.outcomes().iter().any(|(a, o)| a == "a"
        && matches!(o, Outcome::RateLimited { usage_limit: false, retry_after: Some(d), .. } if *d == Duration::from_secs(30))));
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "http_turnrl_all").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "turn_rate_limit_exceeded", "{}", rows[0]);
    assert_eq!(rows[0]["status"], 429, "{}", rows[0]);
    assert!(
        e.outcomes().iter().all(|(_, o)| matches!(
            o,
            Outcome::RateLimited {
                usage_limit: false,
                ..
            }
        )),
        "{:?}",
        e.outcomes()
    );
}

/// SSE: upstream event times, queue / yield times and bytes; a tail after the terminal
/// event (client already gone) does not change the old TTFT / error semantics.
#[tokio::test]
async fn sse_downstream_observation_recorded() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut r = e
        .post("/v1/responses")
        .header("x-test-mode", "slowtail")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    let mut got = Vec::new();
    while let Some(c) = r.chunk().await.unwrap() {
        got.extend_from_slice(&c);
        if String::from_utf8_lossy(&got).contains("response.completed") {
            break;
        }
    }
    drop(r);
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    assert!(u["error"].is_null(), "{u}");
    let (s, o) = obs_of(&e, u["request_id"].as_str().unwrap()).await;
    assert_eq!(s, 200, "{o}");
    let text = ms(&o, "up_first_text_ms");
    assert!(
        text >= 250 && text == u["ttft_ms"].as_u64().unwrap(),
        "{o} {u}"
    );
    assert!(ms(&o, "up_terminal_ms") >= text, "{o}");
    assert!(
        o["up_first_reasoning_ms"].is_null() && o["up_first_tool_ms"].is_null(),
        "{o}"
    );
    assert_eq!(o["response_id"], "r-s", "{o}");
    assert!(
        ms(&o, "sse_first_queue_ms") <= ms(&o, "sse_last_queue_ms"),
        "{o}"
    );
    assert!(
        ms(&o, "sse_first_yield_ms") <= ms(&o, "sse_last_yield_ms"),
        "{o}"
    );
    assert!(ms(&o, "sse_last_yield_ms") >= text, "{o}");
    assert!(
        ms(&o, "sse_yield_bytes") >= ms(&o, "sse_queue_bytes").min(1),
        "{o}"
    );
    assert!(
        ms(&o, "up_bytes") > 0
            && ms(&o, "sse_queue_blocked_max_us") <= ms(&o, "sse_queue_blocked_us"),
        "{o}"
    );
    // The client closed right after the terminal event, before the tail: may be true.
    assert!(o["down_incomplete"].is_boolean(), "{o}");
    // WS-only fields stay null (not 0) on an SSE row.
    for k in [
        "ws_first_send_ms",
        "ws_sink_blocked_us",
        "ws_deflate_down",
        "ws_ext_up",
    ] {
        assert!(o[k].is_null(), "{k}: {o}");
    }
    // Old duration semantics: producer end, not body end; still ≥ the terminal event.
    assert!(
        u["duration_ms"].as_u64().unwrap() >= ms(&o, "up_terminal_ms"),
        "{u} {o}"
    );
}

/// SSE client disconnect mid-stream: the row is still written (once) with the client_closed
/// outcome and whatever downstream observation existed.
#[tokio::test]
async fn sse_observation_on_client_disconnect() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut response = e
        .post("/v1/responses")
        .header("x-test-mode", "idle_after_output")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert!(response.chunk().await.unwrap().is_some());
    drop(response);
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 1))
        .await
        .expect("row written after disconnect");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
    let (s, o) = obs_of(&e, rows[0]["request_id"].as_str().unwrap()).await;
    assert_eq!(s, 200, "{o}");
    assert!(o["up_terminal_ms"].is_null(), "{o}");
    assert!(ms(&o, "sse_yield_chunks") >= 1, "{o}");
    assert_eq!(o["down_incomplete"], true, "{o}");
}
