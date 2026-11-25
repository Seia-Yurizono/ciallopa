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
    assert!(t0.elapsed() < millis(1800));
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
                >= log["activity_idle_ms"].as_u64().unwrap() + millis(80).as_millis() as u64,
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
async fn sse_overload_last_account_waits_and_retries_once() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 1\n").await;
    let t0 = std::time::Instant::now();
    let t = sse_text(&e, "overonce").await;
    assert!(t.contains("r-b") && !t.contains("overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    assert!(t0.elapsed() >= millis(600));
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

#[tokio::test]
async fn encrypted_rejections_preserve_transport_contracts() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    for (i, mode) in ["enc", "enc400", "lateenc", "ws"].into_iter().enumerate() {
        let (status, text) = if mode == "ws" {
            let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
            ws.send(M::text(r#"{"type":"response.create","tag":"enc"}"#))
                .await
                .unwrap();
            let mut frames = Vec::new();
            loop {
                let frame = ws_text(&mut ws).await;
                let done = frame.contains("invalid_encrypted_content");
                frames.push(frame);
                if done {
                    break;
                }
            }
            assert_eq!(frames.len(), 3, "{frames:?}");
            assert!(frames[0].contains("codex.rate_limits"));
            assert!(frames[1].contains("codex.response.metadata"));
            let v: serde_json::Value = serde_json::from_str(frames.last().unwrap()).unwrap();
            assert_eq!(v["type"], "error");
            assert_eq!(v["status"], 400);
            // Rejection ends this turn while leaving the same connection usable.
            ws.send(M::text(r#"{"type":"response.create","n":2}"#))
                .await
                .unwrap();
            assert!(ws_text(&mut ws).await.contains("response.completed"));
            (400, frames.pop().unwrap())
        } else {
            let r = post_mode(&e, mode).await;
            let status = r.status().as_u16();
            if mode != "lateenc" {
                assert_eq!(r.headers()["content-type"], "application/json");
            }
            (status, r.text().await.unwrap())
        };
        assert_eq!(status, if mode == "lateenc" { 200 } else { 400 }, "{mode}");
        assert!(
            text.contains("invalid_encrypted_content") && text.contains(ENC_HINT),
            "{mode}: {text}"
        );
        if mode == "lateenc" {
            assert!(
                text.contains(r#""delta":"hi""#) && text.contains("event: response.failed"),
                "{text}"
            );
            assert!(!text.contains("could not be verified") && !text.contains("event: error"));
            assert!(text.ends_with("\n\n"));
        } else {
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(v["error"]["type"], "invalid_request_error");
            assert_eq!(v["error"]["param"], "input");
            let msg = v["error"]["message"].as_str().unwrap().to_ascii_lowercase();
            assert!(
                msg.contains("encrypted reasoning content")
                    && !msg.contains("ciallopa")
                    && !msg.contains("proxy")
            );
        }
        assert_eq!(
            e.mock.hits.load(Ordering::SeqCst),
            i + 1,
            "no replay: {mode}"
        );
        assert_no_account_outcome(&e);
        let rows = wait_rows(&e, i + 1 + usize::from(mode == "ws")).await;
        let row = if mode == "ws" {
            rows.iter()
                .find(|r| r["error"] == "invalid_encrypted_content" && r["transport"] == "ws")
                .unwrap()
        } else {
            &rows[0]
        };
        assert_eq!(row["error"], "invalid_encrypted_content", "{mode}: {row}");
        assert_eq!(row["err_source"], "upstream");
        assert_eq!(row["switches"], 0);
        let detail: serde_json::Value = detail_of(&e, row["request_id"].as_str().unwrap(), true)
            .await
            .json()
            .await
            .unwrap();
        assert!(
            detail["message"]
                .as_str()
                .unwrap()
                .contains("could not be verified"),
            "{detail}"
        );
    }
    let r = post_mode(&e, "errbody").await;
    assert_eq!(r.status(), 400);
    assert!(r.text().await.unwrap().contains("bad input"));
    sse_text(&e, "").await;
    assert!(e
        .mock
        .seen
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .0
        .iter()
        .any(|(k, v)| k == "authorization" && v.contains("tok-a")));
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

#[tokio::test]
async fn committed_errors_are_forwarded_and_recorded() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    for (i, (mode, wire_code, stored_code, cause)) in [
        (
            "lateover",
            "server_is_overloaded",
            "server_is_overloaded",
            "stream",
        ),
        ("latelimit", "usage_limit_reached", "usage_limit", "quota"),
        (
            "late_request_error",
            "context_length_exceeded",
            "context_length_exceeded",
            "request",
        ),
        (
            "late_rate_limit",
            "rate_limit_exceeded",
            "rate_limit_exceeded",
            "rate_limit",
        ),
        (
            "late_turn_rate_limit",
            "turn_rate_limit_exceeded",
            "turn_rate_limit_exceeded",
            "rate_limit",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let start = e.outcomes().len();
        let text = sse_text(&e, mode).await;
        assert!(
            text.contains("delta") && text.contains(wire_code),
            "{mode}: {text}"
        );
        assert_eq!(
            e.mock.hits.load(Ordering::SeqCst),
            i + 1,
            "no replay after output: {mode}"
        );
        let rows = wait_rows(&e, i + 1).await;
        let row = &rows[0];
        assert_eq!(row["error"], stored_code, "{mode}: {row}");
        assert_eq!(row["cause"], cause, "{mode}: {row}");
        assert_eq!(row["err_source"], "stream");
        let outcomes = e.outcomes();
        let outcomes = &outcomes[start..];
        assert!(
            match mode {
                "lateover" => outcomes
                    .iter()
                    .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })),
                "latelimit" => outcomes.iter().any(|(a, o)| a == "a"
                    && matches!(
                        o,
                        Outcome::RateLimited {
                            usage_limit: true,
                            ..
                        }
                    )),
                "late_rate_limit" | "late_turn_rate_limit" =>
                    outcomes.iter().any(|(_, o)| matches!(
                        o,
                        Outcome::RateLimited {
                            usage_limit: false,
                            ..
                        }
                    )) && outcomes.iter().all(|(_, o)| !matches!(
                        o,
                        Outcome::RateLimited {
                            usage_limit: true,
                            ..
                        }
                    )),
                _ =>
                    !row["upstream_fault"].as_bool().unwrap()
                        && outcomes.iter().all(|(_, o)| matches!(
                            o,
                            Outcome::Success { .. } | Outcome::ClientError { .. }
                        )),
            },
            "{mode}: {outcomes:?}"
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
    let prep = ms(&o, "prep_ms");
    let headers = ms(&o, "headers_at_ms");
    let send = ms(&o, "send_to_headers_ms");
    // This fixture has one attempt. Rounding two intervals can lose one millisecond.
    assert!((prep + send..=prep + send + 1).contains(&headers), "{o}");
    assert!(ms(&o, "cred_ms") <= prep && headers <= text, "{o}");
    let delivered = ms(&o, "down_first_content_ms");
    assert!(
        (text..=ms(&o, "sse_last_yield_ms")).contains(&delivered),
        "{o}"
    );
    assert_eq!(o.get("hold_ms"), Some(&serde_json::Value::Null), "{o}");
    assert!(
        text >= millis(250).as_millis() as u64 && text == u["ttft_ms"].as_u64().unwrap(),
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

#[tokio::test]
async fn sse_prep_includes_reading_the_request_body() {
    use tower::ServiceExt;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // This future starts only when the gateway polls the incoming body.
    let delay = Duration::from_millis(20);
    let body = Body::from_stream(futures::stream::once(async move {
        tokio::time::sleep(delay).await;
        Ok::<_, std::io::Error>(Bytes::from_static(br#"{"model":"gpt-5.5","stream":true}"#))
    }));
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .body(body)
        .unwrap();
    let response = e.app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);
    axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let rows = wait_rows(&e, 1).await;
    let (status, obs) = obs_of(&e, rows[0]["request_id"].as_str().unwrap()).await;
    assert_eq!(status, 200);
    assert!(ms(&obs, "prep_ms") >= delay.as_millis() as u64, "{obs}");
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
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(response);
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 1))
        .await
        .expect("row written after disconnect");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
    let detail: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let message = detail["message"].as_str().unwrap();
    let silent: f64 = message
        .split("upstream silent for ")
        .nth(1)
        .unwrap()
        .split('s')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!((0.1..5.0).contains(&silent), "{detail}");
    let (s, o) = obs_of(&e, rows[0]["request_id"].as_str().unwrap()).await;
    assert_eq!(s, 200, "{o}");
    assert!(o["up_terminal_ms"].is_null(), "{o}");
    assert!(ms(&o, "sse_yield_chunks") >= 1, "{o}");
    assert_eq!(o["down_incomplete"], true, "{o}");
}
