use super::scaffold::*;
use super::*;

#[tokio::test]
async fn pending_connect_keeps_its_egress_classification_across_reload() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tower::ServiceExt;

    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let proxy = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            assert!(line.starts_with("CONNECT 127.0.0.1:1 "), "{line}");
            loop {
                line.clear();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            socket.get_mut().write_all(
                b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ).await.unwrap();
        });
        let mut cfg = Config::parse(&format!(
            "api_keys = [\"{KEY}\"]\nupstream = \"https://127.0.0.1:1/backend-api\"\nproxy_url = \"http://{address}\""
        )).unwrap();
        let upstream = Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap());
        let config = Arc::new(ciallopa_core::settings::SharedConfig::new(cfg.clone()));
        let rec = Arc::new(Rec::default());
        let dir = tempfile::tempdir().unwrap();
        let usage = usage_recorder(dir.path());
        let app = ciallopa::build_app(AppState {
            accounts: Arc::new(Store::rotating(&["a", "b"])),
            router: rec.clone(),
            upstream: upstream.clone(),
            usage: Some(usage.clone()),
            config: config.clone(),
            egress: Default::default(),
        });
        let request = http::Request::post("/v1/responses")
            .header("authorization", format!("Bearer {KEY}"))
            .body(Body::from(r#"{"stream":true,"input":[]}"#)).unwrap();
        let response = tokio::spawn(app.oneshot(request));
        ready_rx.await.unwrap();
        // The in-flight CONNECT still uses the old proxy; new attempts now go direct.
        cfg.proxy_enabled = false;
        upstream.reconfigure(&cfg).unwrap();
        config.store(cfg);
        release_tx.send(()).unwrap();
        let response = response.await.unwrap().unwrap();
        assert_eq!(response.status(), 502);
        let body = axum::body::to_bytes(response.into_body(), 8192).await.unwrap();
        let _: serde_json::Value = serde_json::from_slice(&body).unwrap();
        proxy.await.unwrap();
        usage.flush_blocking().unwrap();
        let rows = usage.recent(ciallopa_usage::RecentQuery { limit: 10, ..Default::default() }).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].error.as_deref(), Some("proxy_connect"));
        assert_eq!(rec.0.lock().unwrap().len(), 1, "a proxy rejection must not switch accounts");
    }).await.expect("CONNECT/reload fixture did not finish");
}

#[tokio::test]
async fn bounded_header_wait_can_fail_over_without_cooling_the_account() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "response_header_timeout_secs = 1\npreparation_timeout_secs = 3\n",
    )
    .await;
    e.warm_http().await;
    let request = e
        .post("/v1/responses")
        .header("x-test-mode", "header_stall_a")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send();
    let request = tokio::spawn(request);
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(4), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.text().await.unwrap().contains("r-b"));
    e.mock.release.notify_waiters();
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert!(e
        .outcomes()
        .iter()
        .any(|(_, outcome)| matches!(outcome, Outcome::Transport)));
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{rows:?}");
    let detail: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(detail["attempts"][0]["code"], "header_timeout", "{detail}");
}

#[tokio::test]
async fn bounded_preparation_budget_is_shared_across_sse_replay() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "response_header_timeout_secs = 3\npreparation_timeout_secs = 2\n",
    )
    .await;
    e.warm_http().await;
    let request = tokio::spawn(
        e.post("/v1/responses")
            .header("x-test-mode", "preparation_shared")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    // Half a unit remains. Resetting the budget would take another two units.
    let response = tokio::time::timeout(units(1), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 504);
    response.bytes().await.unwrap();
    e.mock.release.notify_waiters();
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "local", "{rows:?}");
    assert_eq!(rows[0]["account"], "b", "{rows:?}");
}

#[tokio::test]
async fn bounded_preparation_does_not_count_valid_sse_bootstrap_time() {
    let e = env_cfg(
        30,
        None,
        "response_header_timeout_secs = 1\npreparation_timeout_secs = 1\n",
    )
    .await;
    e.warm_http().await;
    let response = tokio::time::timeout(
        Duration::from_secs(4),
        e.post("/v1/responses")
            .header("x-test-mode", "bootstrap_long_quota")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.text().await.unwrap().contains("r-b"));
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        2,
        "the long pre-output stream can still replay"
    );
}

#[tokio::test]
async fn bounded_header_wait_preserves_nonstream_json_and_http_timing() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "response_header_timeout_secs = 2\npreparation_timeout_secs = 2\n",
    )
    .await;
    e.warm_http().await;
    let request = tokio::spawn(
        e.post("/v1/responses")
            .header("x-test-mode", "json_headers")
            .body(r#"{"model":"gpt-5.5","stream":false,"input":[]}"#)
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    e.mock.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"id":"json-ok","status":"completed"}"#
    );
    let rows = wait_rows(&e, 1).await;
    assert_eq!(
        rows[0]["transport"], "http",
        "nonstream headers are not SSE output timing: {rows:?}"
    );
    assert!(
        rows[0]["ttft_ms"].is_u64(),
        "legacy HTTP header timing stays available: {rows:?}"
    );
}

#[tokio::test]
async fn bounded_preparation_preserves_error_headers_when_it_interrupts_body_read() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
    )
    .await;
    e.warm_http().await;
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        e.post("/v1/responses")
            .header("x-test-mode", "error_stall")
            .body(r#"{"model":"gpt-5.5","input":[]}"#)
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 504);
    response.bytes().await.unwrap();
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["upstream_status"], 503, "{rows:?}");
    let detail: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        detail["upstream_request_id"], "stalled-upstream",
        "{detail}"
    );
    assert!(
        detail["message"]
            .as_str()
            .unwrap()
            .contains("no complete response forwarded"),
        "{detail}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
}

/// Codex 0.162 remote compaction v2: a normal `/responses` turn whose input ends with
/// `{"type":"compaction_trigger"}`; reply carries one `compaction` output item.
#[tokio::test]
async fn compaction_v2_passthrough() {
    let e = env(300).await;
    let body = r#"{"model":"gpt-5.5","stream":true,"input":[{"type":"message","role":"user","content":[]},{"type":"compaction_trigger"}],"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"compaction\"}"}}"#;
    for path in ["/v1/responses", "/backend-api/codex/responses"] {
        let r = e
            .post(path)
            .header("x-test-mode", "compact")
            .header("x-codex-beta-features", "remote_compaction_v2")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let t = r.text().await.unwrap();
        assert!(
            t.contains("response.compaction.compacting")
                && t.contains("\"encrypted_content\":\"gAAA\""),
            "{t}"
        );
    }
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for (h, b) in seen.iter() {
        assert!(h
            .iter()
            .any(|(k, v)| k == "content-encoding" && v == "zstd"));
        assert_eq!(
            zstd::decode_all(b.as_ref()).unwrap(),
            body.as_bytes(),
            "compressed compaction turn decodes to the original body"
        );
        assert!(h
            .iter()
            .any(|(k, v)| k == "x-codex-beta-features" && v == "remote_compaction_v2"));
    }
}

/// Item 6: a turn state issued by account a never reaches account b on failover; an unknown
/// one is kept on the first attempt only; the owner keeps receiving its own value.
#[tokio::test]
async fn turn_state_not_carried_across_accounts() {
    let e = env(300).await;
    // a issues ts-owned-a
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "ts_issue")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-codex-turn-state"], "ts-owned-a");
    // same turn: a is limited → failover to b must not carry a's turn state
    for ts in ["ts-owned-a", "ts-never-seen"] {
        e.mock.seen.lock().unwrap().clear();
        let r = e
            .post("/backend-api/codex/responses")
            .header("x-test-mode", "limit")
            .header("x-codex-turn-state", ts)
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let seen = turn_states(&e);
        assert_eq!(seen.len(), 2, "{seen:?}");
        assert!(seen[0].0.contains("tok-a"));
        assert_eq!(seen[0].1.as_deref(), Some(ts), "first attempt keeps it");
        assert!(seen[1].0.contains("tok-b"));
        assert_eq!(seen[1].1, None, "{ts} leaked to account b");
    }
}

#[tokio::test]
async fn http_refresh_error_must_not_authfail() {
    let mock = Mock::default();
    let up = Router::new()
        .route("/backend-api/codex/responses", post(up_responses))
        .with_state(mock.clone());
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[upstream_opts]\nstream_idle_timeout_secs = 30\n"
    ))
    .unwrap();
    let rec = Arc::new(Rec::default());
    let state = AppState {
        accounts: Arc::new(RefreshFail),
        router: rec.clone(),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: None,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let proxy = serve(ciallopa::build_app(state)).await;
    let r = reqwest::Client::new()
        .post(format!("http://{proxy}/backend-api/codex/responses"))
        .bearer_auth(KEY)
        .body("{}")
        .send()
        .await
        .unwrap();
    assert!(r.status().as_u16() >= 400 || r.status().as_u16() == 503 || r.status().as_u16() == 429);
    let o = rec.0.lock().unwrap().clone();
    assert!(
        !o.iter().any(|(_, x)| matches!(x, Outcome::AuthFailed)),
        "{o:?}"
    );
}

#[tokio::test]
async fn upstream_5xx_passed_through_with_provenance() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "down")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    // Never rewritten into a local 429: the upstream status and body pass through; the
    // upstream request id moves aside for the gateway's own (checked against the record below).
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers()["x-upstream-request-id"], "req_down");
    let request_id = r.headers()["x-request-id"].to_str().unwrap().to_string();
    assert!(r.headers().keys().all(|k| !k.as_str().contains("ciallopa")));
    let t = r.text().await.unwrap();
    assert!(t.contains("up busy"), "{t}");
    // One failover to another account, never the same account twice (max_attempts = 1).
    // Each account once, then one wait-and-retry on the last usable account.
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    let rows = wait_rows(&e, 1).await;
    let row = &rows[0];
    assert_eq!(row["err_source"], "upstream", "{row}");
    assert_eq!(row["error"], "server_is_overloaded");
    assert_eq!(row["upstream_status"], 503);
    let id = row["request_id"].as_str().unwrap().to_string();
    assert_eq!(request_id, format!("cp-{id}"));
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    assert_eq!(v["upstream_request_id"], "req_down");
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 3, "{v}");
    assert_eq!(at[0]["action"], "switch");
    assert_eq!(at[1]["action"], "wait+retry");
    assert_eq!(at[2]["action"], "stop");
    assert_ne!(at[0]["account"], at[1]["account"]);
    assert_eq!(at[1]["account"], at[2]["account"]);
}

#[tokio::test]
async fn retried_success_keeps_attempt_chain() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "retry").await;
    let rows = wait_rows(&e, 1).await;
    let row = &rows[0];
    assert_eq!(row["retries"], 1, "{row}");
    assert!(row["error"].is_null(), "{row}");
    assert!(row["err_source"].is_null());
    assert_eq!(row["has_detail"], true);
    let id = row["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 2, "{v}");
    assert_eq!(at[0]["action"], "switch");
    assert_eq!(at[1]["action"], "ok");
}

#[tokio::test]
async fn account_error_families_fail_over() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 0\n").await;
    for mode in [
        "unauth",
        "retry",
        "limit",
        "over400",
        "deact",
        "pay402",
        "nf404",
        "nfmodel",
        "real403",
        "http_turnrl",
    ] {
        // The retry fixture fails its first request; all rows share the same servers.
        e.mock.hits.store(0, Ordering::SeqCst);
        let start = e.outcomes().len();
        let r = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .header("x-test-gzip", "1")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{mode}");
        assert!(
            r.text().await.unwrap().contains("response.completed"),
            "{mode}"
        );
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2, "{mode}");
        let outcomes = e.outcomes();
        if mode == "unauth" {
            assert_eq!(e.store.refreshed.load(Ordering::SeqCst), 1);
        } else {
            let (account, outcome) = &outcomes[start];
            assert_eq!(account, "a", "{mode}");
            assert!(
                match mode {
                    "retry" => matches!(outcome, Outcome::ServerError { .. }),
                    "limit" => matches!(
                        outcome,
                        Outcome::RateLimited {
                            usage_limit: true,
                            ..
                        }
                    ),
                    "http_turnrl" => matches!(
                        outcome,
                        Outcome::RateLimited {
                            usage_limit: false,
                            ..
                        }
                    ),
                    "over400" => matches!(outcome, Outcome::Overloaded { .. }),
                    "deact" | "pay402" => matches!(outcome, Outcome::PaymentRequired { .. }),
                    "nf404" => matches!(outcome, Outcome::NotFound { .. }),
                    "nfmodel" => matches!(outcome, Outcome::ModelUnsupported { .. }),
                    "real403" => matches!(outcome, Outcome::AuthFailed),
                    _ => unreachable!(),
                },
                "{mode}: {outcome:?}"
            );
            assert_eq!(outcomes.last().unwrap().0, "b", "{mode}");
        }
    }
    // A changed duration must reach the next request, beyond the pure classification table.
    let mut cfg = (*e.config.load()).clone();
    cfg.upstream_opts.deactivated_cooldown_secs = 120;
    e.config.store(cfg);
    let start = e.outcomes().len();
    assert_eq!(mode_post(&e, "deact").await.status(), 200);
    assert!(matches!(e.outcomes()[start].1,
        Outcome::PaymentRequired { cooldown, .. } if cooldown == Duration::from_secs(120)));
}

#[tokio::test]
async fn cloudflare_block_is_egress_not_auth() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for (i, mode) in [
        "repro_cf1010_text",
        "repro_cf1010_json",
        "repro_cf_challenge",
    ]
    .into_iter()
    .enumerate()
    {
        let r = mode_post(&e, mode).await;
        assert_eq!(r.status(), 502, "{mode}");
        let body = r.text().await.unwrap();
        let low = body.to_ascii_lowercase();
        assert!(
            !low.contains("cloudflare") && !low.contains("1010") && !low.contains("ciallopa"),
            "{mode}: {body}"
        );
        assert!(body.contains("Bad gateway"), "{mode}: {body}");
        // One upstream hit: the egress is shared, so no switch to b.
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), i + 1, "{mode}");
        let o = e.outcomes();
        assert_eq!(o.len(), i + 1, "{mode}");
        assert!(matches!(o[i].1, Outcome::EgressBlocked), "{mode}: {o:?}");
    }
    assert!(!e
        .outcomes()
        .iter()
        .any(|(_, o)| matches!(o, Outcome::AuthFailed)));
    let rows = wait_rows(&e, 3).await;
    let codes: Vec<&str> = rows.iter().filter_map(|r| r["error"].as_str()).collect();
    assert!(
        codes.contains(&"cf_blocked") && codes.contains(&"challenge"),
        "{rows:?}"
    );
    for r in &rows {
        assert_eq!(r["err_source"], "network", "{r}");
        assert_eq!(r["status"], 502, "{r}");
    }
    let q: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b = &q["egress_block"];
    assert_eq!(b["active"], true, "{q}");
    assert_eq!(b["count"], 3, "{q}");
    assert_eq!(b["code"], "challenge", "{q}");
    assert_eq!(b["cf_ray"], "9a1b2c3d4e5f6789-SJC", "{q}");
    // A normal answer ends the episode on the overview.
    assert_eq!(mode_post(&e, "").await.status(), 200);
    let q: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q["egress_block"]["active"], false, "{q}");
    rec.shutdown();
}

/// `turn_rate_limit_exceeded` ("Codex is at capacity") before any output cools the model on
/// that account and switches, like `rate_limit_exceeded`; never a usage limit.
#[tokio::test]
async fn turn_rate_limit_before_output_cools_model_and_switches() {
    let rate_limited = |e: &Env| {
        let o = e.outcomes();
        assert!(
            o.iter().any(|(a, o)| a == "a"
                && matches!(
                    o,
                    Outcome::RateLimited {
                        usage_limit: false,
                        ..
                    }
                )),
            "{o:?}"
        );
        assert!(
            !o.iter().any(|(_, o)| matches!(
                o,
                Outcome::RateLimited {
                    usage_limit: true,
                    ..
                }
            )),
            "{o:?}"
        );
    };
    let e = env(300).await;
    let t = sse_text(&e, "pre_turnrl").await;
    assert!(!t.contains("turn_rate_limit_exceeded"), "{t}");
    assert!(
        t.contains("r-b") && t.contains("response.completed") && !t.contains("r-a"),
        "{t}"
    );
    rate_limited(&e);
    let e = env(300).await;
    let got = ws_frames(&e, "pre_turnrl_ws").await;
    assert!(
        got.iter().all(|t| !t.contains("turn_rate_limit_exceeded")),
        "{got:?}"
    );
    assert!(
        got.iter()
            .any(|t| t.contains("r-b") && t.contains("response.completed")),
        "{got:?}"
    );
    rate_limited(&e);
}
