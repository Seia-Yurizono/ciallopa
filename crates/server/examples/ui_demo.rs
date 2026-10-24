//! Local demo for the /ui page: fake usage data (a few XSS-looking names, two instances), mock accounts,
//! no upstream traffic. `cargo run --example ui_demo -- 127.0.0.1:8399`
//! Uses a unique temporary directory by default; an optional data directory must not exist.
#![allow(clippy::unwrap_used)]
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use ciallopa_usage::{now_ms, QuotaObs, Record, Recorder, Transport};
use std::sync::Arc;

pub const DEMO_KEY: &str = "sk-demo-0123456789abcdef";
const EVIL: &str = "<img src=x onerror=\"window.__xss=1\">";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let listen = args.get(1).cloned().unwrap_or("127.0.0.1:8399".into());
    let temporary_dir = args.get(2).is_none().then(|| {
        tempfile::Builder::new()
            .prefix("ciallopa-ui-demo-")
            .tempdir()
            .unwrap()
    });
    let dir = if let Some(path) = args.get(2) {
        let dir = std::path::PathBuf::from(path);
        if let Err(err) = std::fs::create_dir(&dir) {
            eprintln!(
                "Cannot create demo directory {} (must not already exist): {err}",
                dir.display()
            );
            std::process::exit(1);
        }
        dir
    } else {
        temporary_dir.as_ref().unwrap().path().to_path_buf()
    };
    println!("ui demo data: {}", dir.display());
    std::fs::create_dir(dir.join("auths")).unwrap();
    let acct = |id: &str, extra: serde_json::Value| {
        let mut v = serde_json::json!({"type":"codex","access_token":"x","refresh_token":"y",
            "expired":"2999-01-01T00:00:00Z","email": format!("{id}@example.com"),
            "account_id": format!("acct-demo-{id}")});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        ciallopa_auth::file::write_auth_file(&dir.join("auths"), id, &v).unwrap();
    };
    acct("alice", serde_json::json!({"plan_type":"plus"}));
    acct("bob", serde_json::json!({"plan_type":"pro"}));
    acct(
        "carol",
        serde_json::json!({"needs_relogin":true,"disabled":true}),
    );
    acct("evil", serde_json::json!({"email": EVIL}));
    // configured, never used: empty card
    acct("dave", serde_json::json!({"plan_type":"team"}));
    // disabled by hand but still holding a reset card (the overview still lets you use it)
    acct(
        "erin",
        serde_json::json!({"plan_type":"plus","disabled":true}),
    );
    // free plan: one 30-day window, no weekly one
    acct("frank", serde_json::json!({"plan_type":"free"}));
    let fake_up = fake_upstream().await;
    let cfg_text = format!(
        "listen = \"{listen}\"\nupstream = \"http://{fake_up}/backend-api\"\ninstance_id = \"demo-vps\"\nauth_dir = \"{}\"\n\
         [[api_keys]]\nlabel = \"laptop\"\nkey = \"{DEMO_KEY}\"\nadmin = true\n\
         [[api_keys]]\nlabel = \"{}\"\nkey = \"sk-demo-evil-0123456789\"\n\
         [usage]\npath = \"{}\"\nretention_days = 0\n\
         [ui]\nallow_reset_consume = true\nreset_credits_refresh_mins = 0\n",
        dir.join("auths").display(),
        EVIL.replace('"', "\\\""),
        dir.join("usage.db").display()
    );
    // A real file so /ui/settings is editable (demo only: allow_config_write = true;
    // UI_DEMO_READONLY=1 keeps the pages read-only).
    let cfg_path = dir.join("config.toml");
    let readonly = std::env::var("UI_DEMO_READONLY").is_ok_and(|v| v == "1");
    std::fs::write(
        &cfg_path,
        if readonly {
            cfg_text.clone()
        } else {
            cfg_text.replace("[ui]\n", "[ui]\nallow_config_write = true\n")
        },
    )
    .unwrap();
    let cfg = Config::load(&cfg_path).unwrap();
    let rec = Recorder::open_as(&cfg.usage, &cfg.instance_id).unwrap();
    // a second proxy instance writing to the same log
    let rec2 = Recorder::open_as(&cfg.usage, "home-nas").unwrap();
    rec.set_prices(
        // gpt-6-luna stays unpriced: the pages show the "unpriced models" hint.
        ciallopa_usage::PriceTable::parse(
            "[[model]]\nmodel = \"gpt-5.5\"\nin = 5.0\ncache = 0.5\nout = 30.0\nfast_x = 2.5\n\
             [[model]]\nmodel = \"gpt-5.6-sol\"\nin = 4.0\ncache = 0.4\nwrite = 5.0\nout = 20.0\n\
             [[model]]\nmodel = \"gpt-5.6-terra\"\nin = 2.0\ncache = 0.2\nwrite = 2.5\nout = 12.0\n",
        )
        .unwrap(),
    );
    // 14 days of fake traffic
    let now = now_ms();
    let reset_p = now / 1000 + 3 * 3600;
    let reset_s = now / 1000 + 4 * 86400;
    let mut rng = 7u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let models = ["gpt-5.5", "gpt-5.6-terra", "gpt-5.6-sol", "gpt-6-luna"];
    let accts = ["alice", "bob"];
    let n = 1500;
    let mut stale = [false; 2];
    for i in 0..n {
        let ts = now - 14 * 86_400_000
            + (i as i64) * (14 * 86_400_000 / n as i64)
            + (next() % 60_000) as i64;
        // dave / erin were last seen hours ago by the other instance: stale quota rows
        for (k, (who, dt, p, w)) in [("dave", 3, 12.0, 41.0), ("erin", 26, 100.0, 64.0)]
            .into_iter()
            .enumerate()
        {
            let at = now - dt * 3_600_000;
            if stale[k] || ts < at {
                continue;
            }
            stale[k] = true;
            let mut r = Record::new(rec2.ids.next_at(at), Transport::Sse, "responses");
            r.status = 200;
            r.model = Some("gpt-5.6-terra".into());
            r.key = Some("laptop".into());
            r.input_tokens = Some(9_000);
            r.output_tokens = Some(800);
            r.duration_ms = 6_000;
            r.account = Some(who.into());
            r.quota = Some(QuotaObs {
                plan: Some(if who == "dave" { "team" } else { "plus" }.into()),
                p_pct: Some(p),
                p_reset: Some(at / 1000 + 2 * 3600),
                p_window_min: Some(300),
                s_pct: Some(w),
                s_reset: Some(now / 1000 + 2 * 86400),
                s_window_min: Some(10080),
            });
            rec2.record(r);
        }
        let rr = if next() % 4 == 0 { &rec2 } else { &rec };
        let mut r = Record::new(
            rr.ids.next_at(ts),
            if i % 3 == 0 {
                Transport::Ws
            } else {
                Transport::Sse
            },
            "responses",
        );
        // XSS-looking names: only a couple of rows (account / model / key)
        let a = if i == n - 20 {
            "evil"
        } else {
            accts[(next() % 2) as usize]
        };
        r.account = Some(a.into());
        r.model = Some(if i == n - 21 {
            EVIL.into()
        } else {
            models[(next() % models.len() as u64) as usize].into()
        });
        r.effort = Some(["low", "medium", "high"][(next() % 3) as usize].into());
        r.key = Some(if i == n - 22 {
            EVIL.into()
        } else if next() % 5 == 0 {
            "ci".into()
        } else {
            "laptop".into()
        });
        r.status = 200;
        let fail = next() % 25 == 0 || i == n - 10 || i == n - 11;
        if fail {
            let limit = i == n - 11 || (next() % 2 == 0 && i != n - 10);
            r.error = Some(if limit {
                "usage_limit".into()
            } else {
                "upstream_5xx".into()
            });
            r.status = if limit { 429 } else { 502 };
            r.err_source = Some("upstream");
            r.upstream_status = Some(r.status);
            // Fake upstream error (redacted on write; XSS payload must render as text).
            let mut d = ciallopa_usage::ErrorDetail::default();
            d.add_header("content-type", "application/json");
            d.add_header("cf-ray", "8c1f2e3d4b5a6978-NRT");
            d.add_header("x-request-id", "req_7f3a9c21e0b44d18");
            d.add_header("x-codex-primary-used-percent", "97");
            d.add_header("retry-after", "120");
            let body = serde_json::json!({
                "error": {
                    "type": if limit { "usage_limit_reached" } else { "server_error" },
                    "message": format!("upstream failed for demo@example.com {EVIL}"),
                    "debug": "Authorization: Bearer sk-demo-FAKEFAKEFAKEFAKEFAKE",
                    "resets_in_seconds": 1800
                },
                "refresh_token": "rt_FAKEFAKEFAKEFAKEFAKEFAKE"
            });
            d.set_body(body.to_string().as_bytes());
            r.detail = Some(Box::new(d));
        } else {
            let inp = 2_000 + (next() % 60_000) as i64;
            r.input_tokens = Some(inp);
            r.cached_tokens = Some(inp * (next() % 90) as i64 / 100);
            r.output_tokens = Some(100 + (next() % 3_000) as i64);
            r.reasoning_tokens = Some((next() % 800) as i64);
            r.ttft_ms = Some(300 + (next() % 2_500) as u32);
        }
        r.duration_ms = r.ttft_ms.unwrap_or(500) + (next() % 20_000) as u32;
        // Model / effort / tier variants (the newest rows cover every case).
        let m = r.model.clone();
        r.resp_model = m.clone();
        r.resp_effort = r.effort.clone();
        r.service_tier = Some("default".into());
        match if i >= n - 9 {
            (n - 1 - i) as u64
        } else {
            next() % 12
        } {
            // Fast served
            0 => {
                r.req_tier = Some("priority".into());
                r.service_tier = Some("priority".into());
            }
            // Fast requested, downgraded
            1 => r.req_tier = Some("priority".into()),
            // Flex served
            2 => {
                r.req_tier = Some("flex".into());
                r.service_tier = Some("flex".into());
            }
            // dated snapshot (variant)
            3 => r.resp_model = m.map(|m| format!("{m}-2026-09-30")),
            // real mismatch
            4 => r.resp_model = Some("gpt-5.6-luna".into()),
            // no request effort: fall back to the response's
            5 => {
                r.effort = None;
                r.resp_effort = Some("medium".into());
            }
            // xhigh
            6 => {
                r.effort = Some("xhigh".into());
                r.resp_effort = Some("xhigh".into());
            }
            // effort differs between request and response
            7 => {
                r.effort = Some("low".into());
                r.resp_effort = Some("medium".into());
            }
            _ => {}
        }
        let prog = i as f64 / n as f64;
        if a != "evil" {
            r.quota = Some(QuotaObs {
                plan: Some(if a == "alice" { "plus" } else { "pro" }.into()),
                p_pct: Some(
                    ((prog * 1000.0) as i64 % 100) as f64 * if a == "alice" { 0.9 } else { 0.4 },
                ),
                p_reset: Some(reset_p),
                p_window_min: Some(300),
                s_pct: Some((prog * if a == "alice" { 78.0 } else { 3.0 }).floor()),
                s_reset: Some(reset_s),
                s_window_min: Some(10080),
            });
        }
        rr.record(r);
    }
    {
        let at = now - 10 * 60_000;
        let mut r = Record::new(rec.ids.next_at(at), Transport::Sse, "responses");
        r.status = 200;
        r.model = Some("gpt-5.6-terra".into());
        r.key = Some("laptop".into());
        r.duration_ms = 3_000;
        r.account = Some("frank".into());
        r.quota = Some(QuotaObs {
            plan: Some("free".into()),
            p_pct: Some(17.0),
            p_reset: Some(now / 1000 + 27 * 86400),
            p_window_min: Some(43200),
            s_pct: None,
            s_reset: None,
            s_window_min: None,
        });
        rec.record(r);
    }
    demo_special_rows(&rec, now);
    rec.flush_blocking().unwrap();
    rec2.flush_blocking().unwrap();
    drop(rec2);
    let state = AppState {
        usage: Some(rec),
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open(cfg.auth_dir.clone())
                .await
                .unwrap(),
        ),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    // bob is cooling (usage limit, 25 min) so the accounts page shows the cooldown row
    {
        state.router.report(
            &Default::default(),
            &"bob".to_string(),
            &ciallopa_core::types::Outcome::RateLimited {
                retry_after: Some(std::time::Duration::from_secs(25 * 60)),
                usage_limit: true,
                quota: None,
            },
        );
    }
    let settings = ciallopa::settings::Settings::new(state, Some(cfg_path), None);
    let ln = tokio::net::TcpListener::bind(&listen).await.unwrap();
    println!("ui demo on http://{listen}/ui (fake upstream {fake_up})");
    let warm = listen.clone();
    tokio::spawn(async move {
        // the account list loads asynchronously: refresh twice
        for ms in [300, 1500] {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            let _ = reqwest::Client::new()
                .post(format!("http://{warm}/usage/v1/reset-credits/refresh"))
                .bearer_auth(DEMO_KEY)
                .json(&serde_json::json!({}))
                .send()
                .await;
        }
    });
    axum::serve(ln, ciallopa::build_app_with(settings))
        .await
        .unwrap();
}

/// Newest rows covering every error source, an attempt timeline, retried-success and a WS prewarm.
fn demo_special_rows(rec: &Recorder, now: i64) {
    use serde_json::json;
    let mk = |dt: i64, t: Transport, ep: &'static str| {
        let mut r = Record::new(rec.ids.next_at(now - dt), t, ep);
        r.account = Some("alice".into());
        r.model = Some("gpt-5.6-terra".into());
        r.resp_model = r.model.clone();
        r.effort = Some("high".into());
        r.resp_effort = r.effort.clone();
        r.key = Some("laptop".into());
        r.service_tier = Some("default".into());
        r.status = 200;
        r
    };
    let ok_tokens = |r: &mut Record| {
        r.input_tokens = Some(24_000);
        r.cached_tokens = Some(19_000);
        r.output_tokens = Some(1_200);
        r.reasoning_tokens = Some(300);
        r.ttft_ms = Some(2_400);
        r.duration_ms = 9_800;
    };
    // WS prewarm (grey row)
    let mut r = mk(30_000, Transport::Ws, "prewarm");
    r.input_tokens = Some(18_000);
    r.cached_tokens = Some(17_500);
    r.output_tokens = Some(0);
    r.duration_ms = 420;
    rec.record(r);
    // stream: overloaded, switched, then ok (retried success with timeline)
    let mut r = mk(60_000, Transport::Sse, "responses");
    ok_tokens(&mut r);
    r.first_account = Some("bob".into());
    r.switches = 1;
    r.detail = Some(Box::new(ciallopa_usage::ErrorDetail {
        attempts: Some(json!([
            {"account":"bob","t_ms":0,"kind":"stream","code":"server_is_overloaded","action":"switch","body":"{\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}"},
            {"account":"alice","t_ms":640,"kind":"http","status":200,"action":"ok"}
        ])),
        ..Default::default()
    }));
    rec.record(r);
    // local: no usable account
    let mut r = mk(90_000, Transport::Sse, "responses");
    r.account = None;
    r.status = 503;
    r.error = Some("no_account".into());
    r.err_source = Some("local");
    r.duration_ms = 3;
    r.detail = Some(Box::new(ciallopa_usage::ErrorDetail {
        message: Some("no usable account (1 needs relogin, 1 disabled)".into()),
        ..Default::default()
    }));
    rec.record(r);
    // network: connect timeout after retry on the same account
    let mut r = mk(120_000, Transport::Sse, "responses");
    r.status = 502;
    r.error = Some("connect_timeout".into());
    r.err_source = Some("network");
    r.retries = 1;
    r.duration_ms = 30_050;
    r.detail = Some(Box::new(ciallopa_usage::ErrorDetail {
        message: Some("connect timeout 30s".into()),
        attempts: Some(json!([
            {"account":"alice","t_ms":0,"kind":"transport","code":"connect_timeout","action":"switch"},
            {"account":"bob","t_ms":15020,"kind":"transport","code":"connect_timeout","action":"stop"}
        ])),
        ..Default::default()
    }));
    rec.record(r);
    // stream: truncated after output
    let mut r = mk(150_000, Transport::Ws, "responses");
    ok_tokens(&mut r);
    r.error = Some("stream_truncated".into());
    r.err_source = Some("stream");
    r.detail = Some(Box::new(ciallopa_usage::ErrorDetail {
        message: Some("upstream closed before response.completed".into()),
        ..Default::default()
    }));
    rec.record(r);
    // Client closed before response headers: no HTTP status was recorded.
    let mut r = mk(165_000, Transport::Sse, "responses");
    r.status = 0;
    r.account = None;
    r.resp_model = None;
    r.resp_effort = None;
    r.service_tier = None;
    r.error = Some("client_closed".into());
    r.err_source = Some("client");
    r.duration_ms = 900;
    rec.record(r);
    // client closed mid-stream (HTTP 200 was already recorded)
    let mut r = mk(180_000, Transport::Sse, "responses");
    r.error = Some("client_closed".into());
    r.err_source = Some("client");
    r.ttft_ms = Some(1_900);
    r.duration_ms = 4_100;
    rec.record(r);
    // incomplete (not an error)
    let mut r = mk(210_000, Transport::Sse, "responses");
    ok_tokens(&mut r);
    r.incomplete = Some("max_output_tokens".into());
    rec.record(r);
}

/// Fake chatgpt.com reset-credit endpoints (never the real upstream): per-account card lists
/// that consume marks redeemed (chosen `credit_id` or the soonest-expiring); `evil` answers 500.
async fn fake_upstream() -> std::net::SocketAddr {
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Json;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Mutex;
    type Cards = Arc<Mutex<HashMap<String, Vec<Value>>>>;
    fn who(h: &HeaderMap) -> String {
        h.get("chatgpt-account-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    }
    /// (title, status, granted days ago, expires in hours; None = never)
    fn start(w: &str) -> Vec<Value> {
        let spec: &[(Option<&str>, &str, i64, Option<i64>)] = if w.ends_with("alice") {
            &[
                (Some("Plus 月度重置"), "available", 20, Some(10 * 24)),
                (None, "available", 3, Some(6)),
                (Some("活动赠送"), "available", 1, None),
                (Some("上月重置"), "redeemed", 40, Some(-48)),
            ]
        } else if w.ends_with("dave") {
            &[(None, "available", 2, Some(5 * 24))]
        } else if w.ends_with("erin") {
            &[(Some("Plus 月度重置"), "available", 9, Some(20))]
        } else {
            &[]
        };
        spec.iter()
            .enumerate()
            .map(|(i, (t, st, g, ex))| {
                let mut v = json!({"id": format!("rc_demo_{i}"), "reset_type": "rate_limit",
                    "status": st, "granted_at": rfc3339_h(-g * 24)});
                if let Some(t) = t {
                    v["title"] = json!(t);
                }
                if let Some(e) = ex {
                    v["expires_at"] = json!(rfc3339_h(*e));
                }
                v
            })
            .collect()
    }
    let list = |State(c): State<Cards>, h: HeaderMap| async move {
        let w = who(&h);
        if w.ends_with("evil") {
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "demo upstream error",
            ));
        }
        let mut m = c.lock().unwrap();
        let v = m.entry(w.clone()).or_insert_with(|| start(&w)).clone();
        let n = v.iter().filter(|c| c["status"] == "available").count();
        Ok(Json(json!({"available_count": n, "credits": v})))
    };
    let consume = |State(c): State<Cards>, h: HeaderMap, Json(b): Json<Value>| async move {
        let w = who(&h);
        let mut m = c.lock().unwrap();
        let v = m.entry(w.clone()).or_insert_with(|| start(&w));
        let want = b
            .get("credit_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let hit = v
            .iter_mut()
            .filter(|c| c["status"] == "available")
            .filter(|c| want.as_deref().is_none_or(|id| c["id"] == id))
            .min_by_key(|c| c["expires_at"].as_str().unwrap_or("9999").to_string());
        match hit {
            Some(c) => {
                c["status"] = json!("redeemed");
                Json(json!({"code": "reset", "windows_reset": 2}))
            }
            None => Json(json!({"code": "no_credit", "windows_reset": 0})),
        }
    };
    let app = axum::Router::new()
        .route("/backend-api/wham/rate-limit-reset-credits", get(list))
        .route(
            "/backend-api/wham/rate-limit-reset-credits/consume",
            post(consume),
        )
        .route(
            "/backend-api/wham/usage",
            get(|h: HeaderMap| async move {
                let now = now_ms() / 1000;
                let w = |pct: f64, secs: i64, left: i64| json!({"used_percent": pct,
                    "limit_window_seconds": secs, "reset_after_seconds": left, "reset_at": now + left});
                Json(if who(&h).ends_with("frank") {
                    json!({"plan_type": "free", "rate_limit": {"allowed": true, "limit_reached": false,
                        "primary_window": w(3.0, 2_592_000, 27 * 86_400), "secondary_window": null}})
                } else {
                    json!({"plan_type": "plus", "rate_limit": {"allowed": true, "limit_reached": false,
                        "primary_window": w(12.0, 18_000, 2 * 3600),
                        "secondary_window": w(40.0, 604_800, 3 * 86_400)}})
                })
            }),
        )
        .with_state(cards_state());
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    a
}

#[allow(clippy::type_complexity)]
fn cards_state() -> Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<serde_json::Value>>>>
{
    Default::default()
}

fn rfc3339_h(hours: i64) -> String {
    chrono_like_s(hours * 3600)
}

/// RFC 3339 UTC `delta` seconds from now (no chrono dependency).
fn chrono_like_s(delta: i64) -> String {
    let secs = now_ms() / 1000 + delta;
    let (d, s) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // civil-from-days (Howard Hinnant)
    let z = d + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{day:02}T{:02}:{:02}:{:02}Z",
        s / 3600,
        s % 3600 / 60,
        s % 60
    )
}
