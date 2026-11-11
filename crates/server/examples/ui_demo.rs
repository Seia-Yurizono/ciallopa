//! Local demo for the /ui page: fake usage data (a few XSS-looking names, two instances), mock accounts,
//! no upstream traffic. `cargo run --example ui_demo -- 127.0.0.1:8399`
//! Uses a unique temporary directory by default; an optional data directory must not exist.
#![allow(clippy::unwrap_used)]
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use ciallopa_usage::{now_ms, Observation, QuotaObs, Record, Recorder, Transport};
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
    // UI_DEMO_BUSY=1: chart stress — the last 26 hours get a diurnal curve with bursts, failure
    // spikes next to quiet hours and scattered client cancels (fake data only). Recorded in time
    // order alongside the base traffic.
    let mut busy: Vec<(i64, u8)> = Vec::new();
    if std::env::var("UI_DEMO_BUSY").is_ok_and(|v| v == "1") {
        for hr in 0..26i64 {
            let hour = ((now - hr * 3_600_000) / 3_600_000 + 8).rem_euclid(24);
            let cnt = if (9..23).contains(&hour) { 30 } else { 6 }
                + (next() % 12) as i64
                + if hr % 7 == 3 { 60 } else { 0 };
            for _ in 0..cnt {
                let at = now - 20 * 60_000 - hr * 3_600_000 - (next() % 3_600_000) as i64;
                let roll = next() % 100;
                let kind = if hr % 5 == 1 && roll < 30 || roll < 2 {
                    1
                } else if hr % 4 == 2 && roll < 12 {
                    2
                } else {
                    0
                };
                busy.push((at, kind));
            }
        }
        busy.sort();
    }
    let mut bj = 0;
    let n = 1500;
    let mut stale = [false; 2];
    for i in 0..n {
        let ts = now - 14 * 86_400_000
            + (i as i64) * (14 * 86_400_000 / n as i64)
            + (next() % 60_000) as i64;
        while bj < busy.len() && busy[bj].0 <= ts {
            busy_record(&rec, busy[bj].0, busy[bj].1);
            bj += 1;
        }
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
                ..Default::default()
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
        let fail = next() % 14 == 0 || i == n - 10 || i == n - 11;
        // Retries and account switches on some requests (the 重试 column in 分布).
        if next() % 9 == 0 {
            r.retries = 1;
        }
        if next() % 13 == 0 {
            r.switches = 1;
            r.first_account = Some(if a == "alice" { "bob" } else { "alice" }.into());
        }
        // Every failure class: upstream quota / rate limit / 5xx, network, stream, local.
        let kind = if fail { next() % 10 } else { 0 };
        if fail && i != n - 10 && i != n - 11 && kind >= 5 {
            let (code, src, status) = match kind {
                5 => ("rate_limit_exceeded", "upstream", 429),
                6 => ("timeout_connect", "network", 504),
                7 => ("stream_truncated", "stream", 200),
                8 => ("first_quota_reset", "local", 429),
                _ => ("all_cooling", "local", 503),
            };
            r.error = Some(code.into());
            r.err_source = Some(src);
            r.status = status;
            if src == "upstream" {
                r.upstream_status = Some(status);
            }
        } else if fail {
            let limit = i == n - 11 || (kind % 2 == 0 && i != n - 10);
            r.error = Some(if limit {
                "usage_limit_reached".into()
            } else {
                "server_error".into()
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
        // Fake Codex sessions: about a dozen consecutive requests share one UUIDv7-like
        // thread id (same-minute ids share their head); some clients send none.
        if i % 17 != 5 {
            let s = (i / 12) as u64;
            let ms = 0x019a_2b3c_0000u64 + s * 977;
            r.thread_id = Some(format!(
                "{:08x}-{:04x}-7{:03x}-8{:03x}-{:012x}",
                ms >> 16,
                ms & 0xffff,
                s % 0xfff,
                (s * 7) % 0xfff,
                s.wrapping_mul(0x9e37_79b9_7f4a) & 0xffff_ffff_ffff
            ));
            r.turn_id = Some(format!("turn-{:04}", i % 12 + 1));
        }
        let prog = i as f64 / n as f64;
        // alice quota comes from demo_quota_history (multi-cycle 30-day series).
        if a != "evil" && a != "alice" {
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
                ..Default::default()
            });
        }
        rr.record(r);
    }
    for &(at, kind) in &busy[bj..] {
        busy_record(&rec, at, kind);
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
            ..Default::default()
        });
        rec.record(r);
    }
    demo_special_rows(&rec, now);
    demo_obs_rows(&rec, now);
    demo_error_code_rows(&rec, now);
    demo_refusals(&rec, now);
    demo_occupancy(&rec, now, &mut next);
    demo_quota_history(&rec, now);
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
/// Local-limit refusals over the last hours: inflight bursts from Codex, a WS cap spike and one
/// oversized body, so the 已拒绝 chip, its breakdown and the per-minute list have data.
fn demo_refusals(rec: &Recorder, now: i64) {
    use ciallopa_usage::{Refusal, RefusalReason as R};
    let ua = "codex_cli_rs/0.130.0 (Mac OS 15.5.0; arm64) iTerm.app/3.5.14";
    let base = |dt: i64, reason: R, key: &str, route: &str| Refusal {
        ts_ms: now - dt,
        reason: Some(reason),
        key: Some(key.into()),
        route: route.into(),
        ua: Some(ua.into()),
        limit: 64,
        inflight: 64,
        ws_conns: 24,
        buffered: 12 << 20,
        ..Default::default()
    };
    for (dt, n, key) in [
        (90_000, 7, "laptop"),
        (95_000, 4, "laptop"),
        (25 * 60_000, 12, "ci"),
        (3 * 3_600_000, 3, "laptop"),
    ] {
        for i in 0..n {
            rec.refuse(base(
                dt + i * 900,
                R::Inflight,
                key,
                "/backend-api/codex/responses",
            ));
        }
    }
    rec.refuse(base(
        40 * 60_000,
        R::Inflight,
        "laptop",
        "/backend-api/codex/responses/compact",
    ));
    for i in 0..5 {
        let mut r = base(
            70 * 60_000 + i * 1_000,
            R::Ws,
            "laptop",
            "/backend-api/codex/responses",
        );
        r.ws = true;
        r.limit = 24;
        r.inflight = 30;
        rec.refuse(r);
    }
    let mut r = base(2 * 3_600_000, R::Body, "ci", "/v1/responses");
    r.limit = 256 << 20;
    r.buffered = 250 << 20;
    r.bytes = Some(18 << 20);
    rec.refuse(r);
    let mut r = base(
        110 * 60_000,
        R::WsMsg,
        "laptop",
        "/backend-api/codex/responses",
    );
    r.ws = true;
    r.model = Some("gpt-5.6-terra".into());
    r.account = Some("alice".into());
    r.limit = 256 << 20;
    r.buffered = 251 << 20;
    r.bytes = Some(9 << 20);
    rec.refuse(r);
}

/// Occupancy history: every minute for the last 26 hours, every 10 minutes before that, with
/// the refusal minutes at the inflight / WS cap (满载) and a few older full minutes.
fn demo_occupancy(rec: &Recorder, now: i64, next: &mut impl FnMut() -> u64) {
    let (li, lw) = (64u64, 24u64);
    let mut added = 0;
    for m in 0..14 * 1440i64 {
        if m > 26 * 60 && m % 10 != 0 {
            continue;
        }
        let ts = now - m * 60_000;
        let hour = (ts / 3_600_000 + 8).rem_euclid(24) as u64;
        let busy = if (9..23).contains(&hour) { 18 } else { 4 };
        let mut peak = 1 + next() % busy;
        let mut ws = next() % (busy / 2 + 1);
        if [1, 2, 25, 40, 180].contains(&m) || next().is_multiple_of(400) {
            peak = li;
        }
        if m == 70 {
            ws = lw;
        }
        rec.occupancy(ciallopa_usage::OccupancySample {
            ts_ms: ts,
            inflight: peak / 2,
            ws,
            buffered: (next() % 8) << 20,
            peak_inflight: peak,
            peak_ws: ws,
            peak_buffered: (next() % 16) << 20,
            limit_inflight: li,
            limit_ws: lw,
            limit_buffered: 256 << 20,
        });
        added += 1;
        if added % 1000 == 0 {
            rec.flush_blocking().unwrap();
        }
    }
}

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
    r.status = 504;
    r.error = Some("timeout_connect".into());
    r.err_source = Some("network");
    r.retries = 1;
    r.duration_ms = 30_050;
    r.detail = Some(Box::new(ciallopa_usage::ErrorDetail {
        message: Some("connect timeout 30s".into()),
        attempts: Some(json!([
            {"account":"alice","t_ms":0,"kind":"transport","code":"timeout_connect","action":"switch"},
            {"account":"bob","t_ms":15020,"kind":"transport","code":"timeout_connect","action":"stop"}
        ])),
        ..Default::default()
    }));
    rec.record(r);
    // stream: truncated after output
    let mut r = mk(150_000, Transport::Sse, "responses");
    ok_tokens(&mut r);
    r.error = Some("stream_truncated".into());
    r.err_source = Some("stream");
    r.detail = Some(Box::new(ciallopa_usage::ErrorDetail {
        message: Some("upstream stream ended without a terminal event".into()),
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

/// The most recent rows: one per error code whose meaning or name changed, so the requests
/// table shows them together (fake data only).
fn demo_error_code_rows(rec: &Recorder, now: i64) {
    let mk = |dt: i64, t: Transport, ep: &'static str| {
        let mut r = Record::new(rec.ids.next_at(now - dt), t, ep);
        r.account = Some("alice".into());
        r.model = Some("gpt-5.6-terra".into());
        r.resp_model = r.model.clone();
        r.effort = Some("high".into());
        r.key = Some("laptop".into());
        r.status = 200;
        r.duration_ms = 6_000;
        r
    };
    let tokens = |r: &mut Record| {
        r.input_tokens = Some(12_000);
        r.output_tokens = Some(800);
        r.ttft_ms = Some(1_800);
    };
    let detail = |msg: &str, body: &str| {
        let mut d = ciallopa_usage::ErrorDetail {
            message: Some(msg.into()),
            ..Default::default()
        };
        d.set_body(body.as_bytes());
        Some(Box::new(d))
    };
    let mut rows: Vec<Record> = Vec::new();
    // response.incomplete: interrupted (a normal end for Codex) and content_filter
    let mut r = mk(1_000, Transport::Sse, "responses");
    tokens(&mut r);
    r.incomplete = Some("interrupted".into());
    rows.push(r);
    let mut r = mk(2_000, Transport::Sse, "responses");
    tokens(&mut r);
    r.incomplete = Some("content_filter".into());
    rows.push(r);
    // response.failed with a request-side code
    let mut r = mk(3_000, Transport::Sse, "responses");
    r.error = Some("response_failed".into());
    r.err_source = Some("stream");
    r.detail = detail(
        "Your input exceeds the context window of this model.",
        r#"{"type":"response.failed","response":{"status":"failed","error":{"code":"context_length_exceeded","message":"Your input exceeds the context window of this model."}}}"#,
    );
    rows.push(r);
    // WS error frame: connection lifecycle
    let mut r = mk(4_000, Transport::Ws, "responses");
    r.error = Some("websocket_connection_limit_reached".into());
    r.err_source = Some("stream");
    r.detail = detail(
        "Responses websocket connection limit reached (60 minutes).",
        r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"websocket_connection_limit_reached","message":"Responses websocket connection limit reached (60 minutes)."}}"#,
    );
    rows.push(r);
    // WS error frame: 429 rate limit without a quota code
    let mut r = mk(5_000, Transport::Ws, "responses");
    r.error = Some("rate_limit_exceeded".into());
    r.err_source = Some("stream");
    r.detail = detail(
        "rate limit reached mid-turn; closed for client replay",
        r#"{"type":"error","status":429,"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Rate limit reached"}}"#,
    );
    rows.push(r);
    // WS upstream close frame / EOF
    let mut r = mk(6_000, Transport::Ws, "responses");
    r.error = Some("upstream_ws_close".into());
    r.err_source = Some("stream");
    r.detail = detail(
        "upstream websocket closed mid-turn (code 1011) internal error",
        "",
    );
    rows.push(r);
    let mut r = mk(7_000, Transport::Ws, "responses");
    r.error = Some("upstream_ws_eof".into());
    r.err_source = Some("network");
    r.detail = detail(
        "upstream websocket ended without a close frame (Connection reset without closing handshake)",
        "",
    );
    rows.push(r);
    // SSE read error after output
    let mut r = mk(8_000, Transport::Sse, "responses");
    r.ttft_ms = Some(1_500);
    r.error = Some("stream_read_error".into());
    r.err_source = Some("network");
    r.detail = detail("upstream stream error: error decoding response body", "");
    rows.push(r);
    // completed, then the upstream went idle (the answer was complete)
    let mut r = mk(9_000, Transport::Sse, "responses");
    tokens(&mut r);
    r.error = Some("idle_timeout".into());
    r.err_source = Some("network");
    r.detail = detail("upstream stream idle timeout", "");
    rows.push(r);
    // connect timeout and preparation budget
    let mut r = mk(10_000, Transport::Sse, "responses");
    r.status = 504;
    r.error = Some("timeout_connect".into());
    r.err_source = Some("network");
    r.detail = detail("upstream timed out", "");
    rows.push(r);
    let mut r = mk(11_000, Transport::Sse, "responses");
    r.status = 504;
    r.error = Some("preparation_timeout".into());
    r.err_source = Some("local");
    r.detail = detail(
        "preparation budget exhausted across retries/account switches; no further attempt started",
        "",
    );
    rows.push(r);
    // images: non-stream request cut off
    let mut r = mk(12_000, Transport::Http, "images");
    r.model = Some("gpt-image-2".into());
    r.resp_model = r.model.clone();
    r.status = 502;
    r.error = Some("stream_truncated".into());
    r.err_source = Some("stream");
    r.detail = detail("upstream stream ended without a terminal event", "");
    rows.push(r);
    for r in rows {
        rec.record(r);
    }
}

/// One UI_DEMO_BUSY record: 0 = ok, 1 = upstream 502, 2 = client cancel.
fn busy_record(rec: &Recorder, at: i64, kind: u8) {
    let mut r = Record::new(rec.ids.next_at(at), Transport::Sse, "responses");
    r.status = 200;
    r.model = Some("gpt-5.6-terra".into());
    r.key = Some("laptop".into());
    r.account = Some(if at % 2 == 0 { "alice" } else { "bob" }.into());
    match kind {
        1 => {
            r.error = Some("server_error".into());
            r.status = 502;
            r.err_source = Some("upstream");
            r.upstream_status = Some(502);
        }
        2 => {
            r.error = Some("client_closed".into());
            r.err_source = Some("client");
        }
        _ => {
            r.input_tokens = Some(2_000 + at.rem_euclid(60_000));
            r.output_tokens = Some(100 + at.rem_euclid(3_000));
            r.ttft_ms = Some(300 + at.rem_euclid(2_500) as u32);
        }
    }
    r.duration_ms = r.ttft_ms.unwrap_or(500) + at.rem_euclid(20_000) as u32;
    rec.record(r);
}

/// Fake chatgpt.com reset-credit endpoints (never the real upstream): per-account card lists
/// that consume marks redeemed (chosen `credit_id` or the soonest-expiring); `evil` answers 500.
async fn fake_upstream() -> std::net::SocketAddr {
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
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
                let who = who(&h);
                // A deterministic failure for manual UI recovery checks; no real provider is used.
                if who.ends_with("evil") {
                    return (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": "demo quota refresh failure"})),
                    )
                        .into_response();
                }
                Json(if who.ends_with("frank") {
                    json!({"plan_type": "free", "rate_limit": {"allowed": true, "limit_reached": false,
                        "primary_window": w(3.0, 2_592_000, 27 * 86_400), "secondary_window": null},
                        "rate_limit_reset_credits": {"available_count": 0}})
                } else if who.ends_with("bob") {
                    json!({"plan_type": "pro", "rate_limit": {"allowed": false, "limit_reached": true,
                        "primary_window": w(100.0, 18_000, 3600),
                        "secondary_window": w(71.0, 604_800, 4 * 86_400)},
                        "rate_limit_reset_credits": {"available_count": 3}})
                } else {
                    json!({"plan_type": "plus", "rate_limit": {"allowed": true, "limit_reached": false,
                        "primary_window": w(12.0, 18_000, 2 * 3600),
                        "secondary_window": w(40.0, 604_800, 3 * 86_400)}})
                }).into_response()
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

/// Gateway downstream observation scenarios (all synthetic; thread `demo-obs-thread`,
/// turn ids `demo-obs-*`). Times are ms since the record start.
fn demo_obs_rows(rec: &Recorder, now: i64) {
    let mk = |dt: i64, t: Transport, turn: &str, ttft: u32, dur: u32, out: i64| {
        let mut r = Record::new(rec.ids.next_at(now - dt), t, "responses");
        r.account = Some("alice".into());
        r.model = Some("gpt-5.6-terra".into());
        r.resp_model = r.model.clone();
        r.key = Some("laptop".into());
        r.status = 200;
        r.thread_id = Some("demo-obs-thread".into());
        r.turn_id = Some(format!("demo-obs-{turn}"));
        r.input_tokens = Some(12_000);
        r.cached_tokens = Some(9_000);
        r.output_tokens = Some(out);
        r.ttft_ms = Some(ttft);
        r.duration_ms = dur;
        r
    };
    let sse = |text: u32, term: u32, yl: u32, blk: u64, blk_max: u64, bytes: u64| Observation {
        up_first_text_ms: Some(text),
        up_last_event_ms: Some(term),
        up_terminal_ms: Some(term),
        up_events: Some(120),
        up_bytes: Some(bytes),
        response_id: Some("resp_demo".into()),
        upstream_request_id: Some("req-demo".into()),
        sse_first_queue_ms: Some(text),
        sse_last_queue_ms: Some(term),
        sse_queue_blocked_us: Some(blk),
        sse_queue_blocked_max_us: Some(blk_max),
        sse_queue_chunks: Some(118),
        sse_queue_bytes: Some(bytes),
        sse_first_yield_ms: Some(text),
        sse_last_yield_ms: Some(yl),
        sse_yield_chunks: Some(118),
        sse_yield_bytes: Some(bytes),
        down_incomplete: Some(false),
        ..Default::default()
    };
    let ws = |text: u32,
              term: u32,
              last_send: u32,
              blk: u64,
              blk_max: u64,
              down: bool,
              up: Option<&str>| Observation {
        up_first_text_ms: Some(text),
        up_last_event_ms: Some(term),
        up_terminal_ms: Some(term),
        up_events: Some(90),
        up_bytes: Some(48_000),
        response_id: Some("resp_demo_ws".into()),
        upstream_request_id: Some("req-demo-ws".into()),
        ws_first_send_ms: Some(text),
        ws_last_send_ms: Some(last_send),
        ws_sink_blocked_us: Some(blk),
        ws_sink_blocked_max_us: Some(blk_max),
        ws_send_msgs: Some(90),
        ws_send_bytes: Some(48_000),
        down_incomplete: Some(false),
        ws_deflate_down: Some(down),
        ws_ext_down: down.then(|| "permessage-deflate; client_max_window_bits=15".into()),
        ws_deflate_up: Some(up.is_some() && up != Some("unrecognized")),
        ws_ext_up: up.map(str::to_string),
        ..Default::default()
    };
    let put = |mut r: Record, o: Option<Observation>| {
        r.obs = o.map(Box::new);
        rec.record(r);
    };
    // 1 SSE normal
    put(
        mk(90_000, Transport::Sse, "sse-normal", 900, 4_200, 600),
        Some(sse(900, 4_150, 4_160, 0, 0, 52_000)),
    );
    // 2 SSE slow upstream (first text at 18 s)
    put(
        mk(
            120_000,
            Transport::Sse,
            "sse-slow-upstream",
            18_200,
            24_000,
            800,
        ),
        Some(sse(18_200, 23_950, 23_960, 0, 0, 61_000)),
    );
    // 3 SSE downstream backpressure: queue blocked 6.2 s total, yields end after upstream
    put(
        mk(
            150_000,
            Transport::Sse,
            "sse-backpressure",
            1_100,
            9_000,
            4_000,
        ),
        Some(sse(1_100, 8_900, 15_300, 6_200_000, 1_800_000, 410_000)),
    );
    // 4 WS normal, deflate both legs
    put(
        mk(180_000, Transport::Ws, "ws-normal", 700, 3_800, 500),
        Some(ws(
            700,
            3_780,
            3_790,
            0,
            0,
            true,
            Some("permessage-deflate"),
        )),
    );
    // 5 WS sink backpressure
    put(
        mk(210_000, Transport::Ws, "ws-backpressure", 800, 7_000, 3_000),
        Some(ws(
            800,
            6_950,
            11_400,
            4_300_000,
            2_100_000,
            true,
            Some("permessage-deflate"),
        )),
    );
    // 6 WS no client deflate, upstream answer unrecognized
    put(
        mk(240_000, Transport::Ws, "ws-no-deflate", 650, 2_900, 300),
        Some(ws(650, 2_880, 2_885, 0, 0, false, Some("unrecognized"))),
    );
    // 7 old row without observation (pre-v14)
    put(
        mk(270_000, Transport::Sse, "old-no-obs", 1_000, 5_000, 700),
        None,
    );
    // 8 single output token
    put(
        mk(300_000, Transport::Sse, "single-token", 1_400, 1_450, 1),
        Some(sse(1_400, 1_440, 1_445, 0, 0, 3_100)),
    );
    // 9 incomplete (max_output_tokens) with observation
    let mut r = mk(330_000, Transport::Sse, "incomplete", 1_200, 30_000, 8_192);
    r.incomplete = Some("max_output_tokens".into());
    put(r, Some(sse(1_200, 29_900, 29_910, 0, 0, 900_000)));
    // 10 reasoning then tool call, no text
    let mut o = sse(0, 6_000, 6_010, 0, 0, 22_000);
    o.up_first_text_ms = None;
    o.up_first_reasoning_ms = Some(1_300);
    o.up_first_tool_ms = Some(4_800);
    put(
        mk(360_000, Transport::Sse, "reasoning-tool", 1_300, 6_100, 900),
        Some(o),
    );
    // 11 client disconnected mid-stream (no terminal)
    let mut r = mk(390_000, Transport::Sse, "client-gone", 1_000, 7_500, 0);
    r.output_tokens = None;
    r.status = 200;
    r.error = Some("client_closed".into());
    r.err_source = Some("client");
    let mut o = sse(1_000, 7_400, 7_400, 0, 0, 30_000);
    o.up_terminal_ms = None;
    o.down_incomplete = Some(true);
    put(r, Some(o));
}

/// Multi-cycle 30-day quota history for alice: every reset class plus an open cycle.
/// IDs use a high sequence so they do not collide with the bulk 14-day traffic.
fn demo_quota_history(rec: &Recorder, now: i64) {
    rec.flush_blocking().unwrap();
    let day = 86_400_000i64;
    let hour = 3_600_000i64;
    let primary: &[(&str, i64, f64, Option<&str>)] = &[
        ("natural", 27, 1.0, None),
        ("natural", 24, 1.0, None),
        ("early_reset", 21, 1.0, None),
        ("manual_card", 18, 1.0, None),
        ("auto_card", 15, 1.0, None),
        ("external_card", 12, 1.0, None),
        ("limit_change", 10, 1.0, None),
        ("natural", 8, 2.0, None),
        ("natural", 5, 0.4, None),
        ("early_reset", 2, 1.0, Some("priority")),
    ];
    let mut seq = 0i64;
    let path = rec.path().to_path_buf();
    for &(_, days, _, _) in primary {
        let close = now - days * day;
        if days == 18 {
            demo_seed_card(&path, "laptop", close);
        }
        if days == 15 {
            demo_seed_card(&path, "(auto)", close);
        }
    }

    let mut cards = 3i64;
    let mut s_pct = 10.0f64;
    let mut s_reset = (now - 21 * day) / 1000;
    let mut p_pct = 8.0f64;
    let mut p_reset = (now - 27 * day) / 1000 + 300 * 60;

    for &(kind, days, scale, tier) in primary {
        let close = now - days * day;
        demo_write_cycle(
            rec,
            now,
            close,
            kind,
            scale,
            tier,
            &mut seq,
            &mut p_pct,
            &mut p_reset,
            &mut s_pct,
            &mut s_reset,
            &mut cards,
            if days == 21 { Some("natural") } else { None },
        );
        if days == 18 {
            // Secondary-only early reset at -16d (after -18d, before -15d auto card).
            let t = now - 16 * day;
            s_pct += 2.0;
            demo_hist_snap(
                rec,
                &mut seq,
                t - hour,
                p_pct + 3.0,
                p_reset,
                (s_pct + 8.0).min(90.0),
                s_reset,
                cards,
                1.0,
                None,
            );
            s_pct = 4.0;
            s_reset = t / 1000 + 10_080 * 60;
            demo_hist_snap(
                rec,
                &mut seq,
                t,
                p_pct + 4.0,
                p_reset,
                s_pct,
                s_reset,
                cards,
                1.0,
                None,
            );
            p_pct += 4.0;
        }
        if days == 8 {
            let t = now - 7 * day;
            demo_hist_snap(
                rec,
                &mut seq,
                t - hour,
                p_pct + 2.0,
                p_reset,
                (s_pct + 10.0).min(90.0),
                s_reset,
                cards,
                1.0,
                None,
            );
            s_pct = 5.0;
            s_reset = t / 1000 + 10_080 * 60;
            demo_hist_snap(
                rec,
                &mut seq,
                t,
                p_pct + 3.0,
                p_reset,
                s_pct,
                s_reset,
                cards,
                1.0,
                None,
            );
            p_pct += 3.0;
        }
    }

    let open_p = now / 1000 + 3 * 3600;
    for &(ago, pct, sp) in &[
        (18 * hour, 15.0, 22.0),
        (12 * hour, 32.0, 30.0),
        (6 * hour, 48.0, 36.0),
        (hour, 62.0, 42.0),
    ] {
        demo_hist_snap(
            rec,
            &mut seq,
            now - ago,
            pct,
            open_p,
            sp,
            s_reset,
            cards,
            1.0,
            Some("ultrafast"),
        );
    }
}

fn demo_hist_id(seq: &mut i64, ms: i64) -> i64 {
    *seq += 1;
    (ms << ciallopa_usage::SEQ_BITS) | (0xC_0000 + *seq)
}

fn demo_seed_card(path: &std::path::Path, key: &str, at_ms: i64) {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute(
        "INSERT INTO reset_consume(account, credit_id, redeem_request_id, key, instance, \
         started_ms, finished_ms, status, http_status, code) \
         VALUES ('alice', ?1, ?2, ?3, 'demo-vps', ?4, ?5, 'done', 200, 'reset')",
        rusqlite::params![
            format!("demo-credit-{at_ms}"),
            format!("rr-{at_ms}"),
            key,
            at_ms - 2_000,
            at_ms
        ],
    )
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn demo_hist_snap(
    rec: &Recorder,
    seq: &mut i64,
    ms: i64,
    p_pct: f64,
    p_reset: i64,
    s_pct: f64,
    s_reset: i64,
    cards: i64,
    scale: f64,
    tier: Option<&str>,
) {
    let mut r = Record::new(demo_hist_id(seq, ms), Transport::Sse, "responses");
    r.account = Some("alice".into());
    r.model = Some("gpt-5.6-sol".into());
    r.status = 200;
    r.key = Some("laptop".into());
    r.input_tokens = Some((80_000.0 * scale) as i64);
    r.output_tokens = Some((2_000.0 * scale) as i64);
    r.cached_tokens = Some(0);
    r.duration_ms = 4_000;
    r.service_tier = tier.map(|t| t.into());
    r.quota = Some(QuotaObs {
        plan: Some("plus".into()),
        p_pct: Some(p_pct),
        p_reset: Some(p_reset),
        p_window_min: Some(300),
        s_pct: Some(s_pct),
        s_reset: Some(s_reset),
        s_window_min: Some(10_080),
        cards: Some(cards),
    });
    rec.record(r);
}

#[allow(clippy::too_many_arguments)]
fn demo_write_cycle(
    rec: &Recorder,
    now: i64,
    close: i64,
    kind: &str,
    scale: f64,
    tier: Option<&str>,
    seq: &mut i64,
    p_pct: &mut f64,
    p_reset: &mut i64,
    s_pct: &mut f64,
    s_reset: &mut i64,
    cards: &mut i64,
    also_secondary: Option<&str>,
) {
    let day = 86_400_000i64;
    let hour = 3_600_000i64;
    let old_p = if kind == "natural" {
        close / 1000 - 60
    } else if kind == "limit_change" {
        (now - 8 * day) / 1000
    } else {
        close / 1000 + 2 * 3600
    };
    let new_p = if kind == "limit_change" {
        old_p
    } else {
        close / 1000 + 300 * 60
    };
    // Start at/above the previous close percent so 28% → 12% after a mid-window
    // limit_change is not itself classified as another limit_change.
    let start = (*p_pct + 4.0).max(12.0);
    for &(ago, pct) in &[
        (3 * hour, start),
        (2 * hour, (start + 16.0).min(72.0)),
        (hour, (start + 34.0).min(72.0)),
        (20 * 60_000, 72.0),
    ] {
        demo_hist_snap(
            rec,
            seq,
            close - ago,
            pct,
            old_p,
            *s_pct,
            *s_reset,
            *cards,
            scale,
            tier,
        );
        *s_pct = (*s_pct + 1.2).min(96.0);
    }
    let next_p = if kind == "limit_change" { 28.0 } else { 2.0 };
    let mut next_s = *s_pct;
    let mut next_sr = *s_reset;
    let mut next_cards = *cards;
    if matches!(kind, "manual_card" | "auto_card" | "external_card") {
        next_cards = (*cards - 1).max(0);
    }
    if also_secondary.is_some() {
        next_s = 3.0;
        next_sr = close / 1000 + 10_080 * 60;
    }
    demo_hist_snap(
        rec, seq, close, next_p, new_p, next_s, next_sr, next_cards, scale, tier,
    );
    *p_pct = next_p;
    *p_reset = new_p;
    *s_pct = next_s;
    *s_reset = next_sr;
    *cards = next_cards;
}
