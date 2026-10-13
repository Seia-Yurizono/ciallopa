//! Local demo for the /ui page: fake usage data (incl. XSS-looking names), mock accounts,
//! no upstream traffic. `cargo run --example ui_demo -- 127.0.0.1:8399 /tmp/ui-demo`
#![allow(clippy::unwrap_used)]
use cpa_lite_core::Config;
use cpa_lite_forward::AppState;
use cpa_lite_usage::{now_ms, QuotaObs, Record, Recorder, Transport};
use std::sync::Arc;

pub const DEMO_KEY: &str = "sk-demo-0123456789abcdef";
const EVIL: &str = "<img src=x onerror=\"window.__xss=1\">";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let listen = args.get(1).cloned().unwrap_or("127.0.0.1:8399".into());
    let dir = std::path::PathBuf::from(args.get(2).cloned().unwrap_or("/tmp/ui-demo".into()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("auths")).unwrap();
    let acct = |id: &str, extra: serde_json::Value| {
        let mut v = serde_json::json!({"type":"codex","access_token":"x","refresh_token":"y",
            "expired":"2999-01-01T00:00:00Z","email": format!("{id}@example.com")});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        cpa_lite_auth::file::write_auth_file(&dir.join("auths"), id, &v).unwrap();
    };
    acct("alice", serde_json::json!({"plan_type":"plus"}));
    acct("bob", serde_json::json!({"plan_type":"pro"}));
    acct(
        "carol",
        serde_json::json!({"needs_relogin":true,"disabled":true}),
    );
    acct("evil", serde_json::json!({"email": EVIL}));
    let cfg = Config::parse(&format!(
        "listen = \"{listen}\"\ninstance_id = \"demo-vps\"\nauth_dir = \"{}\"\n\
         [[api_keys]]\nlabel = \"laptop\"\nkey = \"{DEMO_KEY}\"\n\
         [[api_keys]]\nlabel = \"{}\"\nkey = \"sk-demo-evil-0123456789\"\n\
         [usage]\npath = \"{}\"\nretention_days = 0\n",
        dir.join("auths").display(),
        EVIL.replace('"', "\\\""),
        dir.join("usage.db").display()
    ))
    .unwrap();
    let rec = Recorder::open_as(&cfg.usage, &cfg.instance_id).unwrap();
    rec.set_prices(
        cpa_lite_usage::PriceTable::load(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../prices.example.toml"
        )))
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
    let models = [
        "gpt-5.5",
        "gpt-5.6-terra",
        "gpt-5.6-sol",
        "gpt-6-luna",
        EVIL,
    ];
    let accts = ["alice", "bob", "evil"];
    let n = 1500;
    for i in 0..n {
        let ts = now - 14 * 86_400_000
            + (i as i64) * (14 * 86_400_000 / n as i64)
            + (next() % 60_000) as i64;
        let mut r = Record::new(
            rec.ids.next_at(ts),
            if i % 3 == 0 {
                Transport::Ws
            } else {
                Transport::Sse
            },
            "responses",
        );
        let a = accts[(next() % 3) as usize];
        r.account = Some(a.into());
        r.model = Some(models[(next() % models.len() as u64) as usize].into());
        r.effort = Some(["low", "medium", "high"][(next() % 3) as usize].into());
        r.key = Some(if next() % 5 == 0 {
            EVIL.into()
        } else {
            "laptop".into()
        });
        r.status = 200;
        let fail = next() % 25 == 0 || i == n - 3;
        if fail {
            let limit = next() % 2 == 0 && i != n - 3;
            r.error = Some(if limit {
                "usage_limit".into()
            } else {
                "upstream_5xx".into()
            });
            r.status = 502;
            // Fake upstream error (redacted on write; XSS payload must render as text).
            let mut d = cpa_lite_usage::ErrorDetail::default();
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
        if next() % 10 == 0 {
            r.service_tier = Some("priority".into());
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
        rec.record(r);
    }
    rec.flush_blocking().unwrap();
    let state = AppState {
        usage: Some(rec),
        accounts: Arc::new(
            cpa_lite_auth::FileAccountStore::open(cfg.auth_dir.clone())
                .await
                .unwrap(),
        ),
        router: Arc::new(cpa_lite_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(cpa_lite_upstream::UpstreamClient::new(&cfg).unwrap()),
        config: Arc::new(cfg),
    };
    let ln = tokio::net::TcpListener::bind(&listen).await.unwrap();
    println!("ui demo on http://{listen}/ui");
    axum::serve(ln, cpa_lite::build_app(state)).await.unwrap();
}
