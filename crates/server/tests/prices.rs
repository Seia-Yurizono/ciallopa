//! Price updates on request: status + unpriced hint, check (diff, no write), apply (write with
//! backup, prices used at once), unchanged ⇒ no write, stale check rejected, dataset failure
//! leaves the table alone, `prices_source` hot reload.
#![allow(clippy::unwrap_used)]
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use ciallopa::settings::Settings;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

const KEY: &str = "sk-cpa-0123456789abcdef";

#[derive(Clone, Default)]
struct Dataset(Arc<Mutex<Option<Value>>>);

async fn dataset(State(d): State<Dataset>) -> axum::response::Response {
    match d.0.lock().unwrap().clone() {
        Some(v) => Json(v).into_response(),
        None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn serve(app: Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    addr.to_string()
}

fn model(i: f64, o: f64) -> Value {
    json!({"litellm_provider": "openai", "mode": "chat", "input_cost_per_token": i * 1e-6,
           "output_cost_per_token": o * 1e-6, "cache_read_input_token_cost": i * 1e-7})
}

fn cfg_text(dir: &std::path::Path, source: &str) -> String {
    format!(
        "api_keys = [{{label = \"admin\", key = \"{KEY}\", admin = true}}]\nupstream = \"http://127.0.0.1:9/backend-api\"\nauth_dir = \"{}\"\n[usage]\npath = \"{}\"\nprices = \"{}\"\nprices_source = \"{source}\"\nflush_ms = 5\n",
        dir.join("auths").display(),
        dir.join("usage.db").display(),
        dir.join("prices.toml").display(),
    )
}

#[tokio::test]
async fn check_apply_unchanged_stale_failure_and_source_reload() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("auths")).unwrap();
    let ds = Dataset::default();
    let ds2 = Dataset::default();
    let data = serve(
        Router::new()
            .route("/litellm.json", get(dataset))
            .with_state(ds.clone())
            .merge(
                Router::new()
                    .route("/other.json", get(dataset))
                    .with_state(ds2.clone()),
            ),
    )
    .await;
    *ds.0.lock().unwrap() = Some(json!({"gpt-x": model(2.0, 10.0), "gpt-y": model(1.0, 4.0),
        "gpt-image-2": model(5.0, 30.0)}));
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        cfg_text(dir.path(), &format!("http://{data}/litellm.json")),
    )
    .unwrap();
    let cfg = Config::load(&path).unwrap();
    let rec = ciallopa_usage::Recorder::open(&cfg.usage).unwrap();
    let state = AppState {
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open(dir.path().join("auths"))
                .await
                .unwrap(),
        ),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: Some(rec.clone()),
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let settings = Settings::new(state, Some(path.clone()), None);
    let proxy = serve(ciallopa::build_app_with(settings.clone())).await;
    let http = reqwest::Client::new();
    let get = |p: &str| {
        http.get(format!("http://{proxy}{p}"))
            .bearer_auth(KEY)
            .send()
    };
    let post = |p: &str, b: Value| {
        http.post(format!("http://{proxy}{p}"))
            .bearer_auth(KEY)
            .json(&b)
            .send()
    };

    // a used model without a price
    let mut r = ciallopa_usage::Record::new(
        rec.ids.next_at(ciallopa_usage::now_ms() - 1000),
        ciallopa_usage::Transport::Sse,
        "responses",
    );
    r.model = Some("gpt-x".into());
    r.status = 200;
    r.input_tokens = Some(1_000_000);
    r.output_tokens = Some(0);
    assert!(rec.record(r));
    rec.flush_blocking().unwrap();

    let st: Value = get("/admin/v1/prices").await.unwrap().json().await.unwrap();
    assert_eq!(st["models"], 0);
    assert_eq!(st["unpriced"], json!(["gpt-x"]));
    let s: Value = get("/usage/v1/summary")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["summary"]["price_models"], 0);
    assert!(
        s["summary"]["total"]["cost_usd"].is_null(),
        "unpriced is null, never 0"
    );
    assert_eq!(s["summary"]["total"]["unknown_price"], 1);
    assert_eq!(s["summary"]["unknown_price_models"], json!(["gpt-x"]));

    // check: diff only, nothing written
    let c: Value = post("/admin/v1/prices/check", json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (c["added"].as_u64(), c["updated"].as_u64()),
        (Some(2), Some(0)),
        "{c}"
    );
    assert!(!dir.path().join("prices.toml").exists());
    let r = post("/admin/v1/prices/apply", json!({"token": "nope"}))
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    // the failed apply consumed the check: check again, then apply
    let c: Value = post("/admin/v1/prices/check", json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let a: Value = post("/admin/v1/prices/apply", json!({"token": c["token"]}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(a["applied"], 2, "{a}");
    assert!(a["backup"].is_null(), "fresh table: nothing to back up");
    let table = std::fs::read_to_string(dir.path().join("prices.toml")).unwrap();
    assert!(table.contains("gpt-x") && !table.contains("gpt-image"));
    let st: Value = get("/admin/v1/prices").await.unwrap().json().await.unwrap();
    assert_eq!(
        (st["models"].as_u64(), st["unpriced"].clone()),
        (Some(2), json!([]))
    );
    let s: Value = get("/usage/v1/summary")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        (s["summary"]["total"]["cost_usd"].as_f64().unwrap() - 2.0).abs() < 1e-9,
        "{s}"
    );

    // unchanged ⇒ no token, no write
    let m1 = std::fs::metadata(dir.path().join("prices.toml"))
        .unwrap()
        .modified()
        .unwrap();
    let c: Value = post("/admin/v1/prices/check", json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(c["token"].is_null(), "{c}");
    assert_eq!(
        std::fs::metadata(dir.path().join("prices.toml"))
            .unwrap()
            .modified()
            .unwrap(),
        m1
    );

    // price change, but the file is edited between check and apply ⇒ rejected
    ds.0.lock().unwrap().as_mut().unwrap()["gpt-x"] = model(3.0, 10.0);
    let c: Value = post("/admin/v1/prices/check", json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(c["updated"], 1);
    assert_eq!(c["changes"][0]["kind"], "update");
    std::fs::write(
        dir.path().join("prices.toml"),
        format!("{table}\n# edited\n"),
    )
    .unwrap();
    let r = post("/admin/v1/prices/apply", json!({"token": c["token"]}))
        .await
        .unwrap();
    assert_eq!(r.status(), 409);

    // dataset failure: 502, table untouched
    *ds.0.lock().unwrap() = None;
    let before = std::fs::read_to_string(dir.path().join("prices.toml")).unwrap();
    let r = post("/admin/v1/prices/check", json!({})).await.unwrap();
    assert_eq!(r.status(), 502);
    let body: Value = r.json().await.unwrap();
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("HTTP 500"));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("prices.toml")).unwrap(),
        before
    );

    // prices_source is hot: the next check uses the new dataset; apply keeps a .bak
    *ds2.0.lock().unwrap() = Some(json!({"gpt-x": model(4.0, 10.0), "gpt-y": model(1.0, 4.0)}));
    std::fs::write(
        &path,
        cfg_text(dir.path(), &format!("http://{data}/other.json")),
    )
    .unwrap();
    assert!(settings.reload("test", None).await.is_ok());
    let c: Value = post("/admin/v1/prices/check", json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(c["source"], format!("http://{data}/other.json"));
    assert_eq!(c["updated"], 1);
    let a: Value = post("/admin/v1/prices/apply", json!({"token": c["token"]}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(a["applied"], 1);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("prices.toml.bak")).unwrap(),
        before
    );
    let s: Value = get("/usage/v1/summary")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        (s["summary"]["total"]["cost_usd"].as_f64().unwrap() - 4.0).abs() < 1e-9,
        "{s}"
    );
    rec.shutdown();
}

/// Management page per-model edits: ignore (no unpriced hint, zero cost), set a custom price,
/// unset, un-ignore; validation; a one-time backup of a hand-written file; a later sync keeps
/// both ignored and custom models.
#[tokio::test]
async fn model_ignore_custom_price_and_sync_keeps_them() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("auths")).unwrap();
    let ds = Dataset::default();
    let data = serve(
        Router::new()
            .route("/litellm.json", get(dataset))
            .with_state(ds.clone()),
    )
    .await;
    *ds.0.lock().unwrap() = Some(json!({"gpt-x": model(2.0, 10.0), "gpt-local": model(9.0, 90.0)}));
    let ptoml = dir.path().join("prices.toml");
    std::fs::write(&ptoml, "# hand-written table\n").unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        cfg_text(dir.path(), &format!("http://{data}/litellm.json")),
    )
    .unwrap();
    let cfg = Config::load(&path).unwrap();
    let rec = ciallopa_usage::Recorder::open(&cfg.usage).unwrap();
    let state = AppState {
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open(dir.path().join("auths"))
                .await
                .unwrap(),
        ),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: Some(rec.clone()),
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let settings = Settings::new(state, Some(path.clone()), None);
    let proxy = serve(ciallopa::build_app_with(settings.clone())).await;
    let http = reqwest::Client::new();
    let get = |p: &str| {
        http.get(format!("http://{proxy}{p}"))
            .bearer_auth(KEY)
            .send()
    };
    let post = |p: &str, b: Value| {
        http.post(format!("http://{proxy}{p}"))
            .bearer_auth(KEY)
            .json(&b)
            .send()
    };
    for m in ["auto", "gpt-local"] {
        let mut r = ciallopa_usage::Record::new(
            rec.ids.next_at(ciallopa_usage::now_ms() - 1000),
            ciallopa_usage::Transport::Sse,
            "responses",
        );
        r.model = Some(m.into());
        r.status = 200;
        r.input_tokens = Some(1_000_000);
        r.output_tokens = Some(0);
        assert!(rec.record(r));
    }
    rec.flush_blocking().unwrap();
    let st: Value = get("/admin/v1/prices").await.unwrap().json().await.unwrap();
    assert_eq!(st["unpriced"], json!(["auto", "gpt-local"]));

    // validation
    for b in [
        json!({"model":"auto","action":"nuke"}),
        json!({"model":"","action":"ignore"}),
        json!({"model":"a\u{7}b","action":"ignore"}),
        json!({"model":"x","action":"set","in":-1.0,"out":1.0}),
        json!({"model":"x","action":"set","in":1.0}),
        json!({"model":"x","action":"ignore","extra":1}),
    ] {
        let r = post("/admin/v1/prices/models", b.clone()).await.unwrap();
        assert_eq!(r.status(), 400, "{b}");
    }
    assert_eq!(
        std::fs::read_to_string(&ptoml).unwrap(),
        "# hand-written table\n",
        "rejected edits write nothing"
    );

    // ignore auto, price gpt-local
    let r: Value = post(
        "/admin/v1/prices/models",
        json!({"model":"auto","action":"ignore"}),
    )
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(
        (r["changed"].as_bool(), r["backup"].is_string()),
        (Some(true), true),
        "{r}"
    );
    assert!(dir.path().join("prices.toml.bak").exists());
    let r: Value = post(
        "/admin/v1/prices/models",
        json!({"model":"auto","action":"ignore"}),
    )
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(r["changed"], false, "idempotent");
    std::fs::remove_file(dir.path().join("prices.toml.bak")).unwrap();
    let r = post(
        "/admin/v1/prices/models",
        json!({"model":"gpt-local","action":"set","in":1.0,"out":2.0}),
    )
    .await
    .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        !dir.path().join("prices.toml.bak").exists(),
        "only the first edit of a hand-written file is backed up"
    );
    let st: Value = get("/admin/v1/prices").await.unwrap().json().await.unwrap();
    assert_eq!(st["unpriced"], json!([]));
    assert_eq!(st["ignored"], json!(["auto"]));
    assert_eq!(st["custom"][0]["model"], "gpt-local");
    let s: Value = get("/usage/v1/summary")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["summary"]["total"]["unknown_price"], 0, "{s}");
    assert!((s["summary"]["total"]["cost_usd"].as_f64().unwrap() - 1.0).abs() < 1e-9);

    // a sync adds gpt-x but leaves the custom gpt-local price and the ignored auto alone
    let c: Value = post("/admin/v1/prices/check", json!({}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let kinds: Vec<String> = c["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            format!(
                "{}:{}",
                x["kind"].as_str().unwrap(),
                x["model"].as_str().unwrap()
            )
        })
        .collect();
    assert!(
        kinds.contains(&"skip_custom:gpt-local".to_string()),
        "{kinds:?}"
    );
    let a = post("/admin/v1/prices/apply", json!({"token": c["token"]}))
        .await
        .unwrap();
    assert_eq!(a.status(), 200);
    let st: Value = get("/admin/v1/prices").await.unwrap().json().await.unwrap();
    assert_eq!(st["ignored"], json!(["auto"]));
    assert_eq!(st["custom"][0]["in"], 1.0);

    // unset + unignore bring the hint back
    for b in [
        json!({"model":"gpt-local","action":"unset"}),
        json!({"model":"AUTO","action":"unignore"}),
    ] {
        assert_eq!(
            post("/admin/v1/prices/models", b).await.unwrap().status(),
            200
        );
    }
    let st: Value = get("/admin/v1/prices").await.unwrap().json().await.unwrap();
    // gpt-local was skipped by the sync while custom, so it is unpriced again until the next check
    assert_eq!(st["unpriced"], json!(["auto", "gpt-local"]), "{st}");
    assert_eq!(st["ignored"], json!([]));
}
