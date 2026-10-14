//! Live settings: reload pipeline (hot apply, pending restart, rejected reload unchanged,
//! live WebSocket kept), admin config API (ETag / If-Match, write switch, comment-preserving
//! write-back, key management, audit) and account management (removal cleanup).
#![allow(clippy::unwrap_used)]
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{response::Response, routing::get, Router};
use ciallopa::settings::Settings;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const KEY: &str = "sk-cpa-0123456789abcdef";

async fn up_ws(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(|mut s| async move {
        while let Some(Ok(Message::Text(t))) = s.recv().await {
            let _ = s
                .send(Message::Text(
                    format!(r#"{{"type":"response.completed","echo":{t}}}"#).into(),
                ))
                .await;
        }
    })
}

async fn serve(app: Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    addr.to_string()
}

struct Env {
    proxy: String,
    path: PathBuf,
    up: String,
    auth: PathBuf,
    upstream: Arc<ciallopa_upstream::UpstreamClient>,
    router: Arc<ciallopa_routing::CpaRouter>,
    settings: Arc<Settings>,
    _dir: tempfile::TempDir,
    http: reqwest::Client,
}

fn base(up: &str, auth: &std::path::Path, extra: &str) -> String {
    format!(
        "# ciallopa test config\napi_keys = [{{label = \"admin\", key = \"{KEY}\", admin = true}}]   # client keys\nupstream = \"http://{up}/backend-api\"\nauth_dir = \"{}\"\n\n[routing]\n# how accounts are picked\nstrategy = \"round-robin\"\n\n[usage]\nenabled = false\n{extra}",
        auth.display()
    )
}

async fn env(extra: &str) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let auth = dir.path().join("auths");
    std::fs::create_dir_all(&auth).unwrap();
    for id in ["a", "b"] {
        std::fs::write(
            auth.join(format!("{id}.json")),
            json!({"type":"codex","email":format!("{id}@x"),"access_token":format!("tok-{id}"),"account_id":format!("acc-{id}")}).to_string(),
        )
        .unwrap();
    }
    let up = serve(Router::new().route("/backend-api/codex/responses", get(up_ws))).await;
    let path = dir.path().join("config.toml");
    std::fs::write(&path, base(&up, &auth, extra)).unwrap();
    let cfg = Config::load(&path).unwrap();
    let upstream = Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap());
    let router = Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone()));
    let state = AppState {
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open(auth.clone())
                .await
                .unwrap(),
        ),
        router: router.clone(),
        upstream: upstream.clone(),
        usage: None,
        config: Arc::new(cfg.into()),
    };
    let settings = Settings::new(state, Some(path.clone()), None);
    settings.init_known_accounts().await;
    let proxy = serve(ciallopa::build_app_with(settings.clone())).await;
    Env {
        proxy,
        path,
        up,
        auth,
        upstream,
        router,
        settings,
        _dir: dir,
        http: reqwest::Client::new(),
    }
}

impl Env {
    fn url(&self, p: &str) -> String {
        format!("http://{}{p}", self.proxy)
    }
    fn req(&self, m: reqwest::Method, p: &str) -> reqwest::RequestBuilder {
        self.http.request(m, self.url(p)).bearer_auth(KEY)
    }
    fn write(&self, extra: &str) {
        std::fs::write(&self.path, base(&self.up, &self.auth, extra)).unwrap();
    }
    async fn reload(&self) -> reqwest::Response {
        self.req(reqwest::Method::POST, "/admin/v1/config/reload")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
    }
    async fn config(&self) -> (Value, String) {
        let r = self
            .req(reqwest::Method::GET, "/admin/v1/config")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let etag = r.headers()["etag"].to_str().unwrap().to_string();
        (r.json().await.unwrap(), etag)
    }
}

#[tokio::test]
async fn reload_applies_hot_keeps_live_ws_and_records_pending() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env("").await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","n":1}"#))
        .await
        .unwrap();
    assert!(ws
        .next()
        .await
        .unwrap()
        .unwrap()
        .to_text()
        .unwrap()
        .contains("echo"));
    e.upstream.pools.get("a").unwrap();
    assert_eq!(e.upstream.pools.len(), 1);

    // Hot: strategy, connect timeout (client rebuild), cookie jar, new key. Restart: listen.
    let k2 = "sk-cpa-second-key-0123456789";
    std::fs::write(
        &e.path,
        base(&e.up, &e.auth, "").replace(
            &format!("api_keys = [{{label = \"admin\", key = \"{KEY}\", admin = true}}]"),
            &format!("api_keys = [{{label = \"admin\", key = \"{KEY}\", admin = true}}, {{ label = \"two\", key = \"{k2}\" }}]\nlisten = \"127.0.0.1:1\""),
        ).replace("strategy = \"round-robin\"", "strategy = \"fill-first\"")
            + "[upstream_opts]\nconnect_timeout_secs = 5\ncloudflare_cookies = true\n",
    )
    .unwrap();
    let r = e.reload().await;
    assert_eq!(r.status(), 200);
    let j: Value = r.json().await.unwrap();
    let applied: Vec<&str> = j["applied"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["key"].as_str().unwrap())
        .collect();
    assert!(applied.contains(&"routing.strategy"), "{j}");
    assert!(applied.contains(&"upstream_opts.connect_timeout_secs"));
    assert!(!applied.contains(&"listen"));
    assert_eq!(j["pending_restart"][0]["key"], "listen");
    assert!(!j.to_string().contains(k2), "secret in response: {j}");
    assert_eq!(
        e.upstream.pools.len(),
        0,
        "timeout change drops clients (rebuilt lazily)"
    );
    assert!(e.upstream.pools.jar("a").is_some(), "jar preserved");
    let (c, _) = e.config().await;
    assert_eq!(c["running"]["routing.strategy"], "fill-first");
    assert_eq!(c["running"]["listen"], "127.0.0.1:8317");
    // the live WebSocket survives the reload
    ws.send(M::text(r#"{"type":"response.create","n":2}"#))
        .await
        .unwrap();
    assert!(ws
        .next()
        .await
        .unwrap()
        .unwrap()
        .to_text()
        .unwrap()
        .contains("\"n\":2"));
    // new key works immediately for client routes; /admin needs admin = true
    let r = e
        .http
        .get(e.url("/admin/v1/config"))
        .bearer_auth(k2)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = e
        .http
        .get(e.url("/usage/v1/client-fingerprint"))
        .bearer_auth(k2)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn rejected_reload_leaves_running_config_unchanged() {
    let e = env("").await;
    let (before, v1) = e.config().await;
    e.write("[ui]\nallow_config_write = true\n[log]\nlevel = \"info\"\n[upstream_opts]\nmax_attempts = 0\n");
    let r = e.reload().await;
    assert_eq!(r.status(), 422);
    let (after, v2) = e.config().await;
    assert_eq!(before["running"], after["running"]);
    assert_ne!(v1, v2, "version follows the file");
    assert_eq!(after["last_reload"]["ok"], false);
    assert!(after["file_error"]
        .as_str()
        .unwrap()
        .contains("max_attempts"));
    // broken TOML too
    std::fs::write(&e.path, "listen = ").unwrap();
    assert_eq!(e.reload().await.status(), 422);
    assert!(!e.settings.st_config_write_enabled());
}

#[tokio::test]
async fn write_api_switch_etag_comments_keys_and_audit() {
    let e = env("").await;
    let (_, etag) = e.config().await;
    let put = |etag: &str| {
        e.req(reqwest::Method::PUT, "/admin/v1/config")
            .header("if-match", etag)
            .json(&json!({"changes": {"routing.strategy": "fill-first"}}))
    };
    // disabled by default
    assert_eq!(put(&etag).send().await.unwrap().status(), 403);
    e.write("\n[ui]\nallow_config_write = true\n");
    assert_eq!(e.reload().await.status(), 200);
    let (c, etag) = e.config().await;
    assert_eq!(c["write_enabled"], true);
    assert!(!c.to_string().contains("0123456789abcdef"), "key leaked");
    // stale version → 412, missing → 428
    assert_eq!(put("\"stale\"").send().await.unwrap().status(), 412);
    let r = e
        .req(reqwest::Method::PUT, "/admin/v1/config")
        .json(&json!({"changes": {"routing.strategy": "fill-first"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 428);
    // CSRF: no JSON body
    let r = e
        .req(reqwest::Method::PUT, "/admin/v1/config")
        .header("if-match", &etag)
        .body("changes=1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // dry run
    let v: Value = e
        .req(reqwest::Method::POST, "/admin/v1/config/validate")
        .json(&json!({"changes": {"routing.strategy": "fill-first", "listen": "0.0.0.0:1"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["errors"][0]["key"], "listen");
    let v: Value = e
        .req(reqwest::Method::POST, "/admin/v1/config/validate")
        .json(&json!({"changes": {"routing.strategy": "fill-first", "max_body_mb": 64}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["diff"].as_array().unwrap().len(), 2);
    let before = std::fs::read_to_string(&e.path).unwrap();
    assert!(!before.contains("fill-first"), "validate never writes");
    // apply
    let r = e
        .req(reqwest::Method::PUT, "/admin/v1/config")
        .header("if-match", &etag)
        .json(&json!({"changes": {"routing.strategy": "fill-first", "max_body_mb": 64}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let j: Value = r.json().await.unwrap();
    assert_eq!(j["applied"][0]["key"], "routing.strategy");
    assert_eq!(j["pending_restart"][0]["key"], "max_body_mb");
    let text = std::fs::read_to_string(&e.path).unwrap();
    assert!(text.contains("# ciallopa test config") && text.contains("# how accounts are picked"));
    assert!(text.contains("# client keys"));
    assert!(text.contains("strategy = \"fill-first\""));
    assert_eq!(
        std::fs::read_to_string(e.path.with_file_name("config.toml.bak")).unwrap(),
        before
    );
    assert_eq!(
        e.settings.running().routing.strategy,
        config::Strategy::FillFirst
    );
    // the old version is now stale
    assert_eq!(put(&etag).send().await.unwrap().status(), 412);
    // non-editable switches stay file-only
    let (_, etag) = e.config().await;
    let r = e
        .req(reqwest::Method::PUT, "/admin/v1/config")
        .header("if-match", &etag)
        .json(&json!({"changes": {"ui.allow_reset_consume": true}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);

    // keys: generated server-side, shown once, usable at once; delete
    let r = e
        .req(reqwest::Method::POST, "/admin/v1/config/keys")
        .header("if-match", &etag)
        .json(&json!({"label": "phone"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let j: Value = r.json().await.unwrap();
    let k = j["key"].as_str().unwrap().to_string();
    assert!(k.starts_with("sk-cpa-") && k.len() > 40);
    let (c, etag) = e.config().await;
    assert!(!c.to_string().contains(&k));
    let ok = e
        .http
        .get(e.url("/admin/v1/config"))
        .bearer_auth(&k)
        .send()
        .await
        .unwrap();
    // usable at once, but page-generated keys are never admin keys
    assert_eq!(ok.status(), 403);
    let r = e
        .req(reqwest::Method::DELETE, "/admin/v1/config/keys/phone")
        .header("if-match", &etag)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let gone = e
        .http
        .get(e.url("/admin/v1/config"))
        .bearer_auth(&k)
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 401);
    // audit (no secrets)
    let a: Value = e
        .req(reqwest::Method::GET, "/admin/v1/config/audit")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let s = a.to_string();
    assert!(s.contains("routing.strategy") && s.contains("key-add"));
    assert!(!s.contains(&k) && !s.contains("0123456789abcdef"));
}

#[tokio::test]
async fn account_management_and_removal_cleanup() {
    let e = env("").await;
    let patch = json!({"weight": 5, "priority": 2});
    let r = e
        .req(reqwest::Method::PATCH, "/admin/v1/accounts/b")
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "writes need allow_config_write");
    e.write("\n[ui]\nallow_config_write = true\n");
    assert_eq!(e.reload().await.status(), 200);
    let r = e
        .req(reqwest::Method::PATCH, "/admin/v1/accounts/b")
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let l: Value = e
        .req(reqwest::Method::GET, "/admin/v1/accounts")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b = l["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "b")
        .unwrap()
        .clone();
    assert_eq!(
        (b["weight"].as_i64(), b["priority"].as_i64()),
        (Some(5), Some(2))
    );
    assert_eq!(b["status"], "normal");
    assert!(!l.to_string().contains("tok-"), "tokens never listed");
    let disk: Value =
        serde_json::from_slice(&std::fs::read(e.auth.join("b.json")).unwrap()).unwrap();
    assert_eq!(disk["weight"], 5);
    assert_eq!(disk["access_token"], "tok-b", "tokens untouched");

    // runtime state of a
    e.upstream.pools.get("a").unwrap();
    e.upstream.pools.get("b").unwrap();
    ciallopa_core::Router::report(
        &*e.router,
        &RouteRequest {
            model: "m".into(),
            ..Default::default()
        },
        &"a".to_string(),
        &Outcome::Transient { status: Some(503) },
    );
    assert!(e.router.cooling_until_any(&"a".to_string()).is_some());
    let r = e
        .req(reqwest::Method::DELETE, "/admin/v1/accounts/a")
        .json(&json!({"confirm": "b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "confirm must match");
    let r = e
        .req(reqwest::Method::DELETE, "/admin/v1/accounts/a")
        .json(&json!({"confirm": "a"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(!e.auth.join("a.json").exists());
    assert_eq!(e.upstream.pools.len(), 1, "pool of a dropped, b kept");
    assert!(e.router.cooling_until_any(&"a".to_string()).is_none());
    let l: Value = e
        .req(reqwest::Method::GET, "/admin/v1/accounts")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(l["accounts"].as_array().unwrap().len(), 1);
    // import an auth.json document
    let r = e
        .req(reqwest::Method::POST, "/admin/v1/accounts/import")
        .json(&json!({"auth_json": {"tokens": {"access_token": "tok-c", "account_id": "acc-c"}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let l: Value = e
        .req(reqwest::Method::GET, "/admin/v1/accounts")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(l["accounts"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn file_watch_applies_and_external_auth_removal_cleans_up() {
    let e = env("").await;
    let _w = ciallopa::settings::spawn_watcher(e.settings.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    e.write("[upstream_opts]\nmax_attempts = 2\n");
    e.upstream.pools.get("a").unwrap();
    std::fs::remove_file(e.auth.join("a.json")).unwrap();
    let mut ok = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if e.settings.running().upstream_opts.max_attempts == 2 && e.upstream.pools.is_empty() {
            ok = true;
            break;
        }
    }
    assert!(ok, "watcher did not apply the change");
}

#[tokio::test]
async fn admin_api_needs_admin_key() {
    // Security #1: a non-admin key may forward but never manage.
    let e = env("\n[ui]\nallow_config_write = true\n").await;
    let (_, etag) = e.config().await;
    let r = e
        .req(reqwest::Method::POST, "/admin/v1/config/keys")
        .header("if-match", &etag)
        .json(&json!({"label": "phone"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let phone = r.json::<Value>().await.unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string();
    let (c, etag) = e.config().await;
    let keys = c.to_string();
    assert!(
        keys.contains("\"admin\":true") && keys.contains("\"admin\":false"),
        "{keys}"
    );
    for (m, p) in [
        (reqwest::Method::GET, "/admin/v1/config"),
        (reqwest::Method::POST, "/admin/v1/config/reload"),
        (reqwest::Method::DELETE, "/admin/v1/config/keys/admin"),
        (reqwest::Method::GET, "/admin/v1/accounts"),
        (reqwest::Method::PATCH, "/admin/v1/accounts/a"),
        (
            reqwest::Method::POST,
            "/admin/v1/accounts/a/reset-credits/consume",
        ),
    ] {
        let r = e
            .http
            .request(m.clone(), e.url(p))
            .bearer_auth(&phone)
            .header("if-match", &etag)
            .json(&json!({"confirm": "a", "disabled": true}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 403, "{m} {p}");
    }
    // still a valid client key for read APIs
    let r = e
        .http
        .get(e.url("/usage/v1/client-fingerprint"))
        .bearer_auth(&phone)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = e
        .http
        .get(e.url("/usage/v1/reset-credits"))
        .bearer_auth(&phone)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["can_consume"], false);
    assert_eq!(v["is_admin"], false);
    // admin key still works
    assert!(e.config().await.0.is_object());
}

#[tokio::test]
async fn no_admin_key_means_no_management() {
    // When every key lacks admin = true, /admin is forbidden even with a valid key.
    let dir = tempfile::tempdir().unwrap();
    let auth = dir.path().join("auths");
    std::fs::create_dir_all(&auth).unwrap();
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nauth_dir = \"{}\"\n[usage]\nenabled = false\n",
        auth.display()
    ))
    .unwrap();
    let accounts = Arc::new(ciallopa_auth::FileAccountStore::open(auth).await.unwrap());
    let state = AppState {
        accounts,
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: None,
        config: Arc::new(cfg.into()),
    };
    let app = ciallopa::build_app(state);
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    let r = reqwest::Client::new()
        .get(format!("http://{addr}/admin/v1/config"))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
}

#[tokio::test]
async fn account_id_path_traversal_rejected_before_lock() {
    // Security #2: no lock file may appear outside auth_dir.
    let e = env("\n[ui]\nallow_config_write = true\n").await;
    let r = e
        .req(reqwest::Method::PATCH, "/admin/v1/accounts/%2F..%2Fpwn")
        .json(&json!({"weight": 2}))
        .send()
        .await
        .unwrap();
    assert!(r.status() == 400 || r.status() == 404, "{}", r.status());
    let parent = e.auth.parent().unwrap();
    let leaked: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .flatten()
        .filter(|d| d.file_name().to_string_lossy().contains("pwn"))
        .collect();
    assert!(leaked.is_empty(), "{leaked:?}");
}
