use axum::body::{to_bytes, Body};
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use ciallopa_usage::{now_ms, Record, Recorder, Transport};
use http::{Request, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const ADMIN: &str = "sk-runtime-admin-0123456789";
const READER: &str = "sk-runtime-reader-0123456789";

async fn get(app: &axum::Router, path: &str, key: Option<&str>) -> axum::response::Response {
    let mut req = Request::get(path);
    if let Some(key) = key {
        req = req.header("authorization", format!("Bearer {key}"));
    }
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn json(response: axum::response::Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap()
}

#[tokio::test]
async fn runtime_is_admin_only_and_reports_live_state_without_config_file_reads() {
    for usage_enabled in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::parse(&format!(
            "api_keys = [{{label='admin', key='{ADMIN}', admin=true}}, {{label='reader', key='{READER}'}}]\n\
             proxy_url = 'http://demo:private-password@127.0.0.1:7890'\nmax_inflight_requests = 1\n"
        ))
        .unwrap();
        cfg.auth_dir = dir.path().join("auths");
        cfg.usage.path = dir.path().join("usage.db");
        let rec = usage_enabled.then(|| Recorder::open_as(&cfg.usage, "runtime-test").unwrap());
        if let Some(rec) = &rec {
            let now = now_ms();
            for (age, error, endpoint) in [
                (3_601_000, "proxy_connect", "responses"),
                (4_000, "timeout_connect", "responses"),
                (3_000, "proxy_timeout", ciallopa_usage::PREWARM_ENDPOINT),
                (2_000, "proxy_timeout", "responses"),
                (1_000, "proxy_connect", "responses"),
            ] {
                let mut r = Record::new(rec.ids.next_at(now - age), Transport::Sse, endpoint);
                r.status = 502;
                r.error = Some(error.into());
                r.err_source = Some("network");
                rec.record(r);
            }
            rec.flush_blocking().unwrap();
        }
        let state = AppState {
            accounts: Arc::new(
                ciallopa_auth::FileAccountStore::open(cfg.auth_dir.clone())
                    .await
                    .unwrap(),
            ),
            router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
            upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
            usage: rec,
            config: Arc::new(cfg.into()),
            egress: Default::default(),
        };
        let drain = state.upstream.drain.clone();
        let live_config = state.config.clone();
        let _lease = drain.admit(1).unwrap();
        assert!(drain.admit(1).is_err());
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").unwrap();
        let settings = ciallopa::settings::Settings::new(state, Some(path.clone()), None);
        let app = ciallopa::build_app_with(settings.clone());

        for path in ["/admin/v1/config", "/admin/v1/runtime"] {
            for (key, expected) in [
                (None, StatusCode::UNAUTHORIZED),
                (Some("wrong-key"), StatusCode::UNAUTHORIZED),
                (Some(READER), StatusCode::FORBIDDEN),
            ] {
                assert_eq!(get(&app, path, key).await.status(), expected, "{path}");
            }
        }
        let config = json(get(&app, "/admin/v1/config", Some(ADMIN)).await).await;
        let live = json(get(&app, "/admin/v1/runtime", Some(ADMIN)).await).await;
        assert_eq!(live.as_object().unwrap().len(), 3);
        assert_eq!(live["egress"], config["egress"]);
        assert_eq!(live["runtime"], config["runtime"]);
        assert_eq!(live["proxy_errors_1h"], if usage_enabled { 3 } else { 0 });
        assert_eq!(live["runtime"]["inflight"], 1);
        assert_eq!(live["runtime"]["rejected_by"]["inflight"], 1);
        assert!(!live.to_string().contains("private-password"));

        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert_eq!(
            get(&app, "/admin/v1/config", Some(ADMIN)).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );

        let mut direct = (*settings.running()).clone();
        direct.proxy_enabled = false;
        live_config.store(direct);
        let live = json(get(&app, "/admin/v1/runtime", Some(ADMIN)).await).await;
        assert_eq!(
            live["egress"],
            serde_json::json!({"direct": true, "proxy": ""})
        );
    }
}
