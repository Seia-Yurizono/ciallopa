#![allow(clippy::unwrap_used)]
use axum::body::Body;
use cpa_lite_core::Config;
use cpa_lite_forward::AppState;
use http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

async fn app() -> axum::Router {
    let cfg = Config::parse(
        "api_keys = [\"sk-cpa-0123456789abcdef\"]\nauth_dir = \"/nonexistent-cpa-lite\"",
    )
    .unwrap();
    let state = AppState {
        accounts: Arc::new(
            cpa_lite_auth::FileAccountStore::open(cfg.auth_dir.clone())
                .await
                .unwrap(),
        ),
        router: Arc::new(cpa_lite_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(cpa_lite_upstream::UpstreamClient::new(&cfg).unwrap()),
        config: Arc::new(cfg),
    };
    cpa_lite::build_app(state)
}

async fn status(req: Request<Body>) -> StatusCode {
    app().await.oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn health_and_auth() {
    let get = |p: &str| Request::get(p);
    assert_eq!(
        status(get("/healthz").body(Body::empty()).unwrap()).await,
        StatusCode::OK
    );
    assert_eq!(
        status(
            get("/backend-api/codex/models")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(
            get("/backend-api/codex/models")
                .header("authorization", "Bearer wrong-key-xxxxxxxx")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(
            get("/backend-api/codex/models")
                .header("authorization", "Bearer sk-cpa-0123456789abcdef")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(
        status(
            Request::post("/v1/images/generations")
                .header("x-api-key", "sk-cpa-0123456789abcdef")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(
        status(get("/other").body(Body::empty()).unwrap()).await,
        StatusCode::NOT_FOUND
    );
}

#[test]
fn key_compare() {
    let k = vec![
        "sk-cpa-0123456789abcdef".to_string(),
        "sk-cpa-zzzzzzzzzzzzzzzz".into(),
    ];
    assert!(cpa_lite::key_matches(&k, "sk-cpa-zzzzzzzzzzzzzzzz"));
    assert!(!cpa_lite::key_matches(&k, "sk-cpa-0123456789abcde"));
    assert!(!cpa_lite::key_matches(&[], ""));
}
