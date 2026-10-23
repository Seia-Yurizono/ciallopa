#![allow(clippy::unwrap_used)]
use axum::body::Body;
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

async fn app() -> axum::Router {
    let cfg = Config::parse(
        "api_keys = [\"sk-cpa-0123456789abcdef\"]\nauth_dir = \"/nonexistent-ciallopa\"",
    )
    .unwrap();
    let state = AppState {
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open(cfg.auth_dir.clone())
                .await
                .unwrap(),
        ),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: None,
        config: Arc::new(cfg.into()),
    };
    ciallopa::build_app(state)
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
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        status(
            Request::post("/v1/images/generations")
                .header("x-api-key", "sk-cpa-0123456789abcdef")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::BAD_REQUEST
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
    assert!(ciallopa::key_matches(&k, "sk-cpa-zzzzzzzzzzzzzzzz"));
    assert!(!ciallopa::key_matches(&k, "sk-cpa-0123456789abcde"));
    assert!(!ciallopa::key_matches(&[], ""));
}

#[tokio::test]
async fn healthcheck_probe() {
    use std::time::Duration;
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = ln.local_addr().unwrap().port();
    let app = app().await;
    let srv = tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    let t = Duration::from_secs(3);
    // An unspecified listen address is probed on loopback.
    assert!(ciallopa::healthcheck(([0, 0, 0, 0], port).into(), t).await);
    assert!(ciallopa::healthcheck(([127, 0, 0, 1], port).into(), t).await);
    srv.abort();
    let _ = srv.await;
    assert!(!ciallopa::healthcheck(([127, 0, 0, 1], port).into(), t).await);
    // Something that accepts but never answers: bounded by the timeout.
    let mute = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mport = mute.local_addr().unwrap().port();
    let started = std::time::Instant::now();
    assert!(
        !ciallopa::healthcheck(([127, 0, 0, 1], mport).into(), Duration::from_millis(300)).await
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(mute);
}
