#![allow(clippy::unwrap_used)]
use axum::body::Body;
use ciallopa_core::Config;
use ciallopa_forward::AppState;
use http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

async fn app(enabled: bool) -> axum::Router {
    let cfg = Config::parse(&format!("api_keys = [\"sk-cpa-0123456789abcdef\", \"sk-cpa-zzzzzzzzzzzzzzzz\"]\n[ui]\nenabled = {enabled}")).unwrap();
    let state = AppState {
        accounts: Arc::new(super::support::FixedStore::new(&[])),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: None,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    ciallopa::build_app(state)
}

#[tokio::test]
async fn health_and_auth() {
    let app = app(true).await;
    for (path, key, status) in [
        ("/healthz", None, StatusCode::OK),
        ("/backend-api/codex/models", None, StatusCode::UNAUTHORIZED),
        (
            "/backend-api/codex/models",
            Some(""),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/backend-api/codex/models",
            Some("wrong-key-xxxxxxxx"),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/backend-api/codex/models",
            Some("sk-cpa-0123456789abcde"),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/backend-api/codex/models",
            Some("sk-cpa-0123456789abcdef"),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            "/backend-api/codex/models",
            Some("sk-cpa-zzzzzzzzzzzzzzzz"),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ("/other", None, StatusCode::NOT_FOUND),
    ] {
        let mut req = Request::get(path);
        if let Some(key) = key {
            req = req.header("authorization", format!("Bearer {key}"));
        }
        assert_eq!(
            app.clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            status,
            "{path}"
        );
    }
    let response = app
        .oneshot(
            Request::post("/v1/images/generations")
                .header("x-api-key", "sk-cpa-0123456789abcdef")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn healthcheck_probe() {
    use std::time::Duration;
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = ln.local_addr().unwrap().port();
    let app = app(true).await;
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

#[tokio::test]
async fn ui_pages_and_toggle() {
    use base64::Engine;
    use sha2::Digest;
    let app = app(true).await;
    for path in ["/ui", "/ui/settings", "/ui/accounts"] {
        let r = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "{path}");
        assert_eq!(r.headers()["x-content-type-options"], "nosniff");
        assert_eq!(r.headers()["cache-control"], "no-store, no-transform");
        let csp = r.headers()["content-security-policy"]
            .to_str()
            .unwrap()
            .to_string();
        assert!(csp.contains("default-src 'none'") && !csp.contains("unsafe"));
        let body = String::from_utf8(
            axum::body::to_bytes(r.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(!body.contains("sk-cpa-") && !body.contains('\u{FFFD}'));
        for tag in ["script", "style"] {
            let open = format!("<{tag}>");
            let close = format!("</{tag}>");
            let content = body
                .split_once(&open)
                .unwrap()
                .1
                .split_once(&close)
                .unwrap()
                .0;
            let hash = base64::engine::general_purpose::STANDARD
                .encode(sha2::Sha256::digest(content.as_bytes()));
            assert!(
                csp.contains(&format!("'sha256-{hash}'")),
                "{path}: {tag} blocked by CSP"
            );
        }
    }
    let r = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(r.headers()["location"], "/ui");
    let off = self::app(false).await;
    for path in ["/ui", "/ui/settings", "/ui/accounts"] {
        assert_eq!(
            off.clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
}
