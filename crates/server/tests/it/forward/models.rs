use super::scaffold::*;
use super::*;

#[tokio::test]
async fn models_cached_and_wham_passthrough() {
    let e = env(300).await;
    for _ in 0..2 {
        let r = e
            .http
            .get(format!(
                "http://{}/backend-api/codex/models?client_version=1",
                e.proxy
            ))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(r.text().await.unwrap(), MODELS_BODY);
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    let r = e
        .http
        .get(format!("http://{}/api/codex/usage", e.proxy))
        .header("x-api-key", KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bounded_models_body_is_not_cached_when_incomplete() {
    let e = env(30).await;
    for _ in 0..2 {
        let response = e
            .http
            .get(format!("http://{}/v1/models", e.proxy))
            .bearer_auth(KEY)
            .header("x-test-mode", "models_too_large")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 502);
        assert!(response.bytes().await.unwrap().len() < 1024);
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

/// Item 1: the models catalog is passed through byte-for-byte (never rewritten, so ciallopa
/// cannot introduce `service_tiers: null`) on both client paths.
#[tokio::test]
async fn models_catalog_passthrough_no_null_service_tiers() {
    let e = env(300).await;
    for p in [
        "/v1/models",
        "/backend-api/codex/models?client_version=0.167.1",
    ] {
        let r = e
            .http
            .get(format!("http://{}{p}", e.proxy))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        let t = r.text().await.unwrap();
        assert_eq!(t, MODELS_BODY, "{p}");
        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
        for m in v["models"].as_array().unwrap() {
            assert!(m["service_tiers"].is_array(), "{m}");
        }
        assert!(!t.contains("\"service_tiers\":null"));
    }
}
