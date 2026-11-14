use super::scaffold::*;
use super::*;

#[tokio::test]
async fn models_cached_and_wham_passthrough() {
    let e = env(300).await;
    for path in [
        "/v1/models?client_version=1",
        "/backend-api/codex/models?client_version=1",
        "/v1/models?client_version=1",
    ] {
        let r = e
            .http
            .get(format!("http://{}{path}", e.proxy))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.text().await.unwrap(), MODELS_BODY, "{path}");
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
