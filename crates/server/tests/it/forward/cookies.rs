use super::scaffold::*;
use super::*;

/// Each account has its own connection pool: sequential requests reuse the account's own
/// keep-alive connection, but two accounts never share one.
#[tokio::test]
async fn accounts_use_separate_connections() {
    let e = env(300).await;
    for _ in 0..3 {
        let r = e
            .post("/backend-api/codex/responses")
            .header("x-test-mode", "limit")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        r.bytes().await.unwrap();
    }
    let peers = e.mock.peers.lock().unwrap().clone();
    let ports = |tok: &str| {
        let mut v: Vec<u16> = peers
            .iter()
            .filter(|(a, _)| a.contains(tok))
            .map(|(_, p)| *p)
            .collect();
        v.dedup();
        v
    };
    let (pa, pb) = (ports("tok-a"), ports("tok-b"));
    assert_eq!(peers.len(), 6, "{peers:?}");
    assert_eq!(
        pa.len(),
        1,
        "account a reuses its own connection: {peers:?}"
    );
    assert_eq!(
        pb.len(),
        1,
        "account b reuses its own connection: {peers:?}"
    );
    assert_ne!(pa[0], pb[0], "accounts must not share a connection");
}

#[tokio::test]
async fn cloudflare_cookies_disabled_sends_none() {
    let e = env(300).await;
    cookie_round(&e, true).await;
    cookie_round(&e, true).await;
    let seen = cookies_seen(&e);
    assert_eq!(seen.len(), 4);
    assert!(seen.iter().all(|(_, c)| c.is_none()), "{seen:?}");
}

#[tokio::test]
async fn cloudflare_cookies_per_account_allowlist_and_expiry() {
    let e = env_cfg(300, None, "cloudflare_cookies = true").await;
    cookie_round(&e, true).await; // a: 429 + Set-Cookie A, b: Set-Cookie B
    cookie_round(&e, true).await;
    let seen = cookies_seen(&e);
    assert_eq!(seen.len(), 4, "{seen:?}");
    // First round: nothing stored yet, client Cookie never forwarded.
    assert_eq!(seen[0].1, None);
    assert_eq!(seen[1].1, None);
    // Second round: each account replays only its own allowlisted cookies.
    let a = seen[2].1.clone().unwrap();
    assert!(seen[2].0.contains("tok-a"));
    let mut parts: Vec<&str> = a.split("; ").collect();
    parts.sort();
    assert_eq!(parts, vec!["__cf_bm=A", "__oailb=A-lb"]);
    assert!(seen[3].0.contains("tok-b"));
    assert_eq!(seen[3].1.as_deref(), Some("__cf_bm=B"));
    // Expiry: __oailb has Max-Age=1.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    cookie_round(&e, false).await;
    let seen = cookies_seen(&e);
    assert_eq!(seen[4].1.as_deref(), Some("__cf_bm=A"));
    assert_eq!(seen[5].1.as_deref(), Some("__cf_bm=B"));
}

#[tokio::test]
async fn cloudflare_cookies_ws_handshake_shared_with_http() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    for enabled in [false, true] {
        let e = env_cfg(
            300,
            None,
            if enabled {
                "cloudflare_cookies = true"
            } else {
                ""
            },
        )
        .await;
        for _ in 0..2 {
            let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
                .into_client_request()
                .unwrap();
            req.headers_mut()
                .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
            req.headers_mut()
                .insert("cookie", "client=1".parse().unwrap());
            for k in [
                "True-Client-IP",
                "CF-Connecting-IP",
                "X-Forwarded-For",
                "X-Real-IP",
            ] {
                req.headers_mut().insert(k, "203.0.113.7".parse().unwrap());
            }
            let (ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
            assert!(resp.headers().get("set-cookie").is_none());
            drop(ws);
        }
        // WS handshake Set-Cookie (__cf_bm=secret) is replayed on HTTP for the same account.
        let r = e
            .post("/backend-api/codex/responses")
            .body("{}")
            .send()
            .await
            .unwrap();
        r.bytes().await.unwrap();
        let seen = cookies_seen(&e);
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].1, None, "client Cookie never forwarded");
        let want = enabled.then(|| "__cf_bm=secret".to_string());
        assert_eq!(seen[1].1, want, "ws handshake (enabled={enabled})");
        assert_eq!(seen[2].1, want, "http after ws (enabled={enabled})");
        let all = e.mock.seen.lock().unwrap();
        for (h, _) in all.iter() {
            for (k, _) in h.iter() {
                assert!(
                    !matches!(
                        k.as_str(),
                        "true-client-ip" | "cf-connecting-ip" | "x-forwarded-for" | "x-real-ip"
                    ),
                    "{k} forwarded upstream on the ws handshake"
                );
            }
        }
    }
}
