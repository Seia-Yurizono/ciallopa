//! Item 2: Codex background / speculative remote V2 compaction (codex 9e03960, 1a69ad3) sends a
//! compaction `/responses` request concurrently with the foreground turn, carrying the same
//! session headers. With the real router (round-robin + session affinity) both must land on the
//! same account, and concurrency alone must never cool an account down.
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::{body::Body, http::HeaderMap, response::Response, routing::post, Router};
use bytes::Bytes;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const KEY: &str = "sk-cpa-0123456789abcdef";

struct Store;
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        ["a", "b", "c"]
            .iter()
            .map(|i| Account {
                id: i.to_string(),
                email: None,
                chatgpt_account_id: Some(format!("acct-{i}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: false,
            })
            .collect()
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        Ok(Credentials {
            account: id.clone(),
            access_token: format!("tok-{id}"),
            chatgpt_account_id: Some(format!("acct-{id}")),
        })
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

type Hits = Arc<Mutex<Vec<(String, String, bool)>>>;

async fn up(
    axum::extract::State(h): axum::extract::State<Hits>,
    hd: HeaderMap,
    b: Bytes,
) -> Response {
    let tok = hd
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let sid = hd
        .get("session_id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let compact = std::str::from_utf8(&zstd::decode_all(&b[..]).unwrap_or(b.to_vec()))
        .unwrap_or("")
        .contains("compaction_trigger");
    h.lock().unwrap().push((sid, tok, compact));
    // overlap: both requests are in flight at the same time
    tokio::time::sleep(Duration::from_millis(150)).await;
    let body = if compact {
        concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"c\"}}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"gAAA\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"c\"}}\n\n"
        )
    } else {
        concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"m\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"m\"}}\n\n"
        )
    };
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

async fn serve(app: Router) -> std::net::SocketAddr {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = ln.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
    a
}

#[tokio::test]
async fn concurrent_compaction_same_account_no_cooldown() {
    let hits: Hits = Default::default();
    let upa = serve(
        Router::new()
            .route("/backend-api/codex/responses", post(up))
            .with_state(hits.clone()),
    )
    .await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{upa}/backend-api\"\n"
    ))
    .unwrap();
    let router = Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone()));
    let state = AppState {
        accounts: Arc::new(Store),
        router: router.clone(),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: None,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let p = serve(ciallopa::build_app(state)).await;
    let http = reqwest::Client::new();
    let send = |sid: String, compact: bool, path: &'static str| {
        let http = http.clone();
        async move {
            let input = if compact {
                r#"[{"type":"message","role":"user","content":"hi"},{"type":"compaction_trigger"}]"#
            } else {
                r#"[{"type":"message","role":"user","content":"hi"}]"#
            };
            let mut rb = http
                .post(format!("http://{p}{path}"))
                .bearer_auth(KEY)
                .header("session_id", &sid)
                .header("content-type", "application/json")
                .body(format!(
                    r#"{{"model":"gpt-5.5","stream":true,"input":{input}}}"#
                ));
            if compact {
                rb = rb.header("x-codex-beta-features", "remote_compaction_v2");
            }
            let r = rb.send().await.unwrap();
            assert_eq!(r.status(), 200);
            r.text().await.unwrap()
        }
    };
    // several sessions; for each, the very first request is the concurrent pair (worst case:
    // no binding exists yet when both are routed), then another concurrent pair mid-session.
    for s in 0..6 {
        let sid = format!("sess-{s}");
        for round in 0..2 {
            let (m, c) = tokio::join!(
                send(sid.clone(), false, "/v1/responses"),
                send(sid.clone(), true, "/backend-api/codex/responses")
            );
            assert!(m.contains("\"m\""), "round {round}");
            assert!(c.contains("compaction"), "round {round}");
        }
    }
    let h = hits.lock().unwrap().clone();
    assert_eq!(h.len(), 24);
    let mut used = std::collections::HashSet::new();
    for s in 0..6 {
        let sid = format!("sess-{s}");
        let toks: std::collections::HashSet<&str> = h
            .iter()
            .filter(|(x, _, _)| *x == sid)
            .map(|(_, t, _)| t.as_str())
            .collect();
        assert_eq!(toks.len(), 1, "{sid} split across accounts: {toks:?}");
        assert_eq!(h.iter().filter(|(x, _, c)| *x == sid && *c).count(), 2);
        used.extend(toks);
    }
    // round-robin really spreads sessions (affinity, not luck, kept the pairs together)
    assert!(used.len() > 1, "{used:?}");
    for a in ["a", "b", "c"] {
        assert!(
            router.cooldown_until(&a.to_string(), "gpt-5.5").is_none(),
            "{a} cooled down"
        );
    }
}
