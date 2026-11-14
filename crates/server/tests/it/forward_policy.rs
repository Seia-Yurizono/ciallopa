//! HTTP/WS policy differences retained by the shared-attempt-loop refactor (WP0a).
#![allow(clippy::unwrap_used)]

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{body::Body, extract::State, http::HeaderMap, response::Response, routing::post};
use bytes::Bytes;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as ClientMessage};

const KEY: &str = "sk-cpa-0123456789abcdef";
const IDS: [&str; 3] = ["a", "b", "c"];
type ClientWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Store {
    count: AtomicUsize,
    fail_credentials: bool,
}

#[async_trait]
impl AccountStore for Store {
    fn accounts(&self) -> Arc<[Account]> {
        IDS[..self.count.load(Ordering::SeqCst)]
            .iter()
            .map(|id| Account {
                id: (*id).into(),
                email: None,
                chatgpt_account_id: None,
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: false,
            })
            .collect()
    }

    fn generation(&self) -> u64 {
        self.count.load(Ordering::SeqCst) as u64
    }

    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        if self.fail_credentials && id == "a" {
            return Err(AuthError::Refresh("temporary credential failure".into()));
        }
        Ok(Credentials {
            account: id.clone(),
            access_token: format!("tok-{id}"),
            chatgpt_account_id: None,
        })
    }

    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }

    async fn manual_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }

    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

#[derive(Default)]
struct Rec(Mutex<Vec<(String, Outcome)>>);

impl ciallopa_core::Router for Rec {
    fn select(&self, route: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        accounts
            .iter()
            .find(|a| !route.exclude.contains(&a.id))
            .map(|a| Selection {
                account: a.id.clone(),
                from_binding: false,
            })
            .ok_or(RouteError::NoAccounts)
    }

    fn report(&self, _: &RouteRequest, account: &AccountId, outcome: &Outcome) {
        self.0
            .lock()
            .unwrap()
            .push((account.clone(), outcome.clone()));
    }
    fn cooling_until(&self, _: &AccountId) -> Option<std::time::SystemTime> {
        None
    }
    fn model_cooling_until(&self, _: &AccountId, _: &str) -> Option<std::time::SystemTime> {
        None
    }
    fn note_attempt(&self, _: &AccountId) {}
    fn content_ok(&self, _: &AccountId) {}
}

#[derive(Clone, Copy)]
enum Scenario {
    RankedErrors,
    Credentials,
    Server {
        status: u16,
        retry_after: Option<&'static str>,
        no_more_retry: bool,
    },
    GrowingOverload,
    QuotaTurns,
    ServiceUnavailable,
}

#[derive(Clone)]
struct Mock {
    scenario: Scenario,
    store: Arc<Store>,
    hits: Arc<Mutex<Vec<String>>>,
    turns: Arc<Mutex<Vec<String>>>,
    release: Arc<tokio::sync::Notify>,
}

impl Mock {
    fn new(scenario: Scenario, count: usize) -> Self {
        Self {
            scenario,
            store: Arc::new(Store {
                count: AtomicUsize::new(count),
                fail_credentials: matches!(scenario, Scenario::Credentials),
            }),
            hits: Default::default(),
            turns: Default::default(),
            release: Default::default(),
        }
    }
}

fn error_response(status: u16, code: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"error":{"code":code,"message":code}}).to_string(),
        ))
        .unwrap()
}

fn completed(account: &str) -> Value {
    json!({"type":"response.completed","response":{"id":format!("r-{account}"),
        "usage":{"input_tokens":5,"output_tokens":1}}})
}

fn overloaded() -> Value {
    json!({"type":"error","error":{"code":"server_is_overloaded","message":"busy"}})
}

fn frames(mock: &Mock, account: &str, request: &Value) -> Vec<Value> {
    mock.turns.lock().unwrap().push(account.into());
    match mock.scenario {
        Scenario::GrowingOverload => {
            // A reload adds an account after the connection/request captured its switch limit.
            mock.store.count.store(3, Ordering::SeqCst);
            if account != "c" {
                return vec![overloaded()];
            }
        }
        Scenario::QuotaTurns if request["fail_account"] == account => {
            mock.store.count.store(3, Ordering::SeqCst);
            return vec![json!({"type":"error","error":{"code":"usage_limit_reached"}})];
        }
        Scenario::ServiceUnavailable => {
            return vec![json!({"type":"service_unavailable_error",
                "error":{"code":"server_is_overloaded","message":"busy"}})];
        }
        _ => {}
    }
    vec![completed(account)]
}

async fn upstream(
    State(mock): State<Mock>,
    headers: HeaderMap,
    upgrade: Option<WebSocketUpgrade>,
    body: Bytes,
) -> Response {
    let account = headers["authorization"]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer tok-")
        .unwrap()
        .to_string();
    mock.hits.lock().unwrap().push(account.clone());
    match mock.scenario {
        Scenario::RankedErrors => {
            return if account == "a" {
                error_response(429, "usage_limit_reached")
            } else {
                error_response(403, "forbidden_fixture")
            };
        }
        Scenario::Credentials => return error_response(400, "bad_request_fixture"),
        Scenario::Server {
            status,
            retry_after,
            no_more_retry,
        } => {
            let mut response =
                error_response(if account == "a" { status } else { 400 }, "server_fixture");
            if let Some(value) = retry_after {
                response
                    .headers_mut()
                    .insert("retry-after", value.parse().unwrap());
            }
            if no_more_retry {
                response
                    .headers_mut()
                    .insert("x-retry-metadata", "NO_MORE_RETRY".parse().unwrap());
            }
            return response;
        }
        _ => {}
    }
    if let Some(upgrade) = upgrade {
        return upgrade.on_upgrade(move |mut ws| async move {
            while let Some(Ok(message)) = ws.next().await {
                match message {
                    Message::Text(text) => {
                        let request: Value = serde_json::from_str(text.as_str()).unwrap();
                        for frame in frames(&mock, &account, &request) {
                            if ws.send(Message::text(frame.to_string())).await.is_err() {
                                return;
                            }
                        }
                        if matches!(mock.scenario, Scenario::ServiceUnavailable) {
                            mock.release.notified().await;
                            if ws
                                .send(Message::text(completed(&account).to_string()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });
    }
    let request = serde_json::from_slice(&body).unwrap();
    let body: String = frames(&mock, &account, &request)
        .iter()
        .map(|v| format!("data: {v}\n\n"))
        .collect();
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

async fn upstream_http(state: State<Mock>, headers: HeaderMap, body: Bytes) -> Response {
    upstream(state, headers, None, body).await
}

async fn upstream_ws(
    state: State<Mock>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    upstream(state, headers, Some(upgrade), Bytes::new()).await
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    address
}

struct Env {
    address: String,
    mock: Mock,
    rec: Arc<Rec>,
    usage: Option<Arc<ciallopa_usage::Recorder>>,
    drain: Arc<ciallopa_upstream::drain::Drain>,
    _dir: tempfile::TempDir,
}

impl Drop for Env {
    fn drop(&mut self) {
        if let Some(usage) = &self.usage {
            usage.shutdown();
        }
    }
}

impl Env {
    async fn new(scenario: Scenario, count: usize, options: &str) -> Self {
        let mock = Mock::new(scenario, count);
        let upstream = serve(
            axum::Router::new()
                .route(
                    "/backend-api/codex/responses",
                    post(upstream_http).get(upstream_ws),
                )
                .with_state(mock.clone()),
        )
        .await;
        Self::with_upstream(mock, &upstream, options).await
    }

    async fn with_upstream(mock: Mock, upstream: &str, options: &str) -> Self {
        let mut config = Config::parse(&format!(
            r#"
api_keys = ["{KEY}"]
upstream = "http://{upstream}/backend-api"
[upstream_opts]
compress_requests = false
stream_idle_timeout_secs = 2
preparation_timeout_secs = 2
overload_retry_wait_secs = 0
{options}
"#
        ))
        .unwrap();
        config.upstream_opts.time_unit = super::support::TIME_UNIT;
        // This fixture tests policy, so keep its original physical two-second budget.
        config.upstream_opts.preparation_timeout_secs =
            (Duration::from_secs(2).as_nanos() / super::support::TIME_UNIT.as_nanos()) as u64;
        let dir = tempfile::tempdir().unwrap();
        let usage = matches!(
            mock.scenario,
            Scenario::Credentials | Scenario::ServiceUnavailable
        )
        .then(|| {
            ciallopa_usage::Recorder::open(&ciallopa_core::config::UsageConfig {
                path: dir.path().join("usage.db"),
                flush_ms: 5,
                ..Default::default()
            })
            .unwrap()
        });
        let rec = Arc::new(Rec::default());
        let up = Arc::new(ciallopa_upstream::UpstreamClient::new(&config).unwrap());
        let drain = up.drain.clone();
        let state = AppState {
            accounts: mock.store.clone(),
            router: rec.clone(),
            upstream: up,
            config: Arc::new(config.into()),
            usage: usage.clone(),
            egress: Default::default(),
        };
        let address = serve(ciallopa::build_app(state)).await;
        Self {
            address,
            mock,
            rec,
            usage,
            drain,
            _dir: dir,
        }
    }

    async fn post(&self) -> reqwest::Response {
        super::support::http_client()
            .post(format!("http://{}/v1/responses", self.address))
            .timeout(Duration::from_secs(2))
            .bearer_auth(KEY)
            .json(&json!({"model":"gpt-5.5","stream":true,"input":[]}))
            .send()
            .await
            .unwrap()
    }

    async fn ws(&self) -> Result<ClientWs, tokio_tungstenite::tungstenite::Error> {
        let mut request = format!("ws://{}/v1/responses", self.address)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio_tungstenite::connect_async(request),
        )
        .await
        .expect("WS policy fixture timed out")
        .map(|(ws, _)| ws)
    }

    async fn record(&self) -> ciallopa_usage::RecordRow {
        let usage = self.usage.as_ref().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                usage.flush_blocking().unwrap();
                let mut rows = usage
                    .recent(ciallopa_usage::RecentQuery {
                        limit: 10,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                if !rows.is_empty() {
                    return rows.remove(0);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("usage row was not emitted")
    }
}

async fn next_frame(ws: &mut ClientWs) -> ClientMessage {
    tokio::time::timeout(Duration::from_secs(1), ws.next())
        .await
        .expect("WS frame timed out")
        .unwrap()
        .unwrap()
}

fn rejected(error: tokio_tungstenite::tungstenite::Error) -> http::Response<Option<Vec<u8>>> {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => *response,
        other => panic!("expected an HTTP handshake rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn d1_http_returns_ranked_error_ws_returns_last_error() {
    let http = Env::new(Scenario::RankedErrors, 2, "").await;
    let response = http.post().await;
    assert_eq!(response.status(), 429);
    assert_eq!(
        response.json::<Value>().await.unwrap()["error"]["code"],
        "usage_limit_reached"
    );
    assert_eq!(*http.mock.hits.lock().unwrap(), ["a", "b"]);

    let ws = Env::new(Scenario::RankedErrors, 2, "").await;
    let response = rejected(ws.ws().await.unwrap_err());
    assert_eq!(response.status(), 403);
    let body: Value = serde_json::from_slice(response.body().as_ref().unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "forbidden_fixture");
    assert_eq!(*ws.mock.hits.lock().unwrap(), ["a", "b"]);
}

#[tokio::test]
async fn d2_http_records_credentials_and_dial_attempts_ws_records_neither() {
    for websocket in [false, true] {
        let env = Env::new(Scenario::Credentials, 2, "").await;
        if websocket {
            assert_eq!(rejected(env.ws().await.unwrap_err()).status(), 400);
        } else {
            assert_eq!(env.post().await.status(), 400);
        }
        assert_eq!(*env.mock.hits.lock().unwrap(), ["b"]);
        let row = env.record().await;
        assert_eq!(row.account.as_deref(), Some("b"));
        assert_eq!(row.first_account.as_deref(), Some("a"));
        let detail = env
            .usage
            .as_ref()
            .unwrap()
            .detail(i64::from_str_radix(&row.request_id, 16).unwrap())
            .await
            .unwrap()
            .unwrap();
        if websocket {
            assert!(detail.attempts.is_null(), "{detail:?}");
        } else {
            let attempts = detail.attempts.as_array().unwrap();
            assert_eq!(attempts.len(), 2);
            assert_eq!(attempts[0]["account"], "a");
            assert_eq!(attempts[0]["kind"], "refresh");
            assert_eq!(attempts[0]["code"], "refresh_transient");
            assert_eq!(attempts[0]["action"], "switch");
            assert_eq!(attempts[1]["account"], "b");
            assert_eq!(attempts[1]["status"], 400);
            assert_eq!(attempts[1]["action"], "stop");
        }
    }
}

#[tokio::test]
async fn d3_http_stops_on_last_account_transport_error_ws_retries_it() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    for websocket in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = hits.clone();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                }
                socket.read_exact(&mut vec![0; length]).await.unwrap();
                // Consume the request, then close without response headers: a transport error.
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    continue;
                }
                socket.get_mut().write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await.unwrap();
            }
        });
        let env = Env::with_upstream(
            Mock::new(Scenario::RankedErrors, 1),
            &address,
            "max_attempts = 4",
        )
        .await;
        if websocket {
            assert_eq!(rejected(env.ws().await.unwrap_err()).status(), 400);
        } else {
            assert_eq!(env.post().await.status(), 502);
        }
        assert_eq!(hits.load(Ordering::SeqCst), if websocket { 2 } else { 1 });
        let reports = env.rec.0.lock().unwrap();
        assert_eq!(reports[0].0, "a");
        assert!(matches!(reports[0].1, Outcome::Transport));
        assert_eq!(reports.len(), if websocket { 2 } else { 1 });
        server.abort();
    }
}

#[tokio::test]
async fn d5_http_stops_on_last_account_ws_uses_its_own_retry_budget() {
    for (websocket, no_more_retry, expected) in
        [(false, false, 1), (true, false, 5), (true, true, 1)]
    {
        let env = Env::new(
            Scenario::Server {
                status: 503,
                retry_after: Some("0"),
                no_more_retry,
            },
            1,
            if websocket {
                "max_attempts = 1"
            } else {
                "max_attempts = 4"
            },
        )
        .await;
        if websocket {
            assert_eq!(rejected(env.ws().await.unwrap_err()).status(), 503);
        } else {
            assert_eq!(env.post().await.status(), 503);
        }
        assert_eq!(*env.mock.hits.lock().unwrap(), vec!["a"; expected]);
        let reports = env.rec.0.lock().unwrap();
        assert_eq!(reports.len(), expected);
        assert!(reports.iter().all(|(_, o)| matches!(
            o,
            Outcome::ServerError {
                status: 503,
                cooldown: None
            }
        )));
    }
}

#[tokio::test]
async fn d4_ws_server_cooldown_honors_retry_after_like_http() {
    // Parsing/caps/status aliases live in retry_after and policy tables; this proves
    // both transports wire a Retry-After longer than the configured default through.
    for websocket in [false, true] {
        let env = Env::new(
            Scenario::Server {
                status: 503,
                retry_after: Some("120"),
                no_more_retry: true,
            },
            2,
            "transient_cooldown_secs = 15",
        )
        .await;
        let status = if websocket {
            rejected(env.ws().await.unwrap_err()).status()
        } else {
            env.post().await.status()
        };
        assert_eq!(status, 400);
        assert_eq!(*env.mock.hits.lock().unwrap(), ["a", "b"]);
        assert!(matches!(env.rec.0.lock().unwrap()[0].1,
            Outcome::ServerError { status: 503, cooldown: Some(d) } if d == Duration::from_secs(120)));
    }
}

#[tokio::test]
async fn d6_http_tries_new_accounts_ws_keeps_its_captured_overload_switch_limit() {
    let http = Env::new(Scenario::GrowingOverload, 2, "").await;
    let response = http.post().await;
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(text.contains("r-c"), "{text}");
    assert!(!text.contains("server_is_overloaded"), "{text}");
    assert_eq!(*http.mock.turns.lock().unwrap(), ["a", "b", "c"]);

    let env = Env::new(Scenario::GrowingOverload, 2, "").await;
    let mut ws = env.ws().await.unwrap();
    ws.send(ClientMessage::text(
        r#"{"type":"response.create","model":"gpt-5.5"}"#,
    ))
    .await
    .unwrap();
    let frame = next_frame(&mut ws).await;
    assert_eq!(
        serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap(),
        overloaded()
    );
    assert_eq!(*env.mock.turns.lock().unwrap(), ["a", "b"]);
    assert_eq!(env.mock.store.count.load(Ordering::SeqCst), 3);
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn d7_ws_quota_switch_budget_resets_each_turn() {
    let env = Env::new(Scenario::QuotaTurns, 2, "").await;
    let mut ws = env.ws().await.unwrap();
    for (fail, success) in [("a", "b"), ("b", "c")] {
        ws.send(ClientMessage::text(
            json!({"type":"response.create","model":"gpt-5.5",
            "fail_account":fail})
            .to_string(),
        ))
        .await
        .unwrap();
        let frame = next_frame(&mut ws).await;
        assert!(
            frame.is_text(),
            "turn on {fail} must replay to {success}, got {frame:?}"
        );
        assert_eq!(
            serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap(),
            completed(success)
        );
    }
    assert_eq!(*env.mock.turns.lock().unwrap(), ["a", "b", "b", "c"]);
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_service_unavailable_error_is_overloaded_but_does_not_end_turn() {
    let env = Env::new(Scenario::ServiceUnavailable, 1, "").await;
    let mut ws = env.ws().await.unwrap();
    ws.send(ClientMessage::text(
        r#"{"type":"response.create","model":"gpt-5.5"}"#,
    ))
    .await
    .unwrap();
    let frame = next_frame(&mut ws).await;
    let value: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
    assert_eq!(value["type"], "service_unavailable_error");
    assert_eq!(
        env.drain.inflight(),
        1,
        "this frame must keep the running turn admitted"
    );
    assert!(env
        .rec
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|(_, o)| matches!(o, Outcome::Overloaded { .. })));
    env.mock.release.notify_one();
    let frame = next_frame(&mut ws).await;
    assert_eq!(
        serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap(),
        completed("a")
    );
    assert_eq!(env.drain.inflight(), 0);
    let row = env.record().await;
    assert_eq!(row.error.as_deref(), Some("server_is_overloaded"));
    assert_eq!(row.input_tokens, Some(5));
    assert_eq!(row.output_tokens, Some(1));
    ws.close(None).await.unwrap();
}
