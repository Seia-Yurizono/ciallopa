//! Process contract: a session keeps its account across a graceful restart.
#![cfg(unix)]
#![allow(clippy::unwrap_used)]
use axum::{http::HeaderMap, response::Response, routing::post, Router};
use std::sync::{Arc, Mutex};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};

const KEY: &str = "sk-local-affinity-test-0123456789";

async fn start(config: &Path) -> tokio::process::Child {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ciallopa"))
        .arg("-c")
        .arg(config)
        .arg("serve")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.contains("ciallopa listening") {
                return;
            }
        }
        panic!("server exited before listening");
    })
    .await
    .expect("server start");
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    child
}

async fn stop(mut child: tokio::process::Child) {
    let pid = child.id().unwrap() as libc::pid_t;
    // This PID belongs to the child spawned above.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let st = tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .expect("graceful exit")
        .unwrap();
    assert!(st.success());
}

async fn send(
    addr: std::net::SocketAddr,
    session: Option<&str>,
    seen: &Mutex<Vec<String>>,
) -> String {
    let mut req = reqwest::Client::new()
        .post(format!("http://{addr}/v1/responses"))
        .bearer_auth(KEY)
        .body(r#"{"model":"gpt-5","stream":true,"input":[]}"#)
        .timeout(Duration::from_secs(10));
    if let Some(s) = session {
        req = req.header("session_id", s);
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();
    seen.lock().unwrap().pop().unwrap()
}

#[tokio::test]
async fn session_binding_survives_a_graceful_restart() {
    let dir = tempfile::tempdir().unwrap();
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let up = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = up.local_addr().unwrap();
    let rec = seen.clone();
    let app = Router::new().route(
        "/backend-api/codex/responses",
        post(move |h: HeaderMap| {
            let rec = rec.clone();
            async move {
                let auth = h.get("authorization").unwrap().to_str().unwrap().to_string();
                rec.lock().unwrap().push(auth);
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(axum::body::Body::from(
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n",
                    ))
                    .unwrap()
            }
        }),
    );
    let mock = tokio::spawn(async move { axum::serve(up, app).await.unwrap() });
    let auth = dir.path().join("auth");
    std::fs::create_dir(&auth).unwrap();
    for n in ["a", "b"] {
        std::fs::write(
            auth.join(format!("{n}.json")),
            format!(r#"{{"type":"codex","email":"{n}@invalid","access_token":"fake-{n}","account_id":"acct-{n}","expired":"2099-01-01T00:00:00Z"}}"#),
        )
        .unwrap();
    }
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
listen = "{addr}"
api_keys = ["{KEY}"]
upstream = "http://{up_addr}/backend-api"
auth_dir = "{}"
[usage]
path = "{}"
[ui]
reset_credits_refresh_mins = 0
"#,
            auth.display(),
            dir.path().join("usage.db").display()
        ),
    )
    .unwrap();

    let child = start(&config).await;
    // Round-robin: the first pick after a start goes to one account, the session to the other.
    let first = send(addr, None, &seen).await;
    let bound = send(addr, Some("restart-session"), &seen).await;
    assert_ne!(first, bound, "round-robin premise");
    assert_eq!(send(addr, Some("restart-session"), &seen).await, bound);
    stop(child).await;
    assert!(dir.path().join("session_affinity.json").exists());

    let child = start(&config).await;
    assert_eq!(send(addr, Some("restart-session"), &seen).await, bound);
    stop(child).await;
    mock.abort();
}
