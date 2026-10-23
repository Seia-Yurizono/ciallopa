//! One process contract: the second signal closes an unfinished response and exits.
#![cfg(unix)]
#![allow(clippy::unwrap_used)]
use axum::{
    body::{Body, Bytes},
    response::Response,
    routing::post,
    Router,
};
use futures::StreamExt;
use std::{process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};

async fn wait_log(
    lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStderr>>,
    text: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.contains(text) {
                return;
            }
        }
        panic!("server exited before logging {text}");
    })
    .await
    .expect("server lifecycle notification");
}

#[tokio::test]
async fn second_signal_bounds_shutdown_with_an_unfinished_response() {
    let dir = tempfile::tempdir().unwrap();
    let up = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = up.local_addr().unwrap();
    let app = Router::new().route(
        "/backend-api/codex/responses",
        post(|| async {
            let stream = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(
                    b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"held\"}\n\n",
                ))
            })
            .chain(futures::stream::pending());
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream))
                .unwrap()
        }),
    );
    let mock = tokio::spawn(async move {
        axum::serve(up, app).await.unwrap();
    });
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = reservation.local_addr().unwrap();
    let auth = dir.path().join("auth");
    std::fs::create_dir(&auth).unwrap();
    std::fs::write(auth.join("local.json"), r#"{"type":"codex","email":"local@invalid","access_token":"fake-local-token","account_id":"local","expired":"2099-01-01T00:00:00Z"}"#).unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        format!(
            r#"
listen = "{addr}"
api_keys = ["sk-local-shutdown-test-0123456789"]
upstream = "http://{up_addr}/backend-api"
auth_dir = "{}"
[usage]
path = "{}"
[ui]
reset_credits_refresh_mins = 0
[log]
level = "info"
"#,
            auth.display(),
            dir.path().join("usage.db").display()
        ),
    )
    .unwrap();
    drop(reservation);
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ciallopa"))
        .arg("-c")
        .arg(path)
        .arg("serve")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    wait_log(&mut lines, "ciallopa listening").await;
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/v1/responses"))
        .bearer_auth("sk-local-shutdown-test-0123456789")
        .body(r#"{"stream":true,"input":[]}"#)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let pid = child.id().unwrap() as libc::pid_t;
    // This PID belongs to the child above; kill_on_drop also cleans up on assertion failure.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    wait_log(&mut lines, "draining forwarded requests").await;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let exited = tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .expect("second signal must bypass the 60 s drain")
        .unwrap();
    assert!(exited.success());
    drop(response);
    mock.abort();
}
