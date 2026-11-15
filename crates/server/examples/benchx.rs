//! No-gap cloud SSE reproduction harness (2026-11-15 kit, fake data only).
//! SSE stops at response.completed; sharing Client does not prove connection reuse.
//!   benchx mock <addr> <events> <delta_bytes>        — SSE (POST) + WS (GET) fake upstream, no gaps
//!   benchx sse  <url> <conc> <secs> <key> <body_bytes> — closed-loop SSE load for <secs>
//!   benchx ws   <url> <conns> <secs> <key> <body_bytes> — closed-loop WS turns, one socket per worker
//! Output: one JSON line with counts, rps and p50/p90/p95/p99/max for first output and total.
#![allow(clippy::unwrap_used)]
use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn events(n: usize, delta: usize) -> Vec<String> {
    let text: String = "lorem ipsum dolor sit amet "
        .chars()
        .cycle()
        .take(delta)
        .collect();
    let mut v = vec![r#"{"type":"response.created","response":{"id":"r"}}"#.to_string()];
    for i in 0..n {
        v.push(format!(r#"{{"type":"response.output_text.delta","item_id":"m","output_index":0,"content_index":0,"delta":"{i} {text}"}}"#));
    }
    v.push(r#"{"type":"response.completed","response":{"id":"r","usage":{"input_tokens":1200,"input_tokens_details":{"cached_tokens":1000},"output_tokens":64,"output_tokens_details":{"reasoning_tokens":16}}}}"#.to_string());
    v
}

async fn mock(addr: String, n: usize, delta: usize) {
    let ev = Arc::new(events(n, delta));
    let (e1, e2) = (ev.clone(), ev.clone());
    let app = Router::new().route(
        "/backend-api/codex/responses",
        post(move |_b: Bytes| {
            let ev = e1.clone();
            async move {
                let s = futures::stream::iter((0..ev.len()).map(move |i| {
                    Ok::<_, std::io::Error>(Bytes::from(format!("data: {}\n\n", ev[i])))
                }));
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .header("x-codex-primary-used-percent", "5")
                    .body(Body::from_stream(s))
                    .unwrap()
            }
        })
        .get(move |ws: WebSocketUpgrade| {
            let ev = e2.clone();
            async move {
                ws.on_upgrade(move |mut s| async move {
                    while let Some(Ok(m)) = s.recv().await {
                        match m {
                            Message::Text(_) => {
                                for e in ev.iter() {
                                    if s.send(Message::Text(e.as_str().into())).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Message::Close(_) => return,
                            _ => {}
                        }
                    }
                })
            }
        }),
    );
    use axum::serve::ListenerExt;
    // Nagle + delayed ACK otherwise adds ~40 ms to small multi-write responses.
    let ln = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap()
        .tap_io(|t| {
            let _ = t.set_nodelay(true);
        });
    axum::serve(ln, app).await.unwrap();
}

fn body(bytes: usize) -> String {
    let pad: String = "x".repeat(bytes.saturating_sub(160));
    format!(
        r#"{{"model":"gpt-5.5","reasoning":{{"effort":"low"}},"stream":true,"input":[{{"type":"message","role":"user","content":[{{"type":"input_text","text":"hi {pad}"}}]}}]}}"#
    )
}

fn is_output(t: &str) -> bool {
    t.contains("\"response.output_text.delta\"") || t.contains("\"response.output_item.done\"")
}

#[derive(Default)]
struct Out {
    started: u64,
    ok: u64,
    err: u64,
    first: Vec<f64>,
    total: Vec<f64>,
}

fn pct(v: &mut [f64], p: f64) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    Some(v[((v.len() as f64 * p).ceil() as usize).clamp(1, v.len()) - 1])
}

fn report(kind: &str, conc: usize, bytes: usize, wall: f64, mut o: Out) {
    let mut j = serde_json::json!({"kind": kind, "conc": conc, "ok": o.ok, "errors": o.err, "started": o.started,
        "incomplete": o.started - o.ok - o.err, "request_body_bytes": body(bytes).len(), "wall_s": wall,
        "rps": o.ok as f64 / wall});
    for (name, v) in [("first", &mut o.first), ("total", &mut o.total)] {
        for (p, q) in [
            ("p50", 0.5),
            ("p90", 0.9),
            ("p95", 0.95),
            ("p99", 0.99),
            ("max", 1.0),
        ] {
            j[format!("{name}_{p}")] = serde_json::json!(pct(v, q));
        }
    }
    println!("{j}");
}

async fn sse(url: String, conc: usize, secs: u64, key: String, bytes: usize) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(conc)
        .build()
        .unwrap();
    let body = Bytes::from(body(bytes));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let mut tasks = vec![];
    for _ in 0..conc {
        let (http, url, key, body, stop) = (
            http.clone(),
            url.clone(),
            key.clone(),
            body.clone(),
            stop.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut o = Out::default();
            while !stop.load(Ordering::Relaxed) {
                o.started += 1;
                let s = Instant::now();
                let terminal = match http
                    .post(&url)
                    .bearer_auth(&key)
                    .header("content-type", "application/json")
                    .body(body.clone())
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => read_terminal(r, s).await,
                    _ => None,
                };
                if let Some((first, total)) = terminal {
                    o.ok += 1;
                    o.first.extend(first);
                    o.total.push(total);
                } else {
                    o.err += 1;
                }
            }
            o
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut all = Out::default();
    for t in tasks {
        let o = t.await.unwrap();
        all.started += o.started;
        all.ok += o.ok;
        all.err += o.err;
        all.first.extend(o.first);
        all.total.extend(o.total);
    }
    report("sse", conc, bytes, t0.elapsed().as_secs_f64(), all);
}

// Keep the kit's terminal-stop semantics: latency freezes at the terminal marker,
// without draining HTTP EOF. The Client request timeout bounds incomplete streams.
async fn read_terminal(r: reqwest::Response, start: Instant) -> Option<(Option<f64>, f64)> {
    let mut stream = r.bytes_stream();
    let mut buf = Vec::new();
    let mut first = None;
    while let Some(Ok(chunk)) = stream.next().await {
        buf.extend_from_slice(&chunk);
        if first.is_none() && memfind(&buf, b"output_text.delta") {
            first = Some(start.elapsed().as_secs_f64() * 1e3);
        }
        if memfind(
            &buf[buf.len().saturating_sub(chunk.len() + 64)..],
            b"\"response.completed\"",
        ) {
            return Some((first, start.elapsed().as_secs_f64() * 1e3));
        }
        if first.is_some() && buf.len() > 4096 {
            let keep = buf.len() - 64;
            buf.drain(..keep);
        }
    }
    None
}

fn memfind(h: &[u8], n: &[u8]) -> bool {
    h.windows(n.len()).any(|w| w == n)
}

async fn ws(url: String, conc: usize, secs: u64, key: String, bytes: usize) {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let frame = {
        let mut v: serde_json::Value = serde_json::from_str(&body(bytes)).unwrap();
        v["type"] = "response.create".into();
        v.as_object_mut().unwrap().remove("stream");
        v.to_string()
    };
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let mut tasks = vec![];
    for _ in 0..conc {
        let (url, key, frame, stop) = (url.clone(), key.clone(), frame.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut o = Out::default();
            let mut req = url.as_str().into_client_request().unwrap();
            req.headers_mut()
                .insert("authorization", format!("Bearer {key}").parse().unwrap());
            let Ok((mut ws, _)) =
                tokio_tungstenite::connect_async_with_config(req, None, true).await
            else {
                o.started += 1;
                o.err += 1;
                return o;
            };
            'turns: while !stop.load(Ordering::Relaxed) {
                o.started += 1;
                let s = Instant::now();
                if ws.send(M::text(frame.clone())).await.is_err() {
                    o.err += 1;
                    break;
                }
                let mut first = None;
                loop {
                    match tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
                        Ok(Some(Ok(M::Text(t)))) => {
                            if first.is_none() && is_output(t.as_str()) {
                                first = Some(s.elapsed().as_secs_f64() * 1e3);
                            }
                            if t.as_str().contains("\"response.completed\"") {
                                o.ok += 1;
                                o.first.extend(first);
                                o.total.push(s.elapsed().as_secs_f64() * 1e3);
                                break;
                            }
                            if t.as_str().contains("\"type\":\"error\"") {
                                o.err += 1;
                                break 'turns;
                            }
                        }
                        Ok(Some(Ok(M::Ping(_) | M::Pong(_)))) => {}
                        _ => {
                            o.err += 1;
                            break 'turns;
                        }
                    }
                }
            }
            let _ = ws.close(None).await;
            o
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut all = Out::default();
    for t in tasks {
        let o = t.await.unwrap();
        all.started += o.started;
        all.ok += o.ok;
        all.err += o.err;
        all.first.extend(o.first);
        all.total.extend(o.total);
    }
    report("ws", conc, bytes, t0.elapsed().as_secs_f64(), all);
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let n = |i: usize| a[i].parse::<usize>().unwrap();
    match a[0].as_str() {
        "mock" => mock(a[1].clone(), n(2), n(3)).await,
        "sse" => sse(a[1].clone(), n(2), n(3) as u64, a[4].clone(), n(5)).await,
        "ws" => ws(a[1].clone(), n(2), n(3) as u64, a[4].clone(), n(5)).await,
        _ => panic!("usage: see header"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_short_stream_contract() {
        let ev = events(20, 30);
        assert_eq!(ev.len(), 22);
        assert_eq!(
            ev.iter()
                .map(|e| format!("data: {e}\n\n").len())
                .sum::<usize>(),
            3009
        );
        assert_eq!(body(200).len(), 192);
    }

    #[tokio::test]
    async fn terminal_latency_does_not_wait_for_eof() {
        let app = Router::new().route(
            "/",
            post(|| async {
                let chunks = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(
                    b"data: {\"type\":\"response.completed\"}\n\n",
                ))])
                .chain(futures::stream::pending());
                Body::from_stream(chunks)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let start = Instant::now();
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .send()
            .await
            .unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(1), read_terminal(response, start))
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.1 < 1000.0);
        server.abort();
    }
}
