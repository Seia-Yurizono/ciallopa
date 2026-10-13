//! Proxy overhead load test (fake data only).
//!   loadbench mock <addr> <events> <gap_ms>   — fake chatgpt.com Responses SSE upstream
//!   loadbench load <proxy_url> <concurrency> <total> <key>  — SSE load generator (JSON line out)
#![allow(clippy::unwrap_used)]
use axum::body::{Body, Bytes};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn mock(addr: String, events: usize, gap: u64) {
    let app = Router::new().route(
        "/backend-api/codex/responses",
        post(move |_b: Bytes| async move {
            let s = futures::stream::unfold(0usize, move |i| async move {
                let ev = if i == 0 {
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n".to_string()
                } else if i <= events {
                    tokio::time::sleep(Duration::from_millis(gap)).await;
                    format!("data: {{\"type\":\"response.output_text.delta\",\"delta\":\"tok{i} lorem ipsum dolor sit amet\"}}\n\n")
                } else if i == events + 1 {
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":1200,\"input_tokens_details\":{\"cached_tokens\":1000},\"output_tokens\":64,\"output_tokens_details\":{\"reasoning_tokens\":16}}}}\n\n".to_string()
                } else {
                    return None;
                };
                Some((Ok::<_, std::io::Error>(Bytes::from(ev)), i + 1))
            });
            Response::builder()
                .header("content-type", "text/event-stream")
                .header("x-codex-primary-used-percent", "5")
                .body(Body::from_stream(s))
                .unwrap()
        }),
    );
    let ln = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(ln, app).await.unwrap();
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 * p).ceil() as usize).clamp(1, v.len()) - 1]
}

async fn load(url: String, conc: usize, total: usize, key: String) {
    let http = reqwest::Client::builder().pool_max_idle_per_host(conc).build().unwrap();
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let t0 = Instant::now();
    let mut tasks = vec![];
    for _ in 0..conc {
        let (http, url, key, next) = (http.clone(), url.clone(), key.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = vec![];
            while next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < total {
                let s = Instant::now();
                let r = http
                    .post(&url)
                    .bearer_auth(&key)
                    .header("content-type", "application/json")
                    .body(r#"{"model":"gpt-5.5","reasoning":{"effort":"low"},"stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#)
                    .send()
                    .await;
                let Ok(r) = r else { out.push((false, 0.0, 0.0)); continue };
                let ok = r.status().is_success();
                let mut st = r.bytes_stream();
                let mut first = None;
                let mut done = false;
                while let Some(c) = st.next().await {
                    let Ok(c) = c else { break };
                    if first.is_none() && c.windows(5).any(|w| w == b"delta") {
                        first = Some(s.elapsed().as_secs_f64() * 1e3);
                    }
                    if c.windows(18).any(|w| w == b"response.completed") {
                        done = true;
                    }
                }
                out.push((ok && done, first.unwrap_or(0.0), s.elapsed().as_secs_f64() * 1e3));
            }
            out
        }));
    }
    let mut all = vec![];
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let wall = t0.elapsed().as_secs_f64();
    let errs = all.iter().filter(|x| !x.0).count();
    let mut ft: Vec<f64> = all.iter().filter(|x| x.0).map(|x| x.1).collect();
    let mut tot: Vec<f64> = all.iter().filter(|x| x.0).map(|x| x.2).collect();
    println!(
        "{{\"conc\":{conc},\"total\":{},\"errors\":{errs},\"wall_s\":{wall:.2},\"rps\":{:.0},\"first_delta_p50\":{:.2},\"first_delta_p99\":{:.2},\"total_p50\":{:.2},\"total_p99\":{:.2},\"total_max\":{:.2}}}",
        all.len(),
        all.len() as f64 / wall,
        pct(&mut ft, 0.5),
        pct(&mut ft, 0.99),
        pct(&mut tot, 0.5),
        pct(&mut tot, 0.99),
        pct(&mut tot, 1.0),
    );
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "mock" => mock(a[2].clone(), a[3].parse().unwrap(), a[4].parse().unwrap()).await,
        "load" => load(a[2].clone(), a[3].parse().unwrap(), a[4].parse().unwrap(), a[5].clone()).await,
        _ => panic!("mock|load"),
    }
}
