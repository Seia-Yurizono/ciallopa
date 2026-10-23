//! Proxy overhead load test (fake data only).
//!   loadbench mock <addr> <events> <gap_ms>   — fake chatgpt.com Responses SSE upstream
//!   loadbench load <proxy_url> <concurrency> <total> <key> [timeout_secs]
//!
//! Fixed-concurrency, closed-loop traffic: each worker waits for its request to finish before
//! sending another. This measures that workload, not capacity under an open arrival rate.
//! The whole-request timeout defaults to 300 s, including response headers and streamed body.
//! First output means a Responses `*.delta` or `response.output_item.done` event, parsed from
//! complete SSE data; metadata and strings in generated text do not count. Total latency ends
//! at `response.completed`. Failed, incomplete, malformed or truncated streams count as errors.
//! Latency percentiles include successes only; missing samples are JSON null. The historical
//! `first_delta_*` fields alias `first_output_*` (all times in ms).
#![allow(clippy::unwrap_used)]
use axum::body::{Body, Bytes};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn mock(addr: String, events: usize, gap: u64) -> std::io::Result<()> {
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
    let ln = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(ln, app).await
}

/// Keep inspection memory bounded even when the target returns a malformed SSE stream.
const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct Events {
    line: Vec<u8>,
    data: Vec<u8>,
    first_output_ms: Option<f64>,
    terminal: Option<bool>,
}

impl Events {
    fn feed(&mut self, chunk: &[u8], elapsed_ms: f64) -> Result<(), ()> {
        for part in chunk.split_inclusive(|b| *b == b'\n') {
            if self.terminal.is_some() {
                break;
            }
            if self.line.len() + self.data.len() + part.len() > MAX_EVENT_BYTES {
                return Err(());
            }
            self.line.extend_from_slice(part);
            if self.line.last() != Some(&b'\n') {
                continue;
            }
            let mut line = std::mem::take(&mut self.line);
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                if self.data.is_empty() {
                    continue;
                }
                let event: serde_json::Value =
                    serde_json::from_slice(&self.data).map_err(|_| ())?;
                self.data.clear();
                let kind = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if self.first_output_ms.is_none()
                    && ((kind.starts_with("response.") && kind.ends_with(".delta"))
                        || kind == "response.output_item.done")
                {
                    self.first_output_ms = Some(elapsed_ms);
                }
                match kind {
                    "response.completed" => self.terminal = Some(true),
                    "response.failed"
                    | "response.incomplete"
                    | "error"
                    | "service_unavailable_error" => self.terminal = Some(false),
                    _ => {}
                }
            } else if let Some(data) = line.strip_prefix(b"data:") {
                self.data
                    .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
                self.data.push(b'\n');
            }
        }
        Ok(())
    }
}

struct Sample {
    ok: bool,
    first_output_ms: Option<f64>,
    total_ms: f64,
}

async fn sample(http: &reqwest::Client, url: &str, key: &str, timeout: Duration) -> Sample {
    let start = Instant::now();
    let request = async {
        let response = http
            .post(url)
            .bearer_auth(key)
            .header("content-type", "application/json")
            .body(r#"{"model":"gpt-5.5","reasoning":{"effort":"low"},"stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#)
            .send()
            .await;
        let Ok(response) = response else {
            return (false, None);
        };
        if !response.status().is_success() {
            return (false, None);
        }
        let mut stream = response.bytes_stream();
        let mut events = Events::default();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                return (false, events.first_output_ms);
            };
            if events
                .feed(&chunk, start.elapsed().as_secs_f64() * 1e3)
                .is_err()
            {
                return (false, events.first_output_ms);
            }
            if let Some(ok) = events.terminal {
                return (ok, events.first_output_ms);
            }
        }
        (false, events.first_output_ms)
    };
    let (ok, first_output_ms) = tokio::time::timeout(timeout, request)
        .await
        .unwrap_or((false, None));
    Sample {
        ok,
        first_output_ms,
        total_ms: start.elapsed().as_secs_f64() * 1e3,
    }
}

fn pct(v: &mut [f64], p: f64) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    Some(v[((v.len() as f64 * p).ceil() as usize).clamp(1, v.len()) - 1])
}

fn summary(all: &[Sample], conc: usize, wall: f64, timeout: Duration) -> serde_json::Value {
    let errors = all.iter().filter(|s| !s.ok).count();
    let mut first: Vec<_> = all
        .iter()
        .filter(|s| s.ok)
        .filter_map(|s| s.first_output_ms)
        .collect();
    let mut total: Vec<_> = all.iter().filter(|s| s.ok).map(|s| s.total_ms).collect();
    let p50 = pct(&mut first, 0.5);
    let p99 = pct(&mut first, 0.99);
    serde_json::json!({
        "conc": conc,
        "total": all.len(),
        "errors": errors,
        "wall_s": wall,
        "timeout_secs": timeout.as_secs_f64(),
        "rps": (wall > 0.0).then(|| all.len() as f64 / wall),
        "success_rps": (wall > 0.0).then(|| (all.len() - errors) as f64 / wall),
        "first_output_samples": first.len(),
        "first_output_p50": p50,
        "first_output_p99": p99,
        "first_delta_p50": p50,
        "first_delta_p99": p99,
        "total_p50": pct(&mut total, 0.5),
        "total_p99": pct(&mut total, 0.99),
        "total_max": pct(&mut total, 1.0),
    })
}

async fn load(
    url: String,
    conc: usize,
    total: usize,
    key: String,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(conc)
        .build()?;
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let t0 = Instant::now();
    let mut tasks = vec![];
    for _ in 0..conc.min(total) {
        let (http, url, key, next) = (http.clone(), url.clone(), key.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = vec![];
            while next
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |n| (n < total).then(|| n + 1),
                )
                .is_ok()
            {
                out.push(sample(&http, &url, &key, timeout).await);
            }
            out
        }));
    }
    let mut all = vec![];
    for t in tasks {
        all.extend(t.await?);
    }
    let wall = t0.elapsed().as_secs_f64();
    println!("{}", summary(&all, conc, wall, timeout));
    Ok(())
}

enum Command {
    Mock {
        addr: String,
        events: usize,
        gap: u64,
    },
    Load {
        url: String,
        conc: usize,
        total: usize,
        key: String,
        timeout: Duration,
    },
}

const USAGE: &str = "usage: loadbench mock <addr> <events> <gap_ms>\n       loadbench load <proxy_url> <concurrency> <total> <key> [timeout_secs]";

fn parse(args: &[String]) -> Result<Command, String> {
    match args.first().map(String::as_str) {
        Some("mock") if args.len() == 4 => {
            let addr = args[1].clone();
            // Keep the previous hostname:port form as well as IPv4 / bracketed IPv6.
            let hostname = addr.rsplit_once(':').is_some_and(|(host, port)| {
                !host.is_empty()
                    && host
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b".-".contains(&b))
                    && port.parse::<u16>().is_ok()
            });
            if addr.parse::<std::net::SocketAddr>().is_err() && !hostname {
                return Err("addr must be a host:port socket address".into());
            }
            let events: usize = args[2]
                .parse()
                .map_err(|_| "events must be a non-negative integer")?;
            if events > usize::MAX - 2 {
                return Err("events is too large".into());
            }
            let gap = args[3]
                .parse()
                .map_err(|_| "gap_ms must be a non-negative integer")?;
            if Instant::now()
                .checked_add(Duration::from_millis(gap))
                .is_none()
            {
                return Err("gap_ms is too large".into());
            }
            Ok(Command::Mock { addr, events, gap })
        }
        Some("load") if args.len() == 5 || args.len() == 6 => {
            let url = reqwest::Url::parse(&args[1])
                .map_err(|_| "proxy_url must be a valid HTTP(S) URL")?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return Err("proxy_url must be a valid HTTP(S) URL".into());
            }
            let conc = args[2]
                .parse()
                .ok()
                .filter(|n: &usize| *n > 0)
                .ok_or("concurrency must be a positive integer")?;
            let total = args[3]
                .parse()
                .ok()
                .filter(|n: &usize| *n > 0)
                .ok_or("total must be a positive integer")?;
            if args[4].is_empty()
                || http::HeaderValue::from_str(&format!("Bearer {}", args[4])).is_err()
            {
                return Err("key must be non-empty and usable in an HTTP header".into());
            }
            let seconds = args
                .get(5)
                .map(String::as_str)
                .unwrap_or("300")
                .parse()
                .ok()
                .filter(|n: &u64| *n > 0)
                .ok_or("timeout_secs must be a positive integer")?;
            let timeout = Duration::from_secs(seconds);
            if Instant::now().checked_add(timeout).is_none() {
                return Err("timeout_secs is too large".into());
            }
            Ok(Command::Load {
                url: url.into(),
                conc,
                total,
                key: args[4].clone(),
                timeout,
            })
        }
        _ => Err(USAGE.into()),
    }
}

#[tokio::main]
async fn main() {
    let command = match parse(&std::env::args().skip(1).collect::<Vec<_>>()) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let result = match command {
        Command::Mock { addr, events, gap } => mock(addr, events, gap).await.map_err(Into::into),
        Command::Load {
            url,
            conc,
            total,
            key,
            timeout,
        } => load(url, conc, total, key, timeout).await,
    };
    if let Err(error) = result {
        eprintln!("loadbench: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_output_and_completion_survive_every_chunk_boundary() {
        let wire = concat!(
            ": keepalive\r\n\r\n",
            "data: {\"type\":\"response.created\",\"note\":\"delta response.completed\"}\r\n\r\n",
            "event: response.output_text.delta\r\n",
            "data: {\"type\":\"response.output_text.delta\",\r\n",
            "data: \"delta\":\"你好 response.completed\"}\r\n\r\n",
            "data: {\"type\":\"response.completed\"}\n\n",
        )
        .as_bytes();
        for cut in 0..=wire.len() {
            let mut events = Events::default();
            events.feed(&wire[..cut], 10.0).unwrap();
            events.feed(&wire[cut..], 20.0).unwrap();
            assert!(events.first_output_ms.is_some(), "cut {cut}");
            assert_eq!(events.terminal, Some(true), "cut {cut}");
        }
        let mut events = Events::default();
        for byte in wire {
            events.feed(&[*byte], 5.0).unwrap();
        }
        assert_eq!(events.first_output_ms, Some(5.0));
        assert_eq!(events.terminal, Some(true));
    }

    #[test]
    fn metadata_and_output_strings_do_not_forge_output_or_completion() {
        let mut events = Events::default();
        events
            .feed(
                b"data: {\"type\":\"response.created\",\"note\":\"delta response.completed\"}\n\n",
                1.0,
            )
            .unwrap();
        assert!(events.first_output_ms.is_none());
        assert!(events.terminal.is_none());
        events.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n", 2.0).unwrap();
        assert_eq!(events.first_output_ms, Some(2.0));
        assert!(events.terminal.is_none());
        events
            .feed(b"data: {\"type\":\"response.failed\"}\n\n", 3.0)
            .unwrap();
        assert_eq!(events.terminal, Some(false));
    }

    #[test]
    fn malformed_or_unbounded_events_are_rejected() {
        assert!(Events::default().feed(b"data: nope\n\n", 1.0).is_err());
        assert!(Events::default()
            .feed(&vec![b'x'; MAX_EVENT_BYTES + 1], 1.0)
            .is_err());
    }

    #[test]
    fn failed_empty_and_no_output_samples_have_null_percentiles() {
        let failed = [Sample {
            ok: false,
            first_output_ms: Some(1.0),
            total_ms: 100.0,
        }];
        for samples in [&failed[..], &[]] {
            let result = summary(samples, 1, 1.0, Duration::from_secs(300));
            assert_eq!(result["errors"], samples.len());
            for field in [
                "first_delta_p50",
                "first_output_p99",
                "total_p50",
                "total_p99",
                "total_max",
            ] {
                assert!(result[field].is_null(), "{result}");
            }
        }
        let empty_output = [Sample {
            ok: true,
            first_output_ms: None,
            total_ms: 10.0,
        }];
        let result = summary(&empty_output, 1, 1.0, Duration::from_secs(300));
        assert!(result["first_output_p50"].is_null());
        assert_eq!(result["total_p50"], 10.0);
        assert_eq!(result["first_output_samples"], 0);
    }

    #[test]
    fn cli_validates_arguments_and_keeps_default_timeout() {
        let args = |s: &str| s.split_whitespace().map(str::to_string).collect::<Vec<_>>();
        assert!(parse(&[]).is_err());
        assert!(parse(&args("mock 127.0.0.1:18080 0 0")).is_ok());
        assert!(parse(&args("mock localhost:18080 1 0")).is_ok());
        assert!(parse(&args("mock [::1]:18080 1 0")).is_ok());
        for invalid in [
            "load http://localhost 0 1 key",
            "load http://localhost 1 0 key",
            "load ftp://localhost 1 1 key",
            "load http://localhost 1 1 key 0",
            "mock invalid 1 0",
            "mock 127.0.0.1:18080 -1 0",
        ] {
            assert!(parse(&args(invalid)).is_err(), "{invalid}");
        }
        let Command::Load { timeout, .. } = parse(&args("load http://localhost 2 4 key")).unwrap()
        else {
            panic!("load")
        };
        assert_eq!(timeout, Duration::from_secs(300));
        let Command::Load { timeout, .. } =
            parse(&args("load http://localhost 2 4 key 10")).unwrap()
        else {
            panic!("load")
        };
        assert_eq!(timeout, Duration::from_secs(10));
    }
}
