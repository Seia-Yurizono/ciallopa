//! Usage-log benchmarks with fake data. See <local-path>
//!   writer   <rows> <batch...>        — writer throughput per batch size
//!   enqueue  <rows> <queue> <batch>   — producer try_send latency + drops under overload
//!   gen      <db> <rows> <days> <accounts> <models>  — bulk fake rows spread over <days>
//!   query    <db>                     — summary/records latency for 24h/7d/30d
//!   purge    <db> <days_to_drop> <chunk> — purge oldest days (chunked)
//!   crashw   <db>                     — write forever (for kill -9 tests)
//!   check    <db>                     — integrity_check + count
#![allow(clippy::unwrap_used)]
use cpa_lite_core::config::UsageConfig;
use cpa_lite_usage::*;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn cfg(p: &str, queue: usize, batch: usize, flush_ms: u64) -> UsageConfig {
    UsageConfig {
        enabled: true,
        path: PathBuf::from(p),
        retention_days: 0,
        queue_size: queue,
        batch_size: batch,
        flush_ms,
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn fake(r: &Recorder, rng: &mut Rng, ms: i64, accounts: u64, models: u64) -> Record {
    let id = r.ids.next_at(ms);
    let ws = rng.below(3) != 0;
    let mut x = Record::new(id, if ws { Transport::Ws } else { Transport::Sse }, "responses");
    let a = rng.below(accounts);
    x.account = Some(format!("codex-{a:08x}-user{a}@example.com-plus"));
    x.model = Some(format!("gpt-5.{}", rng.below(models)));
    x.effort = Some(["low", "medium", "high", "xhigh"][rng.below(4) as usize].into());
    x.status = 200;
    let err = rng.below(100);
    if err < 3 {
        x.error = Some(["usage_limit", "idle_timeout", "client_closed"][err as usize].into());
    }
    if rng.below(50) == 0 {
        x.switches = 1;
        x.first_account = Some(format!("codex-{:08x}-other@example.com-plus", rng.below(accounts)));
    }
    let ttft = 300 + rng.below(4000) as u32;
    x.ttft_ms = Some(ttft);
    x.duration_ms = ttft + 200 + rng.below(30_000) as u32;
    x.input_tokens = Some(2_000 + rng.below(200_000) as i64);
    x.cached_tokens = Some(rng.below(150_000) as i64);
    x.output_tokens = Some(rng.below(4_000) as i64);
    x.reasoning_tokens = Some(rng.below(2_000) as i64);
    x
}

fn pctl(v: &mut [u64], p: f64) -> u64 {
    v.sort_unstable();
    v[((v.len() as f64 * p).ceil() as usize).clamp(1, v.len()) - 1]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let rt = tokio::runtime::Runtime::new().unwrap();
    match a[1].as_str() {
        "writer" => {
            let rows: usize = a[2].parse().unwrap();
            for b in &a[3..] {
                let batch: usize = b.parse().unwrap();
                let d = tempfile::tempdir().unwrap();
                let p = d.path().join("w.db");
                let r = Recorder::open(&cfg(p.to_str().unwrap(), 65_536, batch, 50)).unwrap();
                let mut rng = Rng(42);
                let t0 = now_ms();
                let t = Instant::now();
                for i in 0..rows {
                    r.record_blocking(fake(&r, &mut rng, t0 + (i / 10) as i64, 8, 4));
                }
                r.flush_blocking().unwrap();
                let el = t.elapsed();
                let s = r.stats();
                r.shutdown();
                let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)
                    + std::fs::metadata(p.with_extension("db-wal")).map(|m| m.len()).unwrap_or(0);
                println!(
                    "{{\"bench\":\"writer\",\"batch\":{batch},\"rows\":{rows},\"secs\":{:.3},\"rows_per_s\":{:.0},\"written\":{},\"bytes_per_row\":{:.1}}}",
                    el.as_secs_f64(),
                    rows as f64 / el.as_secs_f64(),
                    s.written,
                    size as f64 / rows as f64
                );
            }
        }
        "enqueue" => {
            let rows: usize = a[2].parse().unwrap();
            let queue: usize = a[3].parse().unwrap();
            let batch: usize = a[4].parse().unwrap();
            let d = tempfile::tempdir().unwrap();
            let p = d.path().join("q.db");
            let r = Recorder::open(&cfg(p.to_str().unwrap(), queue, batch, 50)).unwrap();
            let mut rng = Rng(7);
            let t0 = now_ms();
            let recs: Vec<Record> = (0..rows).map(|i| fake(&r, &mut rng, t0 + i as i64, 8, 4)).collect();
            let mut lat = Vec::with_capacity(rows);
            let t = Instant::now();
            for x in recs {
                let s = Instant::now();
                r.record(x);
                lat.push(s.elapsed().as_nanos() as u64);
            }
            let el = t.elapsed();
            r.flush_blocking().unwrap();
            let s = r.stats();
            r.shutdown();
            println!(
                "{{\"bench\":\"enqueue\",\"queue\":{queue},\"batch\":{batch},\"offered\":{rows},\"offered_per_s\":{:.0},\"accepted\":{},\"dropped\":{},\"written\":{},\"try_send_ns_p50\":{},\"p99\":{},\"p999\":{},\"max\":{}}}",
                rows as f64 / el.as_secs_f64(),
                s.accepted,
                s.dropped,
                s.written,
                pctl(&mut lat, 0.5),
                pctl(&mut lat, 0.99),
                pctl(&mut lat, 0.999),
                lat.iter().max().unwrap()
            );
        }
        "gen" => {
            let p = &a[2];
            let rows: u64 = a[3].parse().unwrap();
            let days: i64 = a[4].parse().unwrap();
            let accounts: u64 = a[5].parse().unwrap();
            let models: u64 = a[6].parse().unwrap();
            let r = Recorder::open(&cfg(p, 65_536, 4096, 50)).unwrap();
            let mut rng = Rng(99);
            let span = days * 86_400_000;
            let start = now_ms() - span;
            let t = Instant::now();
            for i in 0..rows {
                let ms = start + (i as i128 * span as i128 / rows as i128) as i64;
                r.record_blocking(fake(&r, &mut rng, ms, accounts, models));
            }
            r.flush_blocking().unwrap();
            r.shutdown();
            let size = std::fs::metadata(p).unwrap().len();
            println!(
                "{{\"bench\":\"gen\",\"rows\":{rows},\"days\":{days},\"secs\":{:.1},\"db_bytes\":{size},\"bytes_per_row\":{:.1}}}",
                t.elapsed().as_secs_f64(),
                size as f64 / rows as f64
            );
        }
        "query" => {
            let p = &a[2];
            let r = Recorder::open(&cfg(p, 1024, 256, 50)).unwrap();
            let now = now_ms();
            for (name, ms) in [("24h", 86_400_000i64), ("7d", 7 * 86_400_000), ("30d", 30 * 86_400_000)] {
                let mut ts = vec![];
                let mut cnt = 0;
                for _ in 0..3 {
                    let t = Instant::now();
                    let s = rt.block_on(r.summary(SummaryQuery { from_ms: now - ms, to_ms: now })).unwrap();
                    ts.push(t.elapsed().as_millis() as u64);
                    cnt = s.total.count;
                }
                println!("{{\"bench\":\"summary\",\"range\":\"{name}\",\"rows_in_range\":{cnt},\"ms_runs\":{ts:?},\"ms_min\":{}}}", ts.iter().min().unwrap());
            }
            let t = Instant::now();
            let rows = rt.block_on(r.recent(RecentQuery { limit: 100, ..Default::default() })).unwrap();
            println!("{{\"bench\":\"recent\",\"limit\":100,\"got\":{},\"ms\":{:.2}}}", rows.len(), t.elapsed().as_secs_f64() * 1e3);
            let t = Instant::now();
            let rows = rt
                .block_on(r.recent(RecentQuery { limit: 100, account: Some("codex-00000003-user3@example.com-plus".into()), errors_only: true, ..Default::default() }))
                .unwrap();
            println!("{{\"bench\":\"recent_filtered\",\"got\":{},\"ms\":{:.2}}}", rows.len(), t.elapsed().as_secs_f64() * 1e3);
            r.shutdown();
        }
        "purge" => {
            let p = PathBuf::from(&a[2]);
            let drop_days: i64 = a[3].parse().unwrap();
            let chunk: u64 = a[4].parse().unwrap();
            let (_, before) = integrity_check(&p).unwrap();
            // oldest row time
            let r = Recorder::open(&cfg(a[2].as_str(), 16, 16, 50)).unwrap();
            let oldest = rt
                .block_on(r.recent(RecentQuery { limit: 1, before_ms: Some(i64::MAX >> SEQ_BITS), ..Default::default() }))
                .unwrap();
            r.shutdown();
            let _ = oldest;
            let cutoff = now_ms() - (30 - drop_days) * 86_400_000;
            let t = Instant::now();
            let n = purge_db(&p, cutoff, chunk).unwrap();
            let el = t.elapsed();
            let (_, after) = integrity_check(&p).unwrap();
            println!(
                "{{\"bench\":\"purge\",\"chunk\":{chunk},\"days\":{drop_days},\"deleted\":{n},\"secs\":{:.2},\"rows_per_s\":{:.0},\"rows_before\":{before},\"rows_after\":{after},\"file_bytes_after\":{}}}",
                el.as_secs_f64(),
                n as f64 / el.as_secs_f64(),
                std::fs::metadata(&p).unwrap().len()
            );
        }
        "crashw" => {
            let r = Recorder::open(&cfg(&a[2], 8192, 256, 50)).unwrap();
            let mut rng = Rng(now_ms() as u64 | 1);
            loop {
                for _ in 0..1000 {
                    r.record_blocking(fake(&r, &mut rng, now_ms(), 8, 4));
                }
                std::thread::sleep(Duration::from_micros(200));
            }
        }
        "check" => {
            let (s, n) = integrity_check(&PathBuf::from(&a[2])).unwrap();
            println!("{{\"integrity\":\"{s}\",\"rows\":{n}}}");
        }
        _ => panic!("unknown"),
    }
}
