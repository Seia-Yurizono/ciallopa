//! Usage-log benchmarks with fake data.
//!   writer   <rows> <batch...>        — writer throughput per batch size
//!   enqueue  <rows> <queue> <batch>   — producer try_send latency + drops under overload
//!   gen      <db> <rows> <days> <accounts> <models> [--threads N] [--quota-every K]
//!   query    <db> [--account-index]   — summary/records/quota/history/session timings
//!   purge    <db> <days_to_drop> <chunk> — purge oldest days (chunked)
//!   crashw   <db>                     — write forever (for kill -9 tests)
//!   open     <db>                     — migrate/open/shutdown/checkpoint
//!   plan     <db>                     — read-only account query plans
//!   ablate   <new-dir> <rows> <accounts> <batch> <account_id|account|partial|none>
//!   check    <db>                     — integrity_check + count
#![allow(clippy::unwrap_used)]
use ciallopa_core::config::UsageConfig;
use ciallopa_usage::*;
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
        quota_weights: Default::default(),
        prices: PathBuf::new(),
        prices_source: String::new(),
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
    let mut x = Record::new(
        id,
        if ws { Transport::Ws } else { Transport::Sse },
        "responses",
    );
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
        x.first_account = Some(format!(
            "codex-{:08x}-other@example.com-plus",
            rng.below(accounts)
        ));
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

fn option(args: &[String], name: &str, default: u64) -> u64 {
    args.windows(2)
        .find(|a| a[0] == name)
        .map(|a| a[1].parse().unwrap())
        .unwrap_or(default)
}

fn prices() -> PriceTable {
    let source: String = (0..500)
        .map(|i| format!("[[model]]\nmodel = \"gpt-5.{i}\"\nin = 2.5\nout = 10\ncache = 0.25\n"))
        .collect();
    PriceTable::parse(&source).unwrap()
}

fn measure<T>(name: &str, mut query: impl FnMut() -> T) {
    let mut times = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        std::hint::black_box(query());
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    println!("{}", serde_json::json!({"bench": name, "ms_runs": times}));
}

fn set_req_index(p: &str, v: &str) {
    let c = rusqlite::Connection::open(p).unwrap();
    c.execute_batch("DROP INDEX IF EXISTS requests_account")
        .unwrap();
    let sql = match v {
        "account_id" => "CREATE INDEX requests_account ON requests(account,id)",
        "account" => "CREATE INDEX requests_account ON requests(account)",
        "partial" => "CREATE INDEX requests_account ON requests(account) WHERE account IS NOT NULL",
        "none" => "",
        _ => panic!("index variant"),
    };
    c.execute_batch(sql).unwrap();
    checkpoint(&c);
}

fn checkpoint(c: &rusqlite::Connection) {
    let (busy, _, _): (i64, i64, i64) = c
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(busy, 0, "final checkpoint busy");
}

// Process-level Linux counters, not SSD physical writes. Missing /proc is allowed.
fn proc_io() -> Option<[u64; 3]> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let s = std::fs::read_to_string("/proc/self/io").ok()?;
    let get = |key: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix(key))?
            .trim()
            .parse()
            .ok()
    };
    Some([
        get("write_bytes:")?,
        get("wchar:")?,
        get("cancelled_write_bytes:")?,
    ])
}

fn io_delta(a: Option<[u64; 3]>, b: Option<[u64; 3]>) -> serde_json::Value {
    match a.zip(b) {
        Some((a, b)) => {
            serde_json::json!({"status":"available", "write_bytes":b[0].saturating_sub(a[0]), "wchar":b[1].saturating_sub(a[1]), "cancelled_write_bytes":b[2].saturating_sub(a[2])})
        }
        None => serde_json::json!({"status":"unavailable"}),
    }
}

fn finish_writer(
    r: std::sync::Arc<Recorder>,
    p: &std::path::Path,
    rows: usize,
    elapsed: Duration,
    io0: Option<[u64; 3]>,
    io1: Option<[u64; 3]>,
) -> serde_json::Value {
    let t = Instant::now();
    r.shutdown();
    let stats = r.stats();
    drop(r);
    let c = rusqlite::Connection::open(p).unwrap();
    checkpoint(&c);
    let shutdown_secs = t.elapsed().as_secs_f64();
    let io2 = proc_io();
    let persisted: i64 = c
        .query_row("SELECT count(*) FROM requests", [], |r| r.get(0))
        .unwrap();
    let database = std::fs::metadata(p).unwrap().len();
    let wal = std::fs::metadata(format!("{}-wal", p.display()))
        .map(|m| m.len())
        .unwrap_or(0);
    let stored = (database + wal) as f64 / rows.max(1) as f64;
    assert_eq!(persisted, rows as i64);
    assert_eq!(stats.write_errors, 0);
    assert_eq!(stats.dropped, 0);
    serde_json::json!({"rows":rows, "secs":elapsed.as_secs_f64(), "enqueue_to_flush_secs":elapsed.as_secs_f64(), "shutdown_final_checkpoint_secs":shutdown_secs,
        "rows_per_s":rows as f64 / elapsed.as_secs_f64(), "written":stats.written, "dropped":stats.dropped, "write_errors":stats.write_errors,
        "persisted_rows":persisted, "database_bytes":database, "wal_bytes":wal, "bytes_per_row":stored, "stored_bytes_per_row":stored,
        "bytes_per_row_basis":"database_plus_wal_after_final_checkpoint", "io_enqueue_to_flush":io_delta(io0,io1), "io_shutdown_final_checkpoint":io_delta(io1,io2),
        "sqlite_version":rusqlite::version(), "seed":42})
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
                let io0 = proc_io();
                let t = Instant::now();
                for i in 0..rows {
                    r.record_blocking(fake(&r, &mut rng, t0 + (i / 10) as i64, 8, 4));
                }
                r.flush_blocking().unwrap();
                let elapsed = t.elapsed();
                let io1 = proc_io();
                let mut result = finish_writer(r, &p, rows, elapsed, io0, io1);
                result["bench"] = "writer".into();
                result["batch"] = batch.into();
                result["queue"] = 65_536.into();
                result["flush_ms"] = 50.into();
                result["accounts"] = 8.into();
                result["models"] = 4.into();
                result["start_ms"] = t0.into();
                result["timing_boundary"] = "fake_generation_plus_enqueue_to_flush".into();
                println!("{result}");
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
            let recs: Vec<Record> = (0..rows)
                .map(|i| fake(&r, &mut rng, t0 + i as i64, 8, 4))
                .collect();
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
            let threads = option(&a, "--threads", 0);
            let quota_every = option(&a, "--quota-every", 0);
            assert!(
                !std::path::Path::new(&format!("{p}-wal")).exists()
                    && !std::path::Path::new(&format!("{p}-shm")).exists(),
                "gen refuses existing sidecars"
            );
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(p)
                .expect("gen requires a new database path");
            let r = Recorder::open(&cfg(p, 65_536, 4096, 50)).unwrap();
            r.set_prices(prices());
            let mut rng = Rng(99);
            let span = days * 86_400_000;
            let start = now_ms() - span;
            let io0 = proc_io();
            let t = Instant::now();
            for i in 0..rows {
                let ms = start + (i as i128 * span as i128 / rows as i128) as i64;
                let mut row = fake(&r, &mut rng, ms, accounts, models);
                if threads > 0 {
                    row.thread_id = Some(format!("session-{:016x}", i % threads));
                }
                if quota_every > 0 && i % quota_every == 0 {
                    row.quota = Some(QuotaObs {
                        plan: Some("plus".into()),
                        p_pct: Some(i as f64 / rows as f64 * 90.0),
                        s_pct: Some(i as f64 / rows as f64 * 90.0),
                        p_reset: Some((start + span) / 1000 + 3600),
                        s_reset: Some((start + span) / 1000 + 7 * 86400),
                        p_window_min: Some(10080),
                        s_window_min: Some(10080),
                        ..Default::default()
                    });
                }
                r.record_blocking(row);
            }
            r.flush_blocking().unwrap();
            let elapsed = t.elapsed();
            let io1 = proc_io();
            let mut result =
                finish_writer(r, std::path::Path::new(p), rows as usize, elapsed, io0, io1);
            result["bench"] = "gen".into();
            result["seed"] = 99.into();
            result["days"] = days.into();
            result["accounts"] = accounts.into();
            result["models"] = models.into();
            result["threads"] = threads.into();
            result["quota_every"] = quota_every.into();
            result["start_ms"] = start.into();
            result["db_bytes"] = result["database_bytes"].clone();
            result["timing_boundary"] = "fake_generation_plus_enqueue_to_flush".into();
            println!("{result}");
        }
        "query" => {
            let p = &a[2];
            if a.iter().any(|a| a == "--account-index") {
                rusqlite::Connection::open(p)
                    .unwrap()
                    .execute_batch(
                        "CREATE INDEX IF NOT EXISTS requests_account ON requests(account,id)",
                    )
                    .unwrap();
            }
            println!(
                "{}",
                serde_json::json!({"db_bytes": std::fs::metadata(p).unwrap().len()})
            );
            let r = Recorder::open(&cfg(p, 1024, 256, 50)).unwrap();
            r.set_prices(prices());
            let now = now_ms();
            for (name, ms) in [
                ("24h", 86_400_000i64),
                ("7d", 7 * 86_400_000),
                ("30d", 30 * 86_400_000),
            ] {
                let mut ts = vec![];
                let mut cnt = 0;
                for _ in 0..3 {
                    let t = Instant::now();
                    let s = rt
                        .block_on(r.summary(SummaryQuery {
                            from_ms: now - ms,
                            to_ms: now,
                            ..Default::default()
                        }))
                        .unwrap();
                    ts.push(t.elapsed().as_millis() as u64);
                    cnt = s.total.count;
                }
                println!("{{\"bench\":\"summary\",\"range\":\"{name}\",\"rows_in_range\":{cnt},\"ms_runs\":{ts:?},\"ms_min\":{}}}", ts.iter().min().unwrap());
            }
            let t = Instant::now();
            let rows = rt
                .block_on(r.recent(RecentQuery {
                    limit: 100,
                    ..Default::default()
                }))
                .unwrap();
            println!(
                "{{\"bench\":\"recent\",\"limit\":100,\"got\":{},\"ms\":{:.2}}}",
                rows.len(),
                t.elapsed().as_secs_f64() * 1e3
            );
            let t = Instant::now();
            let rows = rt
                .block_on(r.recent(RecentQuery {
                    limit: 100,
                    filters: ciallopa_usage::Filters {
                        account: Some("codex-00000003-user3@example.com-plus".into()),
                        ..Default::default()
                    },
                    errors_only: true,
                    ..Default::default()
                }))
                .unwrap();
            println!(
                "{{\"bench\":\"recent_filtered\",\"got\":{},\"ms\":{:.2}}}",
                rows.len(),
                t.elapsed().as_secs_f64() * 1e3
            );
            measure("quota", || rt.block_on(r.quota()).unwrap());
            measure("quota_history", || {
                rt.block_on(r.quota_history(None, now - 30 * 86_400_000, now))
                    .unwrap()
            });
            measure("recent_thread", || {
                rt.block_on(r.recent(RecentQuery {
                    limit: 100,
                    thread: Some("session-0000000000000001".into()),
                    ..Default::default()
                }))
                .unwrap()
            });
            measure("resolve_thread", || {
                rt.block_on(r.resolve_thread("0000000000000001".into()))
                    .unwrap()
            });
            r.shutdown();
            drop(r);
            println!(
                "{}",
                serde_json::json!({"db_bytes_after_close":std::fs::metadata(p).unwrap().len()})
            );
        }
        "purge" => {
            let p = PathBuf::from(&a[2]);
            let drop_days: i64 = a[3].parse().unwrap();
            let chunk: u64 = a[4].parse().unwrap();
            let (_, before) = integrity_check(&p).unwrap();
            // oldest row time
            let r = Recorder::open(&cfg(a[2].as_str(), 16, 16, 50)).unwrap();
            let oldest = rt
                .block_on(r.recent(RecentQuery {
                    limit: 1,
                    before_ms: Some(i64::MAX >> SEQ_BITS),
                    ..Default::default()
                }))
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
        "ablate" => {
            let dir = PathBuf::from(&a[2]);
            let rows: usize = a[3].parse().unwrap();
            let accounts: u64 = a[4].parse().unwrap();
            let batch: usize = a[5].parse().unwrap();
            let variant = &a[6];
            let p = dir.join(format!("ablate-{variant}.db"));
            // Exclusive directory creation rejects all existing paths, including symlinks.
            std::fs::create_dir(&dir).expect("ablate requires a new directory");
            let ps = p.to_str().unwrap();
            Recorder::open(&cfg(ps, 65_536, batch, 50))
                .unwrap()
                .shutdown();
            set_req_index(ps, variant);
            let r = Recorder::open(&cfg(ps, 65_536, batch, 50)).unwrap();
            let mut rng = Rng(42);
            let t0 = now_ms() - 7 * 86_400_000;
            let recs: Vec<Record> = (0..rows)
                .map(|i| {
                    let mut x = fake(&r, &mut rng, t0 + i as i64 * 3, accounts, 8);
                    x.thread_id = Some(format!("session-{:016x}", i % 5000));
                    x
                })
                .collect();
            let io0 = proc_io();
            let t = Instant::now();
            for x in recs {
                r.record_blocking(x);
            }
            r.flush_blocking().unwrap();
            let el = t.elapsed();
            let io1 = proc_io();
            let mut result = finish_writer(r, &p, rows, el, io0, io1);
            result["bench"] = "ablate".into();
            result["variant"] = variant.clone().into();
            result["accounts"] = accounts.into();
            result["models"] = 8.into();
            result["batch"] = batch.into();
            result["queue"] = 65_536.into();
            result["flush_ms"] = 50.into();
            result["start_ms"] = t0.into();
            result["threads"] = 5000.into();
            result["timing_boundary"] = "pregenerated_records_enqueue_to_flush".into();
            println!("{result}");
        }
        "plan" => {
            let c = rusqlite::Connection::open_with_flags(
                &a[2],
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            let v: String = c
                .query_row("SELECT sqlite_version()", [], |r| r.get(0))
                .unwrap();
            println!("sqlite {v}");
            for (name, sql) in [
                ("load_cycle", "SELECT r.id, r.in_tok FROM requests r WHERE r.account=?1 AND r.id>=?2 AND r.id<?3 ORDER BY r.id"),
                ("recent_account", "SELECT id FROM requests WHERE id < ?2 AND account = ?1 AND ?3 > 0 ORDER BY id DESC LIMIT 100"),
                ("summary_account", "SELECT count(*) FROM requests WHERE id >= ?2 AND id < ?3 AND account = ?1"),
            ] {
                let mut st = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
                let rows: Vec<String> = st
                    .query_map(rusqlite::params![1, 0, i64::MAX], |r| r.get::<_, String>(3))
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                println!("{name}: {rows:?}");
            }
            for index in ["requests_account", "account"] {
                let mut st = c
                    .prepare("SELECT name FROM pragma_index_xinfo(?1)")
                    .unwrap();
                let cols: Vec<Option<String>> = st
                    .query_map([index], |r| r.get(0))
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                println!("index_xinfo {index}: {cols:?}");
            }
        }
        "open" => {
            let t = Instant::now();
            let r = Recorder::open(&cfg(&a[2], 1024, 256, 50)).unwrap();
            let open_secs = t.elapsed().as_secs_f64();
            r.shutdown();
            drop(r);
            let c = rusqlite::Connection::open(&a[2]).unwrap();
            checkpoint(&c);
            let version: i64 = c
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
            let hwm = std::fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|s| {
                    s.lines().find_map(|l| {
                        l.strip_prefix("VmHWM:")?
                            .split_whitespace()
                            .next()?
                            .parse::<u64>()
                            .ok()
                    })
                });
            println!(
                "{}",
                serde_json::json!({"bench":"open", "open_secs":open_secs, "secs":t.elapsed().as_secs_f64(), "user_version":version, "sqlite_version":rusqlite::version(), "vm_hwm_kib":hwm})
            );
        }
        "check" => {
            let (s, n) = integrity_check(&PathBuf::from(&a[2])).unwrap();
            println!("{{\"integrity\":\"{s}\",\"rows\":{n}}}");
        }
        _ => panic!("unknown"),
    }
}
