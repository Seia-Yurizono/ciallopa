//! Process memory for the dashboard: RSS, allocator stats, the buffered-body budget and
//! WebSocket buffers. In memory only (a small ring of samples), nothing is written to disk.
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use ciallopa_forward::AppState;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// One sample every 10 s, the last 15 minutes.
pub const SAMPLE_EVERY: Duration = Duration::from_secs(10);
pub const SAMPLES: usize = 90;

static JEMALLOC: AtomicBool = AtomicBool::new(false);

/// Called by the binary that installs jemalloc as its global allocator (tests and other
/// embedders use the system allocator, whose numbers jemalloc cannot report).
pub fn jemalloc_is_global() {
    JEMALLOC.store(true, Ordering::Relaxed);
}

#[derive(Clone, Copy, Serialize, Default, Debug, PartialEq)]
pub struct Sample {
    pub at_ms: u64,
    /// Resident set size; None where `/proc/self/status` is unavailable (not Linux).
    pub rss_bytes: Option<u64>,
    pub allocated_bytes: Option<u64>,
    pub resident_bytes: Option<u64>,
    pub buffered_body_bytes: u64,
    pub ws_connections: usize,
    pub ws_bytes: u64,
}

#[derive(Default)]
struct Proc {
    rss: Option<u64>,
    hwm: Option<u64>,
}

/// `VmRSS` / `VmHWM` of this process (Linux, including Docker Desktop on macOS: the
/// container runs in a Linux VM).
fn proc_status() -> Proc {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else {
        return Proc::default();
    };
    let kb = |key: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|v| v.trim().strip_suffix("kB"))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|v| v * 1024)
    };
    Proc {
        rss: kb("VmRSS:"),
        hwm: kb("VmHWM:"),
    }
}

/// jemalloc `stats.allocated` / `stats.resident` after an epoch refresh.
#[cfg(not(target_env = "msvc"))]
fn jemalloc() -> Option<(u64, u64)> {
    use std::ffi::{c_void, CStr};
    use tikv_jemalloc_sys::mallctl;
    if !JEMALLOC.load(Ordering::Relaxed) {
        return None;
    }
    fn read(name: &CStr) -> Option<u64> {
        let mut v: usize = 0;
        let mut len = std::mem::size_of::<usize>();
        // SAFETY: `name` is NUL-terminated; `v` / `len` describe a writable size_t, the
        // type jemalloc documents for these keys.
        let rc = unsafe {
            mallctl(
                name.as_ptr(),
                (&raw mut v).cast::<c_void>(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        (rc == 0).then_some(v as u64)
    }
    let mut epoch: u64 = 1;
    // SAFETY: "epoch" takes a uint64_t to write; no old value is read.
    let rc = unsafe {
        mallctl(
            c"epoch".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (&raw mut epoch).cast::<c_void>(),
            std::mem::size_of::<u64>(),
        )
    };
    if rc != 0 {
        return None;
    }
    Some((read(c"stats.allocated")?, read(c"stats.resident")?))
}

#[cfg(target_env = "msvc")]
fn jemalloc() -> Option<(u64, u64)> {
    None
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn sample(st: &AppState) -> (Sample, Proc) {
    let p = proc_status();
    let je = jemalloc();
    let d = &st.upstream.drain;
    (
        Sample {
            at_ms: now_ms(),
            rss_bytes: p.rss,
            allocated_bytes: je.map(|j| j.0),
            resident_bytes: je.map(|j| j.1),
            buffered_body_bytes: d.body_bytes(),
            ws_connections: d.ws_connections(),
            ws_bytes: d.ws_bytes(),
        },
        p,
    )
}

fn ring() -> &'static Mutex<VecDeque<Sample>> {
    static R: OnceLock<Mutex<VecDeque<Sample>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(VecDeque::with_capacity(SAMPLES)))
}

fn push(s: Sample) {
    let mut r = ring().lock().unwrap_or_else(|e| e.into_inner());
    if r.len() == SAMPLES {
        r.pop_front();
    }
    r.push_back(s);
}

/// Start the 10 s sampler once per process (a few syscalls; no disk writes).
pub fn spawn_sampler(st: AppState) {
    static STARTED: AtomicBool = AtomicBool::new(false);
    if tokio::runtime::Handle::try_current().is_err() || STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(SAMPLE_EVERY);
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            push(sample(&st).0);
        }
    });
}

async fn memory(State(st): State<AppState>) -> impl IntoResponse {
    let (now, p) = sample(&st);
    let cfg = st.config.load();
    let history: Vec<Sample> = ring()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .copied()
        .collect();
    Json(serde_json::json!({
        "now": now,
        "rss_peak_bytes": p.hwm,
        "jemalloc": now.allocated_bytes.is_some(),
        "buffered_body_limit_bytes": cfg.max_buffered_body_mb.saturating_mul(1 << 20),
        "ws_max_connections": cfg.ws_max_connections,
        "ws_max_message_bytes": cfg.ws_max_message_mb.saturating_mul(1 << 20),
        "sample_every_secs": SAMPLE_EVERY.as_secs(),
        "history": history,
    }))
}

pub fn routes(st: AppState) -> Router {
    Router::new()
        .route("/admin/v1/memory", get(memory))
        .with_state(st)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_the_last_samples() {
        for i in 0..(SAMPLES as u64 + 5) {
            push(Sample {
                at_ms: i,
                ..Default::default()
            });
        }
        let r = ring().lock().unwrap();
        assert_eq!(r.len(), SAMPLES);
        assert_eq!(r.front().unwrap().at_ms, 5);
    }

    #[test]
    fn proc_status_reads_rss_on_linux() {
        let p = proc_status();
        if cfg!(target_os = "linux") {
            assert!(p.rss.unwrap() > 0 && p.hwm.unwrap() >= p.rss.unwrap());
        }
    }

    #[test]
    fn jemalloc_reports_only_when_it_is_the_global_allocator() {
        assert_eq!(jemalloc(), None);
        #[cfg(not(target_env = "msvc"))]
        {
            jemalloc_is_global();
            let (a, r) = jemalloc().expect("stats enabled");
            assert!(r >= a, "{a} {r}");
            JEMALLOC.store(false, Ordering::Relaxed);
        }
    }
}
