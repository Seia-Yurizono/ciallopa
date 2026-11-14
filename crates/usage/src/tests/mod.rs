#![allow(clippy::unwrap_used)]
use super::*;
use crate::problem::{failure_class, LIFECYCLE_ERRORS};

mod migrate;
mod occupancy;
mod quota;
mod records;
mod refusals;
mod retention;
mod summary;

pub(crate) fn cfg(dir: &Path) -> UsageConfig {
    UsageConfig {
        path: dir.join("u.db"),
        ..UsageConfig::default()
    }
}

fn rec(r: &Recorder, ms: i64, acct: &str, model: &str, ttft: u32, dur: u32, out: i64) -> i64 {
    let id = r.ids.next_at(ms);
    let mut x = Record::new(id, Transport::Sse, "responses");
    x.account = Some(acct.into());
    x.model = Some(model.into());
    x.effort = Some("low".into());
    x.status = 200;
    x.ttft_ms = Some(ttft);
    x.duration_ms = dur;
    x.input_tokens = Some(10);
    x.output_tokens = Some(out);
    x.cached_tokens = Some(4);
    x.reasoning_tokens = Some(2);
    assert!(r.record(x));
    id
}

fn qrec(r: &Recorder, ms: i64, acct: &str, key: &str, out: i64, p: f64, reset: i64) {
    let mut x = Record::new(r.ids.next_at(ms), Transport::Sse, "responses");
    x.account = Some(acct.into());
    x.model = Some("m".into());
    x.key = Some(key.into());
    x.status = 200;
    x.input_tokens = Some(1000);
    x.cached_tokens = Some(500);
    x.output_tokens = Some(out);
    x.quota = Some(QuotaObs {
        plan: Some("plus".into()),
        p_pct: Some(p),
        p_reset: Some(reset),
        p_window_min: Some(300),
        ..Default::default()
    });
    assert!(r.record(x));
}

fn store_day(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let p = |d: &str| prices::parse_date(d).unwrap();
    // find via parse_date round-trip over a small range
    (2020..2100)
        .flat_map(|y| {
            (1..=12).flat_map(move |m| (1..=31).map(move |d| format!("{y:04}-{m:02}-{d:02}")))
        })
        .find(|s| prices::parse_date(s).is_some_and(|x| x == days) && p(s) == days)
        .unwrap()
}
