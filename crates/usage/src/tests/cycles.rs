//! Cycle boundaries: one real reset makes exactly one boundary (synthetic Pro weekly timeline).
use super::*;
use rusqlite::params;

const WEEK_S: i64 = 7 * 86_400;

fn pro(pct: f64, reset: i64) -> QuotaObs {
    QuotaObs {
        plan: Some("pro".into()),
        p_pct: Some(pct),
        p_reset: Some(reset),
        p_window_min: Some(10080),
        ..Default::default()
    }
}

/// A request that starts and finishes at `ms`.
fn req(r: &Recorder, ms: i64, pct: f64, reset: i64) {
    let id = r.ids.next_at(ms);
    finish(r, id, pct, reset);
}

/// Like the proxy: the id is taken when a request starts (`ids.next()` in forward/usage.rs),
/// the record (with the quota headers of its response) is written when it finishes.
fn finish(r: &Recorder, id: i64, pct: f64, reset: i64) {
    let mut x = Record::new(id, Transport::Ws, "responses");
    x.account = Some("pro".into());
    x.model = Some("gpt-6.1-sol".into());
    x.status = 200;
    x.duration_ms = 1000;
    x.input_tokens = Some(80_000);
    x.cached_tokens = Some(60_000);
    x.output_tokens = Some(2_000);
    x.quota = Some(pro(pct, reset));
    assert!(r.record_blocking(x));
}

fn events(d: &Path) -> Vec<(String, String)> {
    let c = crate::schema::open_ro(&d.join("u.db")).unwrap();
    let mut st = c
        .prepare("SELECT window, kind FROM quota_reset_events ORDER BY at_ms")
        .unwrap();
    st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// After a manual card, concurrent sessions finish out of start order (a request that started
/// earlier reports a lower percent but is written later), and a refresh reports a fractional
/// percent next to the integer response headers. None of that is a reset or a limit change.
#[tokio::test]
async fn one_card_makes_one_cycle_boundary() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    let min = 60_000;
    let base = now_ms() - 120 * min;
    let r1 = base / 1000 + 3 * 86_400;
    for (i, pct) in [10.0, 40.0, 70.0, 99.0].into_iter().enumerate() {
        req(&r, base + i as i64 * 5 * min, pct, r1);
    }
    r.flush_blocking().unwrap();
    // Manual card at base+30min: used drops to 0 and a new week starts.
    let card_ms = base + 30 * min;
    let r2 = card_ms / 1000 + WEEK_S;
    {
        let c = crate::schema::open_write(&d.path().join("u.db")).unwrap();
        c.execute(
            "INSERT INTO reset_consume(account,redeem_request_id,key,started_ms,finished_ms,status,code) \
             VALUES('pro','rid-1','k1',?1,?2,'done','reset')",
            params![card_ms - 2_000, card_ms],
        )
        .unwrap();
    }
    // Ids in start order; records in finish order. `long` started just before the card and
    // finished after it, so its response still reports the old window.
    let long = r.ids.next_at(card_ms - 60_000);
    let a = r.ids.next_at(card_ms + 10_000);
    let b = r.ids.next_at(card_ms + min);
    let c38 = r.ids.next_at(base + 38 * min);
    let c40 = r.ids.next_at(base + 40 * min);
    let c45 = r.ids.next_at(base + 45 * min);
    let c48 = r.ids.next_at(base + 48 * min);
    let c50 = r.ids.next_at(base + 50 * min);
    finish(&r, a, 0.0, r2);
    finish(&r, long, 99.0, r1);
    finish(&r, b, 1.0, r2);
    // Concurrent sessions: a request that started earlier finishes later.
    finish(&r, c40, 3.0, r2);
    finish(&r, c38, 2.0, r2);
    finish(&r, c45, 3.0, r2);
    finish(&r, c50, 4.0, r2);
    finish(&r, c48, 3.0, r2);
    r.flush_blocking().unwrap();
    // A refresh with a fractional percent, then integer response headers.
    assert!(r.observe_quota("pro", "manual", pro(4.6, r2)));
    r.flush_blocking().unwrap();
    req(&r, now_ms() + 1_000, 4.0, r2);
    r.flush_blocking().unwrap();

    let ev = events(d.path());
    assert_eq!(
        ev,
        vec![("primary".to_string(), "manual_card".to_string())],
        "one card, one boundary"
    );
    let h = r
        .quota_history(Some("pro"), 0, now_ms() + 10_000)
        .await
        .unwrap();
    let closed: Vec<_> = h[0].cycles.iter().filter(|c| !c.open).collect();
    assert_eq!(closed.len(), 1, "{closed:?}");
    assert_eq!(h[0].cycles.iter().filter(|c| c.open).count(), 1);
    r.shutdown();
}

/// A real limit change (percent drops by a lot, resets_at unchanged) is still recorded as an event,
/// but the window goes on: no separate cycle row (it would be a prefix of the open one).
#[tokio::test]
async fn large_drop_without_reset_move_is_still_a_limit_change() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    let base = now_ms() - 60 * 60_000;
    let reset = base / 1000 + 3 * 86_400;
    req(&r, base, 30.0, reset);
    req(&r, base + 60_000, 42.0, reset);
    req(&r, base + 120_000, 21.0, reset);
    r.flush_blocking().unwrap();
    assert_eq!(
        events(d.path()),
        vec![("primary".to_string(), "limit_change".to_string())]
    );
    let h = r
        .quota_history(Some("pro"), 0, now_ms() + 10_000)
        .await
        .unwrap();
    assert!(h[0].cycles.iter().all(|c| c.open), "{:?}", h[0].cycles);
    r.shutdown();
}
