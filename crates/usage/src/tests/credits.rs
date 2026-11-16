//! Purchased credits (点数) after a window is full: synthetic Pro / Plus timelines.
//! Pro weekly capacity 1700 included credits; Plus 5h 340 + weekly 3400.
use super::*;

const MODEL: &str = "gpt-6.1-sol";
const PRO_CAP: f64 = 1700.0;

/// Standard credits for gpt-6.1-sol: in 50 / cached 2.5 / out 250 per 1M.
fn std_credits(inp: i64, cached: i64, out: i64) -> f64 {
    ((inp - cached) as f64 * 50.0 + cached as f64 * 2.5 + out as f64 * 250.0) / 1e6
}

struct Lcg(u64);
impl Lcg {
    fn f(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 11) as f64) / ((1u64 << 53) as f64)
    }
    fn tokens(&mut self) -> (i64, i64, i64) {
        let inp = 60_000 + (self.f() * 80_000.0) as i64;
        let cached = (inp as f64 * (0.6 + 0.3 * self.f())) as i64;
        let out = 800 + (self.f() * 2400.0) as i64;
        (inp, cached, out)
    }
}

fn send(r: &Recorder, ms: i64, acct: &str, (inp, cached, out): (i64, i64, i64), q: QuotaObs) {
    let mut x = Record::new(r.ids.next_at(ms), Transport::Ws, "responses");
    x.account = Some(acct.into());
    x.model = Some(MODEL.into());
    x.status = 200;
    x.duration_ms = 1000;
    x.input_tokens = Some(inp);
    x.cached_tokens = Some(cached);
    x.output_tokens = Some(out);
    x.quota = Some(q);
    assert!(r.record_blocking(x));
}

fn pro(pct: f64, reset: i64) -> QuotaObs {
    QuotaObs {
        plan: Some("pro".into()),
        p_pct: Some(pct),
        p_reset: Some(reset),
        p_window_min: Some(10080),
        ..Default::default()
    }
}

struct ProRun {
    closed: CycleRow,
    overage: f64,
    /// API list price (with `PRICES`) of the included requests and of the credit-funded ones.
    included_usd: f64,
    over_usd: f64,
}

/// Synthetic list price for MODEL, USD per 1M tokens.
const PRICES: &str = "[[model]]\nmodel=\"gpt-6.1-sol\"\nin=5.0\ncache=0.5\nout=25.0\n";
fn list_usd((inp, cached, out): (i64, i64, i64)) -> f64 {
    ((inp - cached) as f64 * 5.0 + cached as f64 * 0.5 + out as f64 * 25.0) / 1e6
}

/// Included usage up to 100%, then `overage_frac × capacity` funded by purchased credits.
/// `refresh_every` > 0 adds a management refresh (unchanged 100%) every N credit requests.
async fn pro_run(overage_frac: f64, refresh_every: usize) -> ProRun {
    pro_run_with(overage_frac, refresh_every, false).await
}

async fn pro_run_with(overage_frac: f64, refresh_every: usize, priced: bool) -> ProRun {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    if priced {
        r.set_prices(crate::prices::PriceTable::parse(PRICES).unwrap());
    }
    let (mut included_usd, mut over_usd) = (0.0, 0.0);
    let reset = now_ms() / 1000 + 100;
    let mut t = now_ms() - 3 * 86_400_000;
    let mut rng = Lcg(7);
    let mut cum = 0.0;
    loop {
        let tok = rng.tokens();
        let c = std_credits(tok.0, tok.1, tok.2);
        if cum + c > PRO_CAP {
            break;
        }
        let pct = (cum / PRO_CAP * 100.0).floor();
        cum += c;
        included_usd += list_usd(tok);
        send(&r, t, "pro", tok, pro(pct, reset));
        t += 30_000;
    }
    send(&r, t, "pro", (120_000, 80_000, 3000), pro(100.0, reset));
    t += 30_000;
    let mut overage = 0.0;
    let mut n = 0;
    while overage < overage_frac * PRO_CAP {
        let tok = rng.tokens();
        overage += std_credits(tok.0, tok.1, tok.2);
        over_usd += list_usd(tok);
        send(&r, t, "pro", tok, pro(100.0, reset));
        t += 30_000;
        n += 1;
        if refresh_every > 0 && n % refresh_every == 0 {
            r.flush_blocking().unwrap();
            assert!(r.observe_quota("pro", "manual", pro(100.0, reset)));
            r.flush_blocking().unwrap();
        }
    }
    // Natural weekly reset closes the cycle.
    send(
        &r,
        now_ms(),
        "pro",
        (1000, 0, 10),
        pro(0.0, reset + 7 * 86_400),
    );
    r.flush_blocking().unwrap();
    let h = r.quota_history(Some("pro"), 0, now_ms() + 1).await.unwrap();
    let closed = h[0]
        .cycles
        .iter()
        .find(|c| !c.open && c.window == "primary")
        .expect("closed weekly cycle")
        .clone();
    r.shutdown();
    ProRun {
        closed,
        overage,
        included_usd,
        over_usd,
    }
}

fn near(x: Option<f64>, truth: f64, tol: f64) -> bool {
    x.is_some_and(|x| (x / truth - 1.0).abs() <= tol)
}

#[tokio::test]
async fn credit_funded_requests_do_not_inflate_the_cycle() {
    let run = pro_run(0.3, 0).await;
    let c = &run.closed;
    assert!(near(c.capacity_credits, PRO_CAP, 0.02), "{c:?}");
    // Before the fix the credit-funded 30% landed in 「已用」(≈130%).
    assert!(
        near(c.credits_used, PRO_CAP, 0.02),
        "used {:?}",
        c.credits_used
    );
    // The credit-funded part is reported separately as 点数.
    assert!(
        near(c.over_credits, run.overage, 0.01),
        "{c:?} vs {}",
        run.overage
    );
    assert!(c.over_api_usd.is_none(), "no price table installed");
}

/// The cycle's API price is the price of its included (额度) requests only; the credit-funded
/// requests have their own price in over_api_usd and are never added to it.
#[tokio::test]
async fn cycle_api_price_excludes_credit_funded_requests() {
    let run = pro_run_with(0.3, 0, true).await;
    let c = &run.closed;
    assert!(
        near(c.over_api_usd, run.over_usd, 0.03),
        "{c:?} vs {}",
        run.over_usd
    );
    // Before the fix api_cost_usd was included + credit-funded (≈ 1.3× here).
    assert!(
        near(c.api_cost_usd, run.included_usd, 0.03),
        "api {:?} vs included {} (+ over {})",
        c.api_cost_usd,
        run.included_usd,
        run.over_usd
    );
}

#[tokio::test]
async fn refreshes_at_full_keep_the_estimate_bounded() {
    let run = pro_run(0.3, 10).await;
    let c = &run.closed;
    assert!(run.overage > 0.0);
    assert!(near(c.capacity_credits, PRO_CAP, 0.02), "{c:?}");
    // Before the fix: `fallback`, coverage gap (「可能有其他端用量」), no upper bound.
    assert_eq!(c.confidence, "ok", "{c:?}");
    assert!(!c.coverage_gap, "{c:?}");
    assert!(c.capacity_hi.is_some(), "{c:?}");
}

/// Plus: five 5h cycles; each runs `over5h × 5h capacity` on credits after the 5h window is
/// full while the weekly percent stays put. Returns the weekly open estimate.
async fn plus_weekly(over5h: f64, inclusive: bool) -> Estimate {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    let (c5, c7) = (340.0, 3400.0);
    let now_s = now_ms() / 1000;
    let week_reset = now_s + 2 * 86_400;
    let start = now_ms() - 30 * 3_600_000;
    let mut rng = Lcg(11);
    let mut wk = 0.0f64;
    for k in 0..5i64 {
        let t0 = start + k * 5 * 3_600_000;
        let p_reset = t0 / 1000 + 5 * 3600;
        let mut t = t0;
        let mut h5 = 0.0f64;
        let q = |p: f64, s: f64| QuotaObs {
            plan: Some("plus".into()),
            p_pct: Some(p),
            p_reset: Some(p_reset),
            p_window_min: Some(300),
            s_pct: Some(s),
            s_reset: Some(week_reset),
            s_window_min: Some(10080),
            ..Default::default()
        };
        loop {
            let tok = rng.tokens();
            let c = std_credits(tok.0, tok.1, tok.2);
            if h5 + c > c5 {
                break;
            }
            let (b5, bw) = (h5, wk);
            h5 += c;
            wk += c;
            let (u5, uw) = if inclusive { (h5, wk) } else { (b5, bw) };
            send(
                &r,
                t,
                "plus",
                tok,
                q((u5 / c5 * 100.0).floor(), (uw / c7 * 100.0).floor()),
            );
            t += 30_000;
        }
        let mut ov = 0.0;
        while ov < over5h * c5 {
            let tok = rng.tokens();
            ov += std_credits(tok.0, tok.1, tok.2);
            send(&r, t, "plus", tok, q(100.0, (wk / c7 * 100.0).floor()));
            t += 30_000;
        }
    }
    r.flush_blocking().unwrap();
    let q = r.quota().await.unwrap();
    r.shutdown();
    q[0].secondary.estimate.clone()
}

#[tokio::test]
async fn plus_weekly_ignores_5h_credit_runs() {
    let e = plus_weekly(0.5, false).await;
    // Before the fix ≈ −80% (weekly percent frozen while credits paid for the 5h overage).
    assert!(near(e.capacity_credits, 3400.0, 0.05), "{e:?}");
}

#[tokio::test]
async fn plus_weekly_dense_snapshots_inclusive_headers() {
    let e = plus_weekly(0.0, true).await;
    // Equal-percent runs from the 5h window used to collapse the adjacent fallback.
    assert!(near(e.capacity_credits, 3400.0, 0.05), "{e:?}");
}

fn with_credits(pct: f64, reset: i64, flags: i64, balance: f64) -> QuotaObs {
    let mut q = pro(pct, reset);
    q.cr_flags = Some(flags);
    q.cr_balance = Some(balance);
    q
}

fn obs_rows(d: &Path) -> Vec<(i64, f64, Option<i64>, Option<f64>)> {
    let c = crate::schema::open_ro(&d.join("u.db")).unwrap();
    let mut st = c
        .prepare("SELECT id, p_pct, cr_flags, cr_balance FROM quota_obs ORDER BY id")
        .unwrap();
    st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[tokio::test]
async fn credit_balance_writes_are_throttled() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    let reset = now_ms() / 1000 + 86_400;
    let t0 = now_ms() - 3_600_000;
    let tok = (1000, 0, 10);
    // At 100%, every request spends credits: one balance per request, 5 s apart.
    let mut bal = 500.0;
    for i in 0..60 {
        send(
            &r,
            t0 + i * 5_000,
            "pro",
            tok,
            with_credits(100.0, reset, CR_HAS, bal),
        );
        bal -= 0.5;
    }
    r.flush_blocking().unwrap();
    let rows = obs_rows(d.path());
    // First row, then at most one per minute, and only after ≥ max(1, 1%) = 5 credits.
    assert!(rows.len() <= 6, "{rows:?}");
    assert!(rows.len() >= 2, "{rows:?}");
    for w in rows.windows(2) {
        assert!((w[1].0 - w[0].0) >> SEQ_BITS >= 60_000, "{rows:?}");
        assert!(w[0].3.unwrap() - w[1].3.unwrap() >= 5.0, "{rows:?}");
    }
    // A flag change is written at once; a response without credit fields keeps the state.
    let t1 = t0 + 301_000;
    send(&r, t1, "pro", tok, with_credits(100.0, reset, 0, 0.0));
    send(&r, t1 + 1, "pro", tok, pro(100.0, reset));
    send(&r, t1 + 2, "pro", tok, pro(99.0, reset + 7 * 86_400));
    r.flush_blocking().unwrap();
    let rows = obs_rows(d.path());
    let n = rows.len();
    assert_eq!(rows[n - 2].2, Some(0), "flag change written");
    assert_eq!(rows[n - 1].1, 99.0);
    assert_eq!(
        (rows[n - 1].2, rows[n - 1].3),
        (Some(0), Some(0.0)),
        "state inherited"
    );

    let q = r.quota().await.unwrap();
    let cr = q[0].credits.as_ref().unwrap();
    assert!(!cr.has_credits && !cr.unlimited);
    assert_eq!(cr.balance, Some(0.0));
    r.shutdown();
}

#[tokio::test]
async fn estimate_reset_starts_over_and_undo_restores() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    let reset = now_ms() / 1000 + 100;
    let mut t = now_ms() - 3 * 86_400_000;
    let mut rng = Lcg(3);
    // One closed cycle (natural), then a polluted open one.
    let mut cum = 0.0;
    while cum < 900.0 {
        let tok = rng.tokens();
        let pct = (cum / PRO_CAP * 100.0).floor();
        cum += std_credits(tok.0, tok.1, tok.2);
        send(&r, t, "pro", tok, pro(pct, reset));
        t += 30_000;
    }
    let reset2 = reset + 7 * 86_400;
    send(
        &r,
        now_ms() - 3_600_000,
        "pro",
        (1000, 0, 10),
        pro(0.0, reset2),
    );
    let mut t = now_ms() - 3_500_000;
    for i in 0..40 {
        let tok = rng.tokens();
        send(&r, t, "pro", tok, pro(f64::from(i), reset2));
        t += 1_000;
    }
    r.flush_blocking().unwrap();
    let before = r.quota().await.unwrap()[0].primary.estimate.clone();
    assert!(before.credits_used.unwrap() > 0.0);
    assert!(before.since_ms.is_none());

    assert!(matches!(
        r.reset_estimate("nobody", None, None).await,
        Err(UsageError::NotFound("account"))
    ));
    let done = r
        .reset_estimate("pro", Some(WindowKind::Primary), Some("admin".into()))
        .await
        .unwrap();
    assert_eq!(done.windows, vec!["primary"]);
    assert_eq!(done.archived_cycles, 1);
    let after = r.quota().await.unwrap()[0].primary.estimate.clone();
    assert_eq!(after.since_ms, Some(done.from_ms));
    assert_eq!(after.credits_used, Some(0.0), "{after:?}");
    // The latest snapshot is re-observed at the reset as the new origin.
    assert_eq!(after.delta_pct, Some(0.0), "{after:?}");
    assert_eq!(after.sample_from_ms, Some(done.from_ms), "{after:?}");

    // New usage after the reset counts again.
    send(&r, now_ms(), "pro", (100_000, 0, 1000), pro(40.0, reset2));
    r.flush_blocking().unwrap();
    let h = r.quota_history(Some("pro"), 0, now_ms() + 1).await.unwrap();
    let closed: Vec<_> = h[0].cycles.iter().filter(|c| !c.open).collect();
    assert_eq!(closed.len(), 1);
    assert!(closed[0].archived && closed[0].trend.is_none());
    let open = h[0].cycles.iter().find(|c| c.open).unwrap();
    assert!(open.credits_used.unwrap() > 0.0 && open.credits_used.unwrap() < 10.0);
    assert_eq!(h[0].epochs.len(), 1);
    assert!(h[0].epochs[0].active && h[0].epochs[0].id == done.epoch_id);

    assert_eq!(
        r.undo_estimate_reset("pro", done.epoch_id).await.unwrap(),
        1
    );
    assert!(matches!(
        r.undo_estimate_reset("pro", done.epoch_id).await,
        Err(UsageError::NotFound("reset"))
    ));
    let undone = r.quota().await.unwrap()[0].primary.estimate.clone();
    assert!(undone.since_ms.is_none());
    assert!(undone.credits_used.unwrap() > before.credits_used.unwrap());
    let h = r.quota_history(Some("pro"), 0, now_ms() + 1).await.unwrap();
    assert!(h[0].cycles.iter().all(|c| !c.archived));
    assert!(!h[0].epochs[0].active && h[0].epochs[0].undone_ms.is_some());
    r.shutdown();
}

#[tokio::test]
async fn estimate_reset_all_windows_shares_one_id() {
    let d = tempfile::tempdir().unwrap();
    let r = Recorder::open_as(&cfg(d.path()), "host-a").unwrap();
    send(
        &r,
        now_ms(),
        "plus",
        (1000, 0, 10),
        pro(5.0, now_ms() / 1000 + 600),
    );
    r.flush_blocking().unwrap();
    let done = r.reset_estimate("plus", None, None).await.unwrap();
    assert_eq!(done.windows, vec!["primary", "secondary"]);
    assert_eq!(
        r.undo_estimate_reset("plus", done.epoch_id).await.unwrap(),
        2
    );
    r.shutdown();
}
