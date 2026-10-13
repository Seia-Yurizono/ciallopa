//! Ports of CPA selector_test.go / selector_subagent_affinity_test.go /
//! session/info_test.go / codex_quota_test.go / codex_executor_retry_test.go /
//! conductor cooldown tests (table-driven where CPA is).
#![allow(clippy::unwrap_used, clippy::type_complexity)]
use super::*;
use cpa_lite_core::SessionInfo;
use http::HeaderMap;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

fn t0() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

struct Clk(Arc<AtomicU64>);
impl Clk {
    fn advance(&self, d: Duration) {
        self.0.fetch_add(d.as_millis() as u64, Ordering::SeqCst);
    }
}

fn router(cfg: RoutingConfig) -> (CpaRouter, Clk) {
    let ms = Arc::new(AtomicU64::new(0));
    let m2 = ms.clone();
    let clock: Clock = Arc::new(move || t0() + Duration::from_millis(m2.load(Ordering::SeqCst)));
    (CpaRouter::with_clock(cfg, clock), Clk(ms))
}

fn cfg(strategy: Strategy) -> RoutingConfig {
    RoutingConfig { strategy, session_affinity: false, ..RoutingConfig::default() }
}
fn aff(rebind: RebindOn, subagents: bool) -> RoutingConfig {
    RoutingConfig {
        session_affinity: true,
        session_affinity_rebind_on: rebind,
        session_affinity_subagents: subagents,
        ..RoutingConfig::default()
    }
}

fn acct(id: &str) -> Account {
    Account { id: id.into(), email: None, chatgpt_account_id: None, plan_type: None, priority: 0, weight: 1, disabled: false }
}
fn acct_p(id: &str, p: i32) -> Account {
    Account { priority: p, ..acct(id) }
}
fn acct_w(id: &str, w: i64) -> Account {
    Account { weight: w, ..acct(id) }
}

fn rq(model: &str) -> RouteRequest {
    RouteRequest { model: model.into(), ..Default::default() }
}
fn rq_s(model: &str, key: &str) -> RouteRequest {
    RouteRequest { model: model.into(), session: SessionInfo { key: Some(key.into()), ..Default::default() }, exclude: vec![] }
}
fn pick(r: &CpaRouter, req: &RouteRequest, a: &[Account]) -> String {
    r.select(req, a).unwrap().account
}

// ---------------- strategies ----------------

#[test]
fn fill_first_deterministic() {
    let (r, _) = router(cfg(Strategy::FillFirst));
    let a = [acct("b"), acct("a"), acct("c")];
    for _ in 0..5 {
        assert_eq!(pick(&r, &rq("m"), &a), "a");
    }
}

#[test]
fn round_robin_cycles_deterministic() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    let a = [acct("b"), acct("a"), acct("c")];
    let got: Vec<String> = (0..6).map(|_| pick(&r, &rq("m"), &a)).collect();
    assert_eq!(got, ["a", "b", "c", "a", "b", "c"]);
}

#[test]
fn round_robin_cursor_per_model_and_thinking_suffix_shares_cursor() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    let a = [acct("a"), acct("b")];
    assert_eq!(pick(&r, &rq("gpt-5"), &a), "a");
    assert_eq!(pick(&r, &rq("gpt-5(high)"), &a), "b");
    assert_eq!(pick(&r, &rq("other"), &a), "a");
}

#[test]
fn round_robin_priority_buckets() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    let a = [acct_p("c", 0), acct_p("a", 10), acct_p("b", 10)];
    let got: Vec<String> = (0..4).map(|_| pick(&r, &rq("m"), &a)).collect();
    assert_eq!(got, ["a", "b", "a", "b"]);
}

#[test]
fn fill_first_priority_fallback_cooldown() {
    let (r, _) = router(cfg(Strategy::FillFirst));
    let a = [acct_p("high", 10), acct_p("low", 0)];
    assert_eq!(pick(&r, &rq("m"), &a), "high");
    r.report(&rq("m"), &"high".into(), &Outcome::RateLimited { retry_after: None, usage_limit: false, quota: None });
    assert_eq!(pick(&r, &rq("m"), &a), "low");
}

#[test]
fn round_robin_resumes_rotation_across_retry_exclusions() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    let ids = ["aaa", "bbb", "ccc", "ddd", "eee"];
    let a: Vec<Account> = ids.iter().map(|i| acct(i)).collect();
    let (mut first, mut all) = (HashMap::<String, i32>::new(), HashMap::<String, i32>::new());
    for _ in 0..50 {
        let mut req = rq("m");
        for attempt in 0..3 {
            let got = pick(&r, &req, &a);
            if attempt == 0 {
                *first.entry(got.clone()).or_default() += 1;
            }
            *all.entry(got.clone()).or_default() += 1;
            req.exclude.push(got);
        }
    }
    for id in ids {
        assert_eq!(first[id], 10, "{first:?}");
        assert_eq!(all[id], 30, "{all:?}");
    }
}

#[test]
fn wrr_distributes_and_skips_non_positive_weights() {
    let (r, _) = router(cfg(Strategy::WeightedRoundRobin));
    let a = [acct_w("a", 5), acct_w("b", 3), acct_w("c", 2), acct_w("disabled-by-weight", 0), acct_w("neg", -1)];
    let mut c = HashMap::<String, i32>::new();
    for _ in 0..100 {
        *c.entry(pick(&r, &rq("m"), &a)).or_default() += 1;
    }
    assert_eq!((c["a"], c["b"], c["c"]), (50, 30, 20));
    assert!(!c.contains_key("disabled-by-weight") && !c.contains_key("neg"));
}

#[test]
fn wrr_resets_credits_when_weights_change() {
    let (r, _) = router(cfg(Strategy::WeightedRoundRobin));
    for _ in 0..1000 {
        pick(&r, &rq("m"), &[acct_w("a", 1_000_000), acct_w("b", 1)]);
    }
    let a = [acct_w("a", 1), acct_w("b", 1)];
    let mut c = HashMap::<String, i32>::new();
    for _ in 0..20 {
        *c.entry(pick(&r, &rq("m"), &a)).or_default() += 1;
    }
    assert_eq!((c["a"], c["b"]), (10, 10));
}

#[test]
fn wrr_keeps_weight_ratios_when_candidates_excluded() {
    let (r, _) = router(cfg(Strategy::WeightedRoundRobin));
    let a = [acct_w("auth-a", 5), acct_w("auth-b", 3), acct_w("auth-c", 1)];
    for _ in 0..9 {
        pick(&r, &rq("m"), &a);
    }
    let req = RouteRequest { exclude: vec!["auth-a".into()], ..rq("m") };
    let mut c = HashMap::<String, i32>::new();
    for _ in 0..400 {
        *c.entry(pick(&r, &req, &a)).or_default() += 1;
    }
    assert_eq!((c["auth-b"], c["auth-c"]), (300, 100));
}

#[test]
fn wrr_overflow_weight_excluded_and_default_weight_is_one() {
    let (r, _) = router(cfg(Strategy::WeightedRoundRobin));
    let a = [acct_w("big", 1_000_001), acct("x"), acct("y")];
    let mut c = HashMap::<String, i32>::new();
    for _ in 0..10 {
        *c.entry(pick(&r, &rq("m"), &a)).or_default() += 1;
    }
    assert_eq!((c.get("big"), c["x"], c["y"]), (None, 5, 5));
    // round-robin ignores weights entirely (CPA availability candidates = all auths)
    let (rr, _) = router(cfg(Strategy::RoundRobin));
    assert_eq!(pick(&rr, &rq("m"), &[acct_w("z", 0)]), "z");
}

#[test]
fn disabled_and_empty_errors() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    assert_eq!(r.select(&rq("m"), &[]), Err(RouteError::NoAccounts));
    let d = Account { disabled: true, ..acct("a") };
    assert_eq!(r.select(&rq("m"), &[d]), Err(RouteError::NoAccounts));
}

#[test]
fn all_cooldown_returns_earliest_retry_after() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    let a = [acct("a"), acct("b")];
    r.report(&rq("m"), &"a".into(), &Outcome::RateLimited { retry_after: Some(Duration::from_secs(120)), usage_limit: false, quota: None });
    r.report(&rq("m"), &"b".into(), &Outcome::RateLimited { retry_after: Some(Duration::from_secs(30)), usage_limit: false, quota: None });
    assert_eq!(r.select(&rq("m"), &a), Err(RouteError::AllCoolingDown { retry_after: Duration::from_secs(30) }));
    // per-model: other models still route
    assert!(r.select(&rq("other"), &a).is_ok());
}

// ---------------- cooldown rules ----------------

struct CoolCase {
    name: &'static str,
    outcomes: Vec<Outcome>,
    model_scoped: bool,
    want: Option<Duration>,
}

#[test]
fn cooldown_table() {
    let rl = |ra: Option<u64>, ul: bool| Outcome::RateLimited { retry_after: ra.map(Duration::from_secs), usage_limit: ul, quota: None };
    let cases = vec![
        CoolCase { name: "429 retry-after used", outcomes: vec![rl(Some(300), false)], model_scoped: true, want: Some(Duration::from_secs(300)) },
        CoolCase { name: "429 retry-after floored to 10s", outcomes: vec![rl(Some(2), false)], model_scoped: true, want: Some(Duration::from_secs(10)) },
        CoolCase { name: "429 no hint backoff starts at 1s", outcomes: vec![rl(None, false)], model_scoped: true, want: Some(Duration::from_secs(1)) },
        CoolCase { name: "429 burst in same window does not escalate", outcomes: vec![rl(None, false), rl(None, false), rl(None, false)], model_scoped: true, want: Some(Duration::from_secs(1)) },
        CoolCase { name: "usage_limit is account-wide", outcomes: vec![rl(Some(3600), true)], model_scoped: false, want: Some(Duration::from_secs(3600)) },
        CoolCase { name: "auth failed 30min account-wide", outcomes: vec![Outcome::AuthFailed], model_scoped: false, want: Some(AUTH_COOLDOWN) },
        CoolCase { name: "404 12h", outcomes: vec![Outcome::ModelUnsupported { retry_after: None }], model_scoped: true, want: Some(NOT_FOUND_COOLDOWN) },
        CoolCase { name: "404 retry-after", outcomes: vec![Outcome::ModelUnsupported { retry_after: Some(Duration::from_secs(90)) }], model_scoped: true, want: Some(Duration::from_secs(90)) },
        CoolCase { name: "cloudflare >= 10s", outcomes: vec![Outcome::Challenge], model_scoped: true, want: Some(Duration::from_secs(10)) },
        CoolCase { name: "5xx 60s", outcomes: vec![Outcome::Transient { status: Some(502) }], model_scoped: true, want: Some(TRANSIENT_COOLDOWN) },
        CoolCase { name: "transport error no cooldown", outcomes: vec![Outcome::Transient { status: None }], model_scoped: true, want: None },
        CoolCase { name: "client error no cooldown", outcomes: vec![Outcome::ClientError { status: 400 }], model_scoped: true, want: None },
        CoolCase { name: "success clears", outcomes: vec![rl(Some(300), true), Outcome::Success { quota: None }], model_scoped: false, want: None },
    ];
    for c in cases {
        let (r, _) = router(cfg(Strategy::RoundRobin));
        for o in &c.outcomes {
            r.report(&rq("m"), &"a".into(), o);
        }
        let got = r.cooldown_until(&"a".into(), "m").map(|t| t.duration_since(t0()).unwrap());
        assert_eq!(got, c.want, "{}", c.name);
        let other = r.cooldown_until(&"a".into(), "other").map(|t| t.duration_since(t0()).unwrap());
        let want_other = if c.model_scoped { None } else { c.want };
        assert_eq!(other, want_other, "{} (other model)", c.name);
    }
}

#[test]
fn quota_backoff_doubles_per_window_and_caps_at_30min() {
    let (r, clk) = router(cfg(Strategy::RoundRobin));
    let mut seen = vec![];
    for _ in 0..14 {
        r.report(&rq("m"), &"a".into(), &Outcome::RateLimited { retry_after: None, usage_limit: false, quota: None });
        let now = t0() + Duration::from_millis(clk.0.load(Ordering::SeqCst));
        let d = r.cooldown_until(&"a".into(), "m").unwrap().duration_since(now).unwrap();
        seen.push(d.as_secs());
        clk.advance(d); // let the window close
    }
    assert_eq!(&seen[..5], &[1, 2, 4, 8, 16]);
    assert_eq!(*seen.last().unwrap(), 1800);
    // challenge backoff uses the same ladder with a 10s floor
    let (r, clk) = router(cfg(Strategy::RoundRobin));
    let mut ch = vec![];
    for _ in 0..6 {
        r.report(&rq("m"), &"a".into(), &Outcome::Challenge);
        let now = t0() + Duration::from_millis(clk.0.load(Ordering::SeqCst));
        let d = r.cooldown_until(&"a".into(), "m").unwrap().duration_since(now).unwrap();
        ch.push(d.as_secs());
        clk.advance(d);
    }
    assert_eq!(ch, [10, 10, 10, 10, 16, 32]);
}

#[test]
fn later_shorter_failure_does_not_shorten_cooldown() {
    let (r, _) = router(cfg(Strategy::RoundRobin));
    r.report(&rq("m"), &"a".into(), &Outcome::AuthFailed);
    r.report(&rq("m"), &"a".into(), &Outcome::RateLimited { retry_after: Some(Duration::from_secs(20)), usage_limit: true, quota: None });
    assert_eq!(r.cooldown_until(&"a".into(), "m").unwrap().duration_since(t0()).unwrap(), AUTH_COOLDOWN);
}

// ---------------- session affinity ----------------

#[test]
fn affinity_same_session_same_auth_and_sessions_distribute() {
    let (r, _) = router(aff(RebindOn::Any, true));
    let a = [acct("a"), acct("b"), acct("c")];
    let s1 = pick(&r, &rq_s("m", "codex:s1"), &a);
    for _ in 0..5 {
        let sel = r.select(&rq_s("m", "codex:s1"), &a).unwrap();
        assert_eq!(sel.account, s1);
        assert!(sel.from_binding);
    }
    let got: Vec<String> = ["codex:s2", "codex:s3"].iter().map(|k| pick(&r, &rq_s("m", k), &a)).collect();
    let mut all = vec![s1];
    all.extend(got);
    all.sort();
    assert_eq!(all, ["a", "b", "c"]);
}

#[test]
fn affinity_no_session_uses_fallback_rotation() {
    let (r, _) = router(aff(RebindOn::Any, true));
    let a = [acct("a"), acct("b")];
    assert_eq!(pick(&r, &rq("m"), &a), "a");
    assert_eq!(pick(&r, &rq("m"), &a), "b");
}

#[test]
fn affinity_binding_outranks_priority_but_cold_uses_priority() {
    let (r, _) = router(aff(RebindOn::Any, true));
    let low_only = [acct_p("low", 0), Account { disabled: true, ..acct_p("high", 10) }];
    assert_eq!(pick(&r, &rq_s("m", "codex:s"), &low_only), "low");
    let both = [acct_p("low", 0), acct_p("high", 10)];
    assert_eq!(pick(&r, &rq_s("m", "codex:s"), &both), "low", "bound session stays");
    assert_eq!(pick(&r, &rq_s("m", "codex:new"), &both), "high", "cold binding uses priority");
}

#[test]
fn affinity_ttl_expires() {
    let (r, clk) = router(RoutingConfig { session_affinity_ttl: "10s".into(), ..aff(RebindOn::Any, true) });
    let a = [acct("a"), acct("b")];
    assert_eq!(pick(&r, &rq_s("m", "codex:s"), &a), "a");
    clk.advance(Duration::from_secs(8));
    assert_eq!(pick(&r, &rq_s("m", "codex:s"), &a), "a"); // sliding
    clk.advance(Duration::from_secs(9));
    assert_eq!(pick(&r, &rq_s("m", "codex:s"), &a), "a");
    clk.advance(Duration::from_secs(11));
    assert_eq!(pick(&r, &rq_s("m", "codex:s"), &a), "b", "expired → cold rotation continues");
}

#[test]
fn affinity_multi_model_keys_are_independent() {
    let (r, _) = router(aff(RebindOn::Any, true));
    let a = [acct("a"), acct("b")];
    assert_eq!(pick(&r, &rq_s("m1", "codex:s"), &a), "a");
    assert_eq!(pick(&r, &rq_s("m2", "codex:s"), &a), "a"); // separate cursor per model
    assert_eq!(pick(&r, &rq_s("m1(high)", "codex:s"), &a), "a", "thinking suffix shares binding");
}

struct RebindCase {
    name: &'static str,
    rebind: RebindOn,
    outcome: Outcome,
    want_bound_after: &'static str,
}

#[test]
fn rebind_on_table() {
    let cases = [
        RebindCase { name: "any: 5xx rebinds", rebind: RebindOn::Any, outcome: Outcome::Transient { status: Some(503) }, want_bound_after: "b" },
        RebindCase { name: "quota: 5xx keeps binding", rebind: RebindOn::Quota, outcome: Outcome::Transient { status: Some(503) }, want_bound_after: "a" },
        RebindCase { name: "quota: 429 rebinds", rebind: RebindOn::Quota, outcome: Outcome::RateLimited { retry_after: Some(Duration::from_secs(60)), usage_limit: false, quota: None }, want_bound_after: "b" },
        RebindCase { name: "quota: usage_limit rebinds", rebind: RebindOn::Quota, outcome: Outcome::RateLimited { retry_after: None, usage_limit: true, quota: None }, want_bound_after: "b" },
        RebindCase { name: "quota: auth rebinds", rebind: RebindOn::Quota, outcome: Outcome::AuthFailed, want_bound_after: "b" },
        RebindCase { name: "quota: 404 rebinds", rebind: RebindOn::Quota, outcome: Outcome::ModelUnsupported { retry_after: None }, want_bound_after: "b" },
        RebindCase { name: "quota: challenge rebinds", rebind: RebindOn::Quota, outcome: Outcome::Challenge, want_bound_after: "b" },
        // CPA: under `any` the failover pick always rebinds, even for transport errors.
        RebindCase { name: "any: transport error failover rebinds", rebind: RebindOn::Any, outcome: Outcome::Transient { status: None }, want_bound_after: "b" },
        RebindCase { name: "quota: transport error keeps binding", rebind: RebindOn::Quota, outcome: Outcome::Transient { status: None }, want_bound_after: "a" },
        RebindCase { name: "quota: client error keeps binding", rebind: RebindOn::Quota, outcome: Outcome::ClientError { status: 400 }, want_bound_after: "a" },
    ];
    for c in cases {
        let (r, clk) = router(RoutingConfig { session_affinity_ttl: "24h".into(), ..aff(c.rebind, true) });
        let a = [acct("a"), acct("b")];
        let req = rq_s("m", "codex:s");
        assert_eq!(pick(&r, &req, &a), "a");
        r.report(&req, &"a".into(), &c.outcome);
        // failover attempt for this request
        let retry = RouteRequest { exclude: vec!["a".into()], ..req.clone() };
        let sel = r.select(&retry, &a).unwrap();
        assert_eq!(sel.account, "b", "{}", c.name);
        r.report(&retry, &"b".into(), &Outcome::Success { quota: None });
        // after every cooldown expired, which account is the session bound to?
        clk.advance(Duration::from_secs(13 * 3600));
        assert_eq!(pick(&r, &req, &a), c.want_bound_after, "{}", c.name);
    }
}

#[test]
fn rebind_quota_preserves_binding_while_bound_account_cools_transiently() {
    let (r, _) = router(aff(RebindOn::Quota, true));
    let a = [acct("a"), acct("b")];
    let req = rq_s("m", "codex:s");
    assert_eq!(pick(&r, &req, &a), "a");
    r.report(&req, &"a".into(), &Outcome::Transient { status: Some(500) });
    // a is cooling: requests fail over to b but binding stays a
    assert_eq!(pick(&r, &req, &a), "b");
    assert_eq!(pick(&r, &req, &a), "b");
    assert!(r.cooldown_until(&"a".into(), "m").is_some());
}

#[test]
fn subagent_inherits_parent_binding_by_default_and_isolates_when_disabled() {
    for (subagents, want_same) in [(true, true), (false, false)] {
        let (r, _) = router(aff(RebindOn::Any, subagents));
        let a = [acct("a"), acct("b")];
        let parent = rq_s("m", "codex:root");
        assert_eq!(pick(&r, &parent, &a), "a");
        let mut h = HeaderMap::new();
        h.insert("session-id", "root".parse().unwrap());
        h.insert("thread-id", "child".parse().unwrap());
        h.insert("x-openai-subagent", "collab_spawn".parse().unwrap());
        let child = RouteRequest { model: "m".into(), session: extract_session(&h, None), exclude: vec![] };
        assert!(child.session.is_subagent);
        assert_eq!(child.session.parent_key.as_deref(), Some("codex:root"));
        let got = pick(&r, &child, &a);
        assert_eq!(got == "a", want_same, "subagents={subagents}");
        // child failure must not unbind the parent
        r.report(&child, &got.clone(), &Outcome::RateLimited { retry_after: Some(Duration::from_secs(60)), usage_limit: false, quota: None });
        if got == "b" {
            assert_eq!(pick(&r, &parent, &a), "a");
        }
    }
}

#[test]
fn fork_inherits_parent_even_when_subagent_affinity_disabled() {
    let (r, _) = router(aff(RebindOn::Any, false));
    let a = [acct("a"), acct("b")];
    assert_eq!(pick(&r, &rq_s("m", "codex:parent-111"), &a), "a");
    let mut h = HeaderMap::new();
    h.insert("session-id", "codex-fork-666".parse().unwrap());
    h.insert("x-codex-turn-metadata", r#"{"session_id":"codex-fork-666","forked_from_thread_id":"parent-111"}"#.parse().unwrap());
    let s = extract_session(&h, None);
    assert!(s.is_fork && !s.is_subagent);
    let fork = RouteRequest { model: "m".into(), session: s, exclude: vec![] };
    assert_eq!(pick(&r, &fork, &a), "a");
}

#[test]
fn prompt_cache_key_binds_conversation_alias() {
    // CPA: pck primary + conv fallback are bound as aliases, so a later request carrying
    // only the conversation id stays on the same account.
    let (r, _) = router(aff(RebindOn::Any, true));
    let a = [acct("a"), acct("b")];
    let b1 = json!({"prompt_cache_key":"k1","conversation":{"id":"c1"}});
    let s1 = extract_session(&HeaderMap::new(), Some(&b1));
    assert_eq!(s1.parent_key.as_deref(), Some("conv:c1"));
    assert!(!s1.is_subagent);
    assert_eq!(pick(&r, &RouteRequest { model: "m".into(), session: s1, exclude: vec![] }, &a), "a");
    pick(&r, &rq("m"), &a); // advance rotation
    let s2 = extract_session(&HeaderMap::new(), Some(&json!({"conversation":"c1"})));
    assert_eq!(pick(&r, &RouteRequest { model: "m".into(), session: s2, exclude: vec![] }, &a), "a");
}

#[test]
fn quota_threshold_avoidance_only_for_new_sessions() {
    let (r, _) = router(RoutingConfig { quota_avoid_threshold: 0.99, ..aff(RebindOn::Quota, true) });
    let a = [acct("a"), acct("b")];
    let bound = rq_s("m", "codex:bound");
    assert_eq!(pick(&r, &bound, &a), "a");
    let hot = QuotaSnapshot {
        primary: Some(cpa_lite_core::QuotaWindow { used_percent: 99.5, window_minutes: Some(300), resets_at: Some(t0() + Duration::from_secs(600)) }),
        secondary: None,
    };
    r.report(&bound, &"a".into(), &Outcome::Success { quota: Some(hot.clone()) });
    assert_eq!(r.quota_snapshot(&"a".into()), Some(hot));
    assert_eq!(pick(&r, &bound, &a), "a", "bound session not moved");
    for k in ["codex:n1", "codex:n2", "codex:n3"] {
        assert_eq!(pick(&r, &rq_s("m", k), &a), "b", "new session avoids hot account");
    }
    assert_eq!(pick(&r, &rq("m"), &a), "b");
    // when every account is hot, avoidance is lifted
    assert_eq!(pick(&r, &rq("m"), &[acct("a")]), "a");
}

#[test]
fn quota_threshold_ignores_expired_windows() {
    let (r, clk) = router(cfg(Strategy::FillFirst));
    let q = QuotaSnapshot {
        primary: Some(cpa_lite_core::QuotaWindow { used_percent: 100.0, window_minutes: None, resets_at: Some(t0() + Duration::from_secs(5)) }),
        secondary: None,
    };
    r.report(&rq("m"), &"a".into(), &Outcome::Success { quota: Some(q) });
    let a = [acct("a"), acct("b")];
    assert_eq!(pick(&r, &rq("m"), &a), "b");
    clk.advance(Duration::from_secs(6));
    assert_eq!(pick(&r, &rq("m"), &a), "a");
}

#[test]
fn concurrent_select_is_safe() {
    let (r, _) = router(aff(RebindOn::Quota, true));
    let r = Arc::new(r);
    let a: Arc<Vec<Account>> = Arc::new((0..5).map(|i| acct(&format!("a{i}"))).collect());
    let hs: Vec<_> = (0..8)
        .map(|t| {
            let (r, a) = (r.clone(), a.clone());
            std::thread::spawn(move || {
                for i in 0..200 {
                    let req = rq_s("m", &format!("codex:{}", (t * 7 + i) % 13));
                    let s = r.select(&req, &a).unwrap();
                    r.report(&req, &s.account, &Outcome::Success { quota: None });
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    // every session ends up with a stable binding
    for k in 0..13 {
        let req = rq_s("m", &format!("codex:{k}"));
        let x = pick(&r, &req, &a);
        assert_eq!(pick(&r, &req, &a), x);
    }
}

// ---------------- session extraction ----------------

fn hm(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.append(http::HeaderName::from_bytes(k.as_bytes()).unwrap(), http::HeaderValue::from_bytes(v.as_bytes()).unwrap());
    }
    h
}

#[test]
fn extract_session_table() {
    let long = "a".repeat(257);
    let cases: Vec<(&str, HeaderMap, Option<serde_json::Value>, Option<&str>, Option<&str>)> = vec![
        // name, headers, body, want key, want parent/fallback
        ("codex hyphen header", hm(&[("Session-Id", "codex-session")]), None, Some("codex:codex-session"), None),
        ("codex underscore header", hm(&[("Session_id", "legacy-codex-session")]), None, Some("codex:legacy-codex-session"), None),
        ("codex thread only", hm(&[("Thread-Id", "t1")]), None, Some("codex:t1"), None),
        ("codex turn metadata session", hm(&[("X-Codex-Turn-Metadata", r#"{"session_id":"m1"}"#)]), None, Some("codex:m1"), None),
        ("codex parent thread", hm(&[("Session-Id", "codex-child-555"), ("x-codex-parent-thread-id", "codex-parent-111")]), None, Some("codex:codex-child-555"), Some("codex:codex-parent-111")),
        ("codex fork", hm(&[("Session-Id", "codex-fork-666"), ("X-Codex-Turn-Metadata", r#"{"session_id":"codex-fork-666","forked_from_thread_id":"codex-parent-111","request_kind":"turn"}"#)]), None, Some("codex:codex-fork-666"), Some("codex:codex-parent-111")),
        ("codex multi-agent v2", hm(&[("Session-Id", "codex-root-001"), ("Thread-Id", "codex-sub-thread-002"), ("X-Codex-Turn-Metadata", r#"{"session_id":"codex-root-001","thread_id":"codex-sub-thread-002","agent_name":"/root/check_readme","parent_thread_id":"codex-root-001","subagent_kind":"thread_spawn"}"#)]), None, Some("codex:codex-root-001:agent:check_readme"), Some("codex:codex-root-001")),
        ("codex fork sid+tid", hm(&[("Session-Id", "parent-thread-00"), ("Thread-Id", "child-thread-01"), ("X-Codex-Turn-Metadata", r#"{"session_id":"parent-thread-00","thread_id":"child-thread-01","forked_from_thread_id":"parent-thread-00"}"#)]), None, Some("codex:child-thread-01"), Some("codex:parent-thread-00")),
        ("codex header + body fork", hm(&[("Session-Id", "parent-sess-uuid")]), Some(json!({"thread_id":"child-thread-uuid","metadata":{"forked_from_thread_id":"parent-sess-uuid"}})), Some("codex:child-thread-uuid"), Some("codex:parent-sess-uuid")),
        ("codex beats x-session-id and pck", hm(&[("Session-Id", "codex-session"), ("X-Session-Id", "generic")]), Some(json!({"prompt_cache_key":"p"})), Some("codex:codex-session"), None),
        ("x-session-id beats affinity", hm(&[("X-Session-Id", "generic-session"), ("X-Session-Affinity", "aff")]), None, Some("header:generic-session"), None),
        ("opencode affinity", hm(&[("X-Session-Affinity", "ses_opencode")]), None, Some("affinity:ses_opencode"), None),
        ("pi slot", hm(&[("X-Slot-Session-Id", "s1")]), None, Some("slot:s1"), None),
        ("client request id beats body", hm(&[("X-Client-Request-Id", "client-session")]), Some(json!({"prompt_cache_key":"p","conversation":{"id":"c"}})), Some("clientreq:client-session"), None),
        ("prompt cache key", HeaderMap::new(), Some(json!({"prompt_cache_key":"prompt-session"})), Some("pck:prompt-session"), None),
        ("pck beats conversation, conv becomes fallback", HeaderMap::new(), Some(json!({"conversation":{"id":"conversation-session"},"prompt_cache_key":"shared-cache-bucket"})), Some("pck:shared-cache-bucket"), Some("conv:conversation-session")),
        ("conversation object", HeaderMap::new(), Some(json!({"conversation":{"id":"conv-object"}})), Some("conv:conv-object"), None),
        ("conversation string", HeaderMap::new(), Some(json!({"conversation":"conv-string"})), Some("conv:conv-string"), None),
        ("legacy conversation_id", HeaderMap::new(), Some(json!({"conversation_id":"conv-12345"})), Some("conv:conv-12345"), None),
        ("metadata.user_id", HeaderMap::new(), Some(json!({"metadata":{"user_id":"user_abc123"}})), Some("user:user_abc123"), None),
        ("body thread fork", HeaderMap::new(), Some(json!({"thread_id":"child-thread-01","forked_from_thread_id":"parent-thread-00"})), Some("thread:child-thread-01"), Some("thread:parent-thread-00")),
        ("nested request session", HeaderMap::new(), Some(json!({"request":{"session_id":"nested"}})), Some("session:nested"), None),
        ("no metadata", HeaderMap::new(), Some(json!({"model":"x"})), None, None),
        ("empty", HeaderMap::new(), None, None, None),
        ("whitespace rejected", hm(&[("X-Session-Id", "   ")]), None, None, None),
        ("control char rejected", hm(&[("Session-Id", "bad\tsession")]), None, None, None),
        ("too long rejected", hm(&[("X-Client-Request-Id", &long)]), None, None, None),
        ("invalid stronger falls through", hm(&[("X-Session-Id", "bad\tx"), ("X-Session-Affinity", "ok")]), None, Some("affinity:ok"), None),
        ("invalid pck falls to conversation", HeaderMap::new(), Some(json!({"prompt_cache_key":"   ","conversation":{"id":"valid-conversation"}})), Some("conv:valid-conversation"), None),
        ("message hash short", HeaderMap::new(), Some(json!({"messages":[{"role":"system","content":"You are helpful"},{"role":"user","content":"Hello"}]})), Some("msg:db09d768eef0153d"), None),
        ("message hash full + short fallback", HeaderMap::new(), Some(json!({"messages":[{"role":"system","content":"You are helpful"},{"role":"user","content":[{"type":"text","text":"Hello"}]},{"role":"assistant","content":"Hi there"}]})), Some("msg:a57ba910c8152040"), Some("msg:db09d768eef0153d")),
        ("responses api hash", HeaderMap::new(), Some(json!({"instructions":"You are helpful","input":[{"type":"reasoning"},{"role":"user","content":[{"type":"input_text","text":"Hello"}]}]})), Some("msg:db09d768eef0153d"), None),
        ("gemini hash", HeaderMap::new(), Some(json!({"contents":[{"role":"user","parts":[{"text":"Hello"}]}]})), Some("msg:8d89bea5dd55f36f"), None),
    ];
    for (name, h, b, key, parent) in cases {
        let s = extract_session(&h, b.as_ref());
        assert_eq!(s.key.as_deref(), key, "{name}");
        assert_eq!(s.parent_key.as_deref(), parent, "{name}");
    }
}

#[test]
fn extract_session_flags() {
    let fork = extract_session(&hm(&[("Session-Id", "f"), ("X-Codex-Turn-Metadata", r#"{"forked_from_thread_id":"p"}"#)]), None);
    assert!(fork.is_fork && !fork.is_subagent);
    let sub = extract_session(&hm(&[("Session-Id", "c"), ("x-codex-parent-thread-id", "p")]), None);
    assert!(sub.is_subagent && !sub.is_fork);
    let sub0 = extract_session(&hm(&[("Session-Id", "c"), ("X-Openai-Subagent", "false")]), None);
    assert!(!sub0.is_subagent);
    let pck = extract_session(&HeaderMap::new(), Some(&json!({"prompt_cache_key":"k","conversation":"c"})));
    assert!(!pck.is_subagent, "pck/conv fallback is an alias, not a parent");
}

#[test]
fn bound_session_identity_safety() {
    assert_eq!(bound_session_identity("short"), "short");
    let long = format!("codex:{}", "é".repeat(200));
    let b = bound_session_identity(&long);
    assert!(b.len() <= 255, "{}", b.len());
    assert!(b.contains('#'));
    assert_eq!(b, bound_session_identity(&long));
    assert_ne!(b, bound_session_identity(&format!("{long}x")));
    let mut h = HeaderMap::new();
    h.insert("session-id", "x".repeat(250).parse().unwrap());
    let k = extract_session(&h, None).key.unwrap();
    assert!(k.len() <= 256 && k.starts_with("codex:x"));
}

#[test]
fn hierarchy_helpers() {
    use session::{is_hierarchy_parent, is_subagent_session};
    assert!(is_subagent_session("codex:s:agent:x", ""));
    assert!(is_subagent_session("codex:c", "codex:p"));
    assert!(!is_subagent_session("pck:k", "conv:c"));
    assert!(is_hierarchy_parent("a", "b"));
    assert!(!is_hierarchy_parent("a:1", "b"));
}

// ---------------- quota parsing ----------------

fn hg<'a>(h: &'a HeaderMap, k: &str) -> &'a str {
    h.get(k).map(|v| v.to_str().unwrap()).unwrap_or("")
}

#[test]
fn quota_event_headers_table() {
    let cases: Vec<(&str, &str, Vec<(&str, &str)>, Vec<&str>)> = vec![
        ("active limit", r#"{"type":"codex.rate_limits","plan_type":"pro","metered_limit_name":"codex_bengalfox","rate_limits":{"primary":{"used_percent":2,"window_minutes":10080,"reset_at":1782951970},"secondary":null}}"#,
            vec![("X-Codex-Active-Limit", "codex_bengalfox"), ("X-Codex-Primary-Used-Percent", "2"), ("X-Codex-Primary-Window-Minutes", "10080"), ("X-Codex-Primary-Reset-At", "1782951970"), ("X-Codex-Plan-Type", "pro")], vec![]),
        ("additional + credits", r#"{"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"allowed":true,"limit_reached":false,"primary":{"used_percent":48,"window_minutes":10080,"reset_after_seconds":523210,"reset_at":1786677299},"secondary":null},"additional_rate_limits":{"GPT-5.3-Codex-Spark":{"allowed":true,"limit_reached":false,"primary":{"used_percent":3,"window_minutes":300,"reset_after_seconds":10148,"reset_at":1787231961},"secondary":{"used_percent":63,"window_minutes":10080,"reset_after_seconds":75420,"reset_at":1787290791}}},"credits":{"has_credits":false,"unlimited":false,"balance":"0"}}"#,
            vec![("X-Codex-Primary-Used-Percent", "48"), ("X-Codex-Primary-Reset-After-Seconds", "523210"), ("X-Codex-Additional-GPT-5.3-Codex-Spark-Limit-Name", "GPT-5.3-Codex-Spark"), ("X-Codex-Additional-GPT-5.3-Codex-Spark-Primary-Used-Percent", "3"), ("X-Codex-Additional-GPT-5.3-Codex-Spark-Secondary-Used-Percent", "63"), ("X-Codex-Credits-Has-Credits", "false"), ("X-Codex-Credits-Unlimited", "false"), ("X-Codex-Credits-Balance", "0")], vec![]),
        ("error frame", r#"{"type":"error","status_code":429,"headers":{"X-Codex-Primary-Used-Percent":"100","X-Codex-Primary-Window-Minutes":"10080","X-Codex-Primary-Reset-After-Seconds":"437380","X-Codex-Credits-Balance":"0","X-Codex-Turn-State":"must-not","Set-Cookie":"must-not"}}"#,
            vec![("X-Codex-Primary-Used-Percent", "100"), ("X-Codex-Primary-Reset-After-Seconds", "437380"), ("X-Codex-Credits-Balance", "0")], vec!["Set-Cookie", "X-Codex-Turn-State"]),
        ("safe error headers", r#"{"type":"error","headers":{"X-Ratelimit-Remaining-Requests":"7","Retry-After":"60","X-Codex-Primary-Over-Secondary-Limit-Percent":"0","X-Codex-Turn-State":"secret","Authorization":"Bearer secret"}}"#,
            vec![("X-Ratelimit-Remaining-Requests", "7"), ("Retry-After", "60"), ("X-Codex-Primary-Over-Secondary-Limit-Percent", "0")], vec!["X-Codex-Turn-State", "Authorization"]),
        ("no invented active limit; partial windows", r#"{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":42,"window_minutes":300},"secondary":{"used_percent":17,"window_minutes":10080,"reset_after_seconds":60}}}"#,
            vec![("X-Codex-Secondary-Used-Percent", "17")], vec!["X-Codex-Primary-Used-Percent", "X-Codex-Active-Limit"]),
        ("array + camelCase", r#"{"type":"codex.rate_limits","planType":"pro","meteredLimitName":"premium","rateLimit":{"allowed":true,"limitReached":false,"primary":{"usedPercent":71,"windowMinutes":10080,"resetAfterSeconds":60}},"additionalRateLimits":[{"limitName":"GPT-5.3-Codex-Spark","rateLimit":{"primary":{"usedPercent":13,"windowMinutes":300,"resetAt":1787399817},"secondary":{"usedPercent":51,"windowMinutes":10080,"resetAfterSeconds":120}}}]}"#,
            vec![("X-Codex-Active-Limit", "premium"), ("X-Codex-Plan-Type", "pro"), ("X-Codex-Primary-Used-Percent", "71"), ("X-Codex-Additional-GPT-5.3-Codex-Spark-Primary-Used-Percent", "13"), ("X-Codex-Additional-GPT-5.3-Codex-Spark-Secondary-Reset-After-Seconds", "120")], vec![]),
        ("invalid active limit keeps windows", r#"{"type":"codex.rate_limits","metered_limit_name":"bad limit","rate_limits":{"primary":{"used_percent":1,"window_minutes":300,"reset_after_seconds":60}}}"#,
            vec![("X-Codex-Primary-Used-Percent", "1")], vec!["X-Codex-Active-Limit"]),
        ("production event", r#"{"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"allowed":true,"limit_reached":false,"primary":{"used_percent":81,"window_minutes":10080,"reset_after_seconds":137160,"reset_at":1787588999},"secondary":null},"code_review_rate_limits":{"primary":{"used_percent":4,"window_minutes":300,"reset_after_seconds":900}},"additional_rate_limits":{"GPT-5.3-Codex-Spark":{"allowed":true,"limit_reached":false,"primary":{"used_percent":0,"window_minutes":300,"reset_after_seconds":18000}}},"credits":{"has_credits":false,"unlimited":false,"balance":"0"},"promo":null}"#,
            vec![("X-Codex-Primary-Reset-At", "1787588999"), ("X-Codex-Code-Review-Primary-Used-Percent", "4"), ("X-Codex-Additional-Gpt-5.3-Codex-Spark-Primary-Used-Percent", "0")], vec!["X-Codex-Secondary-Used-Percent"]),
    ];
    for (name, payload, want, absent) in cases {
        let h = parse_quota_event_headers(payload.as_bytes()).unwrap_or_else(|| panic!("{name}: none"));
        for (k, v) in want {
            assert_eq!(hg(&h, k), v, "{name}: {k}");
        }
        for k in absent {
            assert!(h.get(k).is_none(), "{name}: {k} leaked");
        }
    }
}

#[test]
fn quota_event_headers_rejects() {
    let big = "x".repeat(4096);
    for p in [
        String::new(),
        r#"{"type":"response.completed"}"#.into(),
        r#"{"type":"response.output_text.delta","delta":"hello"}"#.into(),
        format!(r#"{{"sequence_number":7,"type":"response.output_text.delta","delta":"{big}"}}"#),
        r#"{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":101,"window_minutes":300,"reset_after_seconds":60}}}"#.into(),
        r#"{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":42}}}"#.into(),
    ] {
        assert!(parse_quota_event_headers(p.as_bytes()).is_none(), "{p}");
    }
    let large = format!(r#"{{"type":"codex.rate_limits","rate_limits":{{"primary":{{"used_percent":42,"window_minutes":10080,"reset_after_seconds":3600}}}},"padding":"{big}"}}"#);
    assert_eq!(hg(&parse_quota_event_headers(large.as_bytes()).unwrap(), "x-codex-primary-used-percent"), "42");
    let evil = r#"{"type":"codex.rate_limits","additional_rate_limits":[{"limit_name":"evil\r\nX-Injected: 1","primary":{"used_percent":3,"window_minutes":300,"reset_after_seconds":60}}]}"#;
    let h = parse_quota_event_headers(evil.as_bytes()).unwrap();
    assert!(h.iter().all(|(_, v)| !v.to_str().unwrap_or("").contains(['\r', '\n'])));
}

#[test]
fn parse_quota_snapshot_from_headers() {
    let h = hm(&[
        ("x-codex-primary-used-percent", "42.5"),
        ("x-codex-primary-window-minutes", "300"),
        ("x-codex-primary-reset-after-seconds", "60"),
        ("x-codex-secondary-used-percent", "17"),
        ("x-codex-secondary-window-minutes", "10080"),
        ("x-codex-secondary-reset-at", "1700003600"),
    ]);
    let q = parse_quota_at(&h, t0()).unwrap();
    let p = q.primary.unwrap();
    assert_eq!((p.used_percent, p.window_minutes, p.resets_at), (42.5, Some(300), Some(t0() + Duration::from_secs(60))));
    assert_eq!(q.secondary.unwrap().resets_at, Some(t0() + Duration::from_secs(3600)));
    assert!(parse_quota_at(&HeaderMap::new(), t0()).is_none());
    assert!(parse_quota_at(&hm(&[("x-codex-primary-used-percent", "150")]), t0()).is_none());
    // event frame → headers → snapshot
    let ev = parse_quota_event_headers(br#"{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":99,"window_minutes":300,"reset_after_seconds":10}}}"#).unwrap();
    assert_eq!(parse_quota_at(&ev, t0()).unwrap().primary.unwrap().used_percent, 99.0);
}

#[test]
fn parse_codex_retry_after_layouts() {
    let now = t0();
    let cases: [(&str, &str, Option<u64>); 8] = [
        ("relative reset", r#"{"type":"usage_limit_reached","resets_in_seconds":3600}"#, Some(3600)),
        ("absolute reset wins", r#"{"type":"usage_limit_reached","resets_at":1700000300,"resets_in_seconds":1}"#, Some(300)),
        ("expired absolute falls back", r#"{"type":"usage_limit_reached","resets_at":1699999940,"resets_in_seconds":77}"#, Some(77)),
        ("type trimmed/case-insensitive", r#"{"type":" USAGE_LIMIT_REACHED ","resets_in_seconds":30}"#, Some(30)),
        ("missing reset", r#"{"type":"usage_limit_reached"}"#, None),
        ("expired reset", r#"{"type":"usage_limit_reached","resets_at":1699999940}"#, None),
        ("nonpositive reset", r#"{"type":"usage_limit_reached","resets_at":0,"resets_in_seconds":-1}"#, None),
        ("transient limit", r#"{"type":"rate_limit_error","resets_in_seconds":30}"#, None),
    ];
    for layout in ["nested", "top-level"] {
        for (name, body, want) in cases {
            let body = if layout == "nested" { format!(r#"{{"error":{body}}}"#) } else { body.to_string() };
            assert_eq!(parse_codex_retry_after(429, body.as_bytes(), now), want.map(Duration::from_secs), "{layout}/{name}");
        }
    }
    assert_eq!(parse_codex_retry_after(500, br#"{"type":"usage_limit_reached","resets_in_seconds":3}"#, now), None);
    assert!(is_usage_limit(br#"{"error":{"type":"usage_limit_reached"}}"#));
    assert!(!is_usage_limit(br#"{"error":{"type":"rate_limit_exceeded"}}"#));
}

#[test]
fn retry_after_header_parsing() {
    let now = t0();
    assert_eq!(parse_retry_after_header("120", now), Some(Duration::from_secs(120)));
    assert_eq!(parse_retry_after_header("-1", now), None);
    assert_eq!(parse_retry_after_header("junk", now), None);
    assert_eq!(parse_retry_after_header("999999", now), Some(quota::MAX_HEADER_RETRY_AFTER));
    let date = httpdate::fmt_http_date(now + Duration::from_secs(90));
    assert_eq!(parse_retry_after_header(&date, now), Some(Duration::from_secs(90)));
    let past = httpdate::fmt_http_date(now - Duration::from_secs(90));
    assert_eq!(parse_retry_after_header(&past, now), Some(Duration::ZERO));
    // body reset wins over header
    let h = hm(&[("retry-after", "5")]);
    assert_eq!(retry_after_for_429(&h, br#"{"error":{"type":"usage_limit_reached","resets_in_seconds":60}}"#, now), Some(Duration::from_secs(60)));
    assert_eq!(retry_after_for_429(&h, b"{}", now), Some(Duration::from_secs(5)));
}

#[test]
fn cloudflare_challenge_detection() {
    for (s, want) in [
        ("<html>Just a moment...</html> cloudflare", true),
        ("/cdn-cgi/challenge-platform/h/b", true),
        ("cf-mitigated: challenge", true),
        ("plain forbidden", false),
    ] {
        assert_eq!(is_cloudflare_challenge(s), want, "{s}");
    }
}

#[test]
fn canonical_model_key_strips_thinking_suffix() {
    for (i, o) in [("gpt-5", "gpt-5"), (" gpt-5(high) ", "gpt-5"), ("(x)", "(x)"), ("a(b", "a(b")] {
        assert_eq!(canonical_model_key(i), o);
    }
}
