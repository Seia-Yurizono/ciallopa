//! Price table (`prices.toml`) and the per-request cost formula.
//!
//! Costs are **notional** for subscription (ChatGPT plan) traffic: they show what the same
//! tokens would cost on the API, not what was billed.
//!
//! ```text
//! cost = (in − cached − cache_write)·P_in + cached·P_cache + cache_write·P_write + out·P_out
//! ```
//! `out` already contains reasoning tokens (not added again). Long context: when total input
//! (incl. cached) > `long.threshold`, the three input-side prices × `long.in_x`, output ×
//! `long.out_x`. Tier: an explicit entry for the tier wins, else the standard entry ×
//! multiplier (`flex_x`, `fast_x`; per model or `[defaults]`). Dated entries
//! (`effective_from` / `effective_until`, inclusive, UTC) win over undated ones on the
//! request's date. No match ⇒ `None` (never 0).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "kebab-case")]
pub enum Tier {
    #[default]
    Standard,
    Flex,
    /// "Fast mode" (formerly priority processing).
    #[serde(alias = "priority", alias = "fast")]
    PriorityFast,
}
impl Tier {
    /// From a request / response `service_tier` value.
    pub fn from_service_tier(s: Option<&str>) -> Tier {
        match s.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("flex") => Tier::Flex,
            Some("priority" | "fast" | "priority-fast") => Tier::PriorityFast,
            _ => Tier::Standard,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Tier::Standard => "standard",
            Tier::Flex => "flex",
            Tier::PriorityFast => "priority-fast",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Long {
    /// Total input tokens (incl. cached) above which long-context prices apply.
    pub threshold: i64,
    pub in_x: f64,
    pub out_x: f64,
}

/// One `[[model]]` entry. Prices in USD per 1M tokens.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub model: String,
    #[serde(default, skip_serializing_if = "is_std")]
    pub tier: Tier,
    #[serde(rename = "in")]
    pub input: f64,
    /// Cached input (cache read). Missing ⇒ input price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<f64>,
    /// Cache write. Missing ⇒ input price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<f64>,
    pub out: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long: Option<Long>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flex_x: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_x: Option<f64>,
    /// `YYYY-MM-DD`, inclusive (UTC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}
fn is_std(t: &Tier) -> bool {
    *t == Tier::Standard
}
impl Entry {
    fn dated(&self) -> bool {
        self.effective_from.is_some() || self.effective_until.is_some()
    }
    fn active_on(&self, day: i64) -> bool {
        let ok = |d: &Option<String>, f: fn(i64, i64) -> bool| {
            d.as_deref().and_then(parse_date).is_none_or(|x| f(day, x))
        };
        ok(&self.effective_from, |d, x| d >= x) && ok(&self.effective_until, |d, x| d <= x)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Defaults {
    pub flex_x: f64,
    pub fast_x: f64,
}
impl Default for Defaults {
    fn default() -> Self {
        Self {
            flex_x: 0.5,
            fast_x: 2.0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PriceTable {
    pub defaults: Defaults,
    #[serde(rename = "model")]
    pub models: Vec<Entry>,
}

/// Tokens of one request.
#[derive(Debug, Clone, Copy, Default)]
pub struct Usage {
    pub input: i64,
    pub cached: i64,
    pub cache_write: i64,
    pub output: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum PriceError {
    #[error("prices: {0}")]
    Io(#[from] std::io::Error),
    #[error("prices: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("prices: {0}")]
    Invalid(String),
}

impl PriceTable {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
    pub fn parse(s: &str) -> Result<Self, PriceError> {
        let t: PriceTable = toml::from_str(s)?;
        for e in &t.models {
            for d in [&e.effective_from, &e.effective_until]
                .into_iter()
                .flatten()
            {
                if parse_date(d).is_none() {
                    return Err(PriceError::Invalid(format!(
                        "{}: bad date {d:?} (YYYY-MM-DD)",
                        e.model
                    )));
                }
            }
            let ps = [
                Some(e.input),
                e.cache,
                e.write,
                Some(e.out),
                e.flex_x,
                e.fast_x,
            ];
            if ps.iter().flatten().any(|p| !p.is_finite() || *p < 0.0) {
                return Err(PriceError::Invalid(format!(
                    "{}: negative/invalid price",
                    e.model
                )));
            }
        }
        Ok(t)
    }
    /// Missing file ⇒ empty table (all costs null).
    pub fn load(p: &Path) -> Result<Self, PriceError> {
        match std::fs::read_to_string(p) {
            Ok(s) => Self::parse(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn to_toml(&self) -> String {
        toml::to_string(self).unwrap_or_default()
    }

    fn find(&self, model: &str, tier: Tier, day: i64) -> Option<&Entry> {
        let m = model.trim();
        let mut undated = None;
        for e in &self.models {
            if e.model.eq_ignore_ascii_case(m) && e.tier == tier && e.active_on(day) {
                if e.dated() {
                    return Some(e);
                }
                undated.get_or_insert(e);
            }
        }
        undated
    }

    /// USD for one request; `None` = unknown model/tier/date (caller flags `unknown_price`).
    pub fn cost(&self, model: Option<&str>, tier: Tier, ts_ms: i64, u: Usage) -> Option<f64> {
        let model = model?;
        let day = ts_ms.div_euclid(86_400_000);
        let (e, mult) = match self.find(model, tier, day) {
            Some(e) => (e, 1.0),
            None => {
                let e = self.find(model, Tier::Standard, day)?;
                let x = match tier {
                    Tier::Standard => 1.0,
                    Tier::Flex => e.flex_x.unwrap_or(self.defaults.flex_x),
                    Tier::PriorityFast => e.fast_x.unwrap_or(self.defaults.fast_x),
                };
                (e, x)
            }
        };
        let (mut pi, mut pc, mut pw, mut po) = (
            e.input,
            e.cache.unwrap_or(e.input),
            e.write.unwrap_or(e.input),
            e.out,
        );
        if let Some(l) = e.long.filter(|l| u.input > l.threshold) {
            pi *= l.in_x;
            pc *= l.in_x;
            pw *= l.in_x;
            po *= l.out_x;
        }
        let cached = u.cached.max(0);
        let write = u.cache_write.max(0);
        let fresh = (u.input - cached - write).max(0) as f64;
        let usd = fresh * pi + cached as f64 * pc + write as f64 * pw + u.output.max(0) as f64 * po;
        Some(usd * mult / 1e6)
    }
}

/// `YYYY-MM-DD` → days since 1970-01-01.
pub fn parse_date(s: &str) -> Option<i64> {
    let mut it = s.trim().splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Howard Hinnant's days_from_civil
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

// ---------------------------------------------------------------- models.dev sync

/// One price change proposed by a sync.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Add(Entry),
    Update {
        old: Box<Entry>,
        new: Box<Entry>,
    },
    /// Local model has a dated (promo) entry: never touched automatically.
    SkipDated(String),
    /// Local standard entry not found upstream (kept).
    Missing(String),
}

/// Standard-tier entries from a models.dev `api.json` for one provider (e.g. `openai`).
pub fn from_models_dev(json: &serde_json::Value, provider: &str) -> Vec<Entry> {
    let r4 = |x: f64| (x * 10_000.0).round() / 10_000.0;
    let mut out = vec![];
    let Some(models) = json
        .get(provider)
        .and_then(|p| p.get("models"))
        .and_then(|m| m.as_object())
    else {
        return out;
    };
    for (id, m) in models {
        let Some(c) = m.get("cost") else { continue };
        let f = |k: &str| c.get(k).and_then(|v| v.as_f64());
        let (Some(input), Some(out_p)) = (f("input"), f("output")) else {
            continue;
        };
        let long = c
            .get("tiers")
            .and_then(|t| t.as_array())
            .and_then(|ts| {
                ts.iter()
                    .find(|t| t.pointer("/tier/type").and_then(|x| x.as_str()) == Some("context"))
            })
            .and_then(|t| {
                let size = t.pointer("/tier/size")?.as_i64()?;
                let ti = t.get("input")?.as_f64()?;
                let to = t.get("output")?.as_f64()?;
                (input > 0.0 && out_p > 0.0).then(|| Long {
                    threshold: size,
                    in_x: r4(ti / input),
                    out_x: r4(to / out_p),
                })
            });
        out.push(Entry {
            model: id.clone(),
            tier: Tier::Standard,
            input,
            cache: f("cache_read"),
            write: f("cache_write"),
            out: out_p,
            long,
            flex_x: None,
            fast_x: None,
            effective_from: None,
            effective_until: None,
            note: None,
        });
    }
    out.sort_by(|a, b| a.model.cmp(&b.model));
    out
}

/// Diff local standard undated entries against `remote`. Local per-model extras
/// (`fast_x`, `flex_x`, `note`) survive an update.
pub fn diff(local: &PriceTable, remote: &[Entry]) -> Vec<Change> {
    let mut ch = vec![];
    let mut dated: BTreeMap<String, ()> = BTreeMap::new();
    for e in local.models.iter().filter(|e| e.dated()) {
        dated.insert(e.model.to_ascii_lowercase(), ());
    }
    for r in remote {
        let key = r.model.to_ascii_lowercase();
        if dated.contains_key(&key) {
            ch.push(Change::SkipDated(r.model.clone()));
            continue;
        }
        let cur = local.models.iter().find(|e| {
            e.model.eq_ignore_ascii_case(&r.model) && e.tier == Tier::Standard && !e.dated()
        });
        match cur {
            None => ch.push(Change::Add(r.clone())),
            Some(c) => {
                let mut n = r.clone();
                n.model = c.model.clone();
                n.flex_x = c.flex_x;
                n.fast_x = c.fast_x;
                n.note = c.note.clone();
                if n != *c {
                    ch.push(Change::Update {
                        old: Box::new(c.clone()),
                        new: Box::new(n),
                    });
                }
            }
        }
    }
    for e in local
        .models
        .iter()
        .filter(|e| e.tier == Tier::Standard && !e.dated())
    {
        if !remote
            .iter()
            .any(|r| r.model.eq_ignore_ascii_case(&e.model))
        {
            ch.push(Change::Missing(e.model.clone()));
        }
    }
    ch
}

pub fn apply(local: &PriceTable, changes: &[Change]) -> PriceTable {
    let mut t = local.clone();
    for c in changes {
        match c {
            Change::Add(e) => t.models.push(e.clone()),
            Change::Update { old, new } => {
                if let Some(x) = t.models.iter_mut().find(|x| **x == **old) {
                    *x = (**new).clone();
                }
            }
            Change::SkipDated(_) | Change::Missing(_) => {}
        }
    }
    t
}

fn fmt_entry(e: &Entry) -> String {
    let o = |x: Option<f64>| x.map_or("-".into(), |v| v.to_string());
    let mut s = format!(
        "in {} cache {} write {} out {}",
        e.input,
        o(e.cache),
        o(e.write),
        e.out
    );
    if let Some(l) = e.long {
        s += &format!(" long>{}: in×{} out×{}", l.threshold, l.in_x, l.out_x);
    }
    s
}

/// Human-readable diff (one line per change).
pub fn render_diff(changes: &[Change]) -> String {
    let mut s = String::new();
    for c in changes {
        let line = match c {
            Change::Add(e) => format!("+ {:<28} {}", e.model, fmt_entry(e)),
            Change::Update { old, new } => {
                format!(
                    "~ {:<28} {}\n  {:<28} → {}",
                    old.model,
                    fmt_entry(old),
                    "",
                    fmt_entry(new)
                )
            }
            Change::SkipDated(m) => format!("! {m:<28} has dated/promo entry locally; not touched"),
            Change::Missing(m) => format!("? {m:<28} not in source; kept"),
        };
        s.push_str(&line);
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    const T: &str = r#"
[defaults]
flex_x = 0.5
fast_x = 2.0

[[model]]
model = "m"
in = 2.0
cache = 0.2
write = 2.5
out = 12.0
long = { threshold = 272000, in_x = 2.0, out_x = 1.5 }

[[model]]
model = "fx"
in = 5.0
cache = 0.5
out = 30.0
fast_x = 2.5

[[model]]
model = "fx"
tier = "flex"
in = 1.0
out = 1.0

[[model]]
model = "promo"
in = 4.0
cache = 0.4
out = 20.0
effective_until = "2026-11-21"

[[model]]
model = "promo"
in = 9.0
out = 90.0
effective_from = "2026-11-22"
"#;

    fn ms(d: &str) -> i64 {
        parse_date(d).unwrap() * 86_400_000
    }
    fn close(a: Option<f64>, b: f64) {
        let a = a.unwrap();
        assert!((a - b).abs() < 1e-12, "{a} vs {b}");
    }

    #[test]
    fn formula_cached_write_reasoning_in_out() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 200_000,
            cached: 120_000,
            cache_write: 20_000,
            output: 100_000,
        };
        // 60k·2 + 120k·0.2 + 20k·2.5 + 100k·12 (reasoning already in out)
        close(
            t.cost(Some("m"), Tier::Standard, 0, u),
            (60_000.0 * 2.0 + 120_000.0 * 0.2 + 20_000.0 * 2.5 + 100_000.0 * 12.0) / 1e6,
        );
        // missing write price ⇒ input price
        let u2 = Usage {
            input: 100_000,
            cached: 0,
            cache_write: 100_000,
            output: 0,
        };
        close(t.cost(Some("fx"), Tier::Standard, 0, u2), 0.5);
        // case-insensitive model
        assert!(t.cost(Some("M"), Tier::Standard, 0, u).is_some());
    }

    #[test]
    fn long_context_threshold() {
        let t = PriceTable::parse(T).unwrap();
        let at = Usage {
            input: 272_000,
            cached: 72_000,
            cache_write: 0,
            output: 1_000,
        };
        close(
            t.cost(Some("m"), Tier::Standard, 0, at),
            (200_000.0 * 2.0 + 72_000.0 * 0.2 + 1_000.0 * 12.0) / 1e6,
        );
        let over = Usage {
            input: 272_001,
            ..at
        };
        close(
            t.cost(Some("m"), Tier::Standard, 0, over),
            (200_001.0 * 4.0 + 72_000.0 * 0.4 + 1_000.0 * 18.0) / 1e6,
        );
    }

    #[test]
    fn tiers() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 100_000,
            cached: 0,
            cache_write: 0,
            output: 100_000,
        };
        close(t.cost(Some("m"), Tier::Flex, 0, u), 1.4 * 0.5); // default flex ×0.5
        close(t.cost(Some("m"), Tier::PriorityFast, 0, u), 1.4 * 2.0); // default fast ×2
        close(t.cost(Some("fx"), Tier::PriorityFast, 0, u), 3.5 * 2.5); // per-model fast
        close(t.cost(Some("fx"), Tier::Flex, 0, u), 0.2); // explicit tier entry wins
        assert_eq!(
            Tier::from_service_tier(Some("priority")),
            Tier::PriorityFast
        );
        assert_eq!(Tier::from_service_tier(Some("auto")), Tier::Standard);
        assert_eq!(Tier::from_service_tier(None), Tier::Standard);
    }

    #[test]
    fn promo_date_boundary() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 1_000_000,
            cached: 0,
            cache_write: 0,
            output: 0,
        };
        close(
            t.cost(
                Some("promo"),
                Tier::Standard,
                ms("2026-11-21") + 86_399_999,
                u,
            ),
            4.0,
        );
        close(
            t.cost(Some("promo"), Tier::Standard, ms("2026-11-22"), u),
            9.0,
        );
        let only_promo = PriceTable::parse(
            "[[model]]\nmodel=\"p\"\nin=1.0\nout=1.0\neffective_until=\"2026-11-21\"\n",
        )
        .unwrap();
        assert!(only_promo
            .cost(Some("p"), Tier::Standard, ms("2026-11-22"), u)
            .is_none());
        assert!(PriceTable::parse(
            "[[model]]\nmodel=\"p\"\nin=1.0\nout=1.0\neffective_until=\"21.11.2026\"\n"
        )
        .is_err());
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2026-11-21"), Some(20_778));
    }

    #[test]
    fn unknown_is_none_not_zero() {
        let t = PriceTable::parse(T).unwrap();
        let u = Usage {
            input: 10,
            cached: 0,
            cache_write: 0,
            output: 10,
        };
        assert!(t.cost(Some("nope"), Tier::Standard, 0, u).is_none());
        assert!(t.cost(None, Tier::Standard, 0, u).is_none());
        assert!(PriceTable::default()
            .cost(Some("m"), Tier::Standard, 0, u)
            .is_none());
    }

    #[test]
    fn example_file_parses() {
        let s = include_str!("../../../prices.example.toml");
        let t = PriceTable::parse(s).unwrap();
        let u = Usage {
            input: 100_000,
            cached: 0,
            cache_write: 0,
            output: 100_000,
        };
        close(
            t.cost(Some("gpt-5.6-terra"), Tier::Standard, ms("2026-10-13"), u),
            1.4,
        );
        close(
            t.cost(Some("gpt-5.5"), Tier::PriorityFast, ms("2026-10-13"), u),
            3.5 * 2.5,
        );
        close(
            t.cost(Some("gpt-5.6-sol"), Tier::Standard, ms("2026-11-21"), u),
            2.4,
        );
        assert!(
            t.cost(Some("gpt-5.6-sol"), Tier::Standard, ms("2026-11-22"), u)
                .is_none(),
            "regular price unknown"
        );
    }

    #[test]
    fn models_dev_diff() {
        let j: serde_json::Value = serde_json::from_str(
            r#"{"openai":{"models":{
            "m":{"cost":{"input":2,"output":13,"cache_read":0.2,"cache_write":2.5,
                 "tiers":[{"input":4,"output":19.5,"tier":{"type":"context","size":272000}}]}},
            "new":{"cost":{"input":1,"output":2}},
            "promo":{"cost":{"input":4,"output":20}},
            "nocost":{}}}}"#,
        )
        .unwrap();
        let r = from_models_dev(&j, "openai");
        assert_eq!(r.len(), 3);
        let t = PriceTable::parse(T).unwrap();
        let d = diff(&t, &r);
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::Add(e) if e.model == "new")));
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::SkipDated(m) if m == "promo")));
        assert!(d
            .iter()
            .any(|c| matches!(c, Change::Missing(m) if m == "fx")));
        let up = d
            .iter()
            .find_map(|c| match c {
                Change::Update { new, .. } => Some((**new).clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!((up.out, up.long.unwrap().out_x), (13.0, 1.5));
        let t2 = apply(&t, &d);
        let back = PriceTable::parse(&t2.to_toml()).unwrap();
        assert_eq!(back, t2);
        assert!(diff(&back, &r)
            .iter()
            .all(|c| !matches!(c, Change::Add(_) | Change::Update { .. })));
        assert!(render_diff(&d).contains("+ new"));
    }
}
