"use strict";
// Settings model: the draft (unsaved edits over a config snapshot), the load / validate / save /
// discard controller with its race guards, value formatting and the section layout. No DOM here,
// so scripts/settings-check.cjs can drive it in a vm.
const SettingsModel = (() => {
  const t = (k, v) => I18n.t(k, v);

  // ---------- layout: every schema key sits in exactly one section ----------
  // Rows: "key" | {k, p: parent key (greyed while the parent is off)} | {fn: info row id}.
  const SECTIONS = [
    {g: "forward", id: "routing", docs: true, secs: [
      {t: "select", rows: ["routing.strategy", "routing.quota_avoid_threshold", "routing.cooldown_wait_secs"]},
      {t: "affinity", rows: ["routing.session_affinity", {k: "routing.session_affinity_ttl", p: "routing.session_affinity"},
        {k: "routing.session_affinity_subagents", p: "routing.session_affinity"},
        {k: "routing.session_affinity_rebind_on", p: "routing.session_affinity"}]}]},
    {g: "forward", id: "network", secs: [
      {t: "proxy", note: "immediate", rows: ["proxy_enabled", "proxy_url"]},
      {t: "conn", rows: ["ipv6", "upstream_opts.connect_timeout_secs"]}],
    adv: ["upstream", "ws_compression", "upstream_opts.compress_requests", "upstream_opts.gzip_responses",
      "upstream_opts.cloudflare_cookies"]},
    {g: "forward", id: "forward", secs: [
      {t: "limits", rows: ["max_body_mb", "max_buffered_body_mb", "max_inflight_requests", "ws_max_connections", "ws_max_message_mb"]},
      {t: "retry", rows: ["upstream_opts.max_attempts", "upstream_opts.preparation_timeout_secs"]},
      {t: "shutdown", rows: ["shutdown_drain_secs"]},
      {t: "cool", note: "coolNote", cool: true}],
    adv: ["upstream_opts.response_header_timeout_secs", "upstream_opts.stream_idle_timeout_secs",
      "upstream_opts.overload_retry_wait_secs"]},
    {g: "forward", id: "images", secs: [{rows: ["images.enabled", {k: "images.main_model", p: "images.enabled"}]}],
      adv: [{k: "images.default_originator", p: "images.enabled"}, {k: "images.default_user_agent", p: "images.enabled"}]},
    {g: "data", id: "usage", secs: [
      {t: "usageLog", rows: ["usage.enabled", {k: "usage.retention_days", p: "usage.enabled"}]},
      {t: "priceRow", rows: [{fn: "prices"}]}],
    adv: [{k: "usage.quota_weights.input", p: "usage.enabled"}, {k: "usage.quota_weights.cached", p: "usage.enabled"},
      {k: "usage.quota_weights.output", p: "usage.enabled"}, {k: "usage.quota_weights.min_delta_pct", p: "usage.enabled"},
      {k: "usage.queue_size", p: "usage.enabled"}, {k: "usage.batch_size", p: "usage.enabled"}, {k: "usage.flush_ms", p: "usage.enabled"}]},
    {g: "data", id: "prices", secs: [{box: "prices"}, {t: "priceSource", rows: ["usage.prices", "usage.prices_source"]}]},
    {g: "data", id: "accounts", secs: [
      {t: "accounts", rows: [{fn: "accounts"}, "auth_dir"]},
      {t: "refresh", rows: ["ui.reset_credits_refresh_mins", "ui.quota_refresh_mins"]},
      {t: "reset", rows: ["ui.reset_credits_auto_lead_mins", "ui.allow_reset_consume"]}]},
    {g: "system", id: "general", rest: true, secs: [
      {rows: ["instance_id", "log.level"]},
      {t: "runtime", note: "runtimeNote", rows: [{fn: "file"}, "listen", "ui.enabled", "ui.allow_config_write", "usage.path"]},
      {t: "tz", rows: [{fn: "tz"}, {fn: "fp"}]}],
    adv: []},
    {g: "system", id: "keys", secs: [{box: "keys"}]},
    {g: "system", id: "audit", secs: [{box: "audit"}]},
  ];
  const GROUPS = ["forward", "data", "system"];
  // Old anchors (single-page settings, v0.2) land on the section that now holds that group.
  const ALIAS = {"g-base": "general", "g-routing": "routing", "g-upstream_opts": "network", "g-images": "images", "g-log": "general",
    "g-usage": "usage", "g-ui": "accounts", "keysec": "keys"};
  const DOCS = "https://github.com/Seia-Yurizono/ciallopa#how-an-account-is-chosen";
  // Cooldown table: one row per upstream error; the trigger is fixed text or a threshold key.
  const COOL = [
    {id: "5xx", trig: "t3", ks: ["upstream_opts.transient_cooldown_secs"]},
    {id: "overload", tk: "upstream_opts.overload_streak", ks: ["upstream_opts.overload_cooldown_secs",
      "upstream_opts.overload_cooldown_max_secs"]},
    {id: "402d", trig: "t1", ks: ["upstream_opts.deactivated_cooldown_secs"]},
    {id: "402", trig: "t1", ks: ["upstream_opts.payment_required_cooldown_secs"]},
    {id: "404", trig: "t1", ks: ["upstream_opts.not_found_cooldown_secs"]},
  ];

  // "0 = off" numbers: a switch plus the value. [label key of the off state, fallback when switched on].
  const ZO = {"routing.quota_avoid_threshold": ["off", 0.9], "routing.cooldown_wait_secs": ["noWait", 20],
    "max_buffered_body_mb": ["unlimited", 256], "max_inflight_requests": ["unlimited", 64], "ws_max_connections": ["unlimited", 24],
    "upstream_opts.transient_cooldown_secs": ["noCool", 15], "upstream_opts.overload_streak": ["noCool", 2],
    "upstream_opts.overload_retry_wait_secs": ["noRetry", 3], "upstream_opts.deactivated_cooldown_secs": ["noCool", 86400],
    "upstream_opts.payment_required_cooldown_secs": ["noCool", 1800], "upstream_opts.not_found_cooldown_secs": ["switchOnly", 600],
    "usage.retention_days": ["forever", 90], "ui.reset_credits_refresh_mins": ["manual", 5], "ui.quota_refresh_mins": ["manual", 5]};
  const zoOff = k => t("settings.zo." + ZO[k][0]);
  // Background refresh ranges: the shortest key holds the switch (0 = off), the longest sits next to
  // it. A longest of 0 means the same as the shortest (how a config from before the range loads).
  const RZ = {"ui.reset_credits_refresh_mins": "ui.reset_credits_refresh_max_mins", "ui.quota_refresh_mins": "ui.quota_refresh_max_mins"};
  const RZ_ON = [5, 10];
  // Switches that write the file at once (no draft); the toast offers undo.
  const IMM = {proxy_enabled: true, ipv6: true};

  function rzRange(k, src) {
    const mn = Number(src(k)) || 0, mx = Number(src(RZ[k])) || 0;
    return mn > 0 ? [mn, mx > 0 ? mx : mn] : null;
  }
  function rzText(r) {
    if (!r) return t("settings.zo.manual");
    return r[0] === r[1] ? t("settings.every", {a: r[0]}) : t("settings.everyRange", {a: r[0], b: r[1]});
  }

  // ---------- values ----------
  function fmtValue(v) {
    if (v === null || v === undefined) return t("settings.builtin");
    if (typeof v === "object") return JSON.stringify(v);
    if (v === "") return t("settings.empty");
    return String(v);
  }
  function same(a, b) {
    if (typeof a === "number" || typeof b === "number") return Number(a) === Number(b);
    return JSON.stringify(a === undefined ? null : a) === JSON.stringify(b === undefined ? null : b);
  }
  const choiceOf = (f, v) => (f && f.choices || []).find(c => c.value === v);

  // Value in the UI's terms: bool → on/off, 0 of a "0 = off" key → its off label, choice → label (value).
  function display(f, v) {
    if (f.kind === "bool") return v ? t("settings.isOn") : t("settings.isOff");
    if (RZ[f.key]) return null;
    if (ZO[f.key] && Number(v) === 0) return zoOff(f.key);
    const ch = choiceOf(f, v);
    if (ch) return SettingsText.choice(f, ch).label + " (" + v + ")";
    if (v === "") return t("settings.empty");
    if (v === null || v === undefined) return null;
    const u = SettingsText.unit(f);
    return String(v) + (u ? " " + u : "");
  }
  // Default of a key as shown next to its control; null when there is none to show.
  function defaultText(f, base) {
    const d = base && base.defaults ? base.defaults[f.key] : undefined;
    if (d === undefined || f.kind === "keys") return null;
    if (RZ[f.key]) return rzText(rzRange(f.key, k => base.defaults[k]));
    return display(f, d);
  }

  // Server validation errors → [{key, message}], message localized for the common range form.
  function parseErrors(errors, schema) {
    return (errors || []).map(e => {
      const raw = String(e.message || t("settings.errTitle")).replace(/^invalid:\s*/, "");
      const f = e.key ? (schema || []).find(x => x.key === e.key) : (schema || []).find(x => raw.startsWith(x.key + " "));
      const key = f ? f.key : e.key || null;
      const detail = f && raw.startsWith(f.key + " ") ? raw.slice(f.key.length + 1) : raw;
      const m = /^must be in (-?[\d.]+)\.\.=(-?[\d.]+)$/.exec(detail);
      return {key, field: f || null, message: m ? t("settings.errBetween", {a: m[1], b: m[2]}) : detail};
    });
  }

  // ---------- draft ----------
  // base: the accepted snapshot the form shows. edits: key → value to save. invalid: keys whose
  // control holds text that cannot be saved. rev: bumped by every edit (a review / save started at an
  // older rev must not act). loadSeq: bumped by every load (only the newest load may apply). epoch:
  // bumped on logout. stale: the file changed under the draft (or a save landed but the re-read
  // failed) — every control then counts as edited, so returning to an old value still saves.
  function createDraft(o) {
    o = o || {};
    const identity = o.identity || (() => Api.key() + "|" + Api.epoch());
    const draft = {
      base: null, latest: null, etag: "", edits: {}, invalid: new Set(), serverErrors: new Map(),
      rev: 0, loadSeq: 0, epoch: 0, stale: false, phase: "clean", onChange: o.onChange || (() => {}),
      count() {
        return new Set([...Object.keys(this.edits), ...this.invalid]).size;
      },
      has(k) {
        return k in this.edits || this.invalid.has(k);
      },
      isChanged(value, original) {
        return this.stale || value !== original;
      },
      edit(k, value, original, shown) {
        const changed = this.isChanged(shown === undefined ? value : shown, original);
        if (changed) this.edits[k] = value;
        else delete this.edits[k];
        return changed;
      },
      invalidate(k, bad) {
        if (bad) this.invalid.add(k);
        else this.invalid.delete(k);
      },
      settle() {
        this.phase = this.stale ? "stale" : this.count() ? "dirty" : "clean";
      },
      changed() {
        this.rev++;
        this.settle();
        this.onChange();
      },
      rebase(cfg) {
        this.base = cfg;
        this.latest = cfg;
        this.edits = {};
        this.invalid.clear();
        this.serverErrors.clear();
        this.stale = false;
        this.changed();
      },
      preserve(cfg) {
        this.latest = cfg;
        if (this.base && cfg.version !== this.base.version) this.stale = true;
        this.settle();
      },
      // current(edits = true): still the same identity, (for loads) the newest load, and (unless
      // edits is false) no edit since the guard was taken.
      guard(opts) {
        const load = !!(opts && opts.load);
        const seq = this.loadSeq, rev = this.rev, epoch = this.epoch, who = identity();
        return (edits = true) => epoch === this.epoch && who === identity() && (!load || seq === this.loadSeq)
          && (!edits || rev === this.rev);
      },
      beginLoad() {
        this.loadSeq++;
        return this.guard({load: true});
      },
      beginReview() {
        this.phase = "reviewing";
        return {changes: Object.assign({}, this.edits), current: this.guard()};
      },
      async commit(write, current) {
        this.phase = "saving";
        try {
          const reply = await write();
          // The file now holds the draft; until a fresh read lands every control counts as edited.
          if (current(false)) {
            this.stale = true;
            this.settle();
          }
          return reply;
        } finally {
          if (current(false)) this.settle();
        }
      },
      // Logout / unmount: nothing in flight may act any more.
      retire() {
        this.epoch++;
        this.loadSeq++;
      },
      // Current value of a key: the draft edit, else the file value, else the running value.
      cur(k) {
        if (k in this.edits) return this.edits[k];
        const s = this.base && (this.base.values || this.base.running);
        return s ? s[k] : undefined;
      },
      on(k) {
        const v = this.cur(k);
        return typeof v === "number" ? v > 0 : k === "proxy_enabled" ? v !== false : !!v;
      },
      field(k) {
        return this.base && (this.base.schema || []).find(f => f.key === k) || null;
      },
      // Differs from the built-in default (editable keys only).
      isMod(k) {
        const f = this.field(k), b = this.base;
        if (!f || !f.editable || !b.defaults || !(k in b.defaults)) return false;
        const v = this.cur(k);
        return v !== undefined && !same(v, b.defaults[k]);
      },
    };
    return draft;
  }

  // ---------- controller ----------
  // req(method, path, body, headers) → {j, r}. hooks: rebased(cfg, unchanged), preserved(cfg,
  // versionChanged), loaded(cfg) (every accepted load), clearServerErrors(), showErrors(list),
  // review({diff, versionChanged, save, close}), saved(reply, remaining), savedStale(err), toast(text).
  function createController(draft, req, hooks) {
    // Structure that the form is built from; runtime numbers and reload times are left out so an
    // unchanged config does not rebuild the form on every refresh.
    const shape = c => JSON.stringify([c.version, c.write_enabled, c.path, c.values, c.running, c.defaults, c.schema,
      c.pending_restart, c.overrides, c.egress, c.file_error]);

    async function load(discard) {
      const current = draft.beginLoad();
      let reply;
      try {
        reply = await req("GET", "/admin/v1/config");
      } catch (e) {
        if (!current(false)) return false;
        throw e;
      }
      if (!current(false)) return false;
      const {j, r} = reply;
      draft.etag = r && r.headers && r.headers.get ? r.headers.get("etag") || "" : "";
      // Keep the real controls while there is a draft: invalid text cannot be rebuilt from edits.
      const keep = draft.count() > 0 && (!discard || !current());
      if (keep) {
        const moved = !!draft.base && j.version !== draft.base.version;
        draft.preserve(j);
        hooks.preserved(j, moved);
      } else {
        const unchanged = !!draft.base && !discard && !draft.stale && shape(draft.base) === shape(j);
        draft.rebase(j);
        hooks.rebased(j, unchanged);
      }
      if (hooks.loaded) hooks.loaded(j);
      return !keep;
    }

    async function review() {
      const {changes, current} = draft.beginReview();
      let j;
      try {
        ({j} = await req("POST", "/admin/v1/config/validate", {changes}));
      } catch (e) {
        if (!current(false)) return;
        draft.settle();
        throw e;
      }
      if (!current(false)) return;
      if (!current()) {
        draft.settle();
        hooks.toast(t("settings.editsChanged"));
        return;
      }
      hooks.clearServerErrors();
      if (!j.ok) {
        draft.settle();
        hooks.showErrors(parseErrors(j.errors, draft.base && draft.base.schema));
        return;
      }
      const versionChanged = !!draft.base && j.version !== draft.base.version;
      hooks.review({diff: j.diff || [], versionChanged, close: () => draft.settle(), save: async () => {
        if (!current()) throw new Error(t("settings.editsOrLogin"));
        // Write against the version the user just reviewed, not the older GET.
        const {j: r} = await draft.commit(() => req("PUT", "/admin/v1/config", {changes}, {"If-Match": '"' + j.version + '"'}), current);
        if (!current(false)) return;
        try {
          await load(current());
        } catch (e) {
          if (!current(false)) return;
          hooks.savedStale(e);
          return;
        }
        if (!current(false)) return;
        hooks.saved(r, draft.count());
      }});
    }

    // Explicit discard: true when the fresh read replaced the draft, false when newer typing kept it.
    const discard = () => load(true);

    // An immediate switch (proxy / IPv6): PUT one key against the latest version, then re-read.
    async function writeNow(k, v) {
      await req("PUT", "/admin/v1/config", {changes: {[k]: v}}, {"If-Match": draft.etag});
      await load();
    }

    return {load, review, discard, writeNow, shape};
  }

  // Audit polling: one interval while the audit section is shown, none elsewhere.
  function createPoller(fn, ms, timers) {
    timers = timers || {set: (f, n) => setInterval(f, n), clear: id => clearInterval(id)};
    let id = 0;
    return {
      sync(on) {
        if (on && !id) id = timers.set(fn, ms);
        else if (!on && id) {
          timers.clear(id);
          id = 0;
        }
      },
      active: () => !!id,
    };
  }

  // Search: "" = off; the special query "@modified" lists changed rows.
  const MOD_Q = "@modified";
  function matches(q, text, modified) {
    if (!q) return true;
    if (q === MOD_Q) return !!modified;
    return String(text).includes(q.toLowerCase());
  }

  // Keys placed nowhere in SECTIONS land in General › Advanced.
  function unplaced(schema) {
    const placed = new Set(["api_keys"]);
    for (const S of SECTIONS) {
      for (const x of S.secs.flatMap(s => s.rows || []).concat(S.adv || [])) if (!x.fn) placed.add(typeof x === "string" ? x : x.k);
    }
    for (const r of COOL) for (const k of (r.tk ? [r.tk] : []).concat(r.ks)) placed.add(k);
    for (const k in RZ) placed.add(RZ[k]);
    return (schema || []).map(f => f.key).filter(k => !placed.has(k));
  }

  function sectionOf(hash) {
    let id = decodeURIComponent(String(hash || "").replace(/^#/, ""));
    id = ALIAS[id] || id;
    return SECTIONS.some(s => s.id === id) ? id : null;
  }

  // The save review groups changes by `effect`, most impactful first: restart > ws_after_turn >
  // new_connection > hot. An effect this page does not know counts as hot (the server only adds
  // finer values below restart). `top` is what the headline states; `ws` says open client
  // WebSockets are affected (closed or left on the old value), which is when the page shows their count.
  const EFFECTS = ["restart", "ws_after_turn", "new_connection", "hot"];
  function groupByEffect(diff) {
    const eff = d => (EFFECTS.includes(d.effect) ? d.effect : "hot");
    const groups = EFFECTS.map(effect => ({effect, rows: diff.filter(d => eff(d) === effect)})).filter(g => g.rows.length);
    return {groups, top: groups.length ? groups[0].effect : "hot",
      ws: groups.some(g => g.effect === "ws_after_turn" || g.effect === "new_connection")};
  }

  // A failed write as the page reports it: a 412 is a version conflict that keeps the draft.
  function failure(e, edits) {
    if (e && e.status === 412) {
      return {conflict: true, title: t("settings.conflictTitle"), body: t(edits ? "settings.conflictDraft" : "settings.conflictClean")};
    }
    return {conflict: false, title: t("settings.opFailed"), body: Api.adminMsg(e && e.body) || (e && e.message) || String(e)};
  }

  return {failure, SECTIONS, GROUPS, ALIAS, DOCS, COOL, ZO, RZ, RZ_ON, IMM, MOD_Q, zoOff, rzRange, rzText, fmtValue, same, choiceOf,
    display, defaultText, parseErrors, createDraft, createController, createPoller, matches, unplaced, sectionOf, groupByEffect};
})();
