"use strict";
// AccModel: data and state of the accounts view, with no DOM. Pure helpers (quota windows, reset
// cards, refresh results) plus three stateful pieces whose race rules are user-visible behaviour:
//   createStore(deps)  — the account snapshot and its partial reads (quota, reset cards, history);
//   createDevice(deps) — the device-code login flow ("stop waiting" only stops this page);
//   consume helpers    — the reset-card confirm dialog's retry rules.
// scripts/accounts-check.cjs loads this file into a Node vm and drives it with fake deps.
const AccModel = (() => {
  const t = (k, v) => I18n.t(k, v);

  // ---------- quota windows ----------
  // Window label from upstream window_minutes (never from plan or slot): 300 → 5h, 10080 → week,
  // a day or more → N days (free plan: 30 days), else hours / minutes. Without minutes: fallback.
  function wlab(m, fallback) {
    if (!m) return fallback;
    if (m === 300) return t("accounts.w.5h");
    if (m === 10080) return t("accounts.w.week");
    if (m >= 1440) return t("accounts.w.days", {n: Math.round(m / 1440)});
    if (m >= 60) return t("accounts.w.hours", {n: Math.round(m / 60)});
    return t("accounts.w.mins", {n: m});
  }

  // Windows by length: up to a day → short slot "s", longer → long slot "l". A pro or free plan's
  // only window arrives as primary but is long. Without window_minutes the slot decides.
  // → {s: [label, window, "s"] | null, l: [...] | null}
  function wslots(q) {
    const o = {s: null, l: null};
    if (!q) return o;
    for (const [w, d, k] of [[q.primary, t("accounts.w.5h"), "s"], [q.secondary, t("accounts.w.week"), "l"]]) {
      if (!w || w.used_pct === null || w.used_pct === undefined) continue;
      let s = w.window_minutes ? (w.window_minutes > 1440 ? "l" : "s") : k;
      if (o[s]) s = s === "s" ? "l" : "s";
      if (!o[s]) o[s] = [wlab(w.window_minutes, d), w, s];
    }
    return o;
  }

  // Remaining percent (0..100) or null when unknown. A window whose reset time has passed is full
  // again, even if the last snapshot said otherwise.
  function rem(w, now) {
    if (!w || w.used_pct === null || w.used_pct === undefined) return null;
    if (w.resets_at && w.resets_at * 1000 <= (now === undefined ? Date.now() : now)) return 100;
    return Math.max(0, 100 - Number(w.used_pct));
  }

  // Reset time for the cell: relative within a day, else the date. null when not known.
  function resetText(w, now) {
    if (!w || !w.resets_at) return null;
    now = now === undefined ? Date.now() : now;
    const s = w.resets_at - now / 1000;
    if (s <= 0) return t("accounts.q.resetDone");
    return s < 86400 ? t("accounts.q.resetIn", {d: Core.fmt.dur(s)}) : t("accounts.q.resetAt", {time: Core.fmt.time(w.resets_at * 1000,
        true)});
  }

  // The last snapshot is more than an hour old.
  const staleQuota = (qq, now) => !!(qq && qq.observed_at_ms && (now === undefined ? Date.now() : now) - qq.observed_at_ms > 36e5);

  // Estimated capacity of a window → {mid, lo, hi, lower, apiUsd} (credits; apiUsd is the capacity, or
  // for a lower bound its lower bound, at API price) or null.
  function capacity(w) {
    const e = (w && w.estimate) || {};
    const usd = v => Core.isNum(v) ? Number(v) : null;
    if (Core.isNum(e.capacity_credits)) {
      return {mid: Number(e.capacity_credits), lo: Core.isNum(e.capacity_lo) ? Number(e.capacity_lo) : null,
        hi: Core.isNum(e.capacity_hi) ? Number(e.capacity_hi) : null, lower: false, gap: !!e.coverage_gap,
        apiUsd: usd(e.capacity_api_usd)};
    }
    if (Core.isNum(e.capacity_lo)) return {mid: null, lo: Number(e.capacity_lo), hi: null, lower: true, gap: !!e.coverage_gap,
      apiUsd: usd(e.capacity_api_lo)};
    return null;
  }

  // ---------- 点数 (purchased credits), as upstream last reported them ----------
  const crBal = b => (b === null || b === undefined ? "?" : String(+Number(b).toFixed(2)));
  // → {text, none} or null when upstream never reported credits.
  function crText(cr) {
    if (!cr) return null;
    if (cr.unlimited) return {text: t("accounts.cr.unlimited"), none: false};
    if (!cr.has_credits) return {text: t("accounts.cr.none"), none: true};
    return {text: t("accounts.cr.balance", {n: crBal(cr.balance)}), none: false};
  }

  // ---------- reset cards ----------
  // Count: the newer of the card listing and the last quota refresh answer.
  function cardCount(r) {
    if (!r) return null;
    const L = r.available_count, U = r.usage_available_count;
    const l = L !== null && L !== undefined, u = U !== null && U !== undefined;
    if (l && u) return (r.usage_observed_ms || 0) > (r.fetched_ms || 0) ? U : L;
    return l ? L : u ? U : null;
  }

  // limit_reached of the last quota refresh, unless a newer quota observation replaced it.
  function limitHit(r, qq) {
    return !!(r && r.limit_reached && r.usage_observed_ms && !(qq && qq.observed_at_ms > r.usage_observed_ms + 5e3));
  }

  const expOf = c => {
    const x = c && c.expires_at ? Date.parse(c.expires_at) : NaN;
    return isNaN(x) ? Infinity : x;
  };

  // Cards split into usable (soonest first) and the rest (used / expired).
  function cardLists(r, now) {
    now = now === undefined ? Date.now() : now;
    const cs = ((r && r.credits) || []).slice().sort((x, y) => expOf(x) - expOf(y));
    const av = cs.filter(c => (!c.status || c.status === "available") && expOf(c) > now);
    return {available: av, used: cs.filter(c => !av.includes(c))};
  }

  const soonMs = 864e5;
  function nearest(r) {
    const n = r && r.nearest_expires_at ? Date.parse(r.nearest_expires_at) : NaN;
    return isNaN(n) ? null : n;
  }

  // ---------- account status ----------
  const STATUS = {normal: "ok", cooling: "warn", disabled: "off", token_invalid: "err"};
  const STATUS_ORDER = ["normal", "cooling", "disabled", "token_invalid"];
  const statusKind = s => STATUS[s] || "off";
  const usable = a => a.status !== "disabled" && a.status !== "token_invalid";
  const nameOf = a => a.email || a.id;
  const pathOf = a => "/admin/v1/accounts/" + encodeURIComponent(a.id || a);

  // Taking `a` out leaves no account that can serve right now (cooling ones only come back later).
  function lastOne(a, accounts) {
    const live = accounts.filter(x => x.status === "normal").length;
    return a.status === "normal" ? live === 1 : usable(a) && !live && accounts.filter(usable).length === 1;
  }

  function counts(accounts) {
    const c = {normal: 0, cooling: 0, disabled: 0, token_invalid: 0};
    for (const a of accounts) c[a.status] = (c[a.status] || 0) + 1;
    return c;
  }

  // ---------- active quota query (刷新额度) results ----------
  const qskip = r => !r.ok && r.error && r.error.kind === "rate_limited";
  function qerr(e) {
    e = e || {};
    const m = e.message || "";
    switch (e.kind) {
      case "rate_limited": return t("accounts.qe.rateLimited", {s: e.retry_after_s || 30});
      case "http": return t("accounts.qe.http", {status: e.status}) + (m ? " · " + m : "");
      case "network": return t("accounts.qe.network", {msg: m.replace(/^send: /, "")});
      case "timeout": return t("accounts.qe.timeout");
      case "credentials": return t("accounts.qe.credentials", {msg: m.replace(/^credentials: /, "")});
      case "token_invalid": return t("accounts.qe.tokenInvalid");
      case "decode": return t("accounts.qe.decode");
      default: return m || t("api.unknown");
    }
  }
  function qsummary(j) {
    const results = j.results || [];
    const sk = j.skipped !== undefined ? j.skipped : results.filter(qskip).length;
    const cleared = results.filter(r => r.cooldown_cleared).length, p = [];
    if (j.ok) p.push(t("accounts.qs.ok", {n: j.ok}));
    if (cleared) p.push(t("accounts.qs.cleared", {n: cleared}));
    if (sk) p.push(t("accounts.qs.skipped", {n: sk}));
    return p.join(t("accounts.sep")) || t("accounts.qs.nothing");
  }

  // ---------- import files ----------
  // Files over 1 MiB are not sent (the server refuses them anyway); at most 100 per import.
  const MAX_FILE = 1048576;
  async function importPayload(files) {
    if (files.length > 100) throw new Error(t("accounts.imp.tooMany"));
    const send = [], big = [];
    for (const f of files) {
      if (f.size > MAX_FILE) big.push({name: f.name, status: "skipped", reason: "too_large"});
      else send.push({name: f.name, content: await f.text()});
    }
    return {send, big};
  }

  // ---------- the account store ----------
  // deps: get(path) → json (throws Api-style errors), post(path, body) → json, identity() → string
  // (key + login epoch), now(), onChange() after a snapshot changed, onAccount(id) after one row
  // changed. Every read is bound to its load generation and identity: a superseded or old-login
  // answer (success or failure) changes nothing.
  function createStore(deps) {
    const now = () => (deps.now ? deps.now() : Date.now());
    const s = {
      data: null, quota: {}, credits: {}, cardTop: {can: false, adm: false, attempts: []}, auto: {}, autoTop: null, quotaAuto: {},
      loadSeq: 0, auth: null, readAt: 0, rev: 0, switches: new Map(), qerr: {}, needAdmin: false, loading: false, error: null,
      reads: {quota: {at: 0, note: null}, credits: {at: 0, note: null}},
      hist: null, histAt: 0, histPending: null,
    };

    function reset() {
      s.data = null;
      s.quota = {};
      s.credits = {};
      s.cardTop = {can: false, adm: false, attempts: []};
      s.auto = {};
      s.autoTop = null;
      s.quotaAuto = {};
      s.readAt = 0;
      s.switches.clear();
      s.qerr = {};
      s.needAdmin = false;
      s.error = null;
      s.hist = null;
      s.histAt = 0;
      s.rev++;
      for (const m of Object.values(s.reads)) {
        m.at = 0;
        m.note = null;
      }
    }

    // One source of a partial read. 403 / 404 clear that source (no pretending with old data); any
    // other failure keeps the last success and says when it was read.
    function readPart(result, previous, meta, source, valueKey) {
      if (result.status === "fulfilled") {
        const next = {};
        for (const x of (result.value && result.value.accounts) || []) next[x.account] = valueKey ? x[valueKey] : x;
        meta.at = now();
        meta.note = null;
        return next;
      }
      const e = result.reason || {};
      if (e.status === 403 || e.status === 404) {
        meta.at = 0;
        meta.note = {source, kind: e.status === 403 ? "denied" : source === "quota" && s.data && s.data.usage_enabled === false ? "off" :
          "unavailable"};
        return {};
      }
      meta.note = {source, kind: "fail", why: Api.why(e), at: meta.at};
      return previous;
    }

    // Card listing flags: who may use / refresh cards, the last attempts.
    function readCardTop(r) {
      if (r.status === "fulfilled") {
        const j = r.value || {};
        s.cardTop = {can: !!j.can_consume, adm: !!j.is_admin, attempts: j.attempts || []};
        return;
      }
      const e = r.reason;
      if (e && (e.status === 403 || e.status === 404)) s.cardTop = {can: false, adm: false, attempts: []};
    }

    // Per-account switches (auto card use, auto quota refresh) and their settings; a failed listing
    // keeps the last answer. A quota refresh range reads as fixed when only the minimum is given.
    function readAuto(r) {
      if (r.status !== "fulfilled") return;
      const j = r.value || {}, m = {}, qm = {};
      for (const x of j.accounts || []) {
        m[x.account] = {on: !!x.auto, next: x.auto_next_ms || null};
        qm[x.account] = !!x.quota_auto;
      }
      s.auto = m;
      s.quotaAuto = qm;
      s.autoTop = {lead: j.auto_lead_mins, ok: !!j.auto_available, qmins: j.quota_refresh_mins || 0,
        qmax: j.quota_refresh_mins ? j.quota_refresh_max_mins || j.quota_refresh_mins : 0};
    }

    // load({prewarm}) → true when applied, false when superseded (or admin required); throws when
    // the account list itself failed (data and its time stay as they were).
    async function load(o) {
      o = o || {};
      const seq = ++s.loadSeq, id = deps.identity();
      const current = () => seq === s.loadSeq && id === deps.identity();
      if (s.auth !== id) {
        reset();
        s.auth = id;
      }
      const rev = s.rev;
      s.loading = true;
      try {
        const main = deps.get("/admin/v1/accounts" + (o.prewarm ? "?include_prewarm=1" : ""), {current});
        const parts = Promise.allSettled([deps.get("/usage/v1/quota", {current}), deps.get("/usage/v1/reset-credits", {current})]);
        let j;
        try {
          j = await main;
        } catch (e) {
          await parts;
          throw e;
        }
        if (!current()) return false;
        if (!j || !Array.isArray(j.accounts)) throw new Error(t("accounts.err.shape"));
        const [q, rc] = await parts;
        if (!current()) return false;
        s.data = j;
        s.readAt = now();
        s.error = null;
        s.needAdmin = false;
        s.quota = readPart(q, s.quota, s.reads.quota, "quota");
        s.credits = readPart(rc, s.credits, s.reads.credits, "credits", "reset_credits");
        readCardTop(rc);
        if (rev === s.rev) readAuto(rc);
        if (deps.onChange) deps.onChange();
        return true;
      } catch (e) {
        if (!current()) return false;
        if (Api.isAdminError(e)) {
          reset();
          s.needAdmin = true;
          if (deps.onChange) deps.onChange();
          return false;
        }
        s.error = e;
        e.current = current;
        if (deps.onChange) deps.onChange();
        throw e;
      } finally {
        if (current()) s.loading = false;
      }
    }

    // ---------- 30-day quota history: one read for every account ----------
    // Throttled to 60 s unless forced (an action changed the history, or the user pressed refresh).
    function loadHist(force) {
      const id = deps.identity();
      if (s.histPending) return s.histPending;
      if (!force && s.hist && now() - s.histAt < 60e3) return Promise.resolve(s.hist);
      s.histPending = deps.get("/usage/v1/quota/history?days=30", {timeout: 45000}).then(j => {
        if (deps.identity() !== id) return;
        j = j || {};
        const by = {};
        for (const x of j.accounts || []) by[x.account] = x;
        s.hist = {from_ms: j.from_ms, to_ms: j.to_ms, by};
        s.histAt = now();
        if (deps.onChange) deps.onChange();
      }).catch(e => {
        if (deps.identity() !== id || Api.isQuiet(e)) return;
        s.hist = {error: e.status === 404 ? {kind: "off"} : {kind: "fail", why: Api.why(e)}};
        s.histAt = now();
        if (deps.onChange) deps.onChange();
      }).finally(() => {
        s.histPending = null;
      });
      return s.histPending;
    }

    // History of one account: null while loading, {error}, or {from_ms, to_ms, cycles, events, epochs}.
    function histOf(accountId) {
      if (!s.hist) return null;
      if (s.hist.error) return {error: s.hist.error};
      const r = s.hist.by[accountId] || {events: [], cycles: [], epochs: []};
      return Object.assign({from_ms: s.hist.from_ms, to_ms: s.hist.to_ms}, r);
    }

    // ---------- per-account switches ----------
    // The POST reply wins over the requested value; a superseded switch (another click on the same
    // switch, logout, re-login) leaves no trace. Only the affected row re-renders; enabling auto card
    // use reads the next planned time afterwards without making the switch wait for it.
    function beginSwitch(accountId, kind) {
      const slot = accountId + "|" + kind, token = {}, id = deps.identity();
      s.switches.set(slot, token);
      s.rev++;
      return () => id === deps.identity() && s.switches.get(slot) === token;
    }
    async function setSwitch(a, kind, on) {
      const current = beginSwitch(a.id, kind), auto = kind === "auto";
      let j;
      try {
        j = await deps.post(pathOf(a) + (auto ? "/reset-credits/auto" : "/quota-refresh/auto"), {enabled: on});
      } catch (e) {
        if (!current()) return undefined;
        throw e;
      }
      if (!current()) return undefined;
      const enabled = auto ? !!j.auto : !!j.quota_auto;
      const accountId = j.account || a.id;
      if (auto) s.auto[accountId] = {on: enabled, next: null};
      else s.quotaAuto[accountId] = enabled;
      if (deps.onAccount) deps.onAccount(accountId);
      if (auto && enabled) {
        deps.get("/usage/v1/reset-credits", {timeout: 45000}).then(list => {
          if (!current()) return;
          const x = ((list && list.accounts) || []).find(y => y.account === accountId);
          if (x && s.auto[accountId]) {
            s.auto[accountId].next = x.auto_next_ms || null;
            if (deps.onAccount) deps.onAccount(accountId);
          }
        }).catch(e => {
          if (current() && deps.onError) deps.onError(e);
        });
      }
      return current() ? enabled : undefined;
    }

    // ---------- active quota query ----------
    // The answer patches the shown snapshot at once (the stored one follows with the usage writer's
    // next batch). A failure stays on the row until the next success; "asked moments ago" is not one.
    function applyQuota(r) {
      if (r.ok) {
        delete s.qerr[r.account];
        const q = r.quota || {}, old = s.quota[r.account] || {account: r.account};
        s.quota[r.account] = Object.assign({}, old, {quota: {plan: q.plan, observed_at_ms: r.observed_at_ms, observed_by: "manual",
          primary: q.primary, secondary: q.secondary, credits: q.credits !== undefined ? q.credits : old.quota && old.quota.credits}});
        s.credits[r.account] = Object.assign({}, s.credits[r.account], {
          usage_available_count: r.usage_available_count === undefined ? null : r.usage_available_count,
          limit_reached: r.limit_reached === undefined ? null : r.limit_reached, usage_observed_ms: r.observed_at_ms});
      } else if (r.error && r.error.kind !== "rate_limited") s.qerr[r.account] = r.error;
    }

    // ---------- optimistic edits ----------
    // patchLocal(id, fields) applies fields to the shown account now and returns a function that puts
    // the previous values back (only those still showing the optimistic value).
    function patchLocal(accountId, fields) {
      const a = s.data && s.data.accounts.find(x => x.id === accountId);
      if (!a) return () => {};
      const before = {};
      for (const k in fields) before[k] = a[k];
      Object.assign(a, fields);
      if (deps.onAccount) deps.onAccount(accountId);
      return () => {
        const b = s.data && s.data.accounts.find(x => x.id === accountId);
        if (!b) return;
        for (const k in fields) if (b[k] === fields[k]) b[k] = before[k];
        if (deps.onAccount) deps.onAccount(accountId);
      };
    }

    const account = accountId => (s.data ? s.data.accounts.find(x => x.id === accountId) || null : null);

    return {state: s, reset, load, loadHist, histOf, setSwitch, beginSwitch, applyQuota, patchLocal, account,
      readPart, readCardTop, readAuto};
  }

  // ---------- device-code login ----------
  // deps: request(method, path, opts) → json (opts.current: a stale 401 must not log out),
  // identity(), onUpdate(state), onDone(account), now(), setInterval, clearInterval.
  // state.phase: idle | starting | waiting; state.note: {kind, ...} for the last outcome.
  // Stopping only ends this page's wait: the server may still finish an authorization.
  function createDevice(deps) {
    const now = () => (deps.now ? deps.now() : Date.now());
    const si = deps.setInterval || setInterval, ci = deps.clearInterval || clearInterval;
    const st = {phase: "idle", url: "", code: "", until: 0, note: null};
    let seq = 0, timer = null;
    const emit = () => deps.onUpdate && deps.onUpdate(st);

    function end(note) {
      seq++;
      if (timer !== null) ci(timer);
      timer = null;
      st.phase = "idle";
      st.url = "";
      st.code = "";
      st.until = 0;
      st.note = note || null;
      emit();
    }

    async function start() {
      if (st.phase !== "idle") return;
      end();
      const my = seq, id = deps.identity();
      const current = () => my === seq && id === deps.identity();
      st.phase = "starting";
      emit();
      try {
        const j = await deps.request("POST", "/admin/v1/accounts/login/device", {timeout: 45000, current});
        if (!current()) return;
        if (!j || !j.url || !j.code || !j.login_id) throw new Error(t("accounts.dev.badShape"));
        st.phase = "waiting";
        st.url = j.url;
        st.code = j.code;
        st.until = now() + 15 * 60000;
        emit();
        let polling = false;
        timer = si(async () => {
          if (!current() || polling) return;
          if (now() > st.until) {
            end({kind: "expired"});
            return;
          }
          polling = true;
          try {
            const s = await deps.request("GET", "/admin/v1/accounts/login/" + encodeURIComponent(j.login_id), {timeout: 45000, current});
            if (!current()) return;
            if (s && s.status === "done") {
              end({kind: "done", account: s.account});
              if (deps.onDone) deps.onDone(s.account);
            } else if (s && s.status === "error") end({kind: "failed", msg: s.message || ""});
          } catch (e) {
            if (current()) end({kind: "checkFailed", why: Api.why(e)});
          } finally {
            polling = false;
          }
        }, 3000);
      } catch (e) {
        if (current()) end({kind: "startFailed", why: Api.why(e)});
      }
    }

    // User pressed "stop waiting": the code is withdrawn from the page, the note explains the rest.
    function stop() {
      const was = st.phase === "waiting";
      end(was ? {kind: "stopped"} : null);
    }

    return {state: st, start, stop, end, busy: () => st.phase !== "idle", timers: () => (timer === null ? 0 : 1)};
  }

  // ---------- reset card: manual use ----------
  // attempt = {account, credit, rid}. The server requires {confirm: account}; a retry of an attempt
  // whose result is unknown reuses its redeem_request_id so the card is never taken twice.
  function consumeBody(attempt) {
    return Object.assign({confirm: attempt.account}, attempt.credit ? {credit_id: attempt.credit} : {},
      attempt.rid ? {redeem_request_id: attempt.rid} : {});
  }
  // Failure → {next: attempt for the retry button, retry: "pending" | "same" | null}.
  function consumeFailure(attempt, e) {
    if (e && e.status === 409 && e.body && e.body.pending) {
      const p = e.body.pending;
      return {next: {account: attempt.account, credit: p.credit_id || null, rid: p.redeem_request_id}, retry: "pending", pending: p};
    }
    const unknown = !!(e && e.body && e.body.status === "pending");
    return {next: attempt, retry: unknown ? "same" : null};
  }

  // Hints before using a card: no window is low (may be wasted), or a low one resets naturally soon.
  function consumeHint(qq, now) {
    now = now === undefined ? Date.now() : now;
    const o = wslots(qq), w = [o.s, o.l].filter(x => x && rem(x[1], now) !== null);
    if (!w.length) return null;
    const low = w.filter(x => rem(x[1], now) < 20);
    if (!low.length) return {kind: "notLow"};
    const soon = low.find(x => x[1].resets_at && x[1].resets_at * 1000 > now && x[1].resets_at * 1000 - now < 30 * 6e4);
    return soon ? {kind: "resetsSoon", label: soon[0], secs: soon[1].resets_at - now / 1000} : null;
  }

  return {wlab, wslots, rem, resetText, staleQuota, capacity, crBal, crText, cardCount, limitHit, expOf, cardLists, nearest, soonMs,
    STATUS_ORDER, statusKind, usable, nameOf, pathOf, lastOne, counts, qskip, qerr, qsummary, importPayload, createStore, createDevice,
    consumeBody, consumeFailure, consumeHint};
})();
