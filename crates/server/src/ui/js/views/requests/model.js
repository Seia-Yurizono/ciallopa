"use strict";
// Requests view, part 2: pure logic. URL ↔ filter state, API query parameters, the backend's
// problem / cause / next vocabulary, row classification, chart buckets and attempt bars.
// No DOM here: scripts/requests-check.cjs exercises these functions directly.
const RequestsModel = (() => {
  const PAGE = 50;
  const HOUR = 36e5, DAY = 864e5;

  // Request filters (URL names kept from the old page; Overview links use them too).
  const FILTERS = ["fproblem", "fcause", "foutcome", "fclass", "ftier", "fsrc", "fcode", "fthread", "ferr", "fretry"];
  // The ones living under 「更多筛选」 (problem / cause are the group tabs).
  const MORE = ["foutcome", "fclass", "ftier", "fsrc", "fcode", "fthread", "ferr", "fretry"];
  const PROBLEMS = ["", "error", "upstream", "unfinished", "refused"];
  const CAUSES = {
    error: ["quota", "rate_limit", "request", "upstream", "network", "gateway", "stream"],
    upstream: ["unrescued", "rescued"],
    unfinished: ["client_closed", "reconnect", "truncated", "content_filter", "unconfirmed"],
    refused: ["inflight", "ws", "body", "ws_msg"],
  };
  const OUTCOMES = ["business", "completed", "failed", "cancelled", "incomplete", "unknown", "prewarm", "refused"];
  const CLASSES = ["quota", "rate_limit", "request", "upstream", "network", "stream", "local", "other"];
  const SOURCES = ["upstream", "stream", "network", "local", "client", "upstream,stream,network,local"];
  const TIERS = ["priority", "flex", "standard", "dg"];
  const CHOICES = {foutcome: OUTCOMES, fclass: CLASSES, fsrc: SOURCES, ftier: TIERS};
  // Next steps the UI knows how to perform (backend `next`); others are ignored.
  const NEXT = ["refresh_quota", "account", "cooldown", "attempts", "egress", "accounts", "new_session", "detail"];
  const RECORD_ID = /^[0-9a-f]{16}$/;

  const emptyFilters = () => Object.fromEntries(FILTERS.map(k => [k, ""]));

  // parse(query: URLSearchParams, scopeWindow: {from, to, fixed}) → {f, win, record}.
  // Unknown values fall back to the default; a chart-picked window (rfrom/rto) needs a fixed scope
  // window that contains it; `rid` is accepted as an alias of `record`.
  function parse(q, sw) {
    const f = emptyFilters();
    for (const k of FILTERS) {
      let v = (q.get(k) || "").trim();
      if (k === "ferr" || k === "fretry") v = v === "1" ? "1" : "";
      else if (CHOICES[k] && !CHOICES[k].includes(v)) v = "";
      f[k] = v;
    }
    if (!PROBLEMS.includes(f.fproblem)) f.fproblem = "";
    if (!(CAUSES[f.fproblem] || []).includes(f.fcause)) f.fcause = "";
    let win = null;
    const rf = Number(q.get("rfrom")), rt = Number(q.get("rto"));
    if (sw && sw.fixed && q.has("rfrom") && q.has("rto") && Number.isSafeInteger(rf) && Number.isSafeInteger(rt) &&
      rf >= sw.from && rt <= sw.to && rt >= rf) win = {from: rf, to: rt};
    const rid = (q.get("record") || q.get("rid") || "").trim().toLowerCase();
    return {f, win, record: RECORD_ID.test(rid) ? rid : ""};
  }

  // URL patch for ctx.setParams ("" removes a parameter; `rid` is folded into `record`).
  function toParams(st) {
    const o = {};
    for (const k of FILTERS) o[k] = st.f[k] || "";
    o.rfrom = st.win ? String(st.win.from) : "";
    o.rto = st.win ? String(st.win.to) : "";
    o.record = st.record || "";
    o.rid = "";
    return o;
  }

  const refusedMode = f => f.foutcome === "refused" || f.fproblem === "refused";
  // A session spans whatever time it spans: with a session the list ignores the range, but a
  // chart-picked window still applies.
  const threadAll = st => !!st.f.fthread && !st.win;
  const anyFilter = f => FILTERS.some(k => !!f[k]);
  const moreCount = f => MORE.filter(k => !!f[k]).length;

  // Records list parameters. scope: Shell.scope.params() (from, to, account, model, key, instance,
  // exclude_prewarm); prewarm: the 「含预热」 switch.
  function recordQuery(st, scope, prewarm) {
    const f = st.f, o = f.foutcome, tier = f.ftier;
    const base = Object.assign({}, scope);
    delete base.exclude_prewarm;
    return Object.assign(base, threadAll(st) ? {from: "", to: ""} : {}, st.win || {}, {
      thread: f.fthread,
      problem: f.fproblem,
      cause: f.fproblem ? f.fcause : "",
      outcome: o === "business" ? "" : o,
      exclude_prewarm: o === "business" || (o !== "prewarm" && !prewarm) ? 1 : "",
      errors: f.ferr ? 1 : "",
      source: f.fsrc,
      error: f.fcode,
      failure_class: f.fclass,
      retried: f.fretry ? 1 : "",
      tier: tier === "dg" ? "" : tier,
      downgraded: tier === "dg" ? 1 : "",
    });
  }

  // Group tab counts: the same population minus problem / cause; 本地拒绝 is never an outcome here.
  function countQuery(st, scope, prewarm) {
    const q = recordQuery(st, scope, prewarm);
    q.problem = q.cause = "";
    if (q.outcome === "refused") q.outcome = "";
    return q;
  }

  // Refusal groups follow the range, key and instance only (they happen before routing).
  function refusalQuery(st, scope) {
    return Object.assign({from: scope.from, to: scope.to, key: scope.key || "", instance: scope.instance || "",
      reason: st.f.fproblem === "refused" ? st.f.fcause : ""}, st.win || {});
  }
  const scopedOut = scope => !!(scope.account || scope.model);

  function qs(o) {
    const u = new URLSearchParams();
    for (const k in o) if (o[k] !== "" && o[k] !== null && o[k] !== undefined) u.set(k, o[k]);
    return u.toString();
  }

  // ---------- filter rules ----------
  // pick a problem group: resets its cause and the old 结果 / 失败原因 selects.
  function pickProblem(f, id) {
    const n = Object.assign({}, f, {fproblem: id, fcause: ""});
    if (id) n.foutcome = n.fclass = "";
    return n;
  }
  // set one 「更多筛选」 field with the old coupling rules:
  // - a failure class implies 结果 = 失败 and leaves the problem groups;
  // - another 结果 clears the class; any 结果 leaves the problem groups;
  // - a session leaves 本地拒绝 (refusals carry no session).
  function setFilter(f, k, v) {
    const n = Object.assign({}, f);
    v = typeof v === "string" ? v.trim() : v ? "1" : "";
    n[k] = v;
    if (k === "fclass" && v) {
      n.foutcome = "failed";
      n.fproblem = n.fcause = "";
    }
    if (k === "foutcome") {
      if (v !== "failed") n.fclass = "";
      if (v) n.fproblem = n.fcause = "";
    }
    if (k === "fthread" && v && refusedMode(n)) {
      if (n.fproblem === "refused") n.fproblem = n.fcause = "";
      if (n.foutcome === "refused") n.foutcome = "";
    }
    return n;
  }
  const clearRequestFilters = () => emptyFilters();

  // 16 hex → record id; 8+ characters → session (full id or fragment); anything else is invalid.
  function parseSearch(text) {
    const s = String(text || "").trim();
    if (!s) return {kind: "empty"};
    if (RECORD_ID.test(s.toLowerCase())) return {kind: "record", id: s.toLowerCase()};
    if (s.length >= 8 && s.length <= 140 && /^[\w.:-]+$/.test(s)) return {kind: "thread", value: s};
    return {kind: "invalid"};
  }

  // ---------- rows ----------
  // Codex thread ids are UUIDv7: the head is a timestamp shared by nearby sessions, so the short
  // form is the tail.
  const shortId = s => {
    s = String(s || "");
    return s.length > 12 ? "…" + s.slice(-8) : s;
  };

  function normTier(x) {
    x = String(x || "").trim().toLowerCase();
    if (!x) return null;
    if (x === "fast") return "priority";
    if (x === "auto" || x === "default" || x === "standard") return "standard";
    return x;
  }

  function outcomeOf(r) {
    if (r.outcome) return r.outcome;
    if (r.endpoint === "prewarm") return "prewarm";
    if (r.error === "client_closed" || r.err_source === "client") return "cancelled";
    if (r.error === "observation_incomplete" && r.err_source === "local" && !(r.status >= 400)) return "unknown";
    if (r.error || r.status >= 400) return "failed";
    if (r.incomplete && r.incomplete !== "interrupted") return "incomplete";
    return "completed";
  }

  // Status tone for the dot: failed requests by code (429 amber, other 4xx/5xx red), recovered
  // requests amber, completed green; cancelled, unknown, incomplete and prewarm stay neutral.
  function tone(r, outcome) {
    outcome = outcome || outcomeOf(r);
    if (outcome === "failed") return r.status === 429 ? "warn" : "err";
    if (outcome === "completed") return r.rescued ? "warn" : "ok";
    return "off";
  }

  // Error source with the proxy split out of network.
  const PROXY_ERR = ["proxy_connect", "proxy_timeout"];
  const srcOf = r => (r.err_source === "network" && PROXY_ERR.includes(r.error) ? "proxy" : r.err_source || "");
  // observation_incomplete is only a meaningful label when the outcome really is unknown.
  const labelledError = r => (r.error && (r.error !== "observation_incomplete" || outcomeOf(r) === "unknown") ? r.error : "");

  function incompleteCode(r) {
    const x = r.incomplete;
    return !x ? "" : x === "interrupted" || x === "content_filter" ? x : "incomplete_" + x;
  }

  const isStream = r => r.transport === "sse" || r.transport === "ws";
  // TTFT only exists for streamed requests.
  const ttftOf = r => (isStream(r) && r.ttft_ms !== null && r.ttft_ms !== undefined ? Number(r.ttft_ms) : null);

  // Usable next steps of a row: known keys, in backend order. `detail` / `new_session` are not
  // buttons (detail is the row itself, new_session is explained in the conclusion).
  const nextSteps = r => (r.next || []).filter(k => NEXT.includes(k) && k !== "detail" && k !== "new_session");

  // Where a next step goes: {view, hash} for another page, "detail" / "attempts" inside this one.
  // 刷新额度 opens the account on the Accounts page (its normal refresh button); never a reset card.
  function nextTarget(k, r) {
    const acc = r && r.account ? "acc=" + encodeURIComponent(r.account) : "";
    if (k === "refresh_quota" || k === "account" || k === "cooldown") return {view: "accounts", hash: acc};
    if (k === "accounts") return {view: "accounts", hash: ""};
    if (k === "egress") return {view: "settings", hash: "network"};
    if (k === "attempts") return {inPage: "attempts"};
    return {inPage: "detail"};
  }

  // Conclusion of a request, from backend fields only: {kind, head, cause, counted, upstream,
  // newSession} or null for a plain success. `counted`: true = counts toward the error rate.
  function conclusion(r) {
    if (r.rescued) return {kind: "rescued", counted: false, upstream: true};
    if (r.problem === "error") {
      return {kind: "error", cause: r.cause || "", counted: true, upstream: !!r.upstream_fault,
        newSession: (r.next || []).includes("new_session")};
    }
    if (r.problem === "unfinished") return {kind: "unfinished", cause: r.cause || "", counted: false};
    return null;
  }

  // Shallow equality of two flat API rows (the list keeps the node of an unchanged row).
  function sameRecord(a, b) {
    if (!a || !b) return false;
    const ka = Object.keys(a);
    return ka.length === Object.keys(b).length && ka.every(k => a[k] === b[k]);
  }

  // ---------- attempts ----------
  // Attempt bars: each attempt runs from its t_ms to the next attempt's t_ms; the last one ends
  // at the request's duration. `dropped` markers pass through.
  function attemptBars(attempts, duration) {
    if (!Array.isArray(attempts)) return {rows: [], max: 0};
    const real = attempts.filter(a => !a.dropped);
    const end = Math.max(Number(duration) || 0, ...real.map(a => Number(a.t_ms) || 0));
    const rows = [];
    let j = 0;
    for (const a of attempts) {
      if (a.dropped) {
        rows.push({dropped: a.dropped});
        continue;
      }
      const next = real[j + 1];
      const start = Number(a.t_ms) || 0;
      const stop = next ? Number(next.t_ms) || start : end;
      j++;
      const ok = a.action === "ok";
      const tone = ok ? "ok" : a.status === 429 || /^retry|^cooldown/.test(a.action || "") ? "warn" : "err";
      rows.push({a, start, end: Math.max(start, stop), tone});
    }
    return {rows, max: end};
  }

  // ---------- histogram ----------
  // Summary bucket grain for the request chart: hourly up to 7 days, daily beyond.
  const chartBucket = (from, to) => (to - from <= 7 * DAY + HOUR ? "hour" : "day");

  // Chart series from summary buckets (oldest first). With the query window (`win`, and `tz` in
  // minutes east of UTC as sent to the API) every bucket intersecting it is drawn, empty ones as
  // zero bars, so bars sit at their real time; without it only the gaps between buckets are filled.
  // refused: show local refusals instead of requests.
  function histogram(buckets, bucket, refused, win, tz) {
    const step = bucket === "hour" ? HOUR : DAY;
    const list = (buckets || []).filter(b => Number.isFinite(b.start_ms)).slice().sort((a, b) => a.start_ms - b.start_ms);
    const by = new Map(list.map(b => [b.start_ms, b]));
    const row = (start, b) => ({start, end: (b && b.end_ms) || start + step, count: b ? Number(b.count) || 0 : 0,
      failures: b ? Number(b.failures) || 0 : 0, refused: b ? Number(b.refused) || 0 : 0});
    const rows = [];
    if (win && Number.isFinite(win.from) && Number.isFinite(win.to) && win.to > win.from) {
      const off = (Number(tz) || 0) * 60000;
      const first = Math.floor((win.from + off) / step) * step - off;
      for (let s = first; s <= win.to && rows.length < 2000; s += step) rows.push(row(s, by.get(s)));
      // a bucket the grid did not land on (DST shift) is kept at its own time
      for (const b of list) if (!rows.some(r => r.start === b.start_ms)) rows.push(row(b.start_ms, b));
      rows.sort((a, b) => a.start - b.start);
    } else {
      for (const b of list) {
        const prev = rows[rows.length - 1];
        if (prev) for (let s = prev.end; s < b.start_ms && rows.length < 2000; s += step) rows.push(row(s, null));
        rows.push(row(b.start_ms, b));
      }
    }
    const x = rows.map(r => r.start);
    const series = refused ?
      [{id: "refused", values: rows.map(r => r.refused), color: "warn"}] :
      [{id: "failed", values: rows.map(r => Math.min(r.failures, r.count)), color: "err"},
        {id: "other", values: rows.map(r => Math.max(0, r.count - r.failures)), color: "neutral"}];
    const total = rows.reduce((a, r) => a + (refused ? r.refused : r.count), 0);
    return {rows, x, series, total};
  }

  // A brushed / clicked bucket range → list window, clipped to the scope window.
  function windowOf(rows, i0, i1, sw) {
    const a = rows[Math.min(i0, i1)], b = rows[Math.max(i0, i1)];
    if (!a || !b) return null;
    // bucket ends are exclusive; list windows are inclusive (as the overview drill)
    const from = Math.max(a.start, sw.from), to = Math.min(b.end - 1, sw.to);
    return to > from ? {from, to} : null;
  }

  return {PAGE, FILTERS, MORE, PROBLEMS, CAUSES, OUTCOMES, CLASSES, SOURCES, TIERS, RECORD_ID, emptyFilters, parse, toParams,
    refusedMode, threadAll, anyFilter, moreCount, recordQuery, countQuery, refusalQuery, scopedOut, qs, pickProblem, setFilter,
    clearRequestFilters, parseSearch, shortId, normTier, outcomeOf, tone, srcOf, labelledError, incompleteCode, isStream, ttftOf,
    nextSteps, nextTarget, conclusion, sameRecord, attemptBars, chartBucket, histogram, windowOf};
})();
