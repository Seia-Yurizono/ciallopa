"use strict";
// Requests: the investigation view. Problem groups with counts and causes, 「更多筛选」, search by
// record / session ID, a request histogram of the shared scope, the request table (phone: cards)
// with ID-cursor paging and keyed rows, local refusal groups, and the request detail (side by side
// from 1280px, a drawer below, a full-screen page on phones). Request filters live in the URL under
// their old names (fproblem, fcause, foutcome, fclass, ftier, fsrc, fcode, fthread, ferr, fretry,
// rfrom, rto, record), so Overview drill-downs and old links open the same investigation.
// Logic lives in requests/model.js, words in requests/text.js, the detail panel in requests/detail.js.
Views.requests = (() => {
  const {h, fmt, clear} = Core;
  const t = (k, v) => I18n.t(k, v);
  const M = RequestsModel, X = RequestsText;
  const SPLIT_Q = "(min-width: 1280px)";

  let S = null; // per-mount state; null when unmounted

  // ---------- small helpers ----------
  const isSplit = () => typeof matchMedia === "function" && matchMedia(SPLIT_Q).matches && !UI.isPhone();
  const scopeParams = () => Shell.scope.params();
  const prewarmOn = () => !!Shell.scope.get().prewarm;
  const sameDay = ms => new Date(ms).toDateString() === new Date().toDateString();
  const timeText = ms => (sameDay(ms) ? fmt.hms(ms) : fmt.time(ms, true));

  function typing(e) {
    const el = e.target;
    return !!(el && el.closest && el.closest("input,textarea,select,[contenteditable='true']"));
  }

  // ---------- URL ----------
  function persist() {
    S.ctx.setParams(M.toParams(S.st));
  }

  // ---------- rendering: groups, causes, hint, chips ----------
  function renderGroups() {
    const box = S.el.groups;
    const p = S.st.f.fproblem, pc = S.counts;
    const focused = box.contains(document.activeElement) ? document.activeElement.dataset.problem : null;
    clear(box);
    for (const id of M.PROBLEMS) {
      const n = pc ? (id ? pc[id] : pc.all) : undefined;
      const on = id === p;
      const off = id === "refused" && pc && pc.refused === null && !on;
      const b = h("button", {type: "button", class: "rq-group" + ((id === "error" || id === "upstream") && n ? " hot" : ""),
        "aria-pressed": String(on), "data-problem": id, disabled: off || null,
        title: id === "refused" && pc && pc.refused === null ? t("requests.refusedOff") : null},
      h("span", null, t("requests.p." + (id || "all"))),
      n === null || n === undefined ? null : h("span", "rq-group-n", fmt.n(n)));
      b.addEventListener("click", () => change(M.pickProblem(S.st.f, id)));
      box.appendChild(b);
    }
    if (focused !== null && focused !== undefined) {
      const f = box.querySelector('[data-problem="' + CSS.escape(focused) + '"]');
      if (f && !f.disabled) f.focus({preventScroll: true});
    }
    renderCauses();
    renderHint();
  }

  function renderCauses() {
    const box = S.el.causes;
    const p = S.st.f.fproblem, list = M.CAUSES[p];
    const focused = box.contains(document.activeElement) ? document.activeElement.dataset.cause : null;
    clear(box);
    box.classList.toggle("hide", !list);
    if (!list) return;
    const counts = (S.counts && S.counts.causes && S.counts.causes[p]) || {};
    for (const id of [""].concat(list)) {
      const c = UI.chip({label: id ? X.causeLabel(p, id) : t("requests.c.all"), value: id && counts[id] !== undefined ? fmt.n(counts[id])
        : undefined,
        pressed: id === S.st.f.fcause, onClick: () => change(Object.assign({}, S.st.f, {fcause: id}))});
      c.dataset.cause = id;
      if (id && counts[id] === 0) c.classList.add("zero");
      box.appendChild(c);
    }
    if (focused !== null && focused !== undefined) {
      const f = box.querySelector('[data-cause="' + CSS.escape(focused) + '"]');
      if (f) f.focus({preventScroll: true});
    }
  }

  // One line under the groups: the population rule of the group (never a new rate) and a shortcut.
  function renderHint() {
    const box = S.el.hint;
    clear(box);
    const pc = S.counts;
    if (!pc) {
      box.classList.add("hide");
      return;
    }
    const p = S.st.f.fproblem, c = S.st.f.fcause, C = pc.causes || {}, u = C.upstream || {}, parts = [], acts = [];
    const go = (view, hash) => () => Shell.go(Shell.href(view, null, hash));
    if (!p) {
      if (pc.error) parts.push(t("requests.hint.errors", {n: fmt.exact(pc.error)}));
      if (u.rescued) parts.push(t("requests.hint.rescued", {n: fmt.exact(u.rescued)}));
      if (pc.error) acts.push([t("requests.hint.onlyErrors"), () => change(M.pickProblem(S.st.f, "error"))]);
    } else if (p === "error") {
      parts.push(t("requests.hint.errorCounts"));
      const acc = Shell.scope.get().account;
      if (c === "quota") {
        acts.push([t("requests.next.refresh_quota"), go("accounts", acc ? "acc=" + encodeURIComponent(acc) : "")],
          [t("requests.hint.accountsPage"), go("accounts")]);
      } else if (c === "rate_limit") acts.push([t("requests.next.cooldown"), go("accounts")]);
      else if (c === "network") acts.push([t("requests.next.egress"), go("settings", "network")]);
      else if (c === "gateway") acts.push([t("requests.next.accounts"), go("accounts")]);
    } else if (p === "upstream") {
      parts.push(t("requests.hint.upstream"), t("requests.hint.rescuedN", {n: fmt.exact(u.rescued || 0)}),
        t("requests.hint.unrescuedN", {n: fmt.exact(u.unrescued || 0)}));
      if (c !== "rescued" && u.rescued) acts.push([t("requests.hint.onlyRescued"), () => change(Object.assign({}, S.st.f,
        {fcause: "rescued"}))]);
    } else if (p === "unfinished") {
      parts.push(t("requests.hint.unfinished"));
    } else if (p === "refused") {
      parts.push(t("requests.hint.refused"));
      acts.push([t("requests.hint.limits"), go("settings", "forward")]);
    }
    if (parts.length) box.appendChild(h("span", "rq-hint-text", parts.join(" · ")));
    for (const [label, fn] of acts) box.appendChild(UI.button(label + " ›", {kind: "link", size: "sm", onClick: fn}));
    box.classList.toggle("hide", !box.children.length);
  }

  function filterValueText(k, v) {
    if (k === "foutcome") return X.outcomeLabel(v);
    if (k === "fclass") return t("requests.fc." + v);
    if (k === "fsrc") return v === "upstream,stream,network,local" ? t("requests.src.noClient") : X.sourceLabel(v);
    if (k === "ftier") return t("requests.tier." + v);
    if (k === "fthread") return M.shortId(v);
    return v;
  }
  const FILTER_NAME = {foutcome: "requests.f.outcome", fclass: "requests.f.class", fsrc: "requests.f.source", ftier: "requests.f.tier",
    fcode: "requests.f.code", fthread: "requests.f.thread", ferr: "requests.f.legacy", fretry: "requests.f.retryShort"};

  // Removable chips for 「更多筛选」 conditions and a chart-picked period.
  function renderActive() {
    const box = S.el.active;
    const keep = box.contains(document.activeElement) ? document.activeElement.dataset.k : null;
    clear(box);
    const f = S.st.f;
    for (const k of M.MORE) {
      if (!f[k]) continue;
      const name = t(FILTER_NAME[k]);
      const flag = k === "ferr" || k === "fretry";
      const c = UI.chip({label: flag ? null : name, value: flag ? name : filterValueText(k, f[k]), title: k === "fthread" ? f[k] : null,
        ariaLabel: t("requests.f.remove", {name: name + (flag ? "" : " " + f[k])}),
        onRemove: () => change(Object.assign({}, S.st.f, {[k]: ""}))});
      c.dataset.k = k;
      box.appendChild(c);
    }
    if (S.st.win) {
      const w = S.st.win;
      const c = UI.chip({icon: "clock", label: t("requests.f.window"), value: fmt.time(w.from, true) + " – " + fmt.time(w.to, true),
        ariaLabel: t("requests.f.windowClear"), title: t("requests.f.windowClear"), onRemove: () => setWindow(null)});
      c.dataset.k = "win";
      box.appendChild(c);
    }
    if (box.children.length > 1 || (box.children.length && M.anyFilter(f) && (f.fproblem || f.fcause))) {
      const b = UI.button(t("requests.f.clear"), {kind: "link", size: "sm", onClick: () => {
        S.st.win = null;
        change(M.clearRequestFilters());
      }});
      b.dataset.k = "clear";
      box.appendChild(b);
    }
    box.classList.toggle("hide", !box.children.length);
    const n = M.moreCount(f);
    S.el.moreBadge.textContent = n ? String(n) : "";
    S.el.moreBadge.classList.toggle("hide", !n);
    if (keep) {
      const el = box.querySelector('[data-k="' + keep + '"]') || box.querySelector("button") || S.el.moreBtn;
      el.focus({preventScroll: true});
    }
  }

  // ---------- 更多筛选 ----------
  function openMore() {
    const f = () => S.st.f;
    const sel = (k, values, allKey, label) => {
      const s = h("select", {class: "select", "data-k": k}, h("option", {value: ""}, t(allKey)),
        values.map(v => h("option", {value: v}, label(v))));
      s.addEventListener("change", () => change(M.setFilter(f(), k, s.value)));
      return s;
    };
    const text = (k, ph, max) => {
      const i = h("input", {class: "input", type: "text", "data-k": k, placeholder: ph, maxlength: max || 140, spellcheck: "false",
        autocomplete: "off", autocapitalize: "off", enterkeyhint: "done"});
      const apply = () => {
        if (i.value.trim() !== f()[k]) change(M.setFilter(f(), k, i.value));
      };
      i.addEventListener("keydown", e => {
        if (e.key === "Enter") {
          e.preventDefault();
          apply();
        }
      });
      i.addEventListener("change", apply);
      return i;
    };
    const check = (k, label, tip, onChange) => {
      const c = h("input", {type: "checkbox", class: "switch", "data-k": k});
      c.addEventListener("change", onChange || (() => change(M.setFilter(f(), k, c.checked))));
      return h("label", {class: "check", title: tip || null}, c, h("span", "grow", label));
    };
    const outcome = sel("foutcome", M.OUTCOMES, "requests.o.all", v => X.outcomeLabel(v));
    const cls = sel("fclass", M.CLASSES, "requests.fc.all", v => t("requests.fc." + v));
    const src = sel("fsrc", M.SOURCES, "requests.src.all", v => (v.includes(",") ? t("requests.src.noClient") : X.sourceLabel(v)));
    const tier = sel("ftier", M.TIERS, "requests.tier.all", v => t("requests.tier." + v));
    const code = text("fcode", t("requests.f.codePh"), 80);
    const thread = text("fthread", t("requests.f.threadPh"), 140);
    const retry = check("fretry", t("requests.f.retry"));
    const pw = check("prewarm", t("requests.f.prewarm"), null, e => Shell.scope.set({prewarm: e.target.checked}));
    const legacy = check("ferr", t("requests.f.legacy"));
    const refusedNote = h("p", "rq-more-note", t("requests.f.hiddenInRefused"));
    const fields = {
      outcome: UI.field({label: t("requests.f.outcome"), control: outcome}),
      cls: UI.field({label: t("requests.f.class"), control: cls}),
      src: UI.field({label: t("requests.f.source"), control: src}),
      tier: UI.field({label: t("requests.f.tier"), control: tier}),
      code: UI.field({label: t("requests.f.code"), control: code, hint: t("requests.f.enter")}),
      thread: UI.field({label: t("requests.f.thread"), control: thread, hint: t("requests.f.threadTip")}),
    };
    const pwRow = h("div", "rq-more-row", pw, UI.info(t("requests.f.prewarmTip")));
    const body = h("div", "rq-more",
      h("div", "rq-more-grid", fields.outcome, fields.cls, fields.src, fields.tier, fields.code, fields.thread),
      refusedNote, h("div", "rq-more-checks", retry, pwRow, legacy));
    const clearBtn = UI.button(t("requests.f.clear"), {size: "sm", kind: "ghost", onClick: () => {
      const keep = S.st.f;
      change(Object.assign(M.clearRequestFilters(), {fproblem: keep.fproblem, fcause: keep.fcause}));
    }});
    // Controls mirror the state after every change (one change can reset another field).
    const sync = () => {
      const st = f(), refused = M.refusedMode(st);
      for (const el of body.querySelectorAll("[data-k]")) {
        const k = el.dataset.k;
        if (k === "prewarm") el.checked = prewarmOn();
        else if (el.type === "checkbox") el.checked = !!st[k];
        else if (document.activeElement !== el || el.tagName === "SELECT") el.value = st[k] || "";
      }
      for (const key of ["cls", "src", "tier", "code", "thread"]) fields[key].classList.toggle("hide", refused);
      refusedNote.classList.toggle("hide", !refused);
      legacy.classList.toggle("hide", !st.ferr);
      clearBtn.disabled = !M.moreCount(st);
    };
    S.syncMore = sync;
    sync();
    const done = () => {
      if (S.moreLayer) S.moreLayer.close();
    };
    if (UI.isPhone()) {
      S.moreLayer = UI.sheet({title: t("requests.f.more"), body, foot: [clearBtn, UI.button(t("requests.f.done"), {kind: "primary",
        onClick: done})],
        onClose: () => {
          if (!S) return;
          S.syncMore = null;
          S.moreLayer = null;
          // A phone sheet owns a history entry; URL writes made while it was open landed on that
          // entry, so write the state again once the sheet's entry is gone.
          persist();
        }});
    } else {
      const foot = h("div", "rq-more-foot", clearBtn);
      const p = UI.popover(S.el.moreBtn, h("div", null, body, foot), {placement: "bottom-end", className: "rq-more-pop",
        onClose: () => {
          if (S) S.syncMore = null;
        }});
      if (!p) S.syncMore = null;
    }
  }

  // ---------- search: record id → detail, session id → filter ----------
  function onSearch(e) {
    e.preventDefault();
    const input = S.el.search;
    const v = M.parseSearch(input.value);
    if (v.kind === "empty") return;
    if (v.kind === "invalid") {
      setLookup(t("requests.search.short"), "err");
      return;
    }
    if (v.kind === "record") {
      lookup(v.id, input);
      return;
    }
    input.value = "";
    setLookup("");
    change(M.setFilter(S.st.f, "fthread", v.value));
  }

  function setLookup(text, kind) {
    S.el.lookup.textContent = text || "";
    S.el.lookup.className = "rq-lookup" + (kind ? " " + kind : "");
    S.el.lookup.classList.toggle("hide", !text);
  }

  // Find one record by its full ID (separate from the list filters). A late answer of an older
  // lookup or another login does nothing.
  async function lookup(id, from) {
    const my = ++S.lookupSeq;
    const current = () => S && S.lookupSeq === my;
    setLookup(t("requests.search.finding", {id}));
    try {
      const d = await S.ctx.get("/usage/v1/records/" + id, {current});
      if (!current()) return;
      setLookup("");
      if (S.el.search.value.trim().toLowerCase() === id) S.el.search.value = "";
      openDetail(d.record, {opener: from});
    } catch (e) {
      if (!current() || Api.isQuiet(e)) return;
      setLookup(e.status === 404 ? t("requests.search.notFound", {id}) : t("requests.search.failed", {why: Api.why(e)}), "err");
      if (S.st.record === id && !S.detail.record()) {
        S.st.record = "";
        persist();
      }
    }
  }

  // ---------- state changes ----------
  function change(f) {
    S.st.f = f;
    // 本地拒绝 and a session filter cannot both apply.
    persist();
    renderGroups();
    renderActive();
    if (S.syncMore) S.syncMore();
    reload({reset: true});
  }

  function setWindow(win) {
    S.st.win = win;
    persist();
    renderActive();
    reload({reset: true, skipChart: true});
  }

  // Chart click / brush: list only that period. The scope window is fixed first (silently, at the
  // window the chart was drawn for) so the period stays inside it.
  function pickPeriod(i0, i1) {
    const c = S.chart;
    if (!c || !c.rows) return;
    const sw = c.window;
    const win = M.windowOf(c.rows, i0, i1, sw);
    if (!win) return;
    if (!Shell.scope.window().fixed) Shell.scope.set({from: sw.from, to: sw.to}, {silent: true});
    setWindow(win);
  }

  // 只看这个会话: request filters, the chart period and the shared account / model / key / instance
  // scope are cleared, so a session that switched accounts shows every request.
  function viewThread(id) {
    closeDetail({keepFocus: true});
    S.st.win = null;
    Shell.scope.set({account: "", model: "", key: "", instance: ""}, {silent: true});
    change(M.setFilter(M.clearRequestFilters(), "fthread", id));
    S.el.groups.querySelector('[aria-pressed="true"]').focus({preventScroll: true});
  }

  // ---------- loading ----------
  function reload(o) {
    o = o || {};
    if (o.reset) {
      S.pending = null;
      renderPill();
    }
    const jobs = [loadList(false, o), loadCounts()];
    if (!o.skipChart) jobs.push(loadChart());
    const p = Promise.allSettled(jobs);
    if (!o.auto) S.ctx.busy(p);
    return p;
  }

  function listSignature() {
    return M.refusedMode(S.st.f) ? "refused|" + M.qs(M.refusalQuery(S.st, scopeParams())) :
      M.qs(M.recordQuery(S.st, scopeParams(), prewarmOn()));
  }
  // The signature without the rolling window bounds: a refresh of a rolling range is the same list.
  function stableSignature() {
    const p = new URLSearchParams(listSignature().replace(/^refused\|/, ""));
    if (!Shell.scope.window().fixed && !S.st.win) {
      p.delete("from");
      p.delete("to");
    }
    return (M.refusedMode(S.st.f) ? "refused|" : "") + p.toString();
  }

  // Is the reader at the top of a quiet list? Then fresh rows may slide in; otherwise they wait
  // behind the 「N 条新请求」 pill so the list under the reader does not move.
  function quietTop() {
    if (S.detail.record()) return false;
    const card = S.el.listCard.getBoundingClientRect();
    const bar = document.querySelector("[data-topbar]");
    const top = bar ? bar.getBoundingClientRect().bottom : 0;
    return card.top >= top - 4 && !S.el.listCard.contains(document.activeElement);
  }

  async function loadList(append, o) {
    o = o || {};
    if (M.refusedMode(S.st.f)) return loadRefusals(append, o);
    const L = S.list;
    if (o.auto && (L.rows.length > M.PAGE || L.loading)) {
      S.held = L.rows.length > M.PAGE;
      renderFoot();
      return;
    }
    S.held = false;
    const st = S.st, filters = M.recordQuery(st, scopeParams(), prewarmOn());
    const signature = stableSignature();
    const base = append && L.signature === signature ? L : null;
    if (append && (!base || !base.cursor)) return;
    const my = ++S.listSeq;
    const current = () => S && S.listSeq === my && stableSignature() === signature;
    L.loading = true;
    if (!append) S.el.listCard.classList.toggle("is-loading", !!L.loaded);
    const q = M.qs(Object.assign({}, filters, {limit: M.PAGE, before_id: base ? base.cursor : ""}));
    let d;
    try {
      d = await S.ctx.get("/usage/v1/records?" + q, {current}).catch(e => {
        // A fragment shared by several sessions is a 400: an empty list that says so, not a failure.
        if (e.status === 400 && filters.thread) return {records: [], total: 0, next_before_id: null, ambiguous: true};
        throw e;
      });
    } catch (e) {
      if (current()) {
        L.loading = false;
        S.el.listCard.classList.remove("is-loading");
        listFailed(e, o);
      }
      return;
    }
    if (!current()) return;
    L.loading = false;
    S.el.listCard.classList.remove("is-loading");
    S.listError = null;
    // The API resolves a session fragment to the full ID; keep the full ID in the filter and URL.
    if (d.thread && filters.thread && d.thread !== filters.thread && S.st.f.fthread === filters.thread) {
      S.st.f = Object.assign({}, S.st.f, {fthread: d.thread});
      persist();
      renderActive();
      if (S.syncMore) S.syncMore();
    }
    if (append) {
      L.rows = L.rows.concat(d.records.filter(r => !L.rows.some(x => x.request_id === r.request_id)));
      L.cursor = d.next_before_id;
      L.total = d.total;
      renderList({append: true});
      return;
    }
    const fresh = {rows: d.records, cursor: d.next_before_id, total: d.total, ambiguous: !!d.ambiguous, signature: stableSignature()};
    if (o.auto && L.loaded && L.signature === fresh.signature && L.rows.length && !quietTop()) {
      const have = new Set(L.rows.map(r => r.request_id));
      const newer = fresh.rows.filter(r => !have.has(r.request_id) && r.request_id > L.rows[0].request_id).length;
      if (newer) {
        S.pending = fresh;
        S.pendingCount = newer;
        renderPill();
        // rows already on screen still get their final state (a request that finished, a rescue).
        const byId = new Map(fresh.rows.map(r => [r.request_id, r]));
        L.rows = L.rows.map(r => byId.get(r.request_id) || r);
        renderList({});
        return;
      }
    }
    applyFresh(fresh, {animate: !!o.auto && L.loaded});
  }

  function applyFresh(fresh, o) {
    const L = S.list;
    L.rows = fresh.rows;
    L.cursor = fresh.cursor;
    L.total = fresh.total;
    L.ambiguous = fresh.ambiguous;
    L.signature = fresh.signature;
    L.loaded = true;
    L.mode = "records";
    S.pending = null;
    renderPill();
    renderList({animate: o && o.animate});
  }

  function listFailed(e, o) {
    if (Api.isQuiet(e)) return;
    S.listError = e;
    if (!S.list.loaded) {
      Core.fill(S.el.listBody, UI.errorState({error: e, retry: () => reload({reset: true})}));
      return;
    }
    renderFoot();
    if (!o.auto) S.ctx.fail(e, t("requests.title"));
  }

  async function loadRefusals(append, o) {
    const R = S.list;
    const scope = scopeParams();
    const signature = stableSignature();
    if (M.scopedOut(scope)) {
      Object.assign(R, {rows: [], more: false, total: 0, offset: 0, signature, loaded: true, mode: "refused", scopedOut: true});
      renderList({});
      return;
    }
    if (o.auto && R.rows.length > M.PAGE) return;
    const base = append && R.signature === signature && R.mode === "refused" ? R : null;
    const offset = base ? base.offset : 0;
    const my = ++S.listSeq;
    const current = () => S && S.listSeq === my && stableSignature() === signature;
    R.loading = true;
    try {
      const d = await S.ctx.get("/usage/v1/refusals?" + M.qs(Object.assign(M.refusalQuery(S.st, scope), {limit: M.PAGE, offset})),
        {current});
      if (!current()) return;
      R.loading = false;
      S.listError = null;
      const rows = (base ? base.rows : []).concat(d.rows || []);
      Object.assign(R, {rows, offset: offset + (d.rows || []).length, more: (d.rows || []).length === M.PAGE,
        total: d.totals ? d.totals.count : 0, signature, loaded: true, mode: "refused", scopedOut: false, cursor: null});
      renderList({append: !!base});
    } catch (e) {
      if (!current()) return;
      R.loading = false;
      listFailed(e, o);
    }
  }

  // Group counts use the list's population without problem / cause (refused is null under an
  // account, model or request-only filter: the tab is disabled, not zero).
  async function loadCounts() {
    const my = ++S.countSeq;
    const current = () => S && S.countSeq === my;
    try {
      const d = await S.ctx.get("/usage/v1/records/counts?" + M.qs(M.countQuery(S.st, scopeParams(), prewarmOn())), {current});
      if (!current()) return;
      S.counts = d;
    } catch (e) {
      if (!current()) return;
      S.counts = null;
    }
    renderGroups();
  }

  // Histogram: one summary scan of the shared scope with time buckets. Hidden for a session filter
  // (no time limit) and when the range has too few buckets to compare.
  async function loadChart() {
    const my = ++S.chartSeq;
    const current = () => S && S.chartSeq === my;
    const w = Shell.scope.window();
    if (M.threadAll(S.st)) {
      S.chart = null;
      renderChart();
      return;
    }
    const bucket = M.chartBucket(w.from, w.to);
    const scope = scopeParams();
    const q = M.qs(Object.assign({}, scope, {from: w.from, to: w.to, bucket, tz: -new Date(w.to).getTimezoneOffset()}));
    if (S.chartView) S.chartView.setLoading(true);
    try {
      const d = await S.ctx.get("/usage/v1/summary?" + q, {current});
      if (!current()) return;
      S.chart = {buckets: (d.summary && d.summary.buckets) || [], bucket, window: {from: w.from, to: w.to},
        tz: -new Date(w.to).getTimezoneOffset()};
    } catch (e) {
      if (!current()) return;
      if (!S.chart) S.chart = {failed: true};
    } finally {
      if (current() && S.chartView) S.chartView.setLoading(false);
    }
    renderChart();
  }

  function renderChart() {
    const c = S.chart, card = S.el.chartCard;
    if (!c || c.failed) {
      card.classList.toggle("hide", !c || !S.chartView);
      if (!c) card.classList.add("hide");
      return;
    }
    const refused = M.refusedMode(S.st.f);
    const hist = M.histogram(c.buckets, c.bucket, refused, c.window, c.tz);
    c.rows = hist.rows;
    if (hist.rows.length < 6) {
      card.classList.add("hide");
      return;
    }
    card.classList.remove("hide");
    S.el.chartTitle.textContent = refused ? t("requests.chart.refused") : t("requests.chart.title");
    S.el.chartGrain.textContent = c.bucket === "hour" ? t("requests.chart.hourly") : t("requests.chart.daily");
    S.el.chartTotal.textContent = fmt.exact(hist.total);
    const labels = {failed: t("requests.chart.failed"), other: t("requests.chart.other"), refused: t("requests.chart.refused")};
    const opts = {
      x: hist.x,
      series: hist.series.map(s => Object.assign({}, s, {label: labels[s.id]})),
      kind: "bar",
      height: UI.isPhone() ? 96 : 112,
      hint: t("requests.chart.hint"),
      label: t("requests.chart.label"),
      tipTitle: i => fmt.time(hist.rows[i].start, true) + " – " + fmt.hm(hist.rows[i].end),
      onClick: i => pickPeriod(i, i),
      onBrush: (a, b) => pickPeriod(a, b),
    };
    if (!S.chartView) {
      clear(S.el.chartBox);
      S.chartView = Charts.timeSeries(S.el.chartBox, opts);
    } else S.chartView.update(opts);
  }

  // ---------- list rendering ----------
  function statusCell(r) {
    const outcome = M.outcomeOf(r), tone = M.tone(r, outcome), es = M.srcOf(r), code = M.labelledError(r);
    const box = h("span", "rq-st");
    box.appendChild(UI.dot(tone));
    const tips = [r.status ? t("requests.statusTip", {s: r.status}) : t("requests.noStatus")];
    if (r.upstream_status && r.upstream_status !== r.status && r.error) tips.push(t("requests.upstreamTip", {s: r.upstream_status}));
    if (outcome === "prewarm") {
      box.appendChild(UI.tag(X.outcomeLabel("prewarm"), "outline"));
      tips.push(t("requests.prewarmTip"));
    } else {
      if (r.status) box.appendChild(h("span", "rq-st-code" + codeTone(r, outcome), String(r.status)));
      let why = "";
      if (outcome === "failed") why = (code && X.errLabel(code)) || X.causeLabel(r.problem, r.cause) || r.error || "";
      else if (outcome === "cancelled" || outcome === "unknown") why = X.outcomeLabel(outcome);
      else if (outcome === "incomplete") why = r.incomplete === "content_filter" ? t("requests.contentFilter")
        : X.outcomeLabel("incomplete");
      if (r.rescued) box.appendChild(UI.tag(t("requests.rescuedTag"), "warn"));
      else if (why) box.appendChild(h("span", "rq-st-why" + (outcome === "failed" ? " err" : ""), why));
      else if (r.incomplete) box.appendChild(h("span", "rq-st-why mono", M.incompleteCode(r)));
      if (es) tips.push(X.sourceLabel(es) + (r.error ? " · " + r.error : "") + (X.sourceTip(es) ? " — " + X.sourceTip(es) : ""));
      const tip = code ? X.errTip(code) : "";
      if (tip) tips.push(tip);
      if (r.rescued) tips.push(rescueText(r));
      if (r.incomplete) tips.push(M.incompleteCode(r) + ": " + incompleteTip(r));
    }
    box.title = tips.join("\n");
    return box;
  }

  // The code is coloured only when it is itself an error code of a failed request (a stream that
  // broke after HTTP 200 keeps a neutral 200).
  function codeTone(r, outcome) {
    if (outcome !== "failed" || !(r.status >= 400)) return "";
    return r.status === 429 ? " warn" : " err";
  }

  function rescueText(r) {
    return r.switches && r.first_account ? t("requests.rescueSwitch", {a: r.first_account, b: r.account || ""}) :
      t("requests.rescueRetry", {n: fmt.exact(r.retries || 0)});
  }
  function incompleteTip(r) {
    const x = r.incomplete;
    return x === "interrupted" || x === "content_filter" ? t("requests.incTip." + x) : t("requests.incTip.other", {r: x});
  }

  function modelCell(r) {
    const box = h("span", "rq-model");
    box.appendChild(h("span", "rq-model-name", r.model || fmt.NONE));
    const rq = M.normTier(r.requested_service_tier), sv = M.normTier(r.service_tier);
    const down = rq && rq !== "standard" && rq !== sv;
    if (sv === "priority") box.appendChild(UI.tag("⚡Fast", "accent"));
    else if (sv === "flex") box.appendChild(UI.tag("Flex", "outline"));
    if (down) box.appendChild(h("span", "tag warn rq-dg", X.tierLabel(rq)));
    const effort = r.reasoning_effort || r.response_effort;
    if (effort) box.appendChild(h("span", "rq-effort", effort));
    const rm = r.response_model;
    const tips = [t("requests.modelTip", {q: r.model || fmt.NONE, a: rm || fmt.NONE}) +
      (rm && rm !== r.model ? " (" + (r.model_mismatch ? t("requests.mismatch") : t("requests.snapshot")) + ")" : "")];
    if (rq || sv) tips.push(t("requests.tierTip", {q: X.tierLabel(rq || "standard"), a: X.tierLabel(sv)}) + (down ? " ("
      + t("requests.downgraded") + ")" : ""));
    if (r.reasoning_effort || r.response_effort) {
      tips.push(t("requests.effortTip", {q: r.reasoning_effort || fmt.NONE, a: r.response_effort || fmt.NONE}));
    }
    if (rm && rm !== r.model) box.appendChild(h("span", "rq-rm" + (r.model_mismatch ? " mm" : ""), "↳ " + rm));
    box.title = tips.join("\n");
    return box;
  }

  function durCell(r) {
    const d = Math.max(0, Number(r.duration_ms) || 0), ttft = M.ttftOf(r);
    const bar = h("span", {class: "rq-dbar", "aria-hidden": "true"}, h("i"));
    bar.style.setProperty("--d", String(d));
    if (ttft !== null) {
      const b = h("b");
      b.style.setProperty("--t", String(Math.min(ttft, d)));
      bar.appendChild(b);
    }
    const box = h("span", "rq-dur", h("span", "num", fmt.ms(r.duration_ms)), bar);
    box.title = ttft !== null ? t("requests.durTip", {d: fmt.ms(d), t: fmt.ms(ttft), max: fmt.ms(S.list.maxDur)}) :
      t("requests.durTipNoTtft", {d: fmt.ms(d), max: fmt.ms(S.list.maxDur)});
    return box;
  }

  const tokensText = r => (r.input_tokens == null && r.output_tokens == null ? fmt.NONE : fmt.io(r.input_tokens, r.output_tokens));
  function tokensTip(r) {
    if (r.input_tokens == null && r.output_tokens == null) return t("requests.legend.dash");
    return r.cached_tokens ? t("requests.inTip", {i: fmt.exact(r.input_tokens), c: fmt.exact(r.cached_tokens),
      o: fmt.exact(r.output_tokens)}) :
      t("requests.inTipNoCache", {i: fmt.exact(r.input_tokens), o: fmt.exact(r.output_tokens)});
  }

  function costCell(r) {
    if (r.price_status === "unknown_price") return h("span", {class: "muted", title: t("requests.noPriceTip")}, t("requests.noPrice"));
    if (r.price_status === "ignored") return h("span", {class: "muted", title: t("requests.ignoredTip")}, t("requests.ignored"));
    return h("span", {title: fmt.exactUsd(r.cost_usd) || null}, fmt.usd(r.cost_usd));
  }

  function nextCell(r) {
    const k = M.nextSteps(r)[0];
    if (!k || M.outcomeOf(r) === "prewarm") return "";
    const b = UI.button(X.nextLabel(k) + " ›", {kind: "link", size: "sm", title: X.nextTip(k),
      onClick: e => {
        e.stopPropagation();
        runNext(k, r, b);
      }});
    b.dataset.next = k;
    return b;
  }

  function triesCell(r) {
    const n = (r.retries || 0) + (r.switches || 0);
    return n ? h("span", {class: "rq-tries", title: t("requests.tries", {r: r.retries || 0, s: r.switches || 0}) +
      (r.first_account && r.first_account !== r.account ? " · " + r.first_account + " → " + (r.account || "") : "")}, "↻" + n) : "";
  }

  const COLS = [
    {id: "time", label: "requests.col.time", cell: r => h("span", {class: "num rq-time", title: fmt.iso(r.ts_ms)}, timeText(r.ts_ms))},
    {id: "status", label: "requests.col.status", cell: statusCell},
    {id: "model", label: "requests.col.model", cell: modelCell},
    {id: "dur", label: "requests.col.duration", num: true, cell: durCell},
    {id: "account", label: "requests.col.account", p: 2, cell: r => h("span", {class: "trunc rq-acc", title: r.account || null}, r.account
      || fmt.NONE)},
    {id: "tokens", label: "requests.col.tokens", p: 2, num: true, cell: r => h("span", {title: tokensTip(r)}, tokensText(r))},
    {id: "tries", label: "requests.col.tries", p: 2, num: true, cell: triesCell},
    {id: "key", label: "requests.col.key", p: 3, cell: r => h("span", {class: "trunc rq-key", title: r.key || null}, r.key || fmt.NONE)},
    {id: "ttft", label: "requests.col.ttft", p: 3, num: true, cell: r => fmt.ms(M.ttftOf(r))},
    {id: "cost", label: "requests.col.cost", p: 3, num: true, cell: costCell},
    {id: "next", label: "requests.col.next", p: 3, cell: nextCell},
  ];
  const REF_COLS = [
    {id: "time", label: "requests.col.time", cell: refTime},
    {id: "reason", label: "requests.col.reason", cell: r => h("span", {class: "rq-st", title: X.refusalTip(r.reason)},
      UI.tag(X.refusalLabel(r.reason), "warn"), r.transport === "ws" ? UI.tag("WS", "outline") : null)},
    {id: "key", label: "requests.col.key", cell: r => h("span", {class: "trunc rq-key", title: r.key || null}, r.key || fmt.NONE)},
    {id: "route", label: "requests.col.route", cell: r => h("span", {class: "mono trunc rq-route",
      title: [r.route, r.model, r.account].filter(Boolean).join(" · ")}, r.route || fmt.NONE, r.model ? h("span", "muted", " · " + r.model)
        : null)},
    {id: "ua", label: "requests.col.client", p: 3, cell: r => h("span", {class: "trunc muted rq-ua", title: r.ua || null}, r.ua
      || fmt.NONE)},
    {id: "count", label: "requests.col.times", num: true, cell: r => "×" + fmt.exact(r.count)},
    {id: "limit", label: "requests.col.limit", p: 2, num: true, cell: refLimit},
  ];

  function refTime(r) {
    const span = r.first_ms === r.last_ms ? fmt.iso(r.first_ms) : fmt.iso(r.first_ms) + " – " + fmt.iso(r.last_ms);
    return h("span", {class: "num rq-time", title: t("requests.minuteTip", {span})}, sameDay(r.minute_ms) ? fmt.hm(r.minute_ms)
      : fmt.time(r.minute_ms, true));
  }
  function refLimit(r) {
    const buf = r.reason === "body" || r.reason === "ws_msg";
    const occ = r.reason === "ws" ? "WS " + fmt.exact(r.max_ws) : buf ? fmt.bytes(r.max_buffered_bytes) : t("requests.inflight",
      {n: fmt.exact(r.max_inflight)});
    const lim = r.limit ? (buf ? fmt.bytes(r.limit) : fmt.exact(r.limit)) : t("requests.unlimited");
    const tip = t("requests.limitTip", {lim, inf: fmt.exact(r.max_inflight), ws: fmt.exact(r.max_ws),
      buf: fmt.bytes(r.max_buffered_bytes)}) +
      (r.max_bytes ? t("requests.limitTipBytes", {b: fmt.bytes(r.max_bytes)}) : "");
    return h("span", {title: tip}, lim + " / " + occ);
  }

  const refKey = r => [r.minute_ms, r.reason, r.key, r.instance, r.route, r.model, r.account, r.transport, r.ua].join("|");

  // Rows keep their node while the record is unchanged (a new JSON object with equal fields counts
  // as unchanged); a day change re-renders the time labels.
  function rowItem(r, phone, refused) {
    const key = refused ? refKey(r) : r.request_id;
    const day = new Date().toDateString();
    return {key, r, phone, refused, day};
  }

  function buildRow(it) {
    const r = it.r;
    if (it.phone) return buildCard(it);
    const cols = it.refused ? REF_COLS : COLS;
    const outcome = it.refused ? "" : M.outcomeOf(r);
    const cls = (it.refused ? "" : "clickable") + (outcome === "failed" ? " rq-failed" : "") + (outcome === "prewarm" ? " rq-dim" : "");
    const tr = h("tr", {class: cls});
    if (!it.refused) {
      tr.tabIndex = 0;
      tr.setAttribute("aria-label", t("requests.open", {time: timeText(r.ts_ms), model: r.model || ""}));
    }
    for (const c of cols) {
      const td = h("td", {class: [c.num ? "num" : "", c.p ? "p" + c.p : "", "c-" + c.id].join(" ").trim()});
      Core.add(td, [c.cell(r)]);
      tr.appendChild(td);
    }
    return tr;
  }

  function buildCard(it) {
    const r = it.r;
    if (it.refused) {
      return h("div", "list-item rq-card static",
        h("div", "list-line", UI.tag(X.refusalLabel(r.reason), "warn"), h("span", "grow trunc mono small", r.route || ""),
          h("strong", "num", "×" + fmt.exact(r.count))),
        h("div", "list-line sub", refTime(r), h("span", null, "·"), h("span", "trunc", r.key || fmt.NONE), h("span", "grow"), refLimit(r)));
    }
    const outcome = M.outcomeOf(r), tone = M.tone(r, outcome);
    const why = outcome === "prewarm" ? X.outcomeLabel("prewarm") : r.rescued ? t("requests.rescuedTag") :
      outcome === "failed" ? (M.labelledError(r) && X.errLabel(M.labelledError(r))) || X.causeLabel(r.problem, r.cause) || r.error || "" :
        outcome === "completed" ? "" : X.outcomeLabel(outcome);
    const card = h("div", {class: "list-item rq-card" + (outcome === "failed" ? " rq-failed" : ""), role: "button", tabindex: "0",
      "aria-label": t("requests.open", {time: timeText(r.ts_ms), model: r.model || ""})},
    h("div", "list-line", UI.dot(tone), h("span", "rq-card-model trunc", r.model || fmt.NONE),
      r.status ? h("span", "rq-st-code small" + codeTone(r, outcome), String(r.status)) : null,
      h("span", "grow"), h("span", "num rq-card-dur", fmt.ms(r.duration_ms))),
    h("div", "list-line sub",
      why ? h("span", "rq-card-why" + (outcome === "failed" ? " c-err" : tone === "warn" ? " c-warn" : ""), why) : null,
      h("span", "num", timeText(r.ts_ms)), h("span", "trunc", r.account || fmt.NONE), h("span", "grow"),
      h("span", "num", tokensText(r))));
    return card;
  }

  function wireRow(node, it) {
    if (it.refused) return;
    const open = () => openDetail(node._it.r, {opener: node});
    node.addEventListener("click", e => {
      if (e.target.closest("a,button")) return;
      // Selecting text in a row is not a click.
      if (getSelection && String(getSelection()).length) return;
      open();
    });
    node.addEventListener("keydown", e => {
      if (e.key === "Enter" && e.target === node) {
        e.preventDefault();
        open();
      }
    });
    node.addEventListener("focus", () => {
      S.cursor = node._it.key;
    });
  }

  // Header + body container for the current mode (table on desktop, cards on phones).
  function ensureShell(phone, refused) {
    const mode = (phone ? "phone" : "desk") + (refused ? "-ref" : "");
    if (S.listMode === mode && S.rowsBox && S.rowsBox.isConnected) return;
    S.listMode = mode;
    if (phone) {
      S.rowsBox = h("div", {class: "list rq-list", role: "list"});
      S.table = null;
      Core.fill(S.el.listBody, S.rowsBox);
      return;
    }
    const cols = refused ? REF_COLS : COLS;
    const thead = h("thead", null, h("tr", null, cols.map(c => h("th", {scope: "col",
      class: [c.num ? "num" : "", c.p ? "p" + c.p : "", "c-" + c.id].join(" ").trim()}, t(c.label)))));
    S.rowsBox = h("tbody");
    S.table = h("table", {class: "table rq-table" + (refused ? " rq-ref" : ""), "aria-label": t("requests.title")}, thead, S.rowsBox);
    Core.fill(S.el.listBody, h("div", "table-wrap", S.table));
  }

  function emptyState() {
    const L = S.list, f = S.st.f;
    if (L.mode === "refused") {
      if (L.scopedOut) {
        return UI.empty({icon: "filter", title: t("requests.empty.scopedOut"), body: t("requests.empty.scopedOutBody"),
          actions: [UI.button(t("requests.clearScope"), {onClick: () => Shell.scope.set({account: "", model: ""})})]});
      }
      return UI.empty({title: t("requests.empty.refused"), body: t("requests.empty.range")});
    }
    if (L.ambiguous) return UI.empty({icon: "search", title: t("requests.empty.ambiguous"), body: t("requests.empty.ambiguousBody")});
    if (f.fthread) return UI.empty({icon: "search", title: t("requests.empty.thread"), body: t("requests.empty.threadBody")});
    const filtered = M.anyFilter(f) || !!S.st.win;
    return UI.empty({title: t("requests.empty.title"), body: filtered ? t("requests.empty.filtered") : t("requests.empty.range"),
      actions: filtered ? [UI.button(t("requests.f.clear"), {onClick: () => {
        S.st.win = null;
        change(M.clearRequestFilters());
      }})] : null});
  }

  function renderList(o) {
    o = o || {};
    const L = S.list, phone = UI.isPhone(), refused = L.mode === "refused";
    renderHead();
    renderFoot();
    if (!L.rows.length) {
      S.listMode = null;
      S.rowsBox = null;
      Core.fill(S.el.listBody, emptyState());
      return;
    }
    ensureShell(phone, refused);
    if (!refused) {
      L.maxDur = Math.max(1, ...L.rows.map(r => Number(r.duration_ms) || 0));
      if (S.table) S.table.style.setProperty("--dmax", String(L.maxDur));
      else S.rowsBox.style.setProperty("--dmax", String(L.maxDur));
    }
    const items = L.rows.map(r => rowItem(r, phone, refused));
    const box = S.rowsBox;
    const created = [];
    const doRender = () => {
      // A changed record (or a new day) gets a new node in place of the old one.
      for (const n of Array.from(box.children)) {
        const it = items.find(x => x.key === n.dataset.key);
        if (it && n._it && (n._it.day !== it.day || n._it.phone !== it.phone || (!M.sameRecord(n._it.r, it.r) && n._it.r !== it.r))) {
          const fresh = buildRow(it);
          fresh.dataset.key = it.key;
          fresh._it = it;
          wireRow(fresh, it);
          const hadFocus = n.contains(document.activeElement);
          n.replaceWith(fresh);
          if (hadFocus) fresh.focus({preventScroll: true});
        } else if (it && n._it) n._it = Object.assign({}, n._it, {r: it.r});
      }
      const res = Core.keyed(box, items, it => it.key, it => {
        const n = buildRow(it);
        n._it = it;
        wireRow(n, it);
        return n;
      });
      created.push(...res.created);
    };
    // Keep the reading position when rows arrive above; a focused row keeps its node and focus.
    if (o.append || box.contains(document.activeElement)) doRender();
    else Core.steady([box], doRender);
    if (o.animate) for (const n of created) n.classList.add("is-new");
    markSelected();
  }

  function renderHead() {
    const L = S.list;
    let text = "";
    if (L.mode === "refused") text = L.scopedOut ? "" : t("requests.countRefused", {n: fmt.exact(L.total), g: fmt.exact(L.rows.length)});
    else if (L.loaded) text = t("requests.count", {n: fmt.exact(L.total)}) + (M.threadAll(S.st) ? " · " + t("requests.allTime") : "");
    S.el.count.textContent = text;
    S.el.count.title = M.threadAll(S.st) ? t("requests.allTimeTip") : "";
  }

  function renderFoot() {
    const L = S.list, foot = S.el.foot;
    clear(foot);
    const more = L.mode === "refused" ? L.more : !!L.cursor;
    if (more && L.rows.length) {
      const b = UI.button(t("requests.more"), {onClick: () => Api.busyRun(b, t("requests.more"), () => loadList(true, {}))});
      b.dataset.fk = "rq-more";
      foot.appendChild(b);
    }
    if (L.mode !== "refused" && L.cursor && L.rows.length) foot.appendChild(h("span", "muted small", t("requests.shown",
      {n: fmt.exact(L.rows.length)})));
    if (L.rows.length > M.PAGE) {
      foot.appendChild(h("span", "muted small", t("requests.paused")));
      foot.appendChild(UI.button(t("requests.latest"), {kind: "link", size: "sm", onClick: () => backToLatest()}));
    }
    if (S.listError && L.loaded) {
      foot.appendChild(h("span", "c-err small", Api.why(S.listError)));
      foot.appendChild(UI.button(t("ui.retry"), {kind: "link", size: "sm", onClick: () => reload({})}));
    }
    foot.classList.toggle("hide", !foot.children.length);
  }

  function renderPill() {
    const pill = S.el.pill;
    const n = S.pending ? S.pendingCount : 0;
    pill.classList.toggle("hide", !n);
    if (n) pill.querySelector("span").textContent = t("requests.newRows", {n: fmt.exact(n)});
  }

  function backToLatest() {
    if (S.pending) {
      applyFresh(S.pending, {animate: true});
    } else {
      S.list.rows = S.list.rows.slice(0, M.PAGE);
      reload({reset: true});
    }
    S.el.listCard.scrollIntoView({block: "start"});
    scrollBy(0, -((document.querySelector("[data-topbar]") || {offsetHeight: 0}).offsetHeight + 12));
  }

  function markSelected() {
    if (S.detail.record()) S.detail.updateNav();
    if (!S.rowsBox) return;
    const id = S.detail.record() ? S.detail.record().request_id : null;
    for (const n of S.rowsBox.children) n.classList.toggle("is-selected", !!id && n.dataset.key === id);
  }

  // ---------- detail hosting ----------
  function navInfo() {
    const rows = S.list.mode === "refused" ? [] : S.list.rows;
    const r = S.detail.record();
    const i = r ? rows.findIndex(x => x.request_id === r.request_id) : -1;
    return {index: i, total: rows.length, prev: i > 0, next: i >= 0 && i < rows.length - 1};
  }

  function rowNode(id) {
    return S.rowsBox ? S.rowsBox.querySelector('[data-key="' + CSS.escape(id) + '"]') : null;
  }

  function openDetail(r, o) {
    o = o || {};
    if (!r || !M.RECORD_ID.test(r.request_id || "")) return;
    S.st.record = r.request_id;
    S.cursor = r.request_id;
    persist();
    host();
    S.detail.show(r, {attempts: !!o.attempts});
    markSelected();
    const n = rowNode(r.request_id);
    if (n && o.reveal) n.scrollIntoView({block: "nearest"});
  }

  // Put the panel where it belongs for the current width: side pane (≥1280px) or drawer / page.
  function host() {
    const split = isSplit();
    if (split) {
      if (S.layer) {
        S.rehosting = true;
        S.layer.close();
        S.rehosting = false;
        S.layer = null;
      }
      if (!S.el.aside.contains(S.detail.body)) {
        S.el.asideTools.appendChild(S.detail.toolbar);
        S.el.asideBody.appendChild(S.detail.body);
      }
      S.el.grid.classList.add("has-detail");
      S.el.aside.classList.remove("hide");
      return;
    }
    S.el.grid.classList.remove("has-detail");
    S.el.aside.classList.add("hide");
    if (S.layer) return;
    S.layer = UI.drawer({title: S.detail.title() || t("requests.d.title"), head: S.detail.toolbar, body: S.detail.body,
      className: "rq-drawer", onClose: () => {
        if (S && !S.rehosting) {
          S.layer = null;
          closeDetail({fromLayer: true});
        }
      }});
  }

  function closeDetail(o) {
    o = o || {};
    const r = S.detail.record();
    const hadFocus = S.el.aside.contains(document.activeElement);
    S.detail.reset();
    S.st.record = "";
    persist();
    S.el.grid.classList.remove("has-detail");
    S.el.aside.classList.add("hide");
    if (S.layer && !o.fromLayer) {
      const l = S.layer;
      S.layer = null;
      S.rehosting = true;
      l.close();
      S.rehosting = false;
    }
    markSelected();
    if (r && (hadFocus || o.fromLayer) && !o.keepFocus) {
      const n = rowNode(r.request_id);
      if (n && (hadFocus || document.activeElement === document.body)) n.focus({preventScroll: true});
    }
  }

  function navDetail(step) {
    const n = navInfo();
    const rows = S.list.rows;
    const r = n.index < 0 ? null : rows[n.index + step];
    if (r) openDetail(r, {reveal: true});
  }

  function runNext(k, r, from) {
    const target = M.nextTarget(k, r);
    if (target.view) {
      Shell.go(Shell.href(target.view, null, target.hash));
      return;
    }
    openDetail(r, {opener: from, attempts: target.inPage === "attempts"});
  }

  // ---------- keyboard: J / K move, Enter opens, Esc closes the side pane, Alt+↑/↓ in the detail ----------
  function onKey(e) {
    if (e.defaultPrevented || e.ctrlKey || e.metaKey || typing(e)) return;
    const detailOpen = !!S.detail.record();
    if (e.altKey) {
      if (!detailOpen || e.shiftKey) return;
      const step = e.key === "ArrowUp" ? -1 : e.key === "ArrowDown" ? 1 : 0;
      if (step) {
        e.preventDefault();
        navDetail(step);
      }
      return;
    }
    if (e.shiftKey) return;
    // Another layer (sheet, modal, popover) owns the keyboard; the desktop drawer does not.
    if (UI.blocking() || UI.layerCount() > (S.layer ? 1 : 0)) return;
    const k = e.key.toLowerCase();
    if (k === "escape" && detailOpen && !S.layer) {
      e.preventDefault();
      closeDetail({});
      return;
    }
    if (k !== "j" && k !== "k") return;
    if (!S.rowsBox || S.list.mode === "refused" || !S.list.rows.length) return;
    e.preventDefault();
    const rows = S.list.rows;
    let i = rows.findIndex(r => r.request_id === (detailOpen ? S.detail.record().request_id : S.cursor));
    i = i < 0 ? 0 : Core.clamp(i + (k === "j" ? 1 : -1), 0, rows.length - 1);
    const r = rows[i];
    S.cursor = r.request_id;
    const n = rowNode(r.request_id);
    if (detailOpen) openDetail(r, {reveal: true});
    else if (n) {
      n.focus({preventScroll: true});
      n.scrollIntoView({block: "nearest"});
    }
  }

  // ---------- layout ----------
  function build(root) {
    const el = {};
    el.groups = h("div", {class: "rq-groups", role: "group", "aria-label": t("requests.p.label")});
    el.search = h("input", {class: "input rq-search-input", type: "search", placeholder: t("requests.search.ph"),
      "aria-label": t("requests.search.label"),
      title: t("requests.search.tip"), maxlength: "140", spellcheck: "false", autocomplete: "off", autocapitalize: "off", "data-search": "",
      enterkeyhint: "search"});
    const searchForm = h("form", {class: "rq-search", role: "search"}, UI.icon("search"), el.search);
    searchForm.addEventListener("submit", onSearch);
    el.moreBadge = UI.badge("", "accent");
    el.moreBtn = h("button", {type: "button", class: "btn rq-more-btn", "aria-haspopup": "dialog", "aria-expanded": "false"},
      UI.icon("filter"), h("span", "rq-more-label", t("requests.f.more")), el.moreBadge);
    el.moreBtn.addEventListener("click", openMore);
    el.causes = h("div", {class: "rq-causes hide", role: "group", "aria-label": t("requests.c.label")});
    el.hint = h("div", {class: "rq-hint hide", role: "status"});
    el.active = h("div", {class: "rq-active hide", role: "group", "aria-label": t("requests.f.more")});
    el.lookup = h("div", {class: "rq-lookup hide", role: "status"});
    // chart
    el.chartTitle = h("span", "rq-card-title", t("requests.chart.title"));
    el.chartGrain = h("span", "muted small");
    el.chartTotal = h("span", "rq-chart-total num");
    el.chartBox = h("div", "rq-chart", UI.skeleton.block(96));
    el.chartCard = h("section", "card rq-chart-card",
      h("div", "rq-card-head", el.chartTitle, el.chartGrain, h("span", "grow"), el.chartTotal, UI.info(t("requests.chart.tip"))),
      el.chartBox);
    // list
    el.count = h("span", "rq-count");
    el.pill = h("button", {type: "button", class: "rq-pill hide"}, UI.icon("arrow-up"), h("span"));
    el.pill.addEventListener("click", () => backToLatest());
    const legend = () => h("div", "rq-legend",
      h("div", null, UI.tag("⚡Fast", "accent"), " ", t("requests.legend.fast")),
      h("div", null, h("span", "tag warn", "Fast"), " ", t("requests.legend.dg")),
      h("div", null, h("span", "rq-rm", "↳ model"), " ", t("requests.legend.rm")),
      h("div", null, UI.tag(t("requests.o.prewarm"), "outline"), " ", t("requests.legend.prewarm")),
      h("div", null, t("requests.legend.dash")),
      h("div", "desk-only", t("requests.legend.keys")));
    el.listBody = h("div", "rq-list-body", UI.skeleton.rows(10, 6));
    el.foot = h("div", "rq-foot hide");
    el.listCard = h("section", "card card-flush rq-list-card",
      h("div", "rq-card-head", el.count, el.pill, h("span", "grow"), UI.info(legend, t("requests.legend"))), el.listBody, el.foot);
    el.main = h("div", "rq-main", el.chartCard, el.listCard);
    // side-by-side detail (≥1280px)
    el.asideTitle = h("h2", "rq-aside-title", t("requests.d.title"));
    el.asideTools = h("div", "rq-aside-tools");
    el.asideBody = h("div", "rq-aside-body");
    const closeBtn = UI.button("", {kind: "ghost", icon: "x", title: t("ui.close") + " (Esc)", onClick: () => closeDetail({})});
    el.aside = h("aside", {class: "rq-aside card hide", "aria-labelledby": "rq-aside-title"},
      h("div", "rq-aside-head", el.asideTitle, closeBtn), el.asideTools, el.asideBody);
    el.asideTitle.id = "rq-aside-title";
    el.grid = h("div", "rq-grid", el.main, el.aside);
    root.appendChild(h("div", "rq",
      h("div", "rq-bar", el.groups, h("div", "rq-tools", searchForm, el.moreBtn)),
      el.causes, el.hint, el.active, el.lookup, el.grid));
    return el;
  }

  // ---------- view contract ----------
  function mount(root, ctx) {
    S = {ctx, st: M.parse(ctx.route.query, Shell.scope.window()), list: {rows: [], loaded: false, mode: "records"}, counts: null,
      chart: null, chartView: null, pending: null, pendingCount: 0, listSeq: 0, countSeq: 0, chartSeq: 0, lookupSeq: 0,
      layer: null, cursor: null, syncMore: null, held: false};
    S.el = build(root);
    S.detail = makeDetail();
    // The old URL could carry `rid`; normalise to `record` without a history entry.
    persist();
    renderGroups();
    renderActive();
    reload({});
    if (S.st.record) lookup(S.st.record, null);

    ctx.onRefresh(o => reload({auto: !!(o && o.auto)}));
    ctx.onScope(() => {
      const w = Shell.scope.window();
      if (S.st.win && !(w.fixed && S.st.win.from >= w.from && S.st.win.to <= w.to)) S.st.win = null;
      persist();
      renderActive();
      if (S.syncMore) S.syncMore();
      reload({reset: true});
    });
    ctx.onLang(() => onLang());
    ctx.listen(document, "keydown", onKey);
    const mqPhone = matchMedia(UI.PHONE_Q), mqSplit = matchMedia(SPLIT_Q);
    const onMedia = () => {
      S.listMode = null;
      if (S.list.loaded) renderList({});
      if (S.detail.record()) host();
      if (S.chart) renderChart();
    };
    ctx.listen(mqPhone, "change", onMedia);
    ctx.listen(mqSplit, "change", onMedia);
    ctx.onCleanup(() => {
      S.detail.reset();
      if (S.layer) {
        S.rehosting = true;
        S.layer.close();
      }
      if (S.chartView) S.chartView.destroy();
    });
  }

  function makeDetail() {
    return RequestsDetail.create({
      ctx: S.ctx,
      onNav: navDetail,
      canNav: navInfo,
      onNext: (k, r) => runNext(k, r, null),
      onThread: viewThread,
      threadFilter: () => S.st.f.fthread,
      linkFor: id => {
        const u = new URL(location.href);
        const w = Shell.scope.window();
        u.searchParams.set("record", id);
        u.searchParams.delete("rid");
        u.searchParams.set("from", String(w.from));
        u.searchParams.set("to", String(w.to));
        u.searchParams.delete("range");
        return u.href;
      },
      onShow: (r, title) => {
        S.el.asideTitle.textContent = title;
        if (S.layer) S.layer.setTitle(title);
      },
    });
  }

  function onLang() {
    if (!S) return;
    const root = S.ctx.root;
    const keepDetail = S.detail.record();
    const scrollY = window.scrollY;
    // Rebuild the chrome in the new language; data and state stay.
    if (S.chartView) S.chartView.destroy();
    S.chartView = null;
    if (S.layer) {
      S.rehosting = true;
      S.layer.close();
      S.rehosting = false;
      S.layer = null;
    }
    S.detail.reset();
    clear(root);
    S.el = build(root);
    S.listMode = null;
    S.detail = makeDetail();
    renderGroups();
    renderActive();
    if (S.list.loaded) renderList({});
    renderChart();
    if (keepDetail) openDetail(keepDetail, {});
    window.scrollTo(0, scrollY);
  }

  function onRoute(route) {
    if (!S) return;
    const next = M.parse(route.query, Shell.scope.window());
    const prevRecord = S.st.record;
    S.st = next;
    renderGroups();
    renderActive();
    reload({reset: true});
    if (next.record && next.record !== prevRecord) lookup(next.record, null);
    else if (!next.record && S.detail.record()) closeDetail({keepFocus: true});
  }

  function unmount() {
    S = null;
  }

  return {
    title: () => t("requests.title"),
    scope: ["time", "account", "model", "key", "instance"],
    mount,
    unmount,
    onRoute,
    // for scripts/requests-check.cjs
    _state: () => S,
  };
})();
