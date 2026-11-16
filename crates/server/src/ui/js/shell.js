"use strict";
// Shell: routing, navigation chrome, shared scope, refresh scheduling, theme / language, login.
// Views are mounted into #view with a ctx (see ui/README.md "View contract").
const Shell = (() => {
  const {h, fmt} = Core;
  const t = (k, v) => I18n.t(k, v);

  // ---------- routes ----------
  const ROUTES = [
    {view: "overview", path: "/ui", key: "o"},
    {view: "requests", path: "/ui/requests", key: "r"},
    {view: "accounts", path: "/ui/accounts", key: "a"},
    {view: "settings", path: "/ui/settings", key: "s"},
    {view: "system", path: "/ui/system", key: "y"},
  ];
  // Old /ui?… request-list links: these parameters mean "open the request list".
  const LEGACY_REQUEST_PARAMS = ["fproblem", "fcause", "foutcome", "fclass", "ftier", "fsrc", "fcode", "fthread", "ferr", "fretry",
    "rfrom", "rto", "record", "rid"];

  const pathOf = view => (ROUTES.find(r => r.view === view) || ROUTES[0]).path;

  // parseRoute(pathname, search, hash) → {view, path, query, hash, redirect?}. `redirect` is the
  // canonical URL when the address is a legacy form (/ui#system, /ui?fproblem=…).
  function parseRoute(pathname, search, hash) {
    const p = pathname.replace(/\/+$/, "") || "/ui";
    const query = new URLSearchParams(search || "");
    hash = hash || "";
    if (p === "/ui" && hash === "#system") {
      return Object.assign(parseRoute("/ui/system", search, ""), {redirect: "/ui/system" + qsOf(query)});
    }
    if (p === "/ui" && LEGACY_REQUEST_PARAMS.some(k => query.has(k))) {
      return Object.assign(parseRoute("/ui/requests", search, hash), {redirect: "/ui/requests" + qsOf(query) + hash});
    }
    const r = ROUTES.find(x => x.path === p) || ROUTES[0];
    return {view: r.view, path: r.path, query, hash};
  }

  const qsOf = q => {
    const s = q.toString();
    return s ? "?" + s : "";
  };

  // ---------- shared scope ----------
  // Time window, account, model, key, instance: shared by the views that declare `scope` fields.
  // URL parameters keep the old names, so old links still open the same investigation.
  const RANGES = {"1h": 36e5, "24h": 864e5, "7d": 7 * 864e5, "30d": 30 * 864e5};
  const DEFAULT_RANGE = "24h";
  const SCOPE_PARAMS = {account: "facct", model: "fmodel", key: "fkey", instance: "finstance"};
  const SCOPE_FIELDS = ["account", "model", "key", "instance"];
  const RANGE_STORE = "ciallopa_f_range";
  const PREWARM_STORE = "ciallopa_prewarm";
  const MAX_FIXED_SPAN = 31 * 864e5;

  // scopeFromQuery(query, storedRange) → {range, from, to, account, model, key, instance}.
  // from/to are null unless the URL fixes a valid window (≤ 31 days). Without any query parameter
  // the last chosen range (stored in this browser) applies.
  function scopeFromQuery(q, storedRange) {
    let range = q.get("range");
    if (!RANGES[range]) range = !Array.from(q.keys()).length && RANGES[storedRange] ? storedRange : DEFAULT_RANGE;
    const s = {range, from: null, to: null};
    const from = Number(q.get("from")), to = Number(q.get("to"));
    if (q.has("from") && q.has("to") && Number.isSafeInteger(from) && Number.isSafeInteger(to) && from >= 0 && to > from &&
      to - from <= MAX_FIXED_SPAN) {
      s.from = from;
      s.to = to;
    }
    for (const f of SCOPE_FIELDS) s[f] = (q.get(SCOPE_PARAMS[f]) || "").trim();
    return s;
  }

  // scopeToQuery(scope) → {param: value} with defaults left out.
  function scopeToQuery(s) {
    const o = {};
    if (s.range && s.range !== DEFAULT_RANGE) o.range = s.range;
    if (s.from !== null && s.to !== null && s.from !== undefined && s.to !== undefined) {
      o.from = String(s.from);
      o.to = String(s.to);
    }
    for (const f of SCOPE_FIELDS) if (s[f]) o[SCOPE_PARAMS[f]] = s[f];
    return o;
  }

  // Effective window: fixed bounds, or the rolling range ending now.
  function scopeWindow(s, now) {
    if (s.from !== null && s.to !== null && s.from !== undefined) return {from: s.from, to: s.to, fixed: true};
    const to = now === undefined ? Date.now() : now;
    return {from: to - RANGES[s.range || DEFAULT_RANGE], to, fixed: false};
  }

  const scopeKeys = () => SCOPE_FIELDS.concat(["range", "from", "to"]);

  // Accounts needing attention (badge on the accounts nav item): configured accounts whose
  // login the server reports as invalid.
  function attentionCount(quota) {
    if (!quota || !Array.isArray(quota.accounts)) return null;
    return quota.accounts.filter(a => a.configured !== false && a.needs_relogin).length;
  }

  // Accounts that can still forward requests (not disabled, login valid).
  function usableCount(quota) {
    if (!quota || !Array.isArray(quota.accounts)) return null;
    return quota.accounts.filter(a => a.configured !== false && !a.disabled && !a.needs_relogin).length;
  }

  const scopeState = {range: DEFAULT_RANGE, from: null, to: null, account: "", model: "", key: "", instance: ""};
  const scopeListeners = new Set();
  let prewarm = false;

  const scope = {
    // Current scope (a copy).
    get: () => Object.assign({prewarm}, scopeState),
    window: now => scopeWindow(scopeState, now),
    // API parameters for /usage/v1/* calls of the current scope (from, to, account, model, key,
    // instance; empty values left out; exclude_prewarm=1 unless prewarm is on).
    params(now) {
      const w = scopeWindow(scopeState, now);
      const o = {from: w.from, to: w.to};
      for (const f of SCOPE_FIELDS) if (scopeState[f]) o[f] = scopeState[f];
      if (!prewarm) o.exclude_prewarm = 1;
      return o;
    },
    // set(patch, {silent}) — patch any of range | from | to | account | model | key | instance |
    // prewarm. Setting a range clears fixed bounds unless from/to come in the same patch. The URL
    // is updated in place (replaceState) and listeners run once.
    set(patch, o) {
      let changed = false;
      if (patch.range && RANGES[patch.range] && !("from" in patch)) {
        if (scopeState.from !== null) changed = true;
        scopeState.from = scopeState.to = null;
      }
      for (const k of scopeKeys()) {
        if (!(k in patch)) continue;
        let v = patch[k];
        if (k === "range" && !RANGES[v]) continue;
        if (SCOPE_FIELDS.includes(k)) v = (v || "").trim();
        if ((k === "from" || k === "to") && (v === undefined || v === "")) v = null;
        if (scopeState[k] !== v) {
          scopeState[k] = v;
          changed = true;
        }
      }
      if (patch.range && RANGES[patch.range]) Core.local.set(RANGE_STORE, patch.range);
      if ("prewarm" in patch && !!patch.prewarm !== prewarm) {
        prewarm = !!patch.prewarm;
        Core.local.set(PREWARM_STORE, prewarm ? "1" : "0");
        changed = true;
      }
      if (!changed) return;
      if (current && current.def.scope && current.def.scope.length) writeScopeToUrl();
      renderScope();
      updateNavHrefs();
      if (!o || !o.silent) for (const fn of Array.from(scopeListeners)) fn(scope.get());
    },
    // Fix the current rolling window (for comparing / sharing), or release it.
    fix() {
      const w = scopeWindow(scopeState);
      scope.set({from: w.from, to: w.to});
    },
    unfix() {
      scope.set({from: null, to: null});
    },
    clear(fields) {
      const patch = {};
      for (const f of fields || SCOPE_FIELDS) patch[f] = "";
      scope.set(patch);
    },
    // Query parameters of the scope (for links that carry the investigation along).
    query: () => scopeToQuery(scopeState),
    onChange(fn) {
      scopeListeners.add(fn);
      return () => scopeListeners.delete(fn);
    },
    RANGES,
    FIELDS: SCOPE_FIELDS,
  };

  function writeScopeToUrl() {
    UI.afterHistory(writeScopeNow);
  }

  function writeScopeNow() {
    const q = new URLSearchParams(location.search);
    for (const k of ["range", "from", "to"].concat(Object.values(SCOPE_PARAMS))) q.delete(k);
    const add = scopeToQuery(scopeState);
    for (const k in add) q.set(k, add[k]);
    history.replaceState(history.state, "", location.pathname + qsOf(q) + location.hash);
  }

  // href(view, params?, hash?) — URL of a view carrying the current scope (when the view uses it).
  function href(view, params, hash) {
    const def = Views[view];
    const q = new URLSearchParams();
    const uses = def && def.scope ? def.scope : [];
    const sq = scopeToQuery(scopeState);
    if (uses.includes("time")) for (const k of ["range", "from", "to"]) if (sq[k]) q.set(k, sq[k]);
    for (const f of SCOPE_FIELDS) if (uses.includes(f) && sq[SCOPE_PARAMS[f]]) q.set(SCOPE_PARAMS[f], sq[SCOPE_PARAMS[f]]);
    for (const k in params || {}) if (params[k] !== "" && params[k] !== null && params[k] !== undefined) q.set(k, params[k]);
    return pathOf(view) + qsOf(q) + (hash ? (hash[0] === "#" ? hash : "#" + hash) : "");
  }

  // ---------- state ----------
  let current = null; // {name, def, ctx, root}
  let routeNow = null;
  let started = false;
  const status = {quota: null, runtime: null, at: 0};
  const statusListeners = new Set();
  const banners = new Map();
  const badges = {};
  let statusSeq = 0;

  // ---------- view lifecycle ----------
  function makeCtx(name, root, route) {
    const ac = new AbortController();
    const cleanups = [];
    const at = Api.epoch();
    let alive = true;
    const refreshFns = [];
    const ctx = {
      name,
      root,
      route,
      signal: ac.signal,
      alive: () => alive && Api.epoch() === at,
      onCleanup: fn => cleanups.push(fn),
      // fn({auto}) on manual / automatic refresh; a returned promise drives the progress line.
      onRefresh: fn => refreshFns.push(fn),
      onScope: fn => cleanups.push(scope.onChange(fn)),
      onLang: fn => cleanups.push(I18n.onChange(fn)),
      onStatus: fn => {
        statusListeners.add(fn);
        cleanups.push(() => statusListeners.delete(fn));
      },
      listen(target, type, fn, opts) {
        target.addEventListener(type, fn, opts);
        cleanups.push(() => target.removeEventListener(type, fn, opts));
      },
      interval(fn, ms) {
        const id = setInterval(fn, ms);
        cleanups.push(() => clearInterval(id));
        return id;
      },
      timeout(fn, ms) {
        const id = setTimeout(fn, ms);
        cleanups.push(() => clearTimeout(id));
        return id;
      },
      // Api calls bound to this mount: aborted on unmount; a late success after unmount or an
      // identity change rejects with kind "stale" (silent in busyRun / ctx.fail).
      req: (method, path, body, headers, opts) => Api.req(method, path, body, headers, bind(opts)),
      get: (path, opts) => Api.get(path, bind(opts)),
      post: (path, body, opts) => Api.post(path, body, bind(opts)),
      put: (path, body, opts) => Api.put(path, body, bind(opts)),
      del: (path, opts) => Api.del(path, bind(opts)),
      params: () => new URLSearchParams(location.search),
      // setParams({k: v}, {push}) — merge view-owned query parameters ("" / null removes one).
      setParams: (patch, o) => setParams(patch, o),
      setHash: (hash, o) => UI.afterHistory(() => {
        const u = location.pathname + location.search + (hash ? "#" + String(hash).replace(/^#/, "") : "");
        if (o && o.push) history.pushState({}, "", u);
        else history.replaceState(history.state, "", u);
      }),
      busy: p => busy(p),
      // Default error handling for a failed load: silent for stale / aborted / 401.
      fail: (e, name) => {
        if (!Api.isQuiet(e)) UI.toast(name ? t("api.failed", {name, why: Api.why(e)}) : Api.why(e), {kind: "err"});
      },
      _refresh: refreshFns,
      _dispose() {
        alive = false;
        ac.abort();
        for (const fn of cleanups.reverse()) {
          try {
            fn();
          } catch (e) {
            console.error(e);
          }
        }
      },
    };
    function bind(opts) {
      return Object.assign({timeout: 30000}, opts, {signal: ac.signal, strict: true,
        current: () => ctx.alive() && (!opts || !opts.current || opts.current())});
    }
    return ctx;
  }

  function setParams(patch, o) {
    UI.afterHistory(() => setParamsNow(patch, o));
  }

  function setParamsNow(patch, o) {
    const q = new URLSearchParams(location.search);
    for (const k in patch) {
      const v = patch[k];
      if (v === "" || v === null || v === undefined || v === false) q.delete(k);
      else q.set(k, v === true ? "1" : String(v));
    }
    const u = location.pathname + qsOf(q) + location.hash;
    if (o && o.push) history.pushState({}, "", u);
    else history.replaceState(history.state, "", u);
  }

  function unmountCurrent() {
    if (!current) return;
    const c = current;
    current = null;
    c.ctx._dispose();
    try {
      if (c.def.unmount) c.def.unmount();
    } catch (e) {
      console.error(e);
    }
    Core.clear(c.root);
  }

  function mountView(route) {
    unmountCurrent();
    const def = Views[route.view] || Views.overview;
    const root = Core.byId("view");
    Core.clear(root);
    if (def.scope && def.scope.length) {
      Object.assign(scopeState, scopeFromQuery(route.query, Core.local.get(RANGE_STORE)));
    }
    const ctx = makeCtx(route.view, root, route);
    current = {name: route.view, def, ctx, root};
    root.classList.remove("view-fade");
    void root.offsetWidth;
    root.classList.add("view-fade");
    renderChrome();
    if (def.scope && def.scope.length) writeScopeToUrl();
    try {
      def.mount(root, ctx);
    } catch (e) {
      console.error(e);
      root.appendChild(UI.errorState({error: e}));
    }
  }

  function applyRoute(route, o) {
    o = o || {};
    routeNow = route;
    if (!Api.key()) return;
    if (current && current.name === route.view && !o.remount) {
      if (current.def.scope && current.def.scope.length) {
        const next = scopeFromQuery(route.query, Core.local.get(RANGE_STORE));
        const changed = scopeKeys().some(k => next[k] !== scopeState[k]);
        Object.assign(scopeState, next);
        if (changed) {
          renderScope();
          for (const fn of Array.from(scopeListeners)) fn(scope.get());
        }
      }
      current.ctx.route = route;
      if (current.def.onRoute) current.def.onRoute(route);
      renderChrome();
      return;
    }
    mountView(route);
  }

  // go(url, {replace}) — in-app navigation (History API). Closes layers and popovers first.
  function go(url, o) {
    UI.closeAll();
    UI.afterHistory(() => goNow(url, o || {}));
  }

  function goNow(url, o) {
    const u = new URL(url, location.origin);
    const route = parseRoute(u.pathname, u.search, u.hash);
    const target = route.redirect || u.pathname + u.search + u.hash;
    try {
      history.replaceState(Object.assign({}, history.state, {scroll: scrollY}), "", location.href);
    } catch (_) {
      // state not writable: ignore
    }
    if (o.replace) history.replaceState({}, "", target);
    else history.pushState({}, "", target);
    const sameView = current && current.name === route.view;
    applyRoute(route);
    if (!sameView) scrollTo(0, 0);
    if (!sameView && !o.keepFocus) {
      const v = Core.byId("view");
      if (v && document.activeElement && document.activeElement.closest && document.activeElement.closest(".tabbar,.layer")) {
        v.focus({preventScroll: true});
      }
    }
  }

  function onPopState(e) {
    if (UI.handlePop()) return;
    UI.closeAll({fromPop: true});
    const route = parseRoute(location.pathname, location.search, location.hash);
    if (route.redirect) history.replaceState({}, "", route.redirect);
    applyRoute(route);
    const y = e.state && e.state.scroll;
    if (typeof y === "number") requestAnimationFrame(() => scrollTo(0, y));
  }

  // ---------- refresh ----------
  const AUTO_STORE = "ciallopa_auto";
  const AUTO_SECS_STORE = "ciallopa_auto_secs";
  const AUTO_SECS = [5, 15, 30, 60];
  let autoTimer = null;
  let pending = 0;
  let lastRefresh = 0;

  const autoOn = () => Core.local.get(AUTO_STORE) === "1";
  function autoSecs() {
    const n = Number(Core.local.get(AUTO_SECS_STORE));
    return AUTO_SECS.includes(n) ? n : 15;
  }

  function busy(p) {
    pending++;
    const bar = Core.byId("progress");
    if (bar) bar.classList.add("on");
    const done = () => {
      pending = Math.max(0, pending - 1);
      if (!pending && bar) bar.classList.remove("on");
    };
    Promise.resolve(p).then(done, done);
    return p;
  }

  // refresh({auto}) — status (quota, egress) plus every ctx.onRefresh handler of the current view.
  // An automatic tick is skipped while the previous refresh is still running (slow scans would
  // otherwise supersede each other forever and the screen would never update).
  let refreshing = 0;
  function refresh(o) {
    o = o || {};
    if (!Api.key() || !current) return Promise.resolve();
    if (o.auto && refreshing) return Promise.resolve();
    refreshing++;
    const jobs = [refreshStatus()];
    for (const fn of current.ctx._refresh) {
      try {
        jobs.push(Promise.resolve(fn({auto: !!o.auto})));
      } catch (e) {
        jobs.push(Promise.reject(e));
      }
    }
    const all = Promise.allSettled(jobs).then(() => {
      refreshing--;
      lastRefresh = Date.now();
      renderAuto();
    });
    return o.auto ? all : busy(all);
  }

  function startAuto() {
    stopAuto();
    if (!autoOn() || !Api.key()) return;
    autoTimer = setInterval(() => {
      if (document.hidden || UI.blocking()) return;
      refresh({auto: true});
    }, autoSecs() * 1000);
  }

  function stopAuto() {
    if (autoTimer) clearInterval(autoTimer);
    autoTimer = null;
  }

  function setAuto(on, secs) {
    if (on !== undefined) Core.local.set(AUTO_STORE, on ? "1" : "0");
    if (secs !== undefined && AUTO_SECS.includes(secs)) Core.local.set(AUTO_SECS_STORE, String(secs));
    startAuto();
    renderAuto();
  }

  // ---------- status: quota (badges, banners), runtime (egress tag) ----------
  async function refreshStatus() {
    const seq = ++statusSeq, auth = Api.key();
    const live = () => seq === statusSeq && auth === Api.key();
    const opts = {timeout: 30000, current: live};
    const quota = Api.get("/usage/v1/quota", opts).then(q => {
      if (!live()) return;
      status.quota = q;
    }).catch(() => {});
    const runtime = (Api.noAdmin() ? Promise.reject(new Error("admin")) : Api.get("/admin/v1/runtime", opts)).then(r => {
      if (!live()) return;
      status.runtime = r;
    }).catch(() => {
      if (live()) status.runtime = null;
    });
    await Promise.all([quota, runtime]);
    if (!live()) return;
    status.at = Date.now();
    applyStatus();
    for (const fn of Array.from(statusListeners)) fn(status);
  }

  function applyStatus() {
    const q = status.quota;
    const need = attentionCount(q);
    setBadge("accounts", need ? need : null, "err");
    const block = q && q.egress_block && q.egress_block.active ? q.egress_block : null;
    systemAlert("egress-block", block ? 1 : 0);
    banner("egress-block", block ? () => egressBlockBanner(block) : null);
    const usable = usableCount(q);
    const configured = q && Array.isArray(q.accounts) ? q.accounts.filter(a => a.configured !== false).length : 0;
    banner("no-account", usable === 0 ? () => ({kind: "err", title: t("shell.noAccount"),
      body: configured ? t("shell.noAccountAll") : t("shell.noAccountNone"),
      actions: [h("a", {class: "btn btn-sm", href: "/ui/accounts"}, t("nav.accounts"))]}) : null);
    renderEgress();
  }

  function egressBlockBanner(b) {
    const parts = [];
    if (b.cf_code) parts.push(t("shell.cfCode", {code: b.cf_code}));
    parts.push(t("shell.cfLast", {time: fmt.time(b.last_at_ms), ago: fmt.ago(b.last_at_ms)}));
    parts.push(t("shell.cfCount", {n: fmt.exact(b.count)}));
    return {
      kind: "err",
      title: b.code === "challenge" ? t("shell.cfChallenge") : t("shell.cfBlock"),
      body: parts.join(" · ") + " · " + t("shell.cfAdvice"),
      tip: t("shell.cfTip", {first: fmt.time(b.first_at_ms), account: b.account, status: b.upstream_status}) +
        (b.cf_ray ? " · cf-ray " + b.cf_ray : ""),
      actions: [h("a", {class: "btn btn-sm", href: "/ui/settings#network"}, t("shell.egressSettings"))],
    };
  }

  // banner(id, specFn | null) — global banner under the top bar on every view. specFn() returns
  // UI.banner options and is re-evaluated on language switch. Null removes it.
  function banner(id, specFn) {
    if (specFn) banners.set(id, specFn);
    else banners.delete(id);
    renderBanners();
  }

  function renderBanners() {
    const box = Core.byId("banners");
    if (!box) return;
    Core.clear(box);
    for (const fn of banners.values()) box.appendChild(UI.banner(fn()));
  }

  // systemAlert(id, n) — one source of the system badge (egress block here; memory, recorder gaps and
  // draining from the system view); the badge shows the sum. 0 / null clears the source.
  const sysAlerts = new Map();
  function systemAlert(id, n) {
    if (n) sysAlerts.set(id, Number(n));
    else sysAlerts.delete(id);
    let sum = 0;
    for (const v of sysAlerts.values()) sum += v;
    setBadge("system", sum || null, "err");
  }

  // setBadge(name, n, kind) — count on the nav items of `name` (accounts, system); null hides it.
  function setBadge(name, n, kind) {
    badges[name] = n === null || n === undefined || n === 0 ? null : {n, kind: kind || ""};
    for (const el of document.querySelectorAll('[data-badge="' + name + '"]')) {
      const b = badges[name];
      el.className = "badge" + (b && b.kind ? " " + b.kind : "");
      el.textContent = b ? String(b.n) : "";
    }
  }

  // ---------- chrome rendering ----------
  function renderChrome() {
    const def = current && current.def;
    const title = def ? def.title() : "";
    Core.byId("page-title").textContent = title;
    document.title = title ? title + " · ciallopa" : "ciallopa";
    const view = current && current.name;
    const hash = routeNow ? routeNow.hash : "";
    for (const a of document.querySelectorAll("[data-nav]")) {
      const target = a.dataset.nav;
      const keys = view === "settings" && hash === "#keys";
      const on = target === "keys" ? keys : target === view && !keys;
      if (on) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    }
    const more = Core.byId("tab-more");
    if (more) more.classList.toggle("on", view === "settings" || view === "system");
    updateNavHrefs();
    renderScope();
    renderAuto();
    renderEgress();
    renderTheme();
    renderLang();
  }

  function updateNavHrefs() {
    for (const a of document.querySelectorAll("a[data-nav]")) {
      const v = a.dataset.nav;
      a.setAttribute("href", v === "keys" ? "/ui/settings#keys" : href(v));
    }
  }

  // Scope bar: time (segmented + pin) and filter buttons on desktop; a time button, filter chips
  // and a filter sheet on phones.
  let scopeValues = null, scopeValuesAt = 0, scopeValuesLoad = null;

  function loadScopeValues() {
    if (scopeValues && Date.now() - scopeValuesAt < 60e3) return Promise.resolve(scopeValues);
    if (scopeValuesLoad) return scopeValuesLoad;
    scopeValuesLoad = Api.get("/usage/v1/scope-values", {timeout: 30000}).then(j => {
      scopeValues = j;
      scopeValuesAt = Date.now();
      return j;
    }).catch(() => scopeValues).finally(() => {
      scopeValuesLoad = null;
    });
    return scopeValuesLoad;
  }

  const fieldLabel = f => t("scope." + f);
  const rangeLabel = r => t("scope.range." + r);

  function fixedText() {
    return fmt.time(scopeState.from, true) + " – " + fmt.time(scopeState.to, true);
  }

  function renderScope() {
    const box = Core.byId("scope");
    const chips = Core.byId("scope-chips");
    const timeBtn = Core.byId("scope-time-phone");
    if (!box) return;
    const fields = (current && current.def.scope) || [];
    const keepFocus = box.contains(document.activeElement) ? document.activeElement.dataset.fk : null;
    Core.clear(box);
    Core.clear(chips);
    timeBtn.classList.toggle("hide", !fields.includes("time"));
    if (!fields.length) return;
    if (fields.includes("time")) {
      const fixed = scopeState.from !== null;
      if (fixed) {
        box.appendChild(UI.chip({label: t("scope.fixed"), value: fixedText(), onRemove: () => scope.unfix(),
          title: t("scope.unfix"), ariaLabel: t("scope.unfix") + " " + fixedText()}));
      } else {
        const seg = UI.segmented({className: "scope-time", label: t("scope.time"), value: scopeState.range,
          options: Object.keys(RANGES).map(r => ({value: r, label: t("scope.rangeShort." + r), title: rangeLabel(r)})),
          onChange: r => scope.set({range: r})});
        box.appendChild(seg);
        const pin = UI.button("", {kind: "ghost", size: "sm", icon: "pin", title: t("scope.fix"), onClick: () => scope.fix()});
        pin.dataset.fk = "scope-pin";
        pin.classList.add("desk-only");
        box.appendChild(pin);
      }
      timeBtn.querySelector(".v").textContent = fixed ? t("scope.fixedShort") : t("scope.rangeShort." + scopeState.range);
      timeBtn.classList.toggle("set", fixed);
    }
    const filterFields = SCOPE_FIELDS.filter(f => fields.includes(f));
    if (filterFields.length) {
      const wrap = h("div", "scope-filters");
      for (const f of filterFields) {
        const v = scopeState[f];
        const b = h("button", {type: "button", class: "scope-btn f-each" + (v ? " set" : ""), "aria-expanded": "false",
          "aria-haspopup": "dialog", "data-fk": "scope-" + f, title: v ? fieldLabel(f) + ": " + v : null},
        h("span", "k", fieldLabel(f)), h("span", "v", v || t("scope.all")), UI.icon("chevron-down"));
        b.addEventListener("click", () => openFieldPopover(b, f));
        wrap.appendChild(b);
      }
      const active = filterFields.filter(f => scopeState[f]).length;
      const all = h("button", {type: "button", class: "scope-btn f-all" + (active ? " set" : ""), "aria-expanded": "false",
        "data-fk": "scope-all"}, UI.icon("filter"), h("span", "v", t("scope.filters")), active ? UI.badge(active) : null);
      all.addEventListener("click", () => openFilterSheet(filterFields, all));
      wrap.appendChild(all);
      box.appendChild(wrap);
      // phone chips: active filters (tap removes) and "+ filter"
      for (const f of filterFields) {
        if (!scopeState[f]) continue;
        chips.appendChild(UI.chip({label: fieldLabel(f), value: scopeState[f], onRemove: () => scope.set({[f]: ""}),
          ariaLabel: t("scope.remove", {name: fieldLabel(f), value: scopeState[f]})}));
      }
      chips.appendChild(UI.chip({icon: "plus", value: t("scope.filters"), dashed: true,
        onClick: () => openFilterSheet(filterFields, null)}));
    }
    if (keepFocus) {
      const f = box.querySelector('[data-fk="' + keepFocus + '"]');
      if (f) f.focus({preventScroll: true});
    }
  }

  // One field (desktop popover): account → list; model / key / instance → text + suggestions.
  function openFieldPopover(anchor, f) {
    const body = h("div", "scope-pop");
    const p = UI.popover(anchor, body, {placement: "bottom-start", className: "scope-popover"});
    if (!p) return;
    const apply = v => {
      p.close();
      scope.set({[f]: v});
    };
    body.appendChild(h("div", "menu-label", fieldLabel(f)));
    if (f === "account") {
      const list = h("div", {class: "menu", role: "listbox", "aria-label": fieldLabel(f)});
      const accounts = accountIds();
      const item = (v, label, meta) => {
        const b = h("button", {type: "button", class: "menu-item", role: "option", "aria-selected": String(scopeState.account === v)},
          h("span", "grow trunc", label), meta ? h("span", "menu-meta", meta) : null, UI.icon("check", "check-mark"));
        b.addEventListener("click", () => apply(v));
        return b;
      };
      list.appendChild(item("", t("scope.allAccounts")));
      for (const a of accounts) list.appendChild(item(a, a));
      if (scopeState.account && !accounts.includes(scopeState.account)) list.appendChild(item(scopeState.account, scopeState.account));
      body.appendChild(list);
      p.reposition();
      const sel = list.querySelector('[aria-selected="true"]');
      if (sel) sel.focus({preventScroll: true});
      return;
    }
    const input = h("input", {class: "input", type: "text", value: scopeState[f], placeholder: t("scope.contains"),
      "aria-label": fieldLabel(f), autocomplete: "off", autocapitalize: "off", spellcheck: "false"});
    const list = h("div", {class: "menu", role: "listbox", "aria-label": t("scope.suggestions")});
    const renderList = () => {
      Core.clear(list);
      const q = input.value.trim().toLowerCase();
      const all = (scopeValues && scopeValues[f]) || [];
      const opts = (q ? all.filter(v => v.value.toLowerCase().includes(q)) : all).slice(0, 8);
      for (const v of opts) {
        const b = h("button", {type: "button", class: "menu-item", role: "option", "aria-selected": String(scopeState[f] === v.value)},
          h("span", "grow trunc", v.value), h("span", "menu-meta", v.configured ? t("scope.configured") : fmt.n(v.count)));
        b.addEventListener("click", () => apply(v.value));
        list.appendChild(b);
      }
      p.reposition();
    };
    input.addEventListener("input", renderList);
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") {
        e.preventDefault();
        apply(input.value);
      } else if (e.key === "ArrowDown" && list.firstElementChild) {
        e.preventDefault();
        list.firstElementChild.focus();
      }
    });
    const clearBtn = UI.button(t("ui.clear"), {size: "sm", kind: "ghost", onClick: () => apply("")});
    const ok = UI.button(t("ui.apply"), {size: "sm", onClick: () => apply(input.value)});
    body.append(input, h("div", "scope-hint", t("scope.containsHint")), list, h("div", "scope-pop-foot", h("span", "grow"), clearBtn, ok));
    input.focus();
    renderList();
    loadScopeValues().then(() => {
      if (input.isConnected) renderList();
    });
  }

  function accountIds() {
    const q = status.quota;
    return q && Array.isArray(q.accounts) ? q.accounts.map(a => a.account) : [];
  }

  // All filter fields at once (phone sheet; desktop below 1280px as a sheet too).
  function openFilterSheet(fields) {
    const controls = {};
    const body = h("div", "scope-sheet");
    for (const f of fields) {
      let c;
      if (f === "account") {
        c = h("select", {class: "select"}, h("option", {value: ""}, t("scope.allAccounts")),
          accountIds().map(a => h("option", {value: a}, a)));
        if (scopeState.account && !accountIds().includes(scopeState.account)) {
          c.appendChild(h("option", {value: scopeState.account}, scopeState.account));
        }
        c.value = scopeState.account;
      } else {
        const listId = "scope-dl-" + f;
        c = h("input", {class: "input", type: "text", value: scopeState[f], placeholder: t("scope.contains"), list: listId,
          autocomplete: "off", autocapitalize: "off", spellcheck: "false"});
        const dl = h("datalist", {id: listId});
        loadScopeValues().then(v => {
          for (const x of (v && v[f]) || []) dl.appendChild(h("option", {value: x.value}));
        });
        body.appendChild(dl);
      }
      controls[f] = c;
      body.appendChild(UI.field({label: fieldLabel(f), control: c}));
    }
    body.appendChild(h("div", "scope-hint", t("scope.containsHint")));
    let layer = null;
    const applyAll = () => {
      const patch = {};
      for (const f of fields) patch[f] = controls[f].value;
      layer.close();
      scope.set(patch);
    };
    const clearAll = UI.button(t("scope.clearAll"), {className: "alt", onClick: () => {
      layer.close();
      scope.clear(fields);
    }});
    const ok = UI.button(t("ui.apply"), {kind: "primary", onClick: applyAll});
    body.addEventListener("keydown", e => {
      if (e.key === "Enter" && e.target.tagName === "INPUT") {
        e.preventDefault();
        applyAll();
      }
    });
    layer = UI.sheet({title: t("scope.filters"), body, foot: [clearAll, ok]});
  }

  // Phone time picker: preset rows with a check, then fix / release the window.
  function openTimeSheet() {
    const list = h("div", {class: "menu", role: "listbox", "aria-label": t("scope.time")});
    let layer = null;
    for (const r of Object.keys(RANGES)) {
      const on = scopeState.from === null && scopeState.range === r;
      const b = h("button", {type: "button", class: "menu-item", role: "option", "aria-selected": String(on)},
        h("span", "grow", rangeLabel(r)), UI.icon("check", "check-mark"));
      b.addEventListener("click", () => {
        layer.close();
        scope.set({range: r});
      });
      list.appendChild(b);
    }
    list.appendChild(h("div", "menu-sep"));
    const fixed = scopeState.from !== null;
    const fixBtn = h("button", {type: "button", class: "menu-item"}, UI.icon("pin"),
      h("span", "grow", fixed ? t("scope.unfix") : t("scope.fix")), fixed ? h("span", "menu-meta", fixedText()) : null);
    fixBtn.addEventListener("click", () => {
      layer.close();
      if (fixed) scope.unfix();
      else scope.fix();
    });
    list.appendChild(fixBtn);
    layer = UI.sheet({title: t("scope.time"), body: list});
  }

  // Auto refresh: the button shows the interval and a dot while on; its popover holds the switch
  // and the interval choice.
  function renderAuto() {
    const b = Core.byId("auto-btn");
    if (!b) return;
    const on = autoOn();
    const label = b.querySelector(".v");
    label.textContent = on ? t("refresh.everyShort", {n: autoSecs()}) : t("refresh.off");
    b.querySelector(".dot").className = "dot" + (on ? " ok pulse" : " off");
    b.title = on ? t("refresh.autoOn", {n: autoSecs()}) : t("refresh.autoOff");
    const r = Core.byId("refresh-btn");
    r.title = lastRefresh ? t("refresh.nowAt", {time: fmt.hms(lastRefresh)}) : t("refresh.now");
    r.setAttribute("aria-label", t("refresh.now"));
  }

  function openAutoPopover(anchor) {
    const sw = h("input", {type: "checkbox", class: "switch", id: "auto-switch"});
    sw.checked = autoOn();
    sw.addEventListener("change", () => setAuto(sw.checked));
    const seg = UI.segmented({label: t("refresh.interval"), value: autoSecs(), block: true,
      options: AUTO_SECS.map(n => ({value: n, label: t("refresh.secs", {n})})),
      onChange: n => setAuto(true, n)});
    const segWrap = h("div", null, seg);
    seg.addEventListener("click", () => {
      sw.checked = true;
    });
    UI.popover(anchor, h("div", "stack",
      h("label", {class: "check", for: "auto-switch"}, sw, h("span", "grow", t("refresh.auto"))),
      h("div", "field", h("span", "field-label", t("refresh.interval")), segWrap),
      h("div", "scope-hint", t("refresh.pauseHint"))), {placement: "bottom-end", className: "auto-pop"});
  }

  function renderEgress() {
    const el = Core.byId("egress");
    if (!el) return;
    const r = status.runtime;
    if (!r || !r.egress) {
      el.classList.add("hide");
      return;
    }
    el.classList.remove("hide");
    Core.clear(el);
    if (r.egress.direct) {
      el.className = "tag outline egress";
      el.append(h("span", "dot"), t("egress.direct"));
      el.title = t("egress.directTip");
    } else {
      const n = r.proxy_errors_1h || 0;
      el.className = "tag egress" + (n ? " warn" : " accent");
      el.append(h("span", "dot " + (n ? "warn" : "accent")), n ? t("egress.proxyErrors", {n}) : t("egress.proxy"));
      el.title = t("egress.proxyTip", {proxy: r.egress.proxy || "", n});
    }
  }

  // ---------- theme & language ----------
  const THEME_STORE = "ciallopa_theme";
  const THEMES = ["system", "light", "dark"];
  const theme = () => {
    const v = Core.local.get(THEME_STORE);
    return v === "light" || v === "dark" ? v : "system";
  };

  function applyTheme(v) {
    const r = document.documentElement;
    if (v === "light" || v === "dark") r.dataset.theme = v;
    else delete r.dataset.theme;
  }

  function setTheme(v, o) {
    if (v === "light" || v === "dark") Core.local.set(THEME_STORE, v);
    else Core.local.del(THEME_STORE);
    applyTheme(v);
    renderTheme();
    if (o && o.toast) UI.toast(t("theme.now", {name: t("theme." + theme())}));
  }

  function cycleTheme() {
    setTheme(THEMES[(THEMES.indexOf(theme()) + 1) % THEMES.length], {toast: true});
  }

  function renderTheme() {
    const name = t("theme." + theme());
    const ic = {system: "theme-system", light: "sun", dark: "moon"}[theme()];
    for (const b of document.querySelectorAll("[data-theme-btn]")) {
      const use = b.querySelector("use");
      if (use) use.setAttribute("href", "#i-" + ic);
      b.setAttribute("aria-label", t("theme.switch", {name}));
      b.dataset.tip = t("theme.switch", {name});
      const txt = b.querySelector(".side-text");
      if (txt) txt.textContent = t("theme.short", {name});
    }
  }

  function renderLang() {
    for (const b of document.querySelectorAll("[data-lang-btn]")) {
      const txt = b.querySelector(".side-text");
      if (txt) txt.textContent = t("lang.name");
      b.setAttribute("aria-label", t("lang.switch"));
      b.dataset.tip = t("lang.switch");
    }
  }

  function toggleLang() {
    I18n.setLang(I18n.lang() === "zh" ? "en" : "zh");
  }

  function onLangChange() {
    renderChrome();
    renderBanners();
    renderSideToggle();
    if (current && current.def.onLang) current.def.onLang();
  }

  // ---------- sidebar collapse (≥1024px) ----------
  const SIDE_STORE = "ciallopa_side";
  function renderSideToggle() {
    const app = Core.byId("app");
    const collapsed = Core.local.get(SIDE_STORE) === "collapsed";
    app.dataset.side = collapsed ? "collapsed" : "expanded";
    for (const b of document.querySelectorAll("[data-side-toggle]")) {
      const label = collapsed ? t("nav.expand") : t("nav.collapse");
      b.setAttribute("aria-label", label);
      b.dataset.tip = label + " ( [ )";
      b.setAttribute("aria-expanded", String(!collapsed));
    }
    // icon-rail items need their names as tooltips
    const rail = collapsed || (typeof matchMedia === "function" && matchMedia("(min-width: 768px) and (max-width: 1023px)").matches);
    for (const a of document.querySelectorAll(".side .side-link[data-nav]")) {
      const txt = a.querySelector(".side-text");
      if (rail && txt) a.dataset.tip = txt.textContent;
      else delete a.dataset.tip;
    }
  }

  function toggleSide() {
    const collapsed = Core.local.get(SIDE_STORE) === "collapsed";
    Core.local.set(SIDE_STORE, collapsed ? "expanded" : "collapsed");
    renderSideToggle();
  }

  // ---------- "more" panel (phone) ----------
  function openMore() {
    const tab = Core.byId("tab-more");
    const item = (iconName, label, onClick, o) => {
      const b = h(o && o.href ? "a" : "button", {class: "more-item" + (o && o.danger ? " danger" : ""), type: o && o.href ? null : "button",
        href: o && o.href || null},
      UI.icon(iconName), h("span", "grow", label), o && o.badge ? o.badge : null, o && o.href ? UI.icon("chevron-right", "chev") : null);
      if (onClick) b.addEventListener("click", onClick);
      return b;
    };
    const sysBadge = h("span", {"data-badge": "system"});
    const langSeg = UI.segmented({block: true, label: t("lang.label"), value: I18n.lang(),
      options: [{value: "zh", label: "中文"}, {value: "en", label: "English"}], onChange: v => I18n.setLang(v)});
    const themeSeg = UI.segmented({block: true, label: t("theme.label"), value: theme(),
      options: THEMES.map(v => ({value: v, label: t("theme." + v)})), onChange: v => setTheme(v)});
    const eg = status.runtime && status.runtime.egress;
    const body = h("div", "more-list",
      h("div", "more-group",
        item("settings", t("nav.settings"), null, {href: "/ui/settings"}),
        item("key", t("nav.keys"), null, {href: "/ui/settings#keys"}),
        item("activity", t("nav.system"), null, {href: "/ui/system", badge: sysBadge}),
        eg ? item("globe", t("egress.label") + " · " + (eg.direct ? t("egress.direct") : t("egress.proxy")), null,
          {href: "/ui/settings#network"}) : null),
      h("div", "more-group",
        h("div", "more-pref", h("span", "more-pref-label", t("lang.label")), langSeg),
        h("div", "more-pref", h("span", "more-pref-label", t("theme.label")), themeSeg)),
      h("div", "more-group", item("logout", t("nav.logout"), () => logout(), {danger: true})));
    tab.setAttribute("aria-expanded", "true");
    UI.page({title: t("nav.more"), body, onClose: () => tab.setAttribute("aria-expanded", "false")});
    setBadge("system", badges.system ? badges.system.n : null, badges.system ? badges.system.kind : "");
  }

  // ---------- keyboard ----------
  let gPending = 0;
  function typingIn(el) {
    if (!el) return false;
    const tag = el.tagName;
    return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || el.isContentEditable;
  }

  function onKey(e) {
    if (!current || !Api.key()) return;
    if ((e.ctrlKey || e.metaKey) && !e.altKey && e.key.toLowerCase() === "b") {
      e.preventDefault();
      toggleSide();
      return;
    }
    if (e.ctrlKey || e.metaKey || e.altKey || typingIn(e.target) || UI.layerCount()) return;
    if (gPending && Date.now() - gPending < 1200) {
      gPending = 0;
      const r = ROUTES.find(x => x.key === e.key.toLowerCase());
      if (r) {
        e.preventDefault();
        go(href(r.view));
      }
      return;
    }
    if (e.key === "g") {
      gPending = Date.now();
      return;
    }
    if (e.key === "/") {
      const target = current.root.querySelector("[data-search]") || document.querySelector("#scope [data-fk^='scope-']");
      if (target) {
        e.preventDefault();
        target.focus();
        if (target.tagName === "BUTTON") target.click();
      }
      return;
    }
    if (e.key === "?") {
      e.preventDefault();
      showShortcuts();
      return;
    }
    if (e.key === "[") {
      e.preventDefault();
      toggleSide();
    }
  }

  function showShortcuts() {
    const keys = (...ks) => h("dt", null, ks.map(k => h("kbd", null, k)));
    const rows = [
      [keys("g", "o"), t("nav.overview")],
      [keys("g", "r"), t("nav.requests")],
      [keys("g", "a"), t("nav.accounts")],
      [keys("g", "s"), t("nav.settings")],
      [keys("g", "y"), t("nav.system")],
      [keys("/"), t("keys.search")],
      [keys("j", "k"), t("keys.nextPrev")],
      [keys("Enter"), t("keys.open")],
      [keys("Esc"), t("keys.close")],
      [keys("["), t("keys.sidebar")],
      [keys("?"), t("keys.help")],
    ];
    UI.modal({title: t("keys.title"), body: h("dl", "shortcut-list", rows.map(([k, d]) => [k, h("dd", "muted", d)]))});
  }

  // ---------- login / logout ----------
  function showLogin(msg) {
    Core.byId("app").classList.add("hide");
    Core.byId("login").classList.remove("hide");
    Core.byId("login-err").textContent = msg || "";
    document.title = "ciallopa";
    const input = Core.byId("login-key");
    if (input) input.focus();
  }

  function showApp() {
    Core.byId("login").classList.add("hide");
    Core.byId("app").classList.remove("hide");
    Core.byId("login-err").textContent = "";
  }

  // enter(): show the app for the stored key and mount the routed view.
  function enter() {
    showApp();
    const route = parseRoute(location.pathname, location.search, location.hash);
    if (route.redirect) history.replaceState({}, "", route.redirect);
    applyRoute(route, {remount: true});
    refreshStatus();
    startAuto();
  }

  // logout(message?) — drop the key and every view state of this identity.
  function logout(msg) {
    Api.clearKey();
    Api.bump();
    stopAuto();
    UI.closeAll();
    unmountCurrent();
    statusSeq++;
    status.quota = status.runtime = null;
    scopeValues = null;
    sysAlerts.clear();
    for (const k of Object.keys(badges)) setBadge(k, null);
    banners.clear();
    renderBanners();
    renderEgress();
    showLogin(msg);
  }

  async function submitLogin(e) {
    e.preventDefault();
    const input = Core.byId("login-key");
    const btn = Core.byId("login-submit");
    const err = Core.byId("login-err");
    const k = input.value.trim();
    if (!k || btn.classList.contains("busy")) return;
    btn.classList.add("busy");
    err.textContent = "";
    Api.bump();
    Api.setKey(k);
    const at = Api.epoch();
    try {
      await Api.get("/usage/v1/summary?range=1m", {timeout: 30000});
      if (Api.epoch() !== at) return;
      input.value = "";
      enter();
    } catch (x) {
      if (Api.epoch() === at && x.message !== "401") err.textContent = x.message;
    } finally {
      btn.classList.remove("busy");
    }
  }

  // ---------- start ----------
  function start() {
    if (started) return;
    started = true;
    applyTheme(theme());
    Core.init();
    I18n.init();
    UI.init();
    prewarm = Core.local.get(PREWARM_STORE) === "1";
    Api.onUnauthorized(msg => logout(msg));
    I18n.onChange(onLangChange);

    document.addEventListener("click", e => {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      const a = e.target.closest && e.target.closest("a[href]");
      if (!a || a.target || a.hasAttribute("download")) return;
      const u = new URL(a.href, location.href);
      if (u.origin !== location.origin || !/^\/ui(\/|$)/.test(u.pathname)) return;
      e.preventDefault();
      go(u.pathname + u.search + u.hash);
    });
    addEventListener("popstate", onPopState);
    addEventListener("hashchange", () => {
      const route = parseRoute(location.pathname, location.search, location.hash);
      if (route.redirect) history.replaceState({}, "", route.redirect);
      applyRoute(route);
    });
    document.addEventListener("keydown", onKey);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden && autoOn() && Api.key() && lastRefresh && Date.now() - lastRefresh > autoSecs() * 1000) {
        refresh({auto: true});
      }
    });
    addEventListener("scroll", () => {
      Core.byId("topbar").classList.toggle("scrolled", scrollY > 4);
    }, {passive: true});
    if (typeof matchMedia === "function") {
      matchMedia("(min-width: 768px) and (max-width: 1023px)").addEventListener("change", renderSideToggle);
    }

    Core.byId("login-form").addEventListener("submit", submitLogin);
    Core.byId("refresh-btn").addEventListener("click", e => {
      const b = e.currentTarget;
      b.classList.add("spinning");
      refresh().finally(() => b.classList.remove("spinning"));
    });
    Core.byId("auto-btn").addEventListener("click", e => openAutoPopover(e.currentTarget));
    Core.byId("scope-time-phone").addEventListener("click", openTimeSheet);
    Core.byId("tab-more").addEventListener("click", openMore);
    for (const b of document.querySelectorAll("[data-theme-btn]")) b.addEventListener("click", cycleTheme);
    for (const b of document.querySelectorAll("[data-lang-btn]")) b.addEventListener("click", toggleLang);
    for (const b of document.querySelectorAll("[data-side-toggle]")) b.addEventListener("click", toggleSide);
    for (const b of document.querySelectorAll("[data-logout]")) b.addEventListener("click", () => logout());

    renderSideToggle();
    renderTheme();
    renderLang();
    const route = parseRoute(location.pathname, location.search, location.hash);
    if (route.redirect) history.replaceState({}, "", route.redirect);
    routeNow = route;
    if (Api.key()) enter();
    else showLogin();
  }

  return {ROUTES, LEGACY_REQUEST_PARAMS, RANGES, parseRoute, scopeFromQuery, scopeToQuery, scopeWindow, attentionCount, usableCount,
    scope, href, go, refresh, refreshStatus, status, banner, setBadge, systemAlert, busy, setTheme, theme, setAuto, logout, start,
    current: () => (current ? current.name : null)};
})();
