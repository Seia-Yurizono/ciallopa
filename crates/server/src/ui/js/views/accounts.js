"use strict";
// Accounts: upstream accounts — health summary, one row per account (status, 5h / weekly remaining,
// 30-day capacity sparkline, last 24 h, reset cards), details in a right drawer (phone: full-screen
// page), add / import / rescan. Data and race rules: views/accounts/model.js; history charts:
// history.js; details and actions: detail.js; add-account dialog: add.js.
// Deep link from other views: /ui/accounts#acc=<id> (old name kept), optionally &act=quota|cooldown|cards|sched.
Views.accounts = (() => {
  const {h} = Core;
  const t = (k, v) => I18n.t(k, v);
  const fmt = Core.fmt;
  const M = AccModel;
  const T = {timeout: 45000};

  let env = null; // per mount

  // ---------- hash: #acc=<id>[&act=…] ----------
  function parseHash(hash) {
    const m = /^#?acc=([^&]+)(?:&act=([a-z]+))?$/.exec(hash || "");
    if (!m) return null;
    try {
      return {id: decodeURIComponent(m[1]), act: m[2] || null};
    } catch (_) {
      return null;
    }
  }

  // ---------- layout ----------
  function skeleton() {
    return h("div", "acc-page",
      h("div", "card acc-summary", UI.skeleton.lines(2)),
      h("div", "card card-flush", UI.skeleton.rows(6, 6)));
  }

  function build(root) {
    const els = {
      banners: h("div", "acc-banners"),
      summary: h("div", "acc-sum-main"),
      toolbar: h("div", "acc-toolbar"),
      list: h("div", {class: "acc-list", role: "list", "aria-label": t("nav.accounts")}),
      head: h("div", {class: "acc-row acc-head", "aria-hidden": "true"}),
      notes: h("div", "acc-notes"),
      body: h("div", "acc-body"),
    };
    els.card = h("div", "card card-flush acc-card", els.head, els.list);
    els.page = h("div", "acc-page", els.banners, h("div", "card acc-summary", els.summary, els.toolbar), els.body);
    Core.fill(root, els.page);
    return els;
  }

  // ---------- banners ----------
  function banners() {
    const s = env.store.state, out = [], j = s.data;
    if (s.error && j) {
      const retry = UI.button(t("ui.retry"), {size: "sm", onClick: () => reload(true).catch(() => {})});
      out.push(UI.banner({kind: "err", title: t("accounts.err.read"), body: Api.why(s.error) + " · " + t("accounts.err.kept",
          {time: fmt.hms(s.readAt)}),
        actions: [retry]}));
    }
    if (!j) return Core.fill(env.els.banners, out);
    if (!j.write_enabled) {
      out.push(UI.banner({kind: "info", title: t("accounts.ro.title"), body: [t("accounts.ro.body"), " ", UI.info(t("accounts.ro.how"))]}));
    }
    const notes = Object.values(s.reads).map(r => r.note).filter(Boolean);
    if (notes.length) {
      out.push(UI.banner({kind: "warn", title: t("accounts.reads.title"), body: notes.map(n => h("div", null, noteText(n)))}));
    }
    const cool = j.accounts.filter(a => a.status === "cooling");
    if (cool.length) {
      const ws = cool.filter(a => a.cooling_reason === "workspace_deactivated").length;
      out.push(UI.banner({kind: "warn", title: t("accounts.cool.title", {n: cool.length}), body: ws ? t("accounts.cool.wsN", {n: ws}) :
          t("accounts.cool.body")}));
    }
    const af = j.auth_files;
    if (af) {
      const bad = (af.skipped || []).filter(f => f.reason !== "unsupported_type");
      if (bad.length) {
        const body = [h("div", "small", t("accounts.af.dir", {dir: af.host_dir ? af.host_dir + " (" + t("accounts.af.inContainer",
            {dir: af.dir}) + ")" : af.dir})),
          h("ul", "acc-files", bad.map(f => AccAdd.skipLine(f)))];
        if (bad.some(f => f.reason === "permission_denied") && af.server_uid !== null && af.server_uid !== undefined) {
          const d = (af.host_dir || af.dir).replace(/\/$/, ""), ids = af.server_uid + ":" + af.server_gid;
          const cmd = "sudo chown " + ids + " " + d + "/*.json && sudo chmod 600 " + d + "/*.json";
          const cp = UI.button(t("accounts.af.copy"), {size: "sm", icon: "copy"});
          cp.addEventListener("click", () => Api.busyRun(cp, t("ui.copy"), async () => {
            await Core.copy(cmd);
            UI.toast(t("ui.copied"), {kind: "ok"});
          }));
          body.push(h("div", "acc-fix", h("span", "small", t(af.host_dir ? "accounts.af.fixHost" : "accounts.af.fixServer", {ids})),
              h("code", "mono", cmd), cp));
        }
        out.push(UI.banner({kind: "err", title: t("accounts.af.title", {n: bad.length}), body}));
      }
    }
    Core.fill(env.els.banners, out);
  }

  function noteText(n) {
    const src = t("accounts.src." + n.source);
    if (n.kind === "denied") return t("accounts.reads.denied", {src});
    if (n.kind === "off") return t("accounts.reads.off", {src});
    if (n.kind === "unavailable") return t("accounts.reads.unavailable", {src});
    return t("accounts.reads.fail", {src, why: n.why}) + " · " + (n.at ? t("accounts.reads.kept", {time: fmt.time(n.at)}) :
        t("accounts.reads.noData"));
  }

  // ---------- summary + toolbar ----------
  function summary() {
    const s = env.store.state, j = s.data;
    if (!j) return Core.clear(env.els.summary);
    const c = M.counts(j.accounts), n = j.accounts.length, use = j.accounts.filter(M.usable).length;
    const parts = M.STATUS_ORDER.map(k => ({label: t("accounts.st." + k), value: c[k], color: {normal: "ok", cooling: "warn",
        disabled: "neutral",
      token_invalid: "err"}[k]}));
    Core.fill(env.els.summary,
      h("div", "acc-sum-num", h("span", "acc-sum-big num", String(use)), h("span", "acc-sum-of num", "/ " + n),
        h("span", "acc-sum-label", t("accounts.sum.usable"), UI.info(t("accounts.sum.info")))),
      h("div", "acc-sum-bar", n ? Charts.segments(parts.filter(p => p.value), {label: t("accounts.sum.aria")}) : null,
        h("div", "acc-sum-meta small muted", t("accounts.sum.strategy"), " ", h("span", "mono", j.strategy || fmt.NONE), " · ",
          j.write_enabled ? t("accounts.sum.rw") : t("accounts.sum.ro"), s.readAt ? " · " + t("accounts.sum.updated",
              {time: fmt.hms(s.readAt)}) : "")));
  }

  // inRender: called from render()'s own steady pass. A separate steady pass there would anchor on a
  // row the first render just placed and scroll the page by the toolbar's height.
  function renderToolbar(inRender) {
    if (!env) return;
    const s = env.store.state, j = s.data, W = !!(j && j.write_enabled), has = !!(j && j.accounts.length);
    const kids = [];
    if (env.device.busy() && !AccAdd.isOpen()) {
      const pend = UI.chip({label: t("accounts.tb.devicePending"), icon: "clock", onClick: () => AccAdd.open(env, "device")});
      pend.classList.add("acc-pending");
      kids.push(pend);
    }
    const rescan = UI.button(t("accounts.tb.rescan"), {size: "sm", attrs: {title: t("accounts.tb.rescanTip"), "data-fk": "tb|rescan"}});
    rescan.addEventListener("click", () => AccAdd.rescan(env, rescan));
    const qall = UI.button(t("accounts.tb.quotaAll"), {size: "sm", icon: "refresh", attrs: {title: t("accounts.tb.quotaAllTip"),
        "data-fk": "tb|qall"}});
    qall.addEventListener("click", () => Api.busyRun(qall, t("accounts.tb.quotaAll"), quotaAll, t("accounts.busyQuery")));
    const imp = UI.button(t("accounts.tb.import"), {size: "sm", attrs: {title: t("accounts.tb.importTip"), "data-fk": "tb|imp"}});
    imp.addEventListener("click", () => pickFiles(imp));
    const add = UI.button(t("accounts.tb.add"), {kind: "primary", size: "sm", icon: "plus", attrs: {"aria-haspopup": "dialog",
        "data-fk": "tb|add"}});
    add.addEventListener("click", () => AccAdd.open(env));
    if (UI.isPhone()) {
      if (has) kids.push(qall);
      const more = UI.button("", {size: "sm", icon: "dots", title: t("ui.more"), attrs: {"aria-haspopup": "menu", "data-fk": "tb|more"}});
      more.addEventListener("click", () => UI.menu(more, [{label: t("accounts.tb.rescan"), icon: "refresh",
          onSelect: () => AccAdd.rescan(env, more)},
        W ? {label: t("accounts.tb.import"), icon: "inbox", onSelect: () => pickFiles(more)} : null].filter(Boolean),
            {title: t("nav.accounts")}));
      kids.push(more);
      if (W) kids.push(add);
    } else {
      kids.push(rescan);
      if (has) kids.push(qall);
      if (W) kids.push(imp, add);
    }
    if (inRender === true) Core.fill(env.els.toolbar, kids);
    else Core.steady([env.els.toolbar], () => Core.fill(env.els.toolbar, kids));
  }

  async function quotaAll() {
    const j = await env.ctx.post("/admin/v1/accounts/quota-refresh", {}, T);
    for (const r of j.results || []) env.store.applyQuota(r);
    render();
    const bad = (j.results || []).filter(r => !r.ok && r.error && r.error.kind !== "rate_limited");
    if (!bad.length) {
      UI.toast(M.qsummary(j), {kind: "ok"});
      return;
    }
    const sk = j.skipped !== undefined ? j.skipped : (j.results || []).filter(M.qskip).length;
    const nmOf = id => {
      const a = env.store.account(id);
      return a ? M.nameOf(a) : id;
    };
    UI.toast(t("accounts.qa.partial", {ok: j.ok || 0, bad: bad.length}) + (sk ? t("accounts.sep") + t("accounts.qs.skipped", {n: sk}) :
        "") + "\n" +
      bad.slice(0, 4).map(r => nmOf(r.account) + ": " + M.qerr(r.error)).join("\n") + (bad.length > 4 ? "\n" + t("accounts.qa.more",
          {n: bad.length - 4}) : ""),
    {kind: "err"});
  }

  function pickFiles(btn) {
    const input = h("input", {type: "file", accept: ".json,application/json", multiple: true, class: "hide"});
    input.addEventListener("change", () => {
      const fs = Array.from(input.files || []);
      input.remove();
      if (fs.length) AccAdd.importFiles(env, fs, btn && btn.isConnected && !btn.closest(".layer") ? btn : null);
    });
    document.body.appendChild(input);
    input.click();
  }

  // ---------- rows ----------
  function winHead(slot, fallback) {
    const j = env.store.state.data, labels = new Set();
    for (const a of j.accounts) {
      const q = env.store.state.quota[a.id], x = M.wslots(q && q.quota)[slot];
      if (x) labels.add(x[0]);
    }
    return labels.size ? Array.from(labels).join(" / ") : fallback;
  }

  function headRow() {
    Core.fill(env.els.head,
      h("div", "c-who", t("term.account")),
      h("div", "c-q", winHead("s", t("accounts.w.5h"))),
      h("div", "c-q", winHead("l", t("accounts.w.week"))),
      h("div", "c-mini", t("accounts.col.mini")),
      h("div", "c-use", t("accounts.col.h24")),
      h("div", "c-cards", t("accounts.col.cards")),
      h("div", "c-act"));
  }

  // markStale: this cell carries the row's "old data" note (one per row, the snapshot is per account).
  function qcell(x, qq, slotLabel, markStale) {
    const cell = h("div", "c-q");
    if (!x) {
      // No observation at all → "unknown" (never drawn as 0%). A window upstream does not return for
      // this account (pro has no 5h) → empty cell, no placeholder.
      if (!qq) cell.appendChild(Charts.meter({value: null, text: t("accounts.q.unknown"), tone: "neutral", label: slotLabel,
        ariaLabel: t("accounts.q.unknownAria", {w: slotLabel})}));
      else cell.classList.add("none");
      if (!qq) cell.firstChild.classList.add("unk");
      return cell;
    }
    const [lb, w] = x, r = M.rem(w), stale = M.staleQuota(qq);
    const m = Charts.meter({value: r / 100, tone: "auto", label: lb, stale, text: fmt.pct100(r, 0), ariaLabel: t("accounts.q.leftAria",
        {w: lb})});
    if (stale) m.title = t("accounts.q.staleTip", {time: fmt.time(qq.observed_at_ms)});
    cell.appendChild(m);
    const rs = M.resetText(w);
    const old = stale && markStale;
    if (rs || old) cell.appendChild(h("span", "c-sub num", old ? h("span", {class: "acc-stale", title: m.title}, UI.icon("clock"),
      t("accounts.q.staleShort")) : null, old && rs ? " · " : null, rs));
    return cell;
  }

  function useCell(a) {
    const u = a.usage_24h, cell = h("div", "c-use");
    if (!u) {
      cell.appendChild(h("span", "muted small", env.store.state.data.usage_enabled ? t("accounts.u.none") : fmt.NONE));
      return cell;
    }
    Core.add(cell, [h("span", "num", t("accounts.u.req", {n: fmt.n(u.requests)})),
      h("span", "c-sub num", fmt.n(u.input_tokens + u.output_tokens) + " Token"),
      u.errors ? h("span", "c-sub num c-err", t("accounts.u.errors", {n: fmt.exact(u.errors)})) : null]);
    return cell;
  }

  function cardCell(a) {
    const s = env.store.state, rc = s.credits[a.id], n = M.cardCount(rc), au = s.auto[a.id] && s.auto[a.id].on, cell = h("div", "c-cards");
    if (n === null) {
      cell.appendChild(h("span", {class: "muted", title: rc && rc.error ? t("accounts.rc.failed", {why: rc.error}) :
          t("accounts.rc.notYet")}, "?"));
      return cell;
    }
    const near = M.nearest(rc), soon = n > 0 && near !== null && near - Date.now() < M.soonMs;
    cell.appendChild(h("span", {class: "acc-cardn num" + (n ? "" : " zero") + (soon ? " soon" : ""),
      title: t("accounts.rc.tip", {n}) + (soon ? t("accounts.rc.tipSoon") : "") + (au ? t("accounts.rc.tipAuto") : "")},
    soon ? UI.icon("clock") : null, String(n)));
    if (au) cell.appendChild(h("span", "c-sub", t("accounts.rc.auto")));
    return cell;
  }

  function fillRow(row, a) {
    const s = env.store.state, q = s.quota[a.id], qq = q && q.quota, ws = M.wslots(qq), kind = M.statusKind(a.status);
    const name = h("button", {type: "button", class: "acc-name trunc", "data-fk": a.id + "|open", "aria-haspopup": "dialog",
      title: a.email ? a.email + " (" + a.id + ")" : a.id}, M.nameOf(a));
    name.addEventListener("click", () => openDetail(a.id, name));
    const sub = h("div", "c-sub acc-who-sub", h("span", "trunc", a.id + (a.plan ? " · " + a.plan : "")));
    if (a.status !== "normal") sub.appendChild(h("span", "acc-st " + kind, AccDetail.statusLabel(a.status)));
    if (a.status === "cooling" && a.cooling_until_ms) {
      sub.appendChild(h("span", {class: "num c-warn", "data-until": a.cooling_until_ms, title: t("accounts.d.coolUntil",
          {time: fmt.time(a.cooling_until_ms)})},
        fmt.dur((a.cooling_until_ms - Date.now()) / 1000)));
    }
    const qcellS = qcell(ws.s, qq, t("accounts.w.5h"), true);
    const qcellL = qcell(ws.l, qq, t("accounts.w.week"), !ws.s);
    const cr = M.crText(qq && qq.credits);
    if (cr) (qcellL.classList.contains("none") && !qcellS.classList.contains("none") ? qcellS : qcellL)
      .appendChild(h("span", "c-sub acc-cr" + (cr.none ? " none" : ""), cr.text));
    const hist = env.store.histOf(a.id);
    const more = UI.button("", {kind: "ghost", size: "sm", icon: "dots", title: t("accounts.menu", {name: M.nameOf(a)}),
      attrs: {"aria-haspopup": "menu", "data-fk": a.id + "|menu"}});
    more.addEventListener("click", e => {
      e.stopPropagation();
      rowMenu(a.id, more);
    });
    Core.fill(row,
      h("div", "c-who", UI.dot(kind === "off" ? "off" : kind, AccDetail.statusLabel(a.status)), h("div", "acc-who", name, sub)),
      qcellS, qcellL,
      h("div", "c-mini", h("span", "c-mini-label", t("accounts.col.miniShort")), AccHistory.mini(hist, AccHistory.wins(qq), M.nameOf(a))),
      useCell(a), cardCell(a),
      h("div", "c-act", more));
    row.classList.toggle("is-off", a.status === "disabled" || a.status === "token_invalid");
    row.classList.toggle("is-selected", !!(env.drawer && env.drawer.id === a.id));
  }

  function rowMenu(id, anchor) {
    const a = env.store.account(id);
    if (!a) return;
    const W = env.W();
    const busyRow = (label, fn) => async () => {
      const row = env.els.list.querySelector('[data-key="' + CSS.escape(id) + '"]');
      if (row) row.classList.add("is-busy");
      try {
        await fn();
      } catch (e) {
        if (!Api.isQuiet(e)) UI.toast(t("api.failed", {name: label, why: Api.why(e)}), {kind: "err"});
      } finally {
        if (row) row.classList.remove("is-busy");
      }
    };
    const items = [
      {label: t("accounts.menu.details"), icon: "panel", onSelect: () => openDetail(id, anchor)},
      {label: t("accounts.act.refreshQuota"), icon: "refresh", onSelect: busyRow(t("accounts.act.refreshQuota"),
          () => AccDetail.refreshQuota(a, env))},
      a.status === "cooling" ? {label: t("accounts.act.clearCooldown"), icon: "clock",
        onSelect: busyRow(t("accounts.act.clearCooldown"), () => AccDetail.clearCooldown(a, env))} : null,
      W && a.status !== "token_invalid" ? {label: a.disabled ? t("accounts.act.enable") : t("accounts.act.disable"), icon: a.disabled ?
          "check" : "x",
        onSelect: () => AccDetail.setEnabled(a, !!a.disabled, env)} : null,
      W ? "-" : null,
      W ? {label: t("accounts.act.delete") + "…", icon: "trash", danger: true, onSelect: () => AccDetail.deleteAccount(a, env)} : null,
    ].filter(Boolean);
    UI.menu(anchor, items, {title: M.nameOf(a)});
  }

  function createRow(a) {
    const row = h("div", {class: "acc-row", role: "listitem"});
    row.addEventListener("click", e => {
      if (e.target.closest("button,a,input,label")) return;
      if (getSelection && String(getSelection()).length) return;
      openDetail(row.dataset.key, row.querySelector(".acc-name"));
    });
    row._a = a;
    return row;
  }

  function renderList() {
    const s = env.store.state, j = s.data;
    if (!j.accounts.length) {
      const W = j.write_enabled;
      const add = W ? UI.button(t("accounts.tb.add"), {icon: "plus", onClick: () => AccAdd.open(env)}) : null;
      Core.fill(env.els.body, h("div", "card", UI.empty({icon: "accounts", title: t("accounts.empty.title"), body: W ?
          t("accounts.empty.body") :
        t("accounts.empty.bodyRo"), actions: add ? [add] : null})));
      return;
    }
    if (!env.els.card.isConnected) Core.fill(env.els.body, env.els.card, env.els.notes);
    headRow();
    Core.keyed(env.els.list, j.accounts, a => a.id, a => {
      const r = createRow(a);
      fillRow(r, a);
      return r;
    }, (r, a) => fillRow(r, a));
    const af = j.auth_files, other = af ? (af.skipped || []).filter(f => f.reason === "unsupported_type") : [];
    Core.fill(env.els.notes, other.length ? h("p", "small muted", t("accounts.af.ignored", {n: other.length,
      types: Array.from(new Set(other.map(f => f.detail || t("accounts.af.unknownType")))).join(t("accounts.listSep"))})) : null);
  }

  // ---------- render ----------
  function render() {
    if (!env) return;
    const s = env.store.state;
    if (s.needAdmin) {
      Core.fill(env.root, UI.needAdmin());
      env.els = null;
      return;
    }
    if (!env.els) env.els = build(env.root);
    Core.steady([env.els.banners, env.els.summary, env.els.toolbar, env.els.list], () => {
      banners();
      renderToolbar(true);
      if (!s.data) {
        if (s.error) Core.fill(env.els.body, UI.errorState({error: s.error, retry: () => reload(true).catch(() => {})}));
        return;
      }
      summary();
      renderList();
    });
    renderDrawer();
  }

  function renderAccount(id) {
    if (!env || !env.els) return;
    const a = env.store.account(id);
    const row = a && env.els.list.querySelector('[data-key="' + CSS.escape(id) + '"]');
    if (row) Core.steady([row], () => fillRow(row, a));
    if (env.drawer && env.drawer.id === id) renderDrawer();
  }

  // ---------- details drawer ----------
  function openDetail(id, opener, act) {
    const a = env.store.account(id);
    if (!a) return;
    if (env.drawer && env.drawer.id === id) {
      if (act) AccDetail.highlight(env.drawer.layer.body, act);
      return;
    }
    if (env.drawer) {
      env.drawer.id = id;
      renderDrawer();
      env.drawer.layer.body.scrollTop = 0;
    } else {
      const layer = UI.drawer({title: M.nameOf(a), body: h("div"), className: "acc-drawer", onClose: () => {
        const was = env && env.drawer;
        if (env) env.drawer = null;
        if (env && env.els) for (const r of env.els.list.children) r.classList.remove("is-selected");
        if (was && parseHash(location.hash)) env.ctx.setHash("");
      }});
      if (opener && !UI.isPhone()) layer.opener = opener;
      env.drawer = {layer, id};
      renderDrawer();
    }
    for (const r of env.els.list.children) r.classList.toggle("is-selected", r.dataset.key === id);
    if (act) setTimeout(() => env && env.drawer && AccDetail.highlight(env.drawer.layer.body, act), 220);
  }

  function renderDrawer() {
    const d = env && env.drawer;
    if (!d) return;
    const a = env.store.account(d.id);
    if (!a) {
      d.layer.close();
      return;
    }
    d.layer.setTitle(M.nameOf(a));
    AccDetail.render(d.layer.body, a, env);
  }

  // /ui/accounts#acc=<id>: highlight the row and open its details.
  function anchor() {
    const p = parseHash(location.hash);
    if (!p || !env || !env.store.state.data || !env.els) return;
    const row = env.els.list.querySelector('[data-key="' + CSS.escape(p.id) + '"]');
    if (!row) return;
    for (const r of env.els.list.querySelectorAll(".acc-row.hl")) r.classList.remove("hl");
    row.classList.add("hl");
    row.scrollIntoView({block: "center"});
    openDetail(p.id, row.querySelector(".acc-name"), p.act);
  }

  // ---------- loading ----------
  function reload(forceHist) {
    if (!env) return Promise.resolve(false);
    const my = env;
    const p = my.store.load({prewarm: Shell.scope.get().prewarm}).then(applied => {
      my.store.loadHist(!!forceHist);
      return applied;
    });
    return my.ctx.busy(p);
  }

  return {
    title: () => t("nav.accounts"),
    parseHash,
    mount(root, ctx) {
      root.appendChild(skeleton());
      const me = {ctx, root, els: null, drawer: null, drafts: {}, pasteDraft: ""};
      env = me;
      const alive = () => env === me && ctx.alive();
      const identity = () => Api.key() + "#" + Api.epoch() + (alive() ? "" : "#gone");
      me.store = M.createStore({
        get: (path, o) => ctx.get(path, Object.assign({}, o)),
        post: (path, body) => ctx.post(path, body, T),
        identity,
        onChange: () => alive() && render(),
        onAccount: id => alive() && renderAccount(id),
        onError: e => ctx.fail(e),
      });
      me.device = M.createDevice({
        request: (method, path, o) => Api.req(method, path, method === "POST" ? {} : undefined, null, o).then(r => r.j),
        identity,
        onUpdate: () => {
          if (!alive()) return;
          AccAdd.paint();
          renderToolbar();
        },
        onDone: account => {
          UI.toast(t("accounts.dev.done", {account}), {kind: "ok"});
          reload().catch(e => ctx.fail(e));
        },
      });
      me.W = () => !!(me.store.state.data && me.store.state.data.write_enabled);
      me.reload = () => reload(false);
      me.hist = force => me.store.loadHist(force);
      me.renderAccount = renderAccount;
      me.renderToolbar = renderToolbar;
      me.pickFiles = () => pickFiles(null);
      me.closeDetail = id => {
        if (me.drawer && me.drawer.id === id) me.drawer.layer.close();
      };
      ctx.onCleanup(() => {
        me.device.end();
        AccAdd.close();
        if (me.drawer) me.drawer.layer.close();
      });
      ctx.onRefresh(({auto}) => reload(!auto).catch(e => {
        if (!auto) ctx.fail(e, t("accounts.err.read"));
      }));
      ctx.onLang(() => {
        render();
        AccAdd.paint();
      });
      let pw = Shell.scope.get().prewarm;
      ctx.onScope(sc => {
        if (sc.prewarm !== pw) {
          pw = sc.prewarm;
          reload().catch(e => ctx.fail(e));
        }
      });
      if (typeof matchMedia === "function") {
        const mq = matchMedia(UI.PHONE_Q);
        ctx.listen(mq, "change", () => renderToolbar());
      }
      reload().then(applied => {
        if (applied) anchor();
      }).catch(e => ctx.fail(e, t("accounts.err.read")));
    },
    onRoute() {
      anchor();
    },
    unmount() {
      env = null;
      AccHistory.clearUi();
      AccDetail.resetConsume();
    },
  };
})();
