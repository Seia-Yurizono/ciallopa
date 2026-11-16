"use strict";
// Settings view (/ui/settings): the schema-driven configuration form with a section directory,
// search, "modified only", drafts that survive background refreshes, validation and save with a
// version check, plus prices, API Keys and the change log. Text: settings/text.js; draft and save
// logic: settings/model.js; controls: settings/controls.js; prices / keys / audit: settings/panels.js.
// Section anchors are hashes (#routing, #network, #prices, #keys, #audit …; old #g-* aliases work).
Views.settings = (() => {
  const {h, fmt} = Core;
  const t = (k, v) => I18n.t(k, v);
  const T = SettingsText, M = SettingsModel, C = SettingsControls;
  let S = null; // per-mount state

  // ---------- mount ----------
  function mount(root, ctx) {
    S = {ctx, root, cur: M.sectionOf(ctx.route.hash), query: "", fp: null, msg: null, draftKept: false, ready: false,
      reg: new Map(), deps: [], advs: [], secs: new Map(), keySec: new Map(), info: {}};
    S.draft = M.createDraft({onChange: () => sync()});
    S.ctl = M.createController(S.draft, (m, p, b, hd) => ctx.req(m, p, b, hd), hooks());
    S.poller = M.createPoller(() => {
      if (S && !document.hidden && !S.query && !UI.blocking()) S.audit.load().catch(() => {});
    }, 15000);
    ctx.onCleanup(() => S && S.poller.sync(false));
    buildLayout();

    ctx.listen(window, "beforeunload", e => {
      if (S && S.draft.count()) {
        e.preventDefault();
        e.returnValue = "";
      }
    });
    ctx.listen(document, "click", guardLeave, true);
    // Any typing clears the server's validation marks (the control re-checks itself right after).
    ctx.listen(root, "input", clearServerErrors, true);
    ctx.listen(document, "visibilitychange", () => {
      if (!document.hidden && S && currentSection() === "audit") S.audit.load().catch(() => {});
    });
    if (typeof matchMedia === "function") ctx.listen(matchMedia(UI.PHONE_Q), "change", () => showSection(S.cur));
    ctx.onRefresh(({auto}) => {
      if (!auto) setMsg(null);
      return S.ctl.load().catch(e => {
        if (!auto) fail(e);
      });
    });
    ctx.onStatus(() => sync());
    ctx.onLang(relang);

    S.ctl.load().catch(e => {
      if (!S || Api.isQuiet(e)) return;
      if (Api.isAdminError(e)) Core.fill(root, UI.needAdmin());
      else Core.fill(S.secsEl, UI.errorState({error: e, retry: () => {
        Core.fill(S.secsEl, UI.skeleton.lines(8));
        S.ctl.load().catch(fail);
      }}));
    });
  }

  function unmount() {
    document.documentElement.classList.remove("st-savebar-on");
    if (S) {
      S.draft.retire();
      S.poller.sync(false);
    }
    S = null;
  }

  function onRoute(route) {
    if (!S) return;
    if (S.query) {
      S.search.value = "";
      filter();
    }
    showSection(M.sectionOf(route.hash));
    scrollTo(0, 0);
  }

  // ---------- controller hooks ----------
  function hooks() {
    return {
      rebased(cfg, unchanged) {
        S.draftKept = false;
        if (!unchanged || !S.ready) renderFormSteady();
        S.ready = true;
        renderChrome(cfg);
      },
      preserved(cfg, moved) {
        if (moved) S.draftKept = true;
        renderChrome(cfg);
      },
      loaded() {
        S.prices.load().catch(() => {});
        S.audit.load().catch(() => {});
        S.ctx.get("/usage/v1/client-fingerprint").then(f => {
          S.fp = f;
          renderFp();
        }).catch(() => {});
      },
      clearServerErrors,
      showErrors,
      review: showReview,
      saved(r, remaining) {
        setMsg(null);
        UI.toast(t("settings.saved", {a: (r.applied || []).length, p: (r.pending_restart || []).length, k: remaining}), {kind: "ok"});
      },
      savedStale(e) {
        setMsg({kind: "err", title: t("settings.savedStaleTitle"), body: t("settings.savedStaleBody", {why: Api.why(e)})});
        UI.toast(t("settings.savedStaleTitle"), {kind: "err"});
      },
      toast: text => UI.toast(text),
    };
  }

  function fail(e) {
    if (!e || Api.isQuiet(e) || !S) return;
    const f = M.failure(e, S.draft.count());
    if (f.conflict) {
      setMsg({kind: "err", title: f.title, body: f.body, recheck: S.draft.count() > 0});
      UI.toast(f.title, {kind: "err", action: S.draft.count() ? {label: t("settings.recheck"), run: review} : null});
    } else {
      UI.toast(f.title + (I18n.lang() === "zh" ? "：" : ": ") + f.body, {kind: "err"});
    }
  }

  function setMsg(m) {
    if (!S) return;
    S.msg = m;
    renderBanners();
  }

  // ---------- layout ----------
  function buildLayout() {
    const {root} = S;
    S.search = h("input", {type: "search", class: "input st-search-input", "data-search": true, "aria-label": t("settings.searchLabel"),
      placeholder: t("settings.search"), autocomplete: "off", spellcheck: "false"});
    S.search.addEventListener("input", filter);
    S.search.addEventListener("keydown", e => {
      if (e.key === "Escape" && S.search.value) {
        e.stopPropagation();
        S.search.value = "";
        filter();
      }
    });
    S.modBtn = h("button", {type: "button", class: "chip st-modbtn", "aria-pressed": "false", title: t("settings.onlyModifiedTip")},
      t("settings.onlyModified"));
    S.modBtn.addEventListener("click", () => {
      S.search.value = S.search.value === M.MOD_Q ? "" : M.MOD_Q;
      filter();
    });
    S.navList = h("div", "st-nav-list", UI.skeleton.lines(6));
    S.nav = h("nav", {class: "st-nav", "aria-label": t("settings.sections")},
      h("div", "st-search", h("span", "st-search-icon", UI.icon("search")), S.search), h("div", "st-nav-tools", S.modBtn), S.navList);
    S.head = h("div", "st-head");
    S.bannersEl = h("div", "st-banners");
    S.back = h("a", {class: "st-back phone-only", href: "/ui/settings"}, UI.icon("chevron-left"), t("settings.backToList"));
    S.qhead = h("div", "st-qhead hide");
    S.qnone = h("div", "hide");
    S.secsEl = h("div", "st-secs", h("div", "card card-body", UI.skeleton.lines(8)));
    S.save = UI.saveBar({text: "", saveLabel: t("settings.checkSave"), discardLabel: t("settings.discard"), onSave: review,
      onDiscard: discard});
    S.save.el.classList.add("hide", "st-savebar");
    S.saveNote = h("span", "st-save-note");
    S.save.el.insertBefore(S.saveNote, S.save.discard);
    S.save.save.dataset.fk = "save";
    S.save.discard.dataset.fk = "discard";
    S.main = h("div", "st-main", S.back, S.qhead, S.qnone, S.secsEl, S.save.el);
    S.layout = h("div", "st-layout", S.nav, S.main);
    S.view = h("div", "st-view", S.head, S.bannersEl, S.layout);
    S.panels = {
      prices: SettingsPanels.prices(panelEnv()),
      keys: SettingsPanels.keys(panelEnv()),
      audit: SettingsPanels.audit(panelEnv()),
    };
    S.prices = S.panels.prices;
    S.audit = S.panels.audit;
    Core.fill(root, S.view);
    showSection(S.cur);
  }

  function panelEnv() {
    return {ctx: S.ctx, draft: S.draft, fail, onChange: () => sync(), reload: () => S.ctl.load(),
      afterWrite: () => S && S.audit.load().catch(() => {})};
  }

  // Language switch: rebuild everything, replaying the raw state of every control onto the new form.
  function relang() {
    if (!S) return;
    const states = S.draft.base ? C.capture(S.secsEl) : [];
    const d = S.draft, edits = Object.assign({}, d.edits), invalid = new Set(d.invalid), rev = d.rev;
    const q = S.search.value, prices = S.prices.state();
    buildLayout();
    if (d.base) {
      renderForm();
      C.replay(S.secsEl, states);
      d.edits = edits;
      d.invalid = invalid;
      d.rev = rev;
      d.settle();
      renderChrome(d.latest || d.base);
      sync();
      showSection(S.cur);
    }
    if (prices) S.prices.load().catch(() => {});
    S.audit.load().catch(() => {});
    S.search.value = q;
    filter();
  }

  // ---------- sections ----------
  const phone = () => UI.isPhone();
  const currentSection = () => S.cur || (phone() ? null : M.SECTIONS[0].id);

  function showSection(id) {
    if (!S) return;
    S.cur = id;
    const on = currentSection();
    S.view.classList.toggle("is-home", !on);
    for (const [sid, s] of S.secs) {
      s.el.classList.toggle("is-on", sid === on);
      if (s.nav) {
        if (sid === on) s.nav.setAttribute("aria-current", "page");
        else s.nav.removeAttribute("aria-current");
      }
    }
    S.poller.sync(on === "audit");
    if (on === "audit" && S.ready) S.audit.load().catch(() => {});
  }

  function renderFormSteady() {
    Core.steady([S.secsEl, S.navList], renderForm);
    showSection(S.cur);
    filter();
  }

  function renderForm() {
    const d = S.draft, c = d.base;
    S.reg.clear();
    S.deps.length = 0;
    S.advs.length = 0;
    S.secs.clear();
    S.keySec.clear();
    S.info = {};
    const rest = M.unplaced(c.schema);
    const secs = [], groups = new Map();
    for (const def of M.SECTIONS) {
      const el = buildSection(def, def.rest ? rest : null);
      secs.push(el);
      const nav = navItem(def.id);
      S.secs.set(def.id, {el, nav: nav.a, dot: nav.dot, sum: nav.sum, badge: nav.badge});
      if (!groups.has(def.g)) groups.set(def.g, []);
      groups.get(def.g).push(nav.a);
    }
    Core.fill(S.secsEl, secs);
    Core.fill(S.navList, M.GROUPS.map(g => h("div", "st-nav-group",
      h("div", "st-nav-gtitle", t("settings.g." + g)), h("div", "st-nav-items", groups.get(g) || []))));
    renderFp();
    sync();
    Core.refocus();
  }

  function navItem(id) {
    const dot = h("span", {class: "st-dot hide", title: t("settings.unsavedDot"), "aria-label": t("settings.unsavedDot"), role: "img"});
    const badge = h("span", "badge warn hide");
    const sum = h("span", "st-nav-sum");
    const a = h("a", {class: "st-nav-item", href: "#" + id, "data-sec": id},
      h("span", "st-nav-text", h("span", "st-nav-name", t("settings.p." + id), dot), sum), badge,
      h("span", "st-nav-chev", UI.icon("chevron-right")));
    return {a, dot, sum, badge};
  }

  function buildSection(def, extra) {
    const el = h("section", {class: "st-sec", "data-sec": def.id, "aria-labelledby": "st-h-" + def.id});
    S.curSec = def.id;
    el.appendChild(h("header", "st-sec-head",
      h("h2", {class: "st-sec-title", id: "st-h-" + def.id}, t("settings.p." + def.id)),
      h("p", "st-sec-sub", t("settings.p." + def.id + ".sub"),
        def.docs ? [" · ", h("a", {href: M.DOCS, target: "_blank", rel: "noopener noreferrer"}, t("settings.docs"),
          UI.icon("external"))] : null)));
    for (const s of def.secs) {
      const sub = h("div", "st-sub");
      if (s.t) {
        sub.appendChild(h("div", "st-sub-head", h("h3", "st-sub-title", t("settings.s." + s.t)),
          s.note ? h("span", "st-sub-note", s.note === "immediate" ? t("settings.immediate") : t("settings.s." + s.note)) : null));
      }
      if (s.box === "prices") sub.appendChild(S.prices.el);
      else if (s.box === "keys") sub.appendChild(S.panels.keys.el);
      else if (s.box === "audit") sub.appendChild(S.audit.el);
      else if (s.cool) sub.appendChild(h("div", "card st-card", M.COOL.map(coolRow)));
      else sub.appendChild(h("div", "card st-card", s.rows.map(rowFor).filter(Boolean)));
      el.appendChild(sub);
    }
    const adv = (def.adv || []).concat(extra || []);
    if (adv.length) {
      const sum = h("span", "st-adv-sum");
      const rows = adv.map(rowFor).filter(Boolean);
      const det = h("details", "st-adv", h("summary", null, UI.icon("chevron-right"), h("span", "st-adv-title", t("settings.advanced")),
        sum),
        h("div", "card st-card", rows));
      S.advs.push({el: det, sum, ks: adv.map(x => (typeof x === "string" ? x : x.k)), auto: false});
      el.appendChild(det);
    }
    const keys = Array.from(S.keySec).filter(([, s]) => s === def.id).map(([k]) => k);
    if (keys.some(k => {
      const f = S.draft.field(k);
      return f && f.effect === "restart";
    })) {
      el.appendChild(h("p", "st-legend", UI.tag(t("settings.restart"), "outline"), " ", t("settings.restartLegend")));
    }
    return el;
  }

  // ---------- rows ----------
  function rowFor(spec) {
    if (typeof spec === "string") spec = {k: spec};
    if (spec.fn) return infoRow(spec.fn);
    const f = S.draft.field(spec.k);
    if (!f) return null;
    const k = f.key, wide = f.kind === "enum" && f.choices && f.choices.length && S.draft.base.write_enabled && f.editable;
    const row = h("div", {class: "st-row" + (spec.p ? " st-child" : "") + (wide ? " st-wide" : ""), "data-anchor": "cfg|" + k});
    const why = h("div", "st-why hide");
    const def = h("div", "st-def");
    const ctl = control(f, row);
    const fs = h("fieldset", "st-ctl", ctl.el);
    row.append(labelBlock(f, why), fs, def);
    row.dataset.q = T.searchText(f) + " " + (M.RZ[k] ? T.searchText(S.draft.field(M.RZ[k]) || {key: M.RZ[k]}) : "") +
      " " + t("settings.p." + S.curSec).toLowerCase();
    reg(k, {row, f, set: ctl.set, error: ctl.error, def, multi: false});
    if (M.RZ[k]) S.keySec.set(M.RZ[k], S.curSec);
    if (spec.p) S.deps.push({row, fs, why, p: spec.p});
    return row;
  }

  function reg(k, o) {
    S.reg.set(k, o);
    S.keySec.set(k, S.curSec);
  }

  function labelBlock(f, why) {
    const more = T.details(f), extra = f.key === "proxy_url" ? t("settings.proxyNote") : f.key === "ipv6" ? t("settings.ipv6Note") : "";
    const info = more || extra ? UI.info(() => h("div", "st-info", more ? h("p", null, more) : null, extra ? h("p", null, extra) : null,
      h("p", "mono small muted", f.key)), t("settings.more")) : null;
    return h("div", "st-label",
      h("div", "st-title-line",
        h("span", "st-title", T.title(f)),
        f.effect === "restart" ? h("span", {class: "tag outline st-tag", title: t("settings.restartTip")}, t("settings.restart")) : null,
        f.key === "ui.allow_reset_consume" ? h("span", {class: "tag err st-tag", title: t("settings.irreversibleTip")},
          t("settings.irreversible")) : null,
        M.IMM[f.key] && S.draft.base.write_enabled ? h("span", "tag st-tag", t("settings.imm.tag")) : null,
        h("span", {class: "tag accent st-tag st-modtag", title: t("settings.modifiedTip")}, t("settings.modified")),
        h("span", "tag accent st-tag st-dirtytag", t("settings.unsavedTag")),
        info),
      h("div", {class: "st-sum", title: T.summary(f)}, T.summary(f)),
      h("code", "st-key", f.key + (M.RZ[f.key] ? " · " + M.RZ[f.key] : "")),
      why);
  }

  // The control for a key (or its read-only value) plus its notes.
  function control(f, row) {
    const c = S.draft.base, k = f.key, W = c.write_enabled;
    const run = c.running[k], file = c.values ? c.values[k] : run;
    const box = h("div", "st-ctl-inner");
    let set = null, error = () => {};
    if (M.IMM[k] && W) box.appendChild(immToggle(k, k === "proxy_enabled" ? file !== false : file === true));
    else if (W && f.editable) {
      let x;
      if (M.RZ[k] && S.draft.field(M.RZ[k])) {
        const fx = S.draft.field(M.RZ[k]);
        x = C.range(S.draft, f, fx, file, c.values ? c.values[fx.key] : c.running[fx.key], row);
      } else if (M.ZO[k]) x = C.zeroOff(S.draft, f, file, row);
      else if (f.kind === "bool") x = C.bool(S.draft, f, file, row);
      else if (f.kind === "enum" && f.choices && f.choices.length) x = C.choices(S.draft, f, file, row);
      else x = C.input(S.draft, f, file, row);
      box.appendChild(x.el);
      set = x.set;
      error = x.error;
    } else box.appendChild(readonly(f, run, c));
    const notes = h("div", "st-notes");
    if (k === "proxy_enabled" && file !== false && !(c.values || c.running).proxy_url) notes.appendChild(h("div", null,
      t("settings.proxyNoUrl")));
    if (k === "proxy_url") notes.appendChild(proxyExtra(c));
    if ((S.latestPending || new Set()).has(k)) notes.appendChild(h("div", "st-running", t("settings.running", {v: M.fmtValue(run)})));
    if (k === "log.level" && c.overrides && c.overrides["log.level"]) notes.appendChild(h("div", "c-warn", t("settings.rustLog")));
    if (notes.childNodes.length) box.appendChild(notes);
    return {el: box, set, error};
  }

  function readonly(f, run, c) {
    let text;
    if (M.RZ[f.key]) text = M.rzText(M.rzRange(f.key, k => c.running[k]));
    else text = M.display(f, run);
    if (text === null) text = M.fmtValue(run);
    const ch = M.choiceOf(f, run);
    return h("div", "st-ro",
      h("span", "st-ro-value" + (f.kind === "bool" ? "" : " mono"), text),
      ch ? h("div", "st-ro-help", T.choice(f, ch).help) : null,
      c.write_enabled && !f.editable ? h("div", "st-ro-lock", UI.icon("lock"), t("settings.fileOnlyShort")) : null);
  }

  // Egress proxy / IPv6 switches write the file at once; the toast's undo writes the old value back.
  function immToggle(k, on) {
    const sw = C.switchInput(t("settings.imm." + k), k, on);
    const state = h("span", "st-state", on ? t("settings.isOn") : t("settings.isOff"));
    sw.addEventListener("change", async () => {
      const v = sw.checked;
      if (S.draft.count()) {
        sw.checked = !v;
        UI.toast(t("settings.imm.blocked"), {kind: "err"});
        return;
      }
      sw.disabled = true;
      state.textContent = v ? t("settings.isOn") : t("settings.isOff");
      try {
        await S.ctl.writeNow(k, v);
        UI.toast(t("settings.imm." + k + (v ? ".on" : ".off")), {kind: "ok", undo: async () => {
          if (!S) return;
          if (S.draft.count()) throw new Error(t("settings.imm.blocked"));
          await S.ctl.writeNow(k, !v);
          UI.toast(t("settings.imm.undone", {name: t("settings.imm." + k), state: v ? t("settings.isOff") : t("settings.isOn")}));
        }});
      } catch (e) {
        sw.checked = !v;
        state.textContent = !v ? t("settings.isOn") : t("settings.isOff");
        fail(e);
      } finally {
        sw.disabled = false;
      }
    });
    return h("label", "st-switch", sw, state);
  }

  function proxyExtra(c) {
    const eg = c.egress || {};
    const res = h("span", {class: "st-ptest-res mono", role: "status"});
    const btn = UI.button(t("settings.ptest"), {size: "sm", icon: "activity", attrs: {"data-fk": "ptest"}});
    btn.addEventListener("click", () => Api.busyRun(btn, t("settings.ptest"), async () => {
      res.textContent = "";
      res.className = "st-ptest-res mono";
      const edits = S.draft.edits;
      const j = await S.ctx.post("/admin/v1/proxy/test", "proxy_url" in edits ? {proxy_url: edits.proxy_url} : {});
      const via = j.direct ? t("settings.ptestDirect") : t("settings.ptestVia");
      res.textContent = j.ok ?
        t("settings.ptestOk", {via, ip: j.ip, loc: j.loc ? " · " + j.loc : "", colo: j.colo ? " · " + j.colo : "",
          ms: fmt.ms(j.latency_ms)}) :
        t("settings.ptestFail", {via, status: j.status ? " (HTTP " + j.status + ")" : "", err: j.error || t("api.unknown")});
      res.classList.add(j.ok ? "c-ok" : "c-err");
    }, t("settings.ptesting")));
    return h("div", "st-egress",
      h("div", null, t("settings.egress", {v: eg.direct ? t("settings.direct") : t("settings.viaProxy", {p: eg.proxy})})),
      h("div", "st-ptest", btn, res));
  }

  // Error → cooldown rows: trigger (fixed text or a threshold key) and one or two cooldown keys.
  function coolRow(def) {
    const all = (def.tk ? [def.tk] : []).concat(def.ks);
    const fields = all.map(k => S.draft.field(k)).filter(Boolean);
    if (!fields.length) return null;
    const row = h("div", {class: "st-row st-cool", "data-anchor": "cool|" + def.ks[0]});
    const def$ = h("div", "st-def");
    const why = h("div", "st-why hide");
    const one = (k, label) => {
      const f = S.draft.field(k);
      if (!f) return null;
      const x = control(f, row);
      reg(k, {row, f, set: x.set, error: x.error, def: def$, multi: all.length > 1, label: label || T.title(f)});
      return h("div", "st-mini", label ? h("span", "st-mini-label", label) : null, x.el);
    };
    const fs = h("fieldset", "st-ctl");
    if (def.tk) fs.appendChild(one(def.tk, t("settings.cool.streak")));
    let inner = fs;
    if (def.ks.length > 1) {
      inner = h("fieldset", "st-ctl-group");
      inner.append(one(def.ks[0], t("settings.cool.start")), one(def.ks[1], t("settings.cool.max")), why);
      fs.appendChild(inner);
      S.deps.push({row: null, fs: inner, why, p: def.tk});
    } else fs.appendChild(one(def.ks[0]));
    const more = fields.map(f => (all.length > 1 ? T.title(f) + (I18n.lang() === "zh" ? "：" : ": ") : "") + (T.details(f) || T.summary(f)));
    row.dataset.q = [t("settings.cool." + def.id), ...fields.map(T.searchText)].join(" ").toLowerCase();
    row.append(h("div", "st-label",
      h("div", "st-title-line", h("span", "st-title", t("settings.cool." + def.id)),
        h("span", {class: "tag accent st-tag st-modtag"}, t("settings.modified")),
        h("span", "tag accent st-tag st-dirtytag", t("settings.unsavedTag")),
        UI.info(() => h("div", "st-info", more.map(m => h("p", null, m)), h("p", "mono small muted", all.join(" · "))),
          t("settings.more"))),
      h("div", "st-sum", def.trig ? t("settings.cool.trigger", {t: t("settings.cool." + def.trig)}) : T.summary(fields[0])),
      h("code", "st-key", all.join(" · "))), fs, def$);
    return row;
  }

  // Rows that show information instead of a setting.
  function infoRow(id) {
    const c = S.draft.base;
    const mk = (name, help, value, q) => h("div", {class: "st-row st-inforow", "data-q": (q + " " + name + " " + help).toLowerCase()},
      h("div", "st-label", h("div", "st-title-line", h("span", "st-title", name)), h("div", "st-sum", help)), h("div", "st-ctl", value));
    if (id === "prices") {
      S.info.prices = h("span", "st-info-val");
      return mk(t("settings.info.prices"), t("settings.info.pricesHelp"),
        h("div", "st-ro", S.info.prices, h("a", {class: "st-go", href: "#prices"}, t("settings.info.pricesGo"), UI.icon("chevron-right"))),
        "prices 价格 定价 未定价 unpriced");
    }
    if (id === "accounts") {
      S.info.acct = h("span", "st-info-val");
      return mk(t("settings.info.acct"), t("settings.info.acctHelp"),
        h("div", "st-ro", S.info.acct, h("a", {class: "st-go", href: "/ui/accounts"}, t("settings.info.acctGo"), UI.icon("chevron-right"))),
        "accounts 账号 权重 优先级 weight priority proxy 代理 重置卡");
    }
    if (id === "file") {
      return mk(t("settings.info.file"), c.write_enabled ? t("settings.info.fileRw") : t("settings.info.fileRo"),
        h("div", "st-ro", h("div", "mono st-path", c.path || t("settings.noFile")), h("div", "st-ro-help", t("settings.version",
          {v: c.version}))),
        "config 配置文件 version 版本 path");
    }
    if (id === "tz") {
      let zone = "";
      try {
        zone = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
      } catch (_) {
        // no Intl time zone: offset only
      }
      return mk(t("settings.info.tz"), t("settings.info.tzHelp"), h("span", "mono", (zone ? zone + " " : "") + "(" + fmt.tzOffset() + ")"),
        "timezone 时区 utc");
    }
    if (id === "fp") {
      S.info.fp = h("div", "st-ro");
      return mk(t("settings.info.fp"), t("settings.info.fpHelp"), S.info.fp, "ua user-agent originator fingerprint 指纹 内部请求");
    }
    return null;
  }

  function renderFp() {
    const v = S && S.info.fp;
    if (!v) return;
    const f = S.fp;
    if (!f) {
      Core.fill(v, h("span", "muted", fmt.NONE));
      return;
    }
    const learned = f.source === "learned";
    Core.fill(v, h("div", "mono st-path", f.user_agent || fmt.NONE),
      h("div", "st-ro-help", "originator " + (f.originator || fmt.NONE) + " · " + (learned && f.learned_at_ms ?
        t("settings.info.fpLearned", {t: fmt.time(f.learned_at_ms), ago: fmt.ago(f.learned_at_ms)}) +
        (f.key ? " · " + t("settings.info.fpKey", {k: f.key}) : "") : t("settings.info.fpDefault"))));
  }

  // ---------- derived state after every edit / load ----------
  function sync() {
    if (!S || !S.draft.base || !S.reg.size) return;
    const d = S.draft, W = d.base.write_enabled;
    const defs = new Set(), rows = new Set();
    for (const o of S.reg.values()) {
      defs.add(o.def);
      rows.add(o.row);
    }
    for (const el of defs) Core.clear(el);
    for (const r of rows) r.classList.remove("is-mod");
    for (const [k, o] of S.reg) {
      const mod = d.isMod(k);
      if (mod) o.row.classList.add("is-mod");
      const dt = M.defaultText(o.f, d.base);
      if (dt === null && !mod) continue;
      const line = h("div", "st-def-line" + (mod ? " is-mod" : ""),
        o.multi ? h("span", "st-def-what", o.label + " ") : null, t("settings.default", {v: dt === null ? t("settings.empty") : dt}));
      if (mod && W && o.set) {
        const b = h("button", {type: "button", class: "btn-link st-restore", "data-fk": "rst|" + k, title: t("settings.restoreTip")},
          t("settings.restore"));
        b.addEventListener("click", () => {
          o.set(d.base.defaults[k]);
          b.blur();
        });
        line.append(" · ", b);
      }
      o.def.appendChild(line);
    }
    for (const dep of S.deps) {
      const f = d.field(dep.p), off = !d.on(dep.p);
      dep.fs.disabled = off;
      if (dep.row) dep.row.classList.toggle("is-off", off);
      dep.why.textContent = off ? (dep.p === "upstream_opts.overload_streak" ? t("settings.depOverload") :
        t("settings.depOff", {p: f ? T.title(f) : dep.p})) : "";
      dep.why.classList.toggle("hide", !off);
    }
    for (const a of S.advs) {
      const m = a.ks.filter(k => d.isMod(k)).length, e = a.ks.filter(k => d.has(k)).length;
      a.sum.textContent = " · " + [t("settings.advCount", {n: a.ks.length}), e ? t("settings.advUnsaved", {n: e}) : null,
        m ? t("settings.advMod", {n: m}) : t("settings.advAllDefault")].filter(Boolean).join(" · ");
      if ((m || e) && !a.auto) {
        a.auto = true;
        a.el.open = true;
      }
    }
    const dirtyBySec = new Map();
    for (const k of new Set([...Object.keys(d.edits), ...d.invalid])) {
      const s = S.keySec.get(k);
      if (s) dirtyBySec.set(s, (dirtyBySec.get(s) || 0) + 1);
    }
    for (const [id, s] of S.secs) {
      s.dot.classList.toggle("hide", !dirtyBySec.get(id));
      s.sum.textContent = summary(id);
    }
    const pr = S.secs.get("prices"), un = (S.prices.state() || {}).unpriced || [];
    if (pr) {
      pr.badge.textContent = un.length ? String(un.length) : "";
      pr.badge.classList.toggle("hide", !un.length);
      pr.badge.title = un.length ? t("settings.info.unpriced", {n: un.length}) : "";
    }
    if (S.info.prices) S.info.prices.textContent = pricesText();
    if (S.info.acct) S.info.acct.textContent = acctCount() === null ? "" : t("settings.info.acctN", {n: acctCount()});
    // save bar
    const n = d.count();
    S.save.el.classList.toggle("hide", !n || !W);
    document.documentElement.classList.toggle("st-savebar-on", !!n && W);
    S.save.update(t("settings.unsavedN", {n}) + (d.invalid.size ? " · " + t("settings.invalidN", {n: d.invalid.size}) : ""));
    S.saveNote.textContent = Array.from(dirtyBySec).map(([id, c]) => t("settings.p." + id) + " " + c).join(" · ");
    S.save.save.disabled = d.invalid.size > 0;
    if (!n && S.draftKept) {
      S.draftKept = false;
      renderBanners();
    }
  }

  function acctCount() {
    const q = Shell.status.quota;
    return q && q.accounts ? q.accounts.filter(a => a.configured !== false).length : null;
  }

  function pricesText() {
    const p = S.prices.state();
    if (!p) return "";
    if (!p.usage_enabled) return t("settings.info.usageOff");
    const un = (p.unpriced || []).length;
    return (p.models ? t("settings.info.models", {n: p.models}) : t("settings.info.noTable")) + (un ? " · " + t("settings.info.unpriced",
      {n: un}) : "");
  }

  // One line per section for the phone directory.
  function summary(id) {
    const d = S.draft, yn = k => (d.on(k) ? t("settings.sum.onShort") : t("settings.sum.offShort"));
    switch (id) {
      case "routing": {
        const f = d.field("routing.strategy"), c = f && M.choiceOf(f, d.cur("routing.strategy"));
        return (c ? T.choice(f, c).label : d.cur("routing.strategy") || "") + " · " + t("settings.sum.affinity",
          {v: yn("routing.session_affinity")});
      }
      case "network":
        return (d.on("proxy_enabled") && d.cur("proxy_url") ? t("settings.sum.proxy") : t("settings.direct")) + " · " +
          t("settings.sum.ipv6", {v: yn("ipv6")});
      case "forward": {
        const m = Number(d.cur("max_inflight_requests")) || 0;
        return t("settings.sum.inflight", {v: m || t("settings.zo.unlimited")}) + " · " + t("settings.sum.attempts",
          {v: d.cur("upstream_opts.max_attempts")});
      }
      case "images":
        return d.on("images.enabled") ? t("settings.isOn") + " · " + (d.cur("images.main_model") || "") : t("settings.isOff");
      case "usage": {
        const r = Number(d.cur("usage.retention_days")) || 0;
        return d.on("usage.enabled") ? t("settings.sum.keep", {v: r ? t("settings.sum.days",
          {n: r}) : t("settings.sum.forever")}) : t("settings.isOff");
      }
      case "prices":
        return pricesText();
      case "accounts": {
        const n = acctCount();
        return (n === null ? "" : t("settings.info.acctN", {n}) + " · ") +
          t("settings.sum.resetRefresh", {v: M.rzText(M.rzRange("ui.reset_credits_refresh_mins", k => d.cur(k)))});
      }
      case "general":
        return (d.cur("instance_id") || t("settings.sum.unnamed")) + " · " + t("settings.sum.log", {v: d.cur("log.level") || ""});
      case "keys":
        return t("settings.k.count", {n: ((d.latest || d.base).running.api_keys || []).length});
      case "audit": {
        const last = S.audit.last();
        return last ? t("settings.a.last", {t: fmt.time(last)}) : t("settings.a.none");
      }
      default:
        return "";
    }
  }

  // ---------- chrome: status strip, banners, keys ----------
  function renderChrome(c) {
    S.latestPending = new Set((c.pending_restart || []).map(p => p.key));
    renderHead(c);
    renderBanners();
    S.panels.keys.render(c);
    sync();
  }

  function renderHead(c) {
    const lr = c.last_reload;
    const reload = UI.button(t("settings.reload"), {size: "sm", icon: "refresh", title: t("settings.reloadTip"),
      attrs: {"data-fk": "reload"}});
    reload.addEventListener("click", () => Api.busyRun(reload, t("settings.reload"), async () => {
      try {
        const j = await S.ctx.post("/admin/v1/config/reload", {});
        await S.ctl.load();
        UI.toast(t("settings.reloaded", {n: (j.applied || []).length}), {kind: "ok"});
      } catch (e) {
        fail(e);
        S && S.ctl.load().catch(() => {});
      }
    }, t("settings.reloading")));
    Core.fill(S.head,
      h("span", {class: "tag " + (c.write_enabled ? "ok" : "") + " st-mode",
        title: c.write_enabled ? t("settings.editableTip") : t("settings.readonlyTip")},
        h("span", "dot " + (c.write_enabled ? "ok" : "off")), c.write_enabled ? t("settings.editable") : t("settings.readonly")),
      h("span", {class: "st-head-path mono trunc", title: c.path || ""}, c.path || t("settings.noFile")),
      h("span", "st-head-ver mono", t("settings.version", {v: c.version})),
      h("span", "grow"),
      lr ? h("span", {class: "st-head-last" + (lr.ok ? "" : " c-err"), title: lr.ok ? "" : lr.error || ""},
        t(lr.ok ? "settings.lastApplied" : "settings.lastAppliedFailed", {t: fmt.time(lr.at_ms)})) : null,
      reload);
  }

  function renderBanners() {
    if (!S) return;
    const c = S.draft.latest || S.draft.base, out = [];
    if (S.msg) {
      const close = UI.button("", {kind: "ghost", size: "sm", icon: "x", title: t("settings.dismiss"), onClick: () => setMsg(null)});
      out.push(UI.banner({kind: S.msg.kind || "err", title: S.msg.title, body: S.msg.body,
        actions: [S.msg.recheck ? UI.button(t("settings.recheck"), {size: "sm", onClick: review}) : null, close]}));
    }
    if (c && c.file_error) out.push(UI.banner({kind: "err", title: t("settings.fileErrorTitle"), body: t("settings.fileErrorBody",
      {e: c.file_error})}));
    if (S.draftKept && S.draft.count()) {
      out.push(UI.banner({kind: "info", title: t("settings.draftKeptTitle"), body: t("settings.draftKeptBody")}));
    }
    const pend = c && c.pending_restart || [];
    if (pend.length) {
      out.push(UI.banner({kind: "warn", title: t("settings.pendingTitle", {n: pend.length}), body: h("ul", "st-pend",
        pend.map(p => {
          const f = S.draft.field(p.key);
          return h("li", null, h("span", null, f ? T.title(f) : p.key), " ", h("code", null, p.key), " · ",
            t("settings.pendingItem", {old: M.fmtValue(p.old), new: M.fmtValue(p.new)}));
        }))}));
    }
    if (c && !c.write_enabled) out.push(UI.banner({kind: "info", icon: "lock", title: t("settings.readonlyTitle"),
      body: t("settings.readonlyBody")}));
    Core.fill(S.bannersEl, out);
  }

  // ---------- search ----------
  function filter() {
    if (!S) return;
    const raw = S.search.value.trim(), q = raw.toLowerCase(), modq = q === M.MOD_Q;
    S.query = q;
    S.view.classList.toggle("is-q", !!q);
    S.modBtn.setAttribute("aria-pressed", String(modq));
    const all = Array.from(S.secsEl.querySelectorAll("[data-q]"));
    if (!q) {
      for (const r of S.secsEl.querySelectorAll(".st-qh,.st-qmiss")) r.classList.remove("st-qh", "st-qmiss");
      S.qhead.classList.add("hide");
      S.qnone.classList.add("hide");
      S.poller.sync(currentSection() === "audit");
      return;
    }
    S.poller.sync(false);
    let n = 0;
    for (const r of all) {
      const hit = M.matches(q, r.dataset.q, r.classList.contains("is-mod") || r.classList.contains("is-dirty"));
      r.classList.toggle("st-qh", !hit);
      if (hit) n++;
    }
    for (const [, s] of S.secs) {
      for (const g of s.el.querySelectorAll(".st-sub,.st-adv")) {
        const any = !!g.querySelector("[data-q]:not(.st-qh)");
        g.classList.toggle("st-qh", !any);
        if (any && g.tagName === "DETAILS") g.open = true;
      }
      s.el.classList.toggle("st-qmiss", !s.el.querySelector("[data-q]:not(.st-qh)"));
    }
    const chips = modq ? [] : M.SECTIONS.filter(s => (t("settings.p." + s.id) + " " + t("settings.p." + s.id
      + ".sub")).toLowerCase().includes(q))
      .map(s => h("a", {class: "chip", href: "#" + s.id}, t("settings.p." + s.id), UI.icon("chevron-right")));
    Core.fill(S.qhead, h("h2", "st-q-title", modq ? t("settings.modTitle", {n}) : t("settings.searchTitle", {q: raw, n})),
      chips.length ? h("div", "st-q-chips", h("span", "muted small", t("settings.pageHits")), chips) : null);
    S.qhead.classList.remove("hide");
    const clear = UI.button(t("settings.clearSearch"), {onClick: () => {
      S.search.value = "";
      filter();
      S.search.focus();
    }});
    Core.fill(S.qnone, UI.empty({icon: "search", title: modq ? t("settings.noMod") : t("settings.noMatch"),
      body: modq ? null : t("settings.noMatchBody"), actions: [clear]}));
    S.qnone.classList.toggle("hide", n > 0 || chips.length > 0);
  }

  // ---------- validate, save, discard ----------
  function clearServerErrors() {
    if (!S) return;
    for (const [k, clear] of S.draft.serverErrors) if (!S.draft.invalid.has(k)) clear();
    S.draft.serverErrors.clear();
  }

  function review() {
    if (!S) return;
    return Api.busyRun(S.save.save, t("settings.checkSave"), () => S.ctl.review().catch(fail), t("settings.checking"));
  }

  function showErrors(list) {
    for (const e of list) {
      const o = e.key && S.reg.get(e.key);
      if (o && o.error) {
        o.error(e.message);
        S.draft.serverErrors.set(e.key, () => o.error(""));
      }
    }
    const ok = UI.button(t("settings.gotIt"), {kind: "primary", onClick: () => layer.close()});
    const layer = UI.modal({title: t("settings.errTitle"), danger: true, foot: [ok], focus: ok,
      body: h("div", "stack", h("p", null, t("settings.errLead")), h("ul", "st-errlist", list.map(e => h("li", null,
        e.field ? h("b", null, T.title(e.field) + (I18n.lang() === "zh" ? "：" : ": ")) : e.key ? h("code", null, e.key + " ") : null,
        e.message))))});
    // Jump to the first field the server rejected.
    const first = list.find(e => e.key && S.keySec.get(e.key));
    if (first && !S.query) {
      S.ctx.setHash(S.keySec.get(first.key));
      showSection(S.keySec.get(first.key));
    }
  }

  function showReview(o) {
    const val = (f, v) => (f && M.display(f, v) !== null ? M.display(f, v) : M.fmtValue(v));
    const tbl = UI.table({compact: true, keyOf: d => d.key, label: t("settings.saveTitle", {n: o.diff.length}), columns: [
      {id: "k", label: t("settings.col.key"), render: d => {
        const f = S.draft.field(d.key);
        // Phones drop the effect column; a restart still shows, next to the name.
        return h("div", null, h("div", null, f ? T.title(f) : d.key, d.effect === "hot" ? null :
          h("span", {class: "tag outline st-tag phone-only"}, t("settings.restart"))), h("code", "st-key", d.key));
      }},
      {id: "o", label: t("settings.col.old"), className: "st-rv-old", render: d => h("span", "muted", val(S.draft.field(d.key), d.old))},
      {id: "n", label: t("settings.col.new"), className: "st-rv-new", render: d => h("span", "strong", val(S.draft.field(d.key), d.new))},
      {id: "e", label: t("settings.col.effect"), className: "st-rv-eff", render: d => (d.effect === "hot" ?
        h("span", {class: "muted small", title: t("settings.hotTip")}, t("settings.hot")) :
        h("span", {class: "tag outline", title: t("settings.restartTip")}, t("settings.restart")))}]});
    tbl.setRows(o.diff);
    const body = h("div", "stack",
      o.versionChanged ? UI.banner({kind: "info", title: t("settings.saveNewer")}) : null,
      o.diff.length ? tbl.el : h("p", "muted", t("settings.saveNone")),
      o.diff.some(d => d.effect === "restart") ? h("p", "muted small", t("settings.saveRestartNote")) : null);
    const cancel = UI.button(t("ui.cancel"), {onClick: () => layer.close()});
    const go = UI.button(t("settings.saveGo"), {kind: "primary"});
    const layer = UI.modal({title: t("settings.saveTitle", {n: o.diff.length}), body, foot: [cancel, go], wide: true, focus: go,
      onClose: o.close});
    go.addEventListener("click", () => Api.busyRun(go, t("settings.saveGo"), async () => {
      try {
        await o.save();
        layer.close();
      } catch (e) {
        layer.close();
        fail(e);
      }
    }, t("settings.saving")));
  }

  async function discard() {
    if (!S) return;
    const d = S.draft, current = d.guard();
    const field = Object.keys(d.edits)[0] || Array.from(d.invalid)[0];
    const ok = await UI.confirm({title: t("settings.discardTitle", {n: d.count()}), body: t("settings.discardBody"),
      ok: t("settings.discardGo"), danger: true});
    if (!ok || !S) return;
    if (!current()) {
      UI.toast(t("settings.discardStale"), {kind: "err"});
      return;
    }
    await Api.busyRun(S.save.discard, t("settings.discard"), async () => {
      let replaced;
      try {
        replaced = await S.ctl.discard();
      } catch (e) {
        fail(e);
        return;
      }
      if (!S) return;
      if (!replaced) {
        UI.toast(t("settings.discardKept"));
        return;
      }
      setMsg(null);
      UI.toast(t("settings.discarded"), {kind: "ok"});
      // Focus returns to the first field that had been edited.
      const target = field && S.secsEl.querySelector('[data-fk="' + CSS.escape(field) + '"],[data-fk^="' + CSS.escape(field) + '="]');
      if (target) {
        const sec = S.keySec.get(field);
        if (sec && sec !== currentSection()) showSection(sec);
        target.focus({preventScroll: true});
        const r = target.getBoundingClientRect();
        if (r.top < 80 || r.bottom > innerHeight - 80) target.scrollIntoView({block: "center", inline: "nearest"});
      }
    });
  }

  // Leaving the view with unsaved edits asks first (in-app links; reloads use beforeunload).
  function guardLeave(e) {
    if (!S || !S.draft.count() || e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    const a = e.target.closest && e.target.closest("a[href]");
    if (!a || a.target || a.hasAttribute("download")) return;
    const u = new URL(a.href, location.href);
    if (u.origin !== location.origin || u.pathname === location.pathname || !/^\/ui(\/|$)/.test(u.pathname)) return;
    e.preventDefault();
    e.stopPropagation();
    const n = S.draft.count();
    UI.confirm({title: t("settings.leaveTitle"), body: t("settings.leaveBody", {n}), ok: t("settings.leaveGo"), cancel: t("settings.stay"),
      danger: true}).then(ok => {
      if (ok) Shell.go(u.pathname + u.search + u.hash);
    });
  }

  return {title: () => t("nav.settings"), mount, unmount, onRoute,
    // for scripts/settings-check.cjs
    _state: () => S};
})();
