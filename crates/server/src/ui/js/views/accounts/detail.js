"use strict";
// AccDetail: one account's details (desktop right drawer, phone full-screen page) and every action
// on it: cooldown, quota refresh, scheduling (enable, weight, priority, auto switches), reset cards
// (list, manual use behind a confirm, refresh), quota-estimate reset / undo, token refresh, delete.
// `env` comes from the view: {ctx, store, W(), reload(), hist(force), renderAccount(id)}.
const AccDetail = (() => {
  const {h} = Core;
  const t = (k, v) => I18n.t(k, v);
  const fmt = Core.fmt;
  const M = AccModel;
  const T = {timeout: 45000};

  const nm = M.nameOf;
  const full = a => (a.email ? a.email + " (" + a.id + ")" : a.id);
  const statusLabel = s => (M.STATUS_ORDER.includes(s) ? t("accounts.st." + s) : s);
  const reasonLabel = r => (r && I18n.has("accounts.reason." + r) ? t("accounts.reason." + r) : r);

  function sec(title, o, ...kids) {
    o = o || {};
    return h("section", {class: "acc-sec", "data-sec": o.id || null},
      h("div", "acc-sec-head", h("h3", "acc-sec-title", title, o.info ? UI.info(o.info) : null), o.tools || null), ...kids);
  }
  const row = (label, ...v) => h("div", "acc-kv-row", h("span", "acc-kv-k", label), h("span", "acc-kv-v", ...v));

  // ---------- generic action dialog ----------
  // The confirm button runs `run` with a busy state; success closes, failure keeps the dialog open
  // with an error toast. Focus starts on Cancel; the confirm ignores clicks in its first 400 ms.
  function dialog(o) {
    const cancel = UI.button(t("ui.cancel"));
    const ok = UI.button(o.ok, {kind: o.danger ? "danger-solid" : "primary"});
    const alt = o.alt ? UI.button(o.alt.label, {className: "alt"}) : null;
    const layer = UI.modal({title: o.title, body: o.body, foot: [alt, cancel, ok], danger: o.danger, focus: cancel,
      role: "alertdialog", wide: o.wide});
    cancel.addEventListener("click", () => layer.close());
    const go = (btn, label, fn) => btn.addEventListener("click", () => {
      if (Date.now() - layer.at < 400) return;
      Api.busyRun(btn, label, async () => {
        await fn(layer);
        layer.close();
      }, t("accounts.busy"));
    });
    go(ok, o.ok, o.run);
    if (alt) go(alt, o.alt.label, o.alt.run);
    return layer;
  }

  const p = (cls, ...kids) => h("p", cls, ...kids);
  const warnP = (b, rest) => h("div", "acc-warnbox", h("b", null, b), " ", rest);

  // ---------- account actions ----------
  async function clearCooldown(a, env) {
    const j = await env.ctx.post(M.pathOf(a) + "/clear-cooldown", {}, T);
    UI.toast(j && j.cleared ? t("accounts.toast.cooldownCleared", {name: nm(a)}) : t("accounts.toast.notCooling", {name: nm(a)}),
        {kind: "ok"});
    await env.reload();
  }

  async function refreshQuota(a, env) {
    const r = await env.ctx.post(M.pathOf(a) + "/quota-refresh", {}, T);
    env.store.applyQuota(r);
    env.renderAccount(a.id);
    if (M.qskip(r)) {
      UI.toast(M.qerr(r.error));
      return;
    }
    if (!r.ok) throw Object.assign(new Error(M.qerr(r.error)), {kind: "quota"});
    UI.toast(t("accounts.toast.quotaRefreshed", {name: nm(a)}) + (r.cooldown_cleared ? t("accounts.toast.andCleared") : ""), {kind: "ok"});
  }

  const patch = (env, a, body) => env.ctx.req("PATCH", M.pathOf(a), body, null, T);

  // Enable / disable, optimistic: the row changes now, a failure puts it back. Disabling the last
  // account that can serve asks first (every proxied request would fail).
  async function setEnabled(a, on, env) {
    if (!on && M.lastOne(a, env.store.state.data.accounts)) {
      const yes = await UI.confirm({title: t("accounts.disable.lastTitle", {name: nm(a)}), body: t("accounts.disable.lastBody"),
        ok: t("accounts.act.disable"), danger: true});
      if (!yes) {
        env.renderAccount(a.id);
        return;
      }
    }
    const revert = env.store.patchLocal(a.id, on ? {disabled: false, status: a.status === "disabled" ? "normal" : a.status} :
      {disabled: true, status: a.status === "token_invalid" ? a.status : "disabled"});
    try {
      await patch(env, a, {disabled: !on});
    } catch (e) {
      revert();
      if (!Api.isQuiet(e)) UI.toast(t("api.failed", {name: on ? t("accounts.act.enable") : t("accounts.act.disable"), why: Api.why(e)}),
          {kind: "err"});
      return;
    }
    if (on) UI.toast(t("accounts.toast.enabled", {name: nm(a)}), {kind: "ok"});
    else {
      UI.toast(t("accounts.toast.disabled", {name: nm(a)}), {kind: "ok", undo: async () => {
        await patch(env, a, {disabled: false});
        UI.toast(t("accounts.toast.reenabled", {name: nm(a)}), {kind: "ok"});
        await env.reload();
      }});
    }
    await env.reload().catch(e => env.ctx.fail(e));
  }

  async function saveSchedule(a, weight, priority, env) {
    const ow = a.weight, op = a.priority;
    const revert = env.store.patchLocal(a.id, {weight, priority});
    try {
      await patch(env, a, {weight, priority});
    } catch (e) {
      revert();
      throw e;
    }
    delete env.drafts[a.id];
    UI.toast(t("accounts.toast.schedSaved", {name: nm(a)}), {kind: "ok", undo: async () => {
      await patch(env, a, {weight: ow, priority: op});
      UI.toast(t("accounts.toast.schedReverted", {name: nm(a), w: ow, p: op}), {kind: "ok"});
      await env.reload();
    }});
    await env.reload().catch(e => env.ctx.fail(e));
  }

  function refreshToken(a, env) {
    dialog({title: t("accounts.token.title", {name: nm(a)}), ok: t("accounts.token.ok"), danger: true,
      body: [p("mono small", full(a)), p(null, t("accounts.token.body")), warnP(t("accounts.irreversible"), t("accounts.token.warn")),
        p("muted small", t("accounts.token.auto"))],
      run: async () => {
        await env.ctx.post(M.pathOf(a) + "/refresh", {confirm: a.id}, T);
        UI.toast(t("accounts.toast.tokenRefreshed", {name: nm(a)}), {kind: "ok"});
        await env.reload();
      }});
  }

  function deleteAccount(a, env) {
    const rc = env.store.state.credits[a.id], n = rc && rc.available_count;
    const canPause = !a.disabled && a.status !== "token_invalid";
    dialog({title: t("accounts.del.title", {name: nm(a)}), ok: t("accounts.del.ok"), danger: true,
      body: [p("mono small", full(a)), p(null, t("accounts.del.body")),
        n > 0 ? p("acc-note", t("accounts.del.cards", {n})) : null,
        M.lastOne(a, env.store.state.data.accounts) ? warnP(t("accounts.del.lastTitle"), t("accounts.del.last")) : null,
        canPause ? p("muted small", t("accounts.del.pauseHint")) : null],
      alt: canPause ? {label: t("accounts.del.pause"), run: async () => {
        await patch(env, a, {disabled: true});
        UI.toast(t("accounts.toast.disabled", {name: nm(a)}), {kind: "ok", undo: async () => {
          await patch(env, a, {disabled: false});
          UI.toast(t("accounts.toast.reenabled", {name: nm(a)}), {kind: "ok"});
          await env.reload();
        }});
        await env.reload();
      }} : null,
      run: async () => {
        await env.ctx.req("DELETE", M.pathOf(a), {confirm: a.id}, null, T);
        env.closeDetail(a.id);
        UI.toast(t("accounts.toast.deleted", {name: nm(a)}), {kind: "ok"});
        await env.reload();
      }});
  }

  // ---------- quota estimate reset (usage log only; works with the config read-only too) ----------
  function estimateReset(a, data, ws, w, list, env) {
    const lab = AccHistory.winLabel(w, ws), rs = AccHistory.rows(data.cycles, w);
    const n = rs.filter(c => !c.open && !c.archived).length, hasOpen = rs.some(c => c.open);
    const what = (n ? t("accounts.er.closed", {n}) : t("accounts.er.noClosed")) + (hasOpen ? t("accounts.er.plusOpen") : "");
    let choice = w;
    let box;
    if (list.length > 1) {
      box = h("div", {class: "acc-radios", role: "radiogroup", "aria-label": t("accounts.er.scope")});
      const opt = (v, title, sub, on) => {
        const i = h("input", {type: "radio", name: "acc-er", value: v});
        i.checked = !!on;
        i.addEventListener("change", () => {
          if (i.checked) choice = v;
        });
        return h("label", "acc-radio", i, h("span", null, h("b", null, title), h("small", null, sub)));
      };
      box.append(opt(w, t("accounts.er.thisWin", {w: lab}), what, true),
        opt("all", t("accounts.er.allWins"), list.map(x => AccHistory.winLabel(x, ws)).join(" + ")));
    } else box = p("acc-er-one", h("b", null, t("accounts.er.thisWin", {w: lab})), h("span", "muted", " · " + what));
    dialog({title: t("accounts.er.title", {name: nm(a)}), ok: t("accounts.er.ok"),
      body: [p(null, t("accounts.er.body")), box, p("muted small", t("accounts.er.note"))],
      run: async () => {
        const j = await env.ctx.post(M.pathOf(a) + "/quota-estimate/reset", {window: choice, confirm: a.id}, T);
        UI.toast(t("accounts.toast.estReset", {name: nm(a), w: choice === "all" ? t("accounts.er.all") : lab}), {kind: "ok",
            undo: async () => {
          await env.ctx.post(M.pathOf(a) + "/quota-estimate/reset/" + j.epoch_id + "/undo", {}, T);
          UI.toast(t("accounts.toast.estUndone", {name: nm(a)}), {kind: "ok"});
          await env.hist(true);
          await env.reload();
        }});
        await env.hist(true);
        await env.reload();
      }});
  }

  function undoEstimate(a, ep, b, env) {
    return Api.busyRun(b, t("ui.undo"), async () => {
      await env.ctx.post(M.pathOf(a) + "/quota-estimate/reset/" + ep.id + "/undo", {}, T);
      UI.toast(t("accounts.toast.estUndone", {name: nm(a)}), {kind: "ok"});
      await env.hist(true);
      await env.reload();
    }, t("accounts.busy"));
  }

  // ---------- reset card: manual use, behind the confirm dialog ----------
  const CODES = ["reset", "nothing_to_reset", "no_credit", "already_redeemed"];
  const codeLabel = c => (CODES.includes(c) ? t("accounts.code." + c) : c);
  let consumeCur = null;
  function openConsume(a, card, env) {
    const r = env.store.state.credits[a.id] || {}, now = Date.now();
    const qq = env.store.state.quota[a.id] && env.store.state.quota[a.id].quota;
    const near = M.nearest(r);
    const exp = c => (M.expOf(c) === Infinity ? t("accounts.rc.never") : t("accounts.rc.expires", {time: fmt.time(M.expOf(c), true)}));
    const o = M.wslots(qq), ws = [o.s, o.l].filter(x => x && M.rem(x[1], now) !== null);
    const left = ws.length ? ws.map(([lb, w]) => lb + " " + fmt.pct100(M.rem(w, now), 0) + (w.resets_at && w.resets_at * 1000 > now ?
      " (" + t("accounts.q.resetIn", {d: fmt.dur(w.resets_at - now / 1000)}) + ")" : "")).join(" · ") +
      (M.staleQuota(qq, now) ? " · " + t("accounts.q.stale") : "") : t("accounts.cp.unknownLeft");
    const hint = M.consumeHint(qq, now);
    const result = h("div", {class: "acc-cp-res", role: "status"});
    const body = [
      UI.kv([[t("term.account"), full(a) + (a.disabled ? " · " + t("accounts.st.disabled") : "")],
        [t("accounts.cp.available"), t("accounts.rc.count", {n: r.available_count}) + (near ? " · " + t("accounts.cp.nearest",
            {time: fmt.time(near, true)}) : "")],
        [t("accounts.cp.which"), card ? (card.title ? card.title + " · " : "") + exp(card) : t("accounts.cp.upstreamPicks")],
        [t("accounts.cp.left"), left]]),
      hint ? h("div", "acc-note", h("b", null, t("accounts.cp.hint." + hint.kind)), " ",
        hint.kind === "notLow" ? t("accounts.cp.hintBody.notLow") : t("accounts.cp.hintBody.resetsSoon", {w: hint.label,
            d: fmt.dur(hint.secs)})) : null,
      warnP(t("accounts.irreversible"), t("accounts.cp.warn")),
      result,
    ];
    const cancel = UI.button(t("ui.cancel"));
    const ok = UI.button(t("accounts.cp.ok"), {kind: "danger-solid"});
    const attempt0 = {account: a.id, credit: card ? card.id : null, rid: null};
    consumeCur = attempt0;
    const layer = UI.modal({title: t("accounts.cp.title", {name: nm(a)}), body, foot: [cancel, ok], danger: true, focus: cancel,
      role: "alertdialog", onClose: () => {
        if (consumeCur && consumeCur.layer === layer) consumeCur = null;
      }});
    attempt0.layer = layer;
    cancel.addEventListener("click", () => layer.close());
    ok.addEventListener("click", () => {
      if (Date.now() - layer.at < 400 || ok.disabled) return;
      submitConsume(layer, ok, cancel, result, env);
    });
  }
  async function submitConsume(layer, ok, cancel, result, env) {
    const attempt = consumeCur;
    if (!attempt || attempt.layer !== layer) return;
    ok.disabled = true;
    result.textContent = t("accounts.cp.sending");
    try {
      const j = await Api.post("/admin/v1/accounts/" + encodeURIComponent(attempt.account) + "/reset-credits/consume",
          M.consumeBody(attempt));
      if (consumeCur !== attempt) return;
      result.textContent = t("accounts.cp.result", {code: codeLabel(j.code)}) + (j.code === "reset" ? t("accounts.cp.windows",
          {n: j.windows_reset}) +
        (j.cooldown_cleared ? t("accounts.toast.andCleared") : "") : "") + "\nredeem_request_id: " + j.redeem_request_id;
      ok.classList.add("hide");
      cancel.textContent = t("ui.close");
      cancel.focus();
      UI.toast(t("accounts.cp.toast", {code: codeLabel(j.code)}), {kind: "ok"});
      env.hist(true);
      env.reload().catch(e => env.ctx.fail(e));
    } catch (e) {
      if (consumeCur !== attempt || Api.isQuiet(e)) return;
      const f = M.consumeFailure(attempt, e);
      f.next.layer = layer;
      consumeCur = f.next;
      ok.disabled = false;
      if (f.retry === "pending") {
        result.textContent = t("accounts.cp.pending", {card: f.pending.credit_id ? t("accounts.cp.pendingCard", {id: f.pending.credit_id}) :
          t("accounts.cp.pendingAny")}) + "\nredeem_request_id: " + f.pending.redeem_request_id;
        ok.textContent = t("accounts.cp.retryLast");
        return;
      }
      result.textContent = t("accounts.cp.failed", {why: Api.why(e)}) + (f.retry === "same" ? "\n" + t("accounts.cp.unknown") : "");
      if (f.retry === "same") ok.textContent = t("accounts.cp.retrySame");
    }
  }

  // ---------- sections ----------
  function statusSection(a, env) {
    const s = env.store.state, q = s.quota[a.id], qq = q && q.quota, rc = s.credits[a.id];
    const kind = M.statusKind(a.status);
    const lines = h("div", "acc-kv");
    const tag = UI.tag(statusLabel(a.status), kind === "off" ? "" : kind, {dot: true});
    const main = [tag];
    if (a.status === "cooling" && a.cooling_reason) main.push(h("span", "c-warn small", reasonLabel(a.cooling_reason)));
    lines.appendChild(row(t("accounts.d.status"), ...main));
    if (a.status === "cooling" && a.cooling_until_ms) {
      const cd = h("span", {class: "num", "data-until": a.cooling_until_ms, "data-pre": t("accounts.d.leftPre"),
        title: fmt.time(a.cooling_until_ms)}, t("accounts.d.leftPre") + fmt.dur((a.cooling_until_ms - Date.now()) / 1000));
      const b = UI.button(t("accounts.act.clearCooldown"), {size: "sm", attrs: {"data-fk": a.id + "|cc", "data-act": "cooldown"}});
      b.addEventListener("click", () => Api.busyRun(b, t("accounts.act.clearCooldown"), () => clearCooldown(a, env), t("accounts.busy")));
      lines.appendChild(row(t("term.cooldown"), cd, b, UI.info(t("accounts.d.cooldownInfo"))));
      if (a.cooling_reason === "workspace_deactivated") lines.appendChild(row("", h("span", "small muted", t("accounts.cool.ws"))));
    } else if (a.status !== "normal") lines.appendChild(row("", h("span", "small muted", t("accounts.stDesc." + a.status))));
    if (M.limitHit(rc, qq)) lines.appendChild(row("", h("span", "small c-err", t("accounts.d.limitHit"))));
    if (s.qerr[a.id]) lines.appendChild(row("", h("span", "small c-err", t("accounts.d.quotaErr", {why: M.qerr(s.qerr[a.id])}))));
    const u = a.usage_24h;
    // Each figure is its own nowrap piece, so a narrow drawer wraps between them instead of overflowing.
    const use = u ? [t("accounts.u.req", {n: fmt.exact(u.requests)}), fmt.n(u.input_tokens + u.output_tokens) + " Token",
      Core.isNum(u.cost_usd) ? fmt.usd(u.cost_usd) : null].filter(Boolean) : [];
    lines.appendChild(row(t("accounts.col.h24"), u ? h("span", "acc-figs", use.map(x => h("span", "num", x)),
      u.errors ? h("span", "num c-err", t("accounts.u.errors", {n: u.errors})) : null) : h("span", "muted", s.data.usage_enabled ?
      t("accounts.u.none") : fmt.NONE), UI.info(Shell.scope.get().prewarm ? t("accounts.u.infoPrewarm") : t("accounts.u.info"))));
    const o = a.overload_24h;
    if (o && o.overloaded) lines.appendChild(row("overloaded", h("span", "num", o.overloaded + "/" + o.attempts),
        UI.info(t("accounts.d.overloadInfo"))));
    return sec(t("accounts.d.secStatus"), {id: "status"}, lines);
  }

  function windowBlock(lb, w, qq) {
    const r = M.rem(w), stale = M.staleQuota(qq), c = M.capacity(w);
    const m = Charts.meter({value: r === null ? null : r / 100, tone: "auto", tall: true, stale, text: r === null ? fmt.NONE :
        fmt.pct100(r, 0),
      ariaLabel: t("accounts.q.leftAria", {w: lb})});
    let capLine = null;
    if (c) {
      const txt = c.lower ? t("accounts.q.capLower", {n: fmt.n(c.lo)}) : t("accounts.q.capMid", {n: fmt.n(c.mid)}) +
        (c.lo !== null && c.hi !== null ? " (" + fmt.n(c.lo) + "–" + fmt.n(c.hi) + ")" : "");
      const api = c.apiUsd === null ? "" : " · " + (c.lower ? "≥ " : "≈ ") + fmt.usd(c.apiUsd);
      capLine = h("span", "small muted", txt + api, c.gap ? " · " + t("accounts.q.gap") : "");
    }
    return h("div", "acc-win", h("div", "acc-win-head", h("b", null, lb), h("span", "small muted num", M.resetText(w) || "")),
      m, capLine);
  }

  function quotaSection(a, env) {
    const s = env.store.state, q = s.quota[a.id], qq = q && q.quota, ws = M.wslots(qq);
    const kids = [];
    const rb = UI.button(t("accounts.act.refreshQuota"), {size: "sm", icon: "refresh", attrs: {"data-fk": a.id + "|rq", "data-act": "quota",
      title: t("accounts.act.refreshQuotaTip")}});
    rb.addEventListener("click", () => Api.busyRun(rb, t("accounts.act.refreshQuota"), () => refreshQuota(a, env),
        t("accounts.busyQuery")));
    if (!qq) kids.push(h("p", "small muted", s.reads.quota.note ? t("accounts.q.noRead") : t("accounts.q.none")));
    else {
      const grid = h("div", "acc-wins");
      for (const x of [ws.s, ws.l]) if (x) grid.appendChild(windowBlock(x[0], x[1], qq));
      if (!grid.children.length) grid.appendChild(h("p", "small muted", t("accounts.q.none")));
      kids.push(grid);
      const kv = h("div", "acc-kv");
      const cr = qq.credits, ct = M.crText(cr);
      if (ct) {
        kv.appendChild(row(t("term.credits"), h("span", "num" + (ct.none ? " muted" : ""), cr.unlimited ?
            t("accounts.cr.unlimitedShort") : cr.has_credits ?
          M.crBal(cr.balance) : t("accounts.cr.noneShort")),
        cr.overage_limit_reached ? h("span", "small c-err", t("accounts.cr.overLimit")) : cr.spend_control_reached ?
          h("span", "small c-err", t("accounts.cr.spendLimit")) : null,
        cr.has_credits && !cr.unlimited ? UI.info(t("accounts.cr.info")) : null));
      }
      const est = [ws.s, ws.l].map(x => x && x[1].estimate).find(e => e && Core.isNum(e.api_cost_usd));
      if (est) kv.appendChild(row(t("accounts.q.price"), h("span", "num", fmt.usd(est.api_cost_usd)), UI.info(t("accounts.q.priceInfo"))));
      const po = [ws.s, ws.l].map(x => x && x[1].estimate).find(e => e && Number(e.over_credits) > 0);
      if (po) kv.appendChild(row(t("accounts.q.cyclePts"), h("span", "num acc-pts-text",
        fmt.n(po.over_credits) + (Core.isNum(po.over_api_usd) ?
        " · " + fmt.usd(po.over_api_usd) : ""))));
      if (qq.observed_at_ms) kv.appendChild(row(t("accounts.q.observed"), h("span", {class: "small" + (M.staleQuota(qq) ? " c-warn" :
          " muted"),
        title: fmt.time(qq.observed_at_ms)}, fmt.ago(qq.observed_at_ms) + (qq.observed_by ? " · " + qq.observed_by : ""))));
      if (kv.children.length) kids.push(kv);
    }
    return sec(t("term.quota"), {id: "quota", tools: rb, info: t("accounts.q.info")}, ...kids);
  }

  function historySection(a, env) {
    const s = env.store.state, q = s.quota[a.id], qq = q && q.quota, ws = AccHistory.wins(qq), data = env.store.histOf(a.id);
    return h("section", {class: "acc-sec", "data-sec": "history"}, AccHistory.panel(data, ws, {key: a.id + "|d", account: a,
      onReset: (w, list) => estimateReset(a, data, ws, w, list, env), onUndo: (ep, b) => undoEstimate(a, ep, b, env)}));
  }

  function switchRow(label, checked, o) {
    const sw = h("input", {type: "checkbox", class: "switch", "aria-label": o.aria || label, "data-fk": o.fk});
    sw.checked = !!checked;
    if (o.disabled) sw.disabled = true;
    sw.addEventListener("change", () => o.onChange(sw));
    return h("label", "acc-switch", sw, h("span", "grow", label, o.sub ? h("span", "acc-switch-sub", o.sub) : null), o.info ?
        UI.info(o.info) : null);
  }

  function autoSwitch(a, env, kind) {
    const s = env.store.state, top = s.autoTop, W = env.W();
    const auto = kind === "auto";
    const au = s.auto[a.id], on = auto ? !!(au && au.on) : !!s.quotaAuto[a.id];
    if (!top || !top.ok) return null;
    if (auto ? !au : s.quotaAuto[a.id] === undefined) return null;
    const label = auto ? t("accounts.sw.autoCard") : t("accounts.sw.autoQuota");
    const info = auto ? t("accounts.sw.autoCardInfo", {n: top.lead}) : top.qmins ?
      t("accounts.sw.autoQuotaInfo", {n: top.qmax > top.qmins ? top.qmins + "–" + top.qmax : top.qmins}) : t("accounts.sw.autoQuotaOff");
    const sub = auto && on && au.next ? t("accounts.sw.next", {time: fmt.time(au.next, true)}) : null;
    if (!W) return on ? row(label, h("span", "small", t("accounts.sw.on")), sub ? h("span", "small muted", sub) : null, UI.info(info)) :
        null;
    return switchRow(label, on, {fk: a.id + "|" + kind, aria: label + ": " + nm(a), sub, info, onChange: async sw => {
      const want = sw.checked;
      sw.disabled = true;
      try {
        const enabled = await env.store.setSwitch(a, auto ? "auto" : "quota", want);
        if (enabled === undefined) return;
        sw.checked = enabled;
        UI.toast(t(enabled ? "accounts.toast.swOn" : "accounts.toast.swOff", {name: nm(a), what: label}), {kind: "ok"});
      } catch (e) {
        sw.checked = !want;
        if (!Api.isQuiet(e)) UI.toast(t("api.failed", {name: label, why: Api.why(e)}), {kind: "err"});
      } finally {
        if (sw.isConnected) sw.disabled = false;
      }
    }});
  }

  function scheduleSection(a, env) {
    const W = env.W(), box = h("div", "acc-sched");
    if (W && a.status !== "token_invalid") {
      box.appendChild(switchRow(t("accounts.sw.enabled"), !a.disabled, {fk: a.id + "|tg", aria: t("accounts.sw.enabled") + ": " + nm(a),
        sub: a.disabled ? t("accounts.sw.enabledOff") : null, onChange: sw => setEnabled(a, sw.checked, env)}));
    } else if (a.status !== "token_invalid") box.appendChild(row(t("accounts.sw.enabled"), h("span", "small", a.disabled ?
        t("accounts.sw.off") : t("accounts.sw.on"))));
    if (W) {
      const d = env.drafts[a.id] || {};
      const num = (v, label, fk) => h("input", {type: "number", step: "1", class: "input num acc-num", value: v, "aria-label": label +
          ": " + nm(a),
        "data-fk": fk, inputmode: "numeric"});
      const wi = num(d.w !== undefined ? d.w : a.weight, t("accounts.d.weight"), a.id + "|w");
      const pi = num(d.p !== undefined ? d.p : a.priority, t("accounts.d.priority"), a.id + "|p");
      wi.min = "0";
      const save = UI.button(t("ui.save"), {size: "sm", attrs: {"data-fk": a.id + "|s"}});
      const dirty = () => {
        const ch = wi.value !== String(a.weight) || pi.value !== String(a.priority);
        save.disabled = !ch;
        if (ch) env.drafts[a.id] = {w: wi.value, p: pi.value};
        else delete env.drafts[a.id];
      };
      dirty();
      for (const i of [wi, pi]) {
        i.addEventListener("input", dirty);
        i.addEventListener("keydown", e => {
          if (e.key === "Enter" && !save.disabled) save.click();
        });
      }
      save.addEventListener("click", () => Api.busyRun(save, t("ui.save"), async () => {
        const w = Number(wi.value), pr = Number(pi.value);
        if (!Number.isInteger(w) || !Number.isInteger(pr) || wi.value === "" || pi.value === "") throw new Error(t("accounts.d.intOnly"));
        await saveSchedule(a, w, pr, env);
      }, t("accounts.busySave")));
      box.appendChild(h("div", "acc-wp",
        h("label", "acc-wp-f", h("span", null, t("accounts.d.weight"), UI.info(t("accounts.d.weightInfo"))), wi),
        h("label", "acc-wp-f", h("span", null, t("accounts.d.priority"), UI.info(t("accounts.d.priorityInfo"))), pi), save));
    } else {
      box.appendChild(row(t("accounts.d.weight"), h("span", "num", String(a.weight))));
      box.appendChild(row(t("accounts.d.priority"), h("span", "num", String(a.priority))));
    }
    const sw1 = autoSwitch(a, env, "auto"), sw2 = autoSwitch(a, env, "quota");
    if (sw1) box.appendChild(sw1);
    if (sw2) box.appendChild(sw2);
    return sec(t("accounts.d.secSched"), {id: "sched", info: W ? t("accounts.d.schedInfo") : t("accounts.d.schedRo")}, box);
  }

  const SHOW_USED = "ciallopa_rc_used";
  function cardsSection(a, env) {
    const s = env.store.state, r = s.credits[a.id], can = s.cardTop.can, adm = s.cardTop.adm, box = h("div", "acc-cards");
    let tools = null;
    if (adm) {
      tools = UI.button(t("accounts.rc.refresh"), {size: "sm", attrs: {"data-fk": a.id + "|rcr", title: t("accounts.rc.refreshTip")}});
      tools.addEventListener("click", () => Api.busyRun(tools, t("accounts.rc.refresh"), async () => {
        await env.ctx.post("/usage/v1/reset-credits/refresh", {account: a.id}, T);
        await env.reload();
        UI.toast(t("accounts.toast.cardsRefreshed", {name: nm(a)}), {kind: "ok"});
      }, t("accounts.busy")));
    }
    if (!r || r.available_count === null || r.available_count === undefined) {
      box.appendChild(h("p", "small muted", r && r.error ? t("accounts.rc.failed", {why: r.error}) : t("accounts.rc.notYet")));
    } else {
      const now = Date.now(), near = M.nearest(r), soon = near !== null && near - now < M.soonMs;
      const {available, used} = M.cardLists(r, now);
      box.appendChild(h("div", "acc-cards-sum", h("span", "acc-big num", String(r.available_count)), h("span", "small muted",
          t("accounts.rc.avail")),
        near !== null ? h("span", "small " + (soon ? "c-warn" : "muted"), t("accounts.rc.nearest", {time: fmt.time(near, true)})) :
            null));
      if (r.error) box.appendChild(h("p", "small c-warn", t("accounts.rc.stale", {why: r.error})));
      const showUsed = Core.local.get(SHOW_USED) === "1";
      const ul = h("ul", {class: "acc-clist", "aria-label": t("accounts.rc.list")});
      for (const c of available.concat(showUsed ? used : [])) {
        const x = M.expOf(c), isUsed = !available.includes(c), isNear = !isUsed && x === near, cs = isNear && x - now < M.soonMs;
        let btn = null;
        if (!isUsed && can) {
          btn = UI.button(t("accounts.rc.use"), {size: "sm", attrs: {"data-fk": a.id + "|use" + c.id, "aria-haspopup": "dialog",
            "aria-label": t("accounts.rc.useAria", {when: x === Infinity ? t("accounts.rc.never") : fmt.time(x, true)})}});
          btn.addEventListener("click", () => openConsume(a, c, env));
        }
        ul.appendChild(h("li", {class: (isNear ? "near" : "") + (cs ? " soon" : "") + (isUsed ? " used" : ""), title: c.description ||
            null},
          h("div", "grow", h("div", "num small", x === Infinity ? t("accounts.rc.never") : t("accounts.rc.expires", {time: fmt.time(x,
              true)})),
            h("div", "acc-clist-sub", (c.title || t("term.resetCard")) + (!isUsed && x !== Infinity ? " · " + t("accounts.rc.in",
                {d: fmt.dur((x - now) / 1000)}) : "") +
              (isNear ? " · " + t("accounts.rc.nearestTag") : ""))),
          isUsed ? UI.tag(I18n.has("accounts.rcs." + c.status) ? t("accounts.rcs." + c.status) : c.status || fmt.NONE) : btn));
      }
      if (ul.children.length) box.appendChild(ul);
      const foot = h("div", "acc-row-btns");
      if (used.length) {
        const ub = UI.button(showUsed ? t("accounts.rc.hideUsed") : t("accounts.rc.showUsed", {n: used.length}),
          {size: "sm", kind: "ghost", attrs: {"aria-pressed": String(showUsed), "data-fk": a.id + "|ru"}});
        ub.addEventListener("click", () => {
          Core.local.set(SHOW_USED, showUsed ? "0" : "1");
          env.renderAccount(a.id);
        });
        foot.appendChild(ub);
      }
      if (can && r.available_count > 0) {
        const any = UI.button(t("accounts.rc.useAny"), {size: "sm", attrs: {"data-fk": a.id + "|ua", "aria-haspopup": "dialog",
            title: t("accounts.rc.useAnyTip")}});
        any.addEventListener("click", () => openConsume(a, null, env));
        foot.appendChild(any);
      }
      if (foot.children.length) box.appendChild(foot);
    }
    const last = (s.cardTop.attempts || []).find(x => x.account === a.id);
    if (last) {
      box.appendChild(h("p", {class: "small muted", title: fmt.iso(last.started_ms) + " · redeem_request_id " + last.redeem_request_id},
        t("accounts.rc.last", {time: fmt.time(last.started_ms, true), what: last.code ? codeLabel(last.code) : last.status})));
    }
    if (!can) box.appendChild(h("p", "small muted", adm ? t("accounts.rc.noConsume") : t("accounts.rc.readOnly")));
    return sec(t("term.resetCard"), {id: "cards", tools, info: t("accounts.rc.info")}, box);
  }

  function maintSection(a, env) {
    if (!env.W()) return null;
    const box = h("div", "acc-row-btns");
    const tb = UI.button(t("accounts.act.refreshToken"), {size: "sm", attrs: {"aria-haspopup": "dialog", "data-fk": a.id + "|rt",
      title: t("accounts.act.refreshTokenTip")}});
    tb.addEventListener("click", () => refreshToken(a, env));
    const db = UI.button(t("accounts.act.delete"), {size: "sm", kind: "danger", icon: "trash", attrs: {"aria-haspopup": "dialog",
      "data-fk": a.id + "|del"}});
    db.addEventListener("click", () => deleteAccount(a, env));
    box.append(tb, db);
    return sec(t("accounts.d.secMaint"), {id: "maint"}, box);
  }

  // The drawer body. Sections are rebuilt on every snapshot except one holding a focused input
  // (an edit in progress survives the auto refresh).
  function render(body, a, env) {
    const build = {
      status: () => statusSection(a, env),
      quota: () => quotaSection(a, env),
      history: () => historySection(a, env),
      sched: () => scheduleSection(a, env),
      cards: () => cardsSection(a, env),
      maint: () => maintSection(a, env),
    };
    const order = ["status", "quota", "history", "cards", "sched", "maint"];
    const head = h("div", "acc-d-sub", h("span", "mono small", a.id), a.plan ? UI.tag(a.plan, "outline") : null);
    const active = document.activeElement;
    const keep = {};
    for (const n of Array.from(body.querySelectorAll(":scope > .acc-sec[data-sec]"))) {
      if (active && n.contains(active) && active.matches("input[type=number]")) keep[n.dataset.sec] = n;
    }
    const y = body.scrollTop;
    Core.steady([body], () => {
      const nodes = [head];
      for (const id of order) {
        const n = keep[id] || build[id]();
        if (n) nodes.push(n);
      }
      for (const n of Array.from(body.children)) if (!nodes.includes(n)) n.remove();
      for (const n of nodes) body.appendChild(n);
    });
    body.scrollTop = y;
  }

  // Scroll to a section and flash it (deep links from other views: #acc=<id>&act=quota).
  function highlight(body, act) {
    const map = {quota: "[data-act=quota]", cooldown: "[data-act=cooldown]", cards: "[data-sec=cards]", sched: "[data-sec=sched]"};
    const el = map[act] && body.querySelector(map[act]);
    if (!el) return;
    el.scrollIntoView({block: "center"});
    el.classList.add("acc-flash");
    setTimeout(() => el.classList.remove("acc-flash"), 2400);
  }

  return {render, highlight, refreshQuota, clearCooldown, setEnabled, deleteAccount, statusLabel, reasonLabel, dialog, openConsume,
    submitConsume, current: () => consumeCur, resetConsume: () => {
      consumeCur = null;
    }};
})();
