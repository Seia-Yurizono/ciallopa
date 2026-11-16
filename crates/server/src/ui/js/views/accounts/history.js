"use strict";
// AccHistory: the 30-day quota history of an account — the row sparkline (capacity per cycle as a
// step line, a hollow dot per reset) and the cycle panel (one bullet bar per cycle on one shared
// credits scale). Capacity is an estimate: solid block = estimated capacity, hatch = estimate range,
// faded end = only a lower bound, dashed outline = still estimating. 点数 (purchased credits) is never
// drawn on the quota bar; it gets its own chip. Charts draw at the measured pixel width (viewBox ==
// px box), geometry goes into SVG attributes or CSSOM, never a style attribute (CSP).
const AccHistory = (() => {
  const {h, svg} = Core;
  const t = (k, v) => I18n.t(k, v);
  const fmt = Core.fmt;

  const KINDS = ["natural", "manual_card", "auto_card", "external_card", "early_reset", "limit_change", "open"];
  const kindLabel = k => (KINDS.includes(k) ? t("accounts.qh.kind." + k) : k);
  const kindCls = k => ({natural: "k-nat", early_reset: "k-early", manual_card: "k-card", external_card: "k-card", auto_card: "k-auto",
    limit_change: "k-lim", open: "k-open"}[k] || "k-nat");

  // History rows only say which slot (primary / secondary) a cycle came from, and upstream fills the
  // slots by position: a pro or free plan's only window arrives as primary. So label and order come
  // from the account's current window_minutes, never from the slot. No quota yet: the classic layout
  // (primary 5h, secondary week). A slot the account no longer has is "other window".
  function wins(qq) {
    if (!qq) return null;
    const ws = AccModel.wslots(qq), o = {known: true, primary: null, secondary: null};
    for (const k of ["s", "l"]) {
      const x = ws[k];
      if (x) o[x[1] === qq.primary ? "primary" : "secondary"] = {l: x[0], s: k};
    }
    return o;
  }
  function winLabel(w, ws) {
    if (ws && ws[w]) return ws[w].l;
    if (ws && ws.known) return t("accounts.w.other");
    return w === "secondary" ? t("accounts.w.week") : t("accounts.w.5h");
  }
  function order(ws) {
    const r = w => (ws && ws[w] ? (ws[w].s === "s" ? 0 : 1) : ws ? 2 : w === "primary" ? 0 : 1);
    return ["primary", "secondary"].sort((a, b) => r(a) - r(b));
  }
  const rows = (cycles, w) => (cycles || []).filter(c => c.window === w).slice().sort((a, b) => (a.ended_ms || 0) - (b.ended_ms || 0));
  // Windows to show, short first: the ones the account has now, or (no quota yet) slots with history.
  const winList = (ws, cycles) => order(ws).filter(w => (ws ? !!ws[w] : rows(cycles, w).length > 0));
  // The main window is the long one (week on plus and pro).
  function mainWin(ws, cycles) {
    const l = winList(ws, cycles);
    return l.length ? l[l.length - 1] : "secondary";
  }
  const winKind = (w, ws) => (ws && ws[w] ? ws[w].s : w === "primary" ? "s" : "l");

  const pos = (at, from, to) => Core.clamp((Number(at) - Number(from)) / Math.max(1, Number(to) - Number(from)), 0, 1);
  const cap = c => (c.capacity_credits !== null && c.capacity_credits !== undefined ? c.capacity_credits : c.capacity_lo);
  const used = c => (c.credits_used !== null && c.credits_used !== undefined ? Number(c.credits_used) : null);
  const finite = v => v !== null && v !== undefined && isFinite(v);
  function median(vals) {
    const a = vals.filter(finite).slice().sort((x, y) => x - y);
    if (!a.length) return null;
    const m = Math.floor(a.length / 2);
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  }
  function niceScale(max, tickCount) {
    tickCount = tickCount || 4;
    if (!(max > 0)) return {max: 1, ticks: [0, 1], step: 1};
    const rough = max / (tickCount - 1), exp = Math.pow(10, Math.floor(Math.log10(rough))), f = rough / exp;
    const step = (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * exp, niceMax = Math.ceil(max / step - 1e-9) * step, ticks = [];
    for (let v = 0; v <= niceMax + 1e-9; v += step) ticks.push(+v.toFixed(6));
    return {max: niceMax, ticks, step};
  }
  function quantile(a, p) {
    const i = (a.length - 1) * p, lo = Math.floor(i), hi = Math.ceil(i);
    return a[lo] + (a[hi] - a[lo]) * (i - lo);
  }
  // Tukey upper fence on the values; the IQR is floored at 20% of the median so a flat series does
  // not flag ordinary noise. Fewer than 4 values: nothing counts as an outlier.
  function fence(vals) {
    const a = vals.filter(finite).map(Number).sort((x, y) => x - y);
    if (a.length < 4) return Infinity;
    const q1 = quantile(a, 0.25), q3 = quantile(a, 0.75);
    return q3 + 1.5 * Math.max(q3 - q1, 0.2 * Math.abs(quantile(a, 0.5)));
  }
  // y-domain 0 → robust top (inliers + 10% headroom): a far-off cycle no longer flattens the rest;
  // values above are pinned to the top edge with a ▲. Credits are ratio data, so the axis keeps 0.
  function domain(list) {
    const fc = fence(list.map(cap));
    let top = 0, all = 0;
    for (const c of list) {
      const v = cap(c);
      if (!finite(v)) continue;
      all = Math.max(all, v);
      if (v <= fc) top = Math.max(top, v);
    }
    if (!(top > 0)) top = all;
    return Object.assign(niceScale(top * 1.1, 2), {fence: fc});
  }
  const tick = v => {
    v = Number(v);
    return v >= 1000 ? String(+(v / 1000).toFixed(1)) + "k" : v >= 10 ? String(Math.round(v)) : String(+v.toFixed(2));
  };
  const f1 = v => Number(v).toFixed(1);
  // A cycle covers [started_ms, ended_ms]; the open cycle ends "now".
  function span(c, to) {
    const b = c.ended_ms !== null && c.ended_ms !== undefined ? c.ended_ms : to;
    return [c.started_ms !== null && c.started_ms !== undefined ? c.started_ms : b, b];
  }
  // The reset event at a cycle's end (same window, within 10 minutes).
  function eventAt(ev, c) {
    let best = null, bd = 6e5;
    for (const e of ev || []) {
      if (e.window !== c.window || c.ended_ms === null || c.ended_ms === undefined) continue;
      const d = Math.abs(e.at_ms - c.ended_ms);
      if (d <= bd) {
        bd = d;
        best = e;
      }
    }
    return best;
  }
  // Touching cycles → solid riser; a gap of 3 px or more (nothing observed between) → dashed bridge.
  const link = (prevX, xa) => (xa - prevX >= 3 ? "gap" : "join");
  const rowKind = (c, ev) => {
    if (c.open) return "open";
    const e = eventAt(ev, c);
    return (e && e.kind) || c.close_kind || "natural";
  };
  const endOf = c => (c.open ? (c.cycle_reset_at ? c.cycle_reset_at * 1000 : null) : c.ended_ms);
  // Latest reset of this window that can still be undone.
  function epoch(data, w, now) {
    now = now === undefined ? Date.now() : now;
    return (data.epochs || []).filter(e => e.active && e.window === w && e.undo_until_ms > now)
      .sort((a, b) => b.created_ms - a.created_ms)[0] || null;
  }

  // ---------- drawing at the measured width ----------
  let RO = null;
  function fit(cls, label, draw) {
    const d = h("div", {class: "acc-fit " + cls, role: "img", "aria-label": label});
    d._draw = draw;
    d._w = 0;
    if (typeof ResizeObserver === "function") {
      if (!RO) RO = new ResizeObserver(es => es.forEach(e => redraw(e.target)));
      RO.observe(d);
    }
    return d;
  }
  function redraw(d, force) {
    if (!d.isConnected) {
      if (RO) RO.unobserve(d);
      return;
    }
    const w = Math.floor(d.clientWidth);
    if (w < 40 || (w === d._w && !force)) return;
    d._w = w;
    Core.fill(d, d._draw(w));
  }

  // Hollow reset dots. A dense history (5h) gets smaller dots at least 16 px apart.
  function dots(s, pts, r, pw) {
    const dense = pw && pts.length * 12 > pw, gap = dense ? 16 : 8;
    if (dense) r = Math.min(r, 2.2);
    let lx = -1e9;
    for (const p of pts) {
      if (p.x - lx < gap) continue;
      lx = p.x;
      s.appendChild(svg("circle", {class: "qh-pt", cx: f1(p.x), cy: f1(p.y), r}));
    }
  }

  // Row sparkline: the main window only, step line + a hollow dot per closed cycle.
  function miniSvg(data, W, ws) {
    const w = mainWin(ws, data.cycles), list = rows(data.cycles, w), from = data.from_ms, to = data.to_ms;
    const H = 28, hh = 20, y0 = 4, L = 2, R = 4;
    const X = at => L + pos(at, from, to) * (W - L - R);
    const s = svg("svg", {class: "qh-svg", viewBox: "0 0 " + W + " " + H, width: W, height: H, "aria-hidden": "true"});
    const max = domain(list).max, Y = v => y0 + hh - (Math.min(v, max) / (max || 1)) * hh;
    s.appendChild(svg("line", {class: "qh-lane", x1: L, x2: W - R, y1: y0 + hh + 0.5, y2: y0 + hh + 0.5}));
    let d = "", gd = "", prev = null;
    const pts = [];
    for (const c of list) {
      const [a, b] = span(c, to), xa = X(a), xb = Math.max(X(b), xa + 1), v = cap(c);
      if (!finite(v)) continue;
      const y = f1(Y(v)), k = prev ? link(prev.x, xa) : null;
      if (k === "gap") gd += "M" + f1(prev.x) + "," + prev.y + "L" + f1(xa) + "," + y;
      d += (k === "join" ? "L" : "M") + f1(xa) + "," + y + "L" + f1(xb) + "," + y;
      prev = {x: xb, y};
      if (!c.open) pts.push({x: xb, y: Y(v)});
      if (v > max) s.appendChild(svg("polygon", {class: "qh-over", points: f1(xb - 3) + "," + f1(y0 + 3) + " " + f1(xb + 3) + "," +
        f1(y0 + 3) + " " + f1(xb) + "," + f1(y0 - 2)}));
    }
    if (gd) s.appendChild(svg("path", {class: "qh-step gap", d: gd}));
    if (d) s.appendChild(svg("path", {class: "qh-step", d}));
    dots(s, pts, 2.4, W);
    return s;
  }

  // Row cell content: skeleton / error / no record / sparkline.
  function mini(data, ws, name) {
    if (!data) return h("span", {class: "skel acc-mini-skel", "aria-hidden": "true"});
    if (data.error) return h("span", {class: "muted small", title: errText(data.error)}, fmt.NONE);
    const w = mainWin(ws, data.cycles);
    if (!rows(data.cycles, w).some(c => finite(cap(c)))) return h("span", "muted small", t("accounts.qh.noRecord"));
    return fit("acc-mini", t("accounts.qh.miniLabel", {name, w: winLabel(w, ws)}), W => miniSvg(data, W, ws));
  }

  const errText = e => (e.kind === "off" ? t("accounts.qh.off") : t("accounts.qh.failed", {why: e.why}));

  // ---------- cycle rows ----------
  function scale(list) {
    const fc = fence(list.map(cap));
    let top = 0, all = 0;
    for (const c of list) {
      for (const v of [cap(c), c.capacity_hi, used(c) || 0]) {
        if (!finite(v)) continue;
        all = Math.max(all, Number(v));
        if (v <= fc * 1.5) top = Math.max(top, Number(v));
      }
    }
    return niceScale((top || all || 1) * 1.05, 5);
  }

  function bar(c, S) {
    const cp = cap(c), u = used(c), est = c.open && !finite(cp), X = v => Core.clamp(v / S.max * 100, 0, 100);
    const el = h("div", {class: "cy-bar" + (est ? " est" : ""), "aria-hidden": "true"});
    const put = (cls, left, width) => el.appendChild(h("span", {class: cls, css: Object.assign(left === null ? {} : {left: left + "%"},
      {width: width + "%"})}));
    // No estimate yet: dashed outline at the median of earlier cycles, or the whole track without one.
    const capW = finite(cp) ? X(cp) : est ? (S.med !== null ? X(S.med) : 100) : null;
    if (capW !== null) put(!est && !finite(c.capacity_credits) ? "cap lo" : "cap", null, capW);
    if (!est && finite(c.capacity_lo) && finite(c.capacity_hi) && c.capacity_hi > c.capacity_lo) {
      put("rng", X(c.capacity_lo), X(c.capacity_hi) - X(c.capacity_lo));
    }
    if (u) put("used", null, Math.max(0.6, X(u)));
    return el;
  }
  function ptsChip(c) {
    const o = Number(c.over_credits) || 0;
    if (!(o > 0)) return null;
    return h("span", "cy-pts", t("accounts.qh.ptsChip", {n: fmt.n(o)}) + (finite(c.over_api_usd) ? " · " + fmt.usd(c.over_api_usd) : ""));
  }
  function nums(c) {
    const cp = cap(c), u = used(c), est = c.open && !finite(cp);
    const capText = est ? t("accounts.qh.estimating") : !finite(cp) ? fmt.NONE : (!finite(c.capacity_credits) ? "≥" : "") + fmt.n(cp);
    return h("div", "cy-nums", h("span", "cy-used", u !== null ? fmt.n(u) : fmt.NONE), h("span", "cy-cap", " / " + capText),
      h("div", "cy-usd", fmt.usd(c.api_cost_usd)));
  }
  function conf(c) {
    if (c.coverage_gap) return t("accounts.qh.conf.gap");
    return ["ok", "fallback", "lower_only", "insufficient"].includes(c.confidence) ? t("accounts.qh.conf." + c.confidence) : fmt.NONE;
  }
  function detail(c, k, ev) {
    const it = (l, v, cls) => (v === null || v === undefined || v === "" ? null : h("div", cls || null, h("span", null, l), h("b", null,
        v)));
    const end = endOf(c), e = c.open ? null : eventAt(ev, c), over = Number(c.over_credits) > 0;
    const capText = finite(c.capacity_credits) ? t("accounts.qh.credits", {n: fmt.n(c.capacity_credits)}) :
      finite(c.capacity_lo) ? t("accounts.qh.atLeast", {n: fmt.n(c.capacity_lo)}) : c.open ? t("accounts.qh.estimatingLong") :
        t("accounts.qh.noEstimate");
    const obs = [finite(c.delta_pct) ? "Δ " + Math.round(c.delta_pct) + "%" : null,
      finite(c.snapshots) ? t("accounts.qh.snapshots", {n: c.snapshots}) : null,
      finite(c.requests) ? t("accounts.qh.requests", {n: c.requests}) : null].filter(Boolean).join(" · ");
    const card = e && e.card_source === "auto" ? " · " + t("accounts.qh.cardAuto") : e && e.card_source === "manual" ?
      " · " + t("accounts.qh.cardManual") : "";
    const move = e && finite(e.prev_pct) && finite(e.next_pct) ? " · " + Number(e.prev_pct).toFixed(0) + "% → " +
        Number(e.next_pct).toFixed(0) + "%" : "";
    return h("div", "cy-det",
      over ? h("div", "grp", t("term.quota")) : null,
      it(t("accounts.qh.d.cap"), capText),
      it(t("accounts.qh.d.range"), finite(c.capacity_lo) && finite(c.capacity_hi) ? fmt.n(c.capacity_lo) + "–" + fmt.n(c.capacity_hi) :
          null),
      it(t("accounts.qh.d.used"), finite(c.credits_used) ? fmt.n(c.credits_used) + (finite(c.delta_pct) ? " · " +
          Math.round(c.delta_pct) + "%" : "") : null),
      it(over ? t("accounts.qh.d.quotaPrice") : t("accounts.qh.d.price"), fmt.usd(c.api_cost_usd)),
      over ? h("div", "grp pts", t("accounts.qh.d.ptsGroup")) : null,
      over ? it(t("term.credits"), fmt.n(c.over_credits)) : null,
      over ? it(t("accounts.qh.d.ptsPrice"), fmt.usd(c.over_api_usd)) : null,
      over ? h("div", "grp", t("accounts.qh.d.cycleGroup")) : null,
      it(t("accounts.qh.d.cycle"), fmt.time(c.started_ms, true) + " → " + (end ? fmt.time(end, true) : "…")),
      it(t("accounts.qh.d.obs"), obs || null),
      it(t("accounts.qh.d.conf"), conf(c)),
      it(t("accounts.qh.d.end"), (c.open ? t("accounts.qh.kind.open") : kindLabel(k)) + card + move),
      c.trend === "up" || c.trend === "down" ? it(t("accounts.qh.d.trend"), t("accounts.qh.trend." + c.trend)) : null,
      c.archived ? it(t("accounts.qh.d.archived"), t("accounts.qh.archivedNote"), "wide") : null);
  }

  // View-lifetime UI state: expanded cycles, "show all" lists, the 周 | 5h choice (session).
  const OPEN = new Set(), MORE = new Set();
  const WIN_STORE = "ciallopa_qh_win";
  let winPick = null;
  const pick = () => winPick || Core.session.get(WIN_STORE) || "l";
  function clearUi() {
    OPEN.clear();
    MORE.clear();
  }

  function cycleRows(list, ev, key) {
    const box = h("div", "cy-list");
    if (!list.length) {
      box.appendChild(h("p", "muted small", t("accounts.qh.noCycles")));
      return box;
    }
    const S = scale(list);
    S.med = median(list.filter(c => !c.open && !c.archived && finite(c.capacity_credits)).map(c => c.capacity_credits));
    const newest = list.slice().reverse(), lim = MORE.has(key) ? newest.length : 12;
    for (const c of newest.slice(0, lim)) {
      const k = rowKind(c, ev), id = key + "|" + c.window + "|" + c.started_ms, open = OPEN.has(id), end = endOf(c);
      const meta = h("span", "meta", h("span", "cy-kind " + kindCls(k), c.open ? t("accounts.qh.kind.open") : kindLabel(k)),
        c.archived ? h("span", "cy-kind k-arch", t("accounts.qh.archived")) : null);
      const r = h("div", {class: "cy" + (c.archived ? " arch" : ""), role: "button", tabindex: "0", "aria-expanded": String(open),
          "data-fk": id},
        h("div", "when", h("span", "num", fmt.day(c.started_ms) + " → " + (end ? fmt.day(end) : "…")), meta),
        h("div", "cy-bw", bar(c, S), ptsChip(c)), nums(c));
      const flip = () => {
        if (OPEN.has(id)) OPEN.delete(id);
        else OPEN.add(id);
        const now = OPEN.has(id);
        r.setAttribute("aria-expanded", String(now));
        if (now) r.after(detail(c, k, ev));
        else if (r.nextElementSibling && r.nextElementSibling.classList.contains("cy-det")) r.nextElementSibling.remove();
      };
      r.addEventListener("click", flip);
      r.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          flip();
        }
      });
      box.appendChild(r);
      if (open) box.appendChild(detail(c, k, ev));
    }
    if (newest.length > lim) {
      const more = UI.button(t("accounts.qh.more", {n: newest.length - lim}), {size: "sm", kind: "ghost", className: "cy-more",
        attrs: {"data-fk": key + "|more"}});
      more.addEventListener("click", () => {
        MORE.add(key);
        box.replaceWith(cycleRows(list, ev, key));
      });
      box.appendChild(more);
    }
    const ax = h("div", "sc");
    for (const v of S.ticks) ax.appendChild(h("span", {css: {left: (v / S.max) * 100 + "%"}}, tick(v)));
    box.appendChild(h("div", {class: "cy-axis", "aria-hidden": "true"}, h("div"), ax, h("div")));
    return box;
  }

  function legend() {
    const it = (cls, label) => h("span", null, h("i", cls), label);
    return h("div", "cy-legend", it("l-cap", t("accounts.qh.lg.cap")), it("l-rng", t("accounts.qh.lg.range")),
      it("l-lo", t("accounts.qh.lg.lower")), it("l-used", t("accounts.qh.lg.used")), it("l-est", t("accounts.qh.lg.est")),
      it("l-pts", t("accounts.qh.lg.pts")));
  }

  // panel(data, ws, {key, account, onReset(win, list), onUndo(epoch, button)})
  // Title row: "Quota cycles · 30 days", the 周 | 5h toggle (both windows only), "Reset estimate…"
  // (usage log only: also with the config read-only), undo banner, rows, legend.
  function panel(data, ws, o) {
    o = o || {};
    const key = o.key || "";
    if (!data) return h("div", "qh-box", UI.skeleton.lines(3));
    if (data.error) return h("div", "qh-box", h("p", "small c-err", errText(data.error)));
    const list = winList(ws, data.cycles);
    let cur = list.find(w => winKind(w, ws) === pick()) || mainWin(ws, data.cycles);
    const box = h("div", "qh-box"), epSlot = h("div"), slot = h("div");
    const title = h("h3", "acc-sec-title", t("accounts.qh.title") + (list.length > 1 ? "" : " · " + winLabel(cur, ws)),
      UI.info(() => h("div", "stack", h("p", null, t("accounts.qh.info")), legend())));
    const head = h("div", "qh-head", title);
    const paint = () => {
      Core.clear(slot);
      Core.clear(epSlot);
      const e = o.account ? epoch(data, cur) : null;
      if (e) {
        const b = UI.button(t("ui.undo"), {size: "sm", attrs: {"data-fk": key + "|eu"}});
        b.addEventListener("click", () => o.onUndo && o.onUndo(e, b));
        epSlot.appendChild(h("div", {class: "qh-ep", role: "status"}, h("span", null, t("accounts.qh.epoch", {time: fmt.time(e.from_ms,
            true)})), b,
          h("span", "muted", t("accounts.qh.undoUntil", {time: fmt.time(e.undo_until_ms, true)}))));
      }
      slot.appendChild(cycleRows(rows(data.cycles, cur), data.events, key + "|" + cur));
    };
    const tools = h("div", "qh-tools");
    if (list.length > 1) {
      const seg = UI.segmented({label: t("accounts.qh.winPick"), value: cur, options: list.slice().reverse().map(w => ({value: w,
        label: winLabel(w, ws)})), onChange: w => {
        cur = w;
        winPick = winKind(w, ws);
        Core.session.set(WIN_STORE, winPick);
        paint();
      }});
      tools.appendChild(seg);
    }
    if (o.account && list.length) {
      const rb = UI.button(t("accounts.qh.reset"), {size: "sm", attrs: {"aria-haspopup": "dialog", "data-fk": key + "|er"}});
      rb.addEventListener("click", () => o.onReset && o.onReset(cur, list, rb));
      tools.appendChild(rb);
    }
    head.appendChild(tools);
    box.append(head, epSlot, slot);
    paint();
    box.appendChild(legend());
    return box;
  }

  return {wins, winLabel, order, rows, winList, mainWin, winKind, pos, cap, used, median, niceScale, fence, domain, tick, span, eventAt,
    link, rowKind, endOf, epoch, kindLabel, miniSvg, mini, panel, cycleRows, detail, legend, redraw, clearUi, errText};
})();
