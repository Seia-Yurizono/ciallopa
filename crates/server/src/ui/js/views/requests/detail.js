"use strict";
// Requests view, part 3: the request detail panel. One panel object renders a record (conclusion,
// key numbers, timing bar, attempts, gateway observation, metadata, headers / body) and loads its
// /detail and /observation. Every load is bound to the shown record, the panel generation and the
// login: a late answer never fills another record. The view decides where the panel lives
// (side-by-side pane, drawer or full-screen page) and owns the list navigation.
const RequestsDetail = (() => {
  const {h, fmt, clear} = Core;
  const t = (k, v) => I18n.t(k, v);
  const M = RequestsModel, X = RequestsText;

  // pretty JSON text, or the raw text when it is not JSON
  function pretty(s) {
    if (s === null || s === undefined) return "";
    try {
      return JSON.stringify(JSON.parse(s), null, 2);
    } catch (_) {
      return String(s);
    }
  }

  // Width / offset as CSSOM custom properties (the CSP forbids style attributes).
  function place(el, left, width) {
    el.style.setProperty("--l", (Math.max(0, Math.min(100, left))).toFixed(2) + "%");
    el.style.setProperty("--w", (Math.max(0, Math.min(100 - left, width))).toFixed(2) + "%");
    return el;
  }

  async function copyText(text, okMsg) {
    try {
      await Core.copy(text);
      UI.toast(okMsg, {kind: "ok"});
    } catch (_) {
      UI.toast(t("requests.d.copyFailed"), {kind: "err"});
    }
  }

  function costText(r) {
    if (r.price_status === "unknown_price") return t("requests.noPrice");
    if (r.price_status === "ignored") return t("requests.d.costIgnored");
    return fmt.usd(r.cost_usd);
  }

  // create({ctx, onNav(step), canNav() → {prev, next, index, total}, onNext(k, r), onThread(id), threadFilter() → string,
  //   linkFor(id) → url}) → panel
  function create(o) {
    const ctx = o.ctx;
    let seq = 0, record = null, dtext = "", loading = false;

    // ---------- toolbar (prev / next / position / copy) ----------
    const prevBtn = UI.button("", {kind: "ghost", size: "sm", icon: "arrow-up", title: t("requests.d.prev") + " (K)",
      onClick: () => o.onNav(-1)});
    const nextBtn = UI.button("", {kind: "ghost", size: "sm", icon: "arrow-down", title: t("requests.d.next") + " (J)",
      onClick: () => o.onNav(1)});
    prevBtn.setAttribute("aria-keyshortcuts", "K Alt+ArrowUp");
    nextBtn.setAttribute("aria-keyshortcuts", "J Alt+ArrowDown");
    const position = h("span", {class: "rq-pos", role: "status"});
    const linkBtn = UI.button("", {kind: "ghost", size: "sm", icon: "external", title: t("requests.d.copyLink"),
      onClick: () => record && copyText(o.linkFor(record.request_id), t("requests.d.copiedLink"))});
    const jsonBtn = UI.button("", {kind: "ghost", size: "sm", icon: "copy", title: t("requests.d.copyJson"),
      onClick: () => dtext && copyText(dtext, t("requests.d.copiedJson"))});
    const toolbar = h("div", {class: "rq-dtools", role: "group", "aria-label": t("requests.d.title")},
      prevBtn, nextBtn, position, h("span", "grow"), linkBtn, jsonBtn);

    const body = h("div", {class: "rq-detail", "aria-live": "off"});

    function updateNav() {
      const n = o.canNav();
      prevBtn.disabled = !n.prev;
      nextBtn.disabled = !n.next;
      position.textContent = n.index < 0 ? t("requests.d.byId") : t("requests.d.position", {i: fmt.exact(n.index + 1),
        n: fmt.exact(n.total)});
      const f = document.activeElement;
      if ((f === prevBtn || f === nextBtn) && f.disabled) {
        (!prevBtn.disabled ? prevBtn : !nextBtn.disabled ? nextBtn : jsonBtn).focus({preventScroll: true});
      }
    }

    function title(r) {
      const outcome = M.outcomeOf(r);
      return X.outcomeLabel(outcome) + " · " + (r.model || t("requests.d.title"));
    }

    // ---------- sections ----------
    function conclusionBlock(r, refs) {
      const c = M.conclusion(r);
      const outcome = M.outcomeOf(r), tone = M.tone(r, outcome);
      const box = h("section", "rq-concl " + tone);
      let head, sentence = [];
      if (!c) {
        head = X.outcomeLabel(outcome);
        if (outcome === "prewarm") sentence.push(t("requests.prewarmTip"));
        else if (outcome === "incomplete" || (r.incomplete && !r.error)) sentence.push(incompleteTip(r));
        else if (outcome === "unknown") sentence.push(X.errTip("observation_incomplete"));
      } else if (c.kind === "rescued") {
        head = t("requests.d.rescuedHead") + " · " + rescueText(r);
        sentence.push(t("requests.d.rescuedBody"));
      } else if (c.kind === "error") {
        const n = X.causeLabel("error", c.cause);
        head = t("requests.d.errorHead") + (n ? " · " + n : "");
        sentence.push((I18n.has("requests.d.err." + c.cause) ? t("requests.d.err." + c.cause) : t("requests.d.err.other")) +
          (c.upstream ? (I18n.lang() === "zh" ? "，" : "; ") + t("requests.d.alsoUpstream") : ""));
        if (c.newSession) sentence.push(t("requests.d.newSession"));
      } else {
        const n = X.causeLabel("unfinished", c.cause);
        head = t("requests.d.unfinishedHead") + (n ? " · " + n : "");
        let s = I18n.has("requests.d.un." + c.cause) ? t("requests.d.un." + c.cause) : "";
        if (c.cause === "truncated" && r.incomplete) s += " (" + M.incompleteCode(r) + ")";
        if (s) sentence.push(s);
      }
      const status = r.status ? "HTTP " + r.status : t("requests.noStatus");
      const top = h("div", "rq-concl-top", UI.dot(tone), h("strong", "rq-concl-head", head),
        h("span", "rq-concl-meta", status + " · " + (r.transport || "—").toUpperCase()));
      box.appendChild(top);
      if (c) box.appendChild(h("div", "rq-concl-count", UI.tag(c.counted ? t("requests.d.counted") : t("requests.d.notCounted"),
        c.counted ? "err" : "outline")));
      for (const s of sentence) box.appendChild(h("p", "rq-concl-text", s));
      const acts = c ? M.nextSteps(r) : [];
      if (acts.length) {
        const row = h("div", "rq-concl-acts");
        let primary = true;
        for (const k of acts) {
          const attempts = k === "attempts";
          const b = UI.button(attempts ? t("requests.viewAttempts") : X.nextLabel(k), {size: "sm",
            kind: primary && !attempts ? "primary" : null, title: attempts ? t("requests.viewAttemptsTip") : X.nextTip(k),
            onClick: () => (attempts ? focusAttempts() : o.onNext(k, r))});
          b.dataset.next = k;
          if (attempts) {
            b.classList.add("hide");
            refs.attemptsBtn = b;
          } else primary = false;
          row.appendChild(b);
        }
        row.classList.toggle("hide", acts.every(k => k === "attempts"));
        refs.actsRow = row;
        box.appendChild(row);
      }
      return box;
    }

    function rescueText(r) {
      return r.switches && r.first_account ? t("requests.rescueSwitch", {a: r.first_account, b: r.account || ""}) :
        t("requests.rescueRetry", {n: fmt.exact(r.retries || 0)});
    }

    function incompleteTip(r) {
      const x = r.incomplete;
      if (x === "interrupted" || x === "content_filter") return t("requests.incTip." + x);
      return t("requests.incTip.other", {r: x});
    }

    function stat(label, value, sub, cls) {
      return h("div", "rq-stat" + (cls ? " " + cls : ""), h("div", "rq-stat-k", label), h("div", "rq-stat-v", value),
        sub ? h("div", "rq-stat-s", sub) : null);
    }

    function statsBlock(r) {
      const ttft = M.ttftOf(r);
      const subs = [];
      if (Core.isNum(r.cached_tokens) && r.cached_tokens > 0) subs.push(t("requests.d.cached", {n: fmt.n(r.cached_tokens)}));
      if (Core.isNum(r.reasoning_tokens) && r.reasoning_tokens > 0) subs.push(t("requests.d.reasoning", {n: fmt.n(r.reasoning_tokens)}));
      const tries = (r.retries || 0) + (r.switches || 0);
      return h("div", "rq-stats",
        stat(t("requests.d.ttft"), fmt.ms(ttft), null),
        stat(t("requests.d.total"), fmt.ms(r.duration_ms), tries ? t("requests.tries", {r: r.retries || 0, s: r.switches || 0}) : null),
        stat(t("requests.d.tokens"), r.input_tokens == null && r.output_tokens == null ? fmt.NONE : fmt.io(r.input_tokens, r.output_tokens),
          subs.join(" · ") || null, "wide"),
        stat(t("requests.d.cost"), costText(r), Core.isNum(r.tps) && r.tps > 0 ? t("requests.d.tps", {n: Math.round(r.tps)}) : null));
    }

    // Two-part timing bar: waiting for the first token (light), then generating (solid). A failed
    // request ends in the error colour.
    function timingBlock(r) {
      const d = Number(r.duration_ms);
      if (!(d > 0)) return null;
      const ttft = M.ttftOf(r);
      const failed = M.outcomeOf(r) === "failed";
      const bar = h("div", {class: "rq-tbar" + (failed ? " failed" : ""), role: "img"});
      const legend = h("div", "rq-tlegend");
      if (ttft !== null && ttft <= d) {
        const w = (100 * ttft) / d;
        bar.append(place(h("span", "rq-tseg wait"), 0, w), place(h("span", "rq-tseg gen"), w, 100 - w));
        legend.append(h("span", "rq-tkey", h("i", "wait"), t("requests.d.tlWait"), h("b", "num", fmt.ms(ttft))),
          h("span", "rq-tkey", h("i", "gen"), t("requests.d.tlGen"), h("b", "num", fmt.ms(d - ttft))));
        bar.setAttribute("aria-label", t("requests.d.tlWait") + " " + fmt.ms(ttft) + ", " + t("requests.d.tlGen") + " " + fmt.ms(d - ttft));
      } else {
        bar.append(place(h("span", "rq-tseg gen"), 0, 100));
        legend.append(h("span", "rq-tkey", t("requests.d.tlTotal"), h("b", "num", fmt.ms(d))),
          !M.isStream(r) ? h("span", "rq-tkey muted", t("requests.d.tlNoTtft")) : null);
        bar.setAttribute("aria-label", t("requests.d.tlTotal") + " " + fmt.ms(d));
      }
      return h("section", "rq-dsec", h("div", "rq-tscale", h("span", null, "0"), h("span", null, fmt.ms(d))), bar, legend);
    }

    function errorBlock(r, extra) {
      const es = M.srcOf(r);
      const code = M.labelledError(r);
      if (!r.error && !extra) return null;
      const box = h("section", "rq-err");
      const line = h("div", "rq-err-line");
      if (es) line.appendChild(UI.tag(X.sourceLabel(es) + (es === "upstream" && r.upstream_status ? " " + r.upstream_status : ""),
        es === "client" || es === "local" ? "outline" : "err", {title: X.sourceTip(es)}));
      if (r.error) line.appendChild(h("code", "rq-code", r.error));
      const label = code ? X.errLabel(code) : "";
      if (label) line.appendChild(h("span", "muted", label));
      box.appendChild(line);
      const tip = code ? X.errTip(code) : "";
      if (tip) box.appendChild(h("p", "rq-err-tip", tip));
      if (r.upstream_status && r.upstream_status !== r.status && r.error) {
        box.appendChild(h("p", "rq-err-tip", t("requests.upstreamTip", {s: r.upstream_status})));
      }
      if (extra) box.appendChild(h("pre", "rq-err-msg", extra));
      return box;
    }

    function attemptsBlock(attempts, r, refs) {
      const {rows, max} = M.attemptBars(attempts, r.duration_ms);
      const real = rows.filter(x => !x.dropped);
      const heading = h("h3", {class: "rq-dh", tabindex: "-1"}, t("requests.d.attempts"),
        h("span", "rq-dh-sub", t("requests.d.attemptsN", {n: real.length})),
        r.rescued ? UI.tag(t("requests.rescuedTag"), "warn") : null);
      refs.attemptsHead = heading;
      const list = h("ol", "rq-attempts");
      let prevAccount = null;
      for (const x of rows) {
        if (x.dropped) {
          list.appendChild(h("li", "rq-att dropped", t("requests.d.dropped", {n: x.dropped})));
          continue;
        }
        const a = x.a;
        const what = a.status ? "HTTP " + a.status : a.kind === "transport" ? t("requests.d.netErr") :
          a.kind === "refresh" ? t("requests.d.refreshErr") : a.kind === "preparation" ? t("requests.d.prepWait") : a.kind || "";
        const switched = prevAccount !== null && a.account !== prevAccount;
        const who = h("span", "rq-att-who", switched ? h("span", "rq-att-from", prevAccount + " → ") : null, h("strong", null, a.account
          || "—"));
        prevAccount = a.account;
        const track = h("div", "rq-att-track", place(h("span", "rq-att-bar " + x.tone), max ? (100 * x.start) / max : 0,
          max ? Math.max(1.5, (100 * (x.end - x.start)) / max) : 100));
        const li = h("li", "rq-att " + x.tone,
          h("div", "rq-att-top", UI.dot(x.tone), who, h("span", "rq-att-what", what), a.code ? h("code", "rq-code", a.code) : null,
            h("span", "grow"), h("span", {class: "rq-att-t num", title: t("requests.d.attemptTip", {t: fmt.ms(x.start)})},
              "+" + fmt.ms(x.start))),
          track,
          h("div", "rq-att-act", "→ " + X.actionText(a.action)));
        if (a.body) li.appendChild(h("details", "rq-att-body", h("summary", null, t("requests.d.body")), h("pre", "rq-pre",
          pretty(a.body))));
        list.appendChild(li);
      }
      return h("section", "rq-dsec", heading, list);
    }

    // Gateway observation: two lanes on one axis (upstream receive, downstream hand-off). They
    // overlap in time and are never added up. Blocked time is a cumulative number only.
    const MARKS = [["up_first_reasoning_ms", "reasoning", "r"], ["up_first_text_ms", "text", "t"], ["up_first_tool_ms", "tool", "f"],
      ["up_terminal_ms", "end", "e"]];
    const us = v => fmt.ms(Math.round(Number(v) / 1000));
    function blkText(v) {
      if (v === null || v === undefined || v === "") return fmt.NONE;
      const n = Number(v);
      return Number.isFinite(n) && n >= 0 ? us(n) : fmt.NONE;
    }
    const pair = (a, b, f) => (a == null && b == null ? null : (a == null ? fmt.NONE : f(a)) + " / " + (b == null ? fmt.NONE : f(b)));
    const deflate = (on, ext) => (on == null ? t("requests.o2.dUnknown") : ext === "unrecognized" ? t("requests.o2.dUnrec") :
      on ? t("requests.o2.dOn") : t("requests.o2.dOff"));

    function observationBlock(ob, r) {
      const sse = ob.sse_first_yield_ms != null || ob.sse_first_queue_ms != null, ws = ob.ws_first_send_ms != null;
      const vals = [r.duration_ms, ob.up_last_event_ms, ob.up_terminal_ms, ob.up_first_text_ms, ob.up_first_reasoning_ms,
        ob.up_first_tool_ms, ob.sse_last_queue_ms, ob.sse_last_yield_ms, ob.ws_last_send_ms].filter(v => v != null).map(Number);
      const max = Math.max(1, ...vals);
      const pc = v => (100 * Number(v)) / max;
      const span = (cls, a, b, label) => place(h("span", {class: "rq-os " + cls, title: label + " " + fmt.ms(a) + " – " + fmt.ms(b)}),
        pc(a), Math.max(0.6, pc(Math.max(0, b - a))));
      const lane = (name, kids) => h("div", "rq-olane", h("div", "rq-oname", name), h("div", "rq-otrack", kids));
      const ups = MARKS.map(([k]) => ob[k]).filter(v => v != null);
      const upEnd = ob.up_terminal_ms != null ? ob.up_terminal_ms : ob.up_last_event_ms;
      const upKids = [];
      if (ups.length && upEnd != null) upKids.push(span("up", Math.min(...ups), upEnd, t("requests.o2.upEvents")));
      for (const [k, name, c] of MARKS) {
        if (ob[k] == null) continue;
        const m = h("span", {class: "rq-om " + c, title: t("requests.o2." + name) + " " + fmt.ms(ob[k])});
        m.style.setProperty("--l", pc(ob[k]).toFixed(2) + "%");
        upKids.push(m);
      }
      const lanes = [lane(t("requests.o2.up"), upKids)];
      if (sse) {
        const k = [];
        if (ob.sse_first_queue_ms != null) k.push(span("q", ob.sse_first_queue_ms, ob.sse_last_queue_ms ?? ob.sse_first_queue_ms,
          t("requests.o2.sseQueue")));
        if (ob.sse_first_yield_ms != null) k.push(span("dn", ob.sse_first_yield_ms, ob.sse_last_yield_ms ?? ob.sse_first_yield_ms,
          t("requests.o2.sseYield")));
        lanes.push(lane(t("requests.o2.down"), k));
      }
      if (ws) lanes.push(lane(t("requests.o2.down"), [span("dn", ob.ws_first_send_ms, ob.ws_last_send_ms ?? ob.ws_first_send_ms,
        t("requests.o2.wsSend"))]));
      const durMark = h("span", {class: "rq-odur", title: t("requests.o2.total") + " " + fmt.ms(r.duration_ms)});
      durMark.style.setProperty("--l", pc(r.duration_ms || 0).toFixed(2) + "%");
      const axis = h("div", "rq-olane", h("div", "rq-oname"), h("div", "rq-oaxis", h("span", null, "0"), h("span", null, fmt.ms(max / 2)),
        h("span", null, fmt.ms(max))));
      const blk = sse ? ob.sse_queue_blocked_us : ob.ws_sink_blocked_us, bmx = sse ? ob.sse_queue_blocked_max_us
        : ob.ws_sink_blocked_max_us;
      const marks = MARKS.map(([k, name]) => t("requests.o2." + name) + " " + (ob[k] == null ? fmt.NONE : fmt.ms(ob[k]))).join(", ");
      const chart = h("div", {class: "rq-ochart", role: "img", "aria-label": t("requests.o2.aria", {max: fmt.ms(max), marks,
        total: fmt.ms(r.duration_ms), blk: blkText(blk)})}, lanes, axis, h("div", "rq-ovl", durMark));
      const legend = h("div", "rq-olegend");
      for (const [k, name, c] of MARKS) {
        legend.appendChild(h("span", "rq-okey", h("span", "rq-om static " + c), t("requests.o2." + name), " ",
          h("b", "num", ob[k] == null ? fmt.NONE : fmt.ms(ob[k]))));
      }
      legend.appendChild(h("span", "rq-okey", h("span", "rq-odur static"), t("requests.o2.total"), " ", h("b", "num",
        fmt.ms(r.duration_ms))));
      const key = h("div", "rq-olegend muted", h("span", "rq-okey", h("span", "rq-os up static"), t("requests.o2.upEvents")));
      if (sse) {
        key.append(h("span", "rq-okey", h("span", "rq-os q static"), t("requests.o2.sseQueue")),
          h("span", "rq-okey", h("span", "rq-os dn static"), t("requests.o2.sseYield")));
      }
      if (ws) key.append(h("span", "rq-okey", h("span", "rq-os dn static"), t("requests.o2.wsSend")));
      const facts = h("div", "rq-ofacts",
        h("span", null, t("requests.o2.blocked") + " ", h("b", "num", blkText(blk)),
          blk != null && Number(blk) > 0 ? h("span", "muted", " · " + t("requests.o2.blockedMax", {v: blkText(bmx)})) : null,
          h("span", "muted", " · " + (blk == null ? t("requests.o2.notMeasured") : t("requests.o2.onlyTotal")))));
      if (ob.down_incomplete) facts.appendChild(h("span", "c-err", "▲ " + t("requests.o2.downEarly")));
      if (r.transport === "ws") {
        facts.appendChild(h("span", null, t("requests.o2.deflate") + " " + t("requests.o2.client") + " ",
          h("b", null, deflate(ob.ws_deflate_down, ob.ws_ext_down)), " · " + t("requests.o2.up") + " ",
          h("b", null, deflate(ob.ws_deflate_up, ob.ws_ext_up)), h("span", "muted", " · " + t("requests.o2.deflateNote"))));
      }
      const exact = [["upRT", pair(ob.up_first_reasoning_ms, ob.up_first_text_ms, fmt.ms)],
        ["upTE", pair(ob.up_first_tool_ms, ob.up_terminal_ms, fmt.ms)],
        ["upLast", ob.up_last_event_ms == null ? null : fmt.ms(ob.up_last_event_ms) + (ob.up_events != null ? " · " + fmt.n(ob.up_events)
          : "")],
        ["sseQ", pair(ob.sse_first_queue_ms, ob.sse_last_queue_ms, fmt.ms)], ["sseY", pair(ob.sse_first_yield_ms, ob.sse_last_yield_ms,
          fmt.ms)],
        ["sseB", pair(ob.sse_queue_blocked_us, ob.sse_queue_blocked_max_us, us)], ["wsS", pair(ob.ws_first_send_ms, ob.ws_last_send_ms,
          fmt.ms)],
        ["wsB", pair(ob.ws_sink_blocked_us, ob.ws_sink_blocked_max_us, us)],
        ["bytes", pair(ob.up_bytes, ob.sse_yield_bytes != null ? ob.sse_yield_bytes : ob.ws_send_bytes, v => fmt.n(v) + " B")],
        ["downEarly", ob.down_incomplete == null ? null : ob.down_incomplete ? t("requests.o2.yes") : t("requests.o2.no")],
        ["extDown", ob.ws_ext_down], ["extUp", ob.ws_ext_up], ["resp", ob.response_id], ["upId", ob.upstream_request_id]]
        .filter(([, v]) => v !== null && v !== undefined && v !== "")
        .map(([k, v]) => [t("requests.o2.x." + k), String(v)]);
      return h("section", "rq-dsec",
        h("h3", "rq-dh", t("requests.o2.title"), UI.info(t("requests.o2.note"))),
        chart, legend, key, facts,
        exact.length ? h("details", "rq-fold", h("summary", null, t("requests.o2.exact")), UI.kv(exact)) : null);
    }

    function metaBlock(r, refs) {
      const tierQ = M.normTier(r.requested_service_tier), tierA = M.normTier(r.service_tier);
      const idBox = h("span", "rq-idv", h("code", null, r.request_id),
        UI.button("", {kind: "ghost", size: "sm", icon: "copy", title: t("requests.d.copyId"),
          onClick: () => copyText(r.request_id, t("requests.d.copiedId"))}));
      let session = null;
      if (r.thread_id) {
        const id = r.thread_id;
        session = h("span", "rq-idv", h("code", {title: id}, M.shortId(id)),
          UI.button("", {kind: "ghost", size: "sm", icon: "copy", title: t("requests.d.copySession"),
            onClick: () => copyText(id, t("requests.d.copiedSession"))}),
          o.threadFilter() !== id ? UI.button(t("requests.d.onlySession"), {kind: "link", size: "sm", title: t("requests.d.onlySessionTip"),
            onClick: () => o.onThread(id)}) : null);
      }
      const model = r.model || fmt.NONE;
      const effort = r.reasoning_effort || r.response_effort;
      const effortText = !effort ? null : r.reasoning_effort && r.response_effort && r.response_effort !== r.reasoning_effort ?
        r.reasoning_effort + " → " + r.response_effort : effort;
      const pairs = [
        [t("requests.d.time"), fmt.iso(r.ts_ms)],
        [t("requests.d.id"), idBox],
        [t("requests.d.account"), r.account || null],
        [t("requests.d.firstAccount"), r.first_account && r.first_account !== r.account ? r.first_account : null],
        [t("requests.d.key"), r.key || null],
        [t("requests.d.instance"), r.instance || null],
        [t("requests.d.endpoint"), r.endpoint || null],
        [t("requests.d.transport"), r.transport ? r.transport.toUpperCase() : null],
        [t("requests.d.model"), model],
        [t("requests.d.responseModel"), r.response_model && r.response_model !== r.model ?
          r.response_model + " (" + (r.model_mismatch ? t("requests.mismatch") : t("requests.snapshot")) + ")" : null],
        [t("requests.d.effort"), effortText],
        [t("requests.d.tier"), tierQ || tierA ? X.tierLabel(tierQ || "standard") + " / " + X.tierLabel(tierA) : null],
        [t("requests.d.session"), session],
        [t("requests.d.turn"), r.turn_id || null],
        [t("requests.d.endReason"), r.incomplete ? M.incompleteCode(r) : null],
      ];
      const dl = UI.kv(pairs);
      refs.meta = dl;
      return h("section", "rq-dsec", h("h3", "rq-dh", t("requests.d.meta")), dl);
    }

    function addMeta(refs, label, value) {
      if (!refs.meta || value === null || value === undefined || value === "") return;
      refs.meta.append(h("dt", null, label), h("dd", null, String(value)));
    }

    function foldBlock(label, sub, text) {
      const empty = !text;
      return h("details", "rq-fold", h("summary", null, label, sub ? h("span", "rq-fold-sub", sub) : null),
        empty ? h("p", "muted", t("requests.d.none")) : h("pre", "rq-pre", text));
    }

    // ---------- show ----------
    let refs = {};
    function show(r, opts) {
      opts = opts || {};
      if (!r || !M.RECORD_ID.test(r.request_id || "")) return;
      const mySeq = ++seq;
      // Focus inside content that is about to be replaced moves to the toolbar.
      if (body.contains(document.activeElement)) jsonBtn.focus({preventScroll: true});
      record = r;
      refs = {};
      const current = () => seq === mySeq && ctx.alive() && record === r;
      const status = h("div", {class: "rq-dload", role: "status", "aria-atomic": "true"});
      const retry = UI.button(t("requests.d.retry"), {size: "sm", title: t("requests.d.retryTip"), onClick: () => {
        if (!retry.disabled && current()) show(r, {retry: true, keepScroll: true});
      }});
      retry.classList.add("hide");
      const errBox = h("div", "rq-slot");
      const late = h("div", "rq-late");
      const obsBox = h("div", "rq-slot");
      const keepTop = opts.keepScroll ? scrollParent().scrollTop : 0;
      Core.fill(body,
        conclusionBlock(r, refs),
        statsBlock(r),
        timingBlock(r),
        errBox,
        r.has_detail ? h("div", "rq-dload-row", status, retry) : null,
        late,
        obsBox,
        metaBlock(r, refs));
      Core.fill(errBox, errorBlock(r, null));
      if (!opts.keepScroll) scrollParent().scrollTop = 0;
      else scrollParent().scrollTop = keepTop;
      dtext = JSON.stringify({record: r}, null, 2);
      jsonBtn.disabled = !!r.has_detail;
      updateNav();
      if (o.onShow) o.onShow(r, title(r));

      loadObservation(r, current, obsBox);
      if (!r.has_detail) return;
      loading = true;
      status.textContent = opts.retry ? t("requests.d.retrying") : t("requests.d.loading");
      retry.disabled = true;
      ctx.get("/usage/v1/records/" + r.request_id + "/detail", {current}).then(d => {
        if (!current()) return;
        Core.fill(errBox, errorBlock(r, d.message || null));
        if (d.upstream_request_id) addMeta(refs, t("requests.d.upstreamId"), d.upstream_request_id);
        const kids = [];
        if (Array.isArray(d.attempts) && d.attempts.length) {
          kids.push(attemptsBlock(d.attempts, r, refs));
          if (refs.attemptsBtn) {
            refs.attemptsBtn.classList.remove("hide");
            refs.actsRow.classList.remove("hide");
          }
        }
        const hasHeaders = !!(d.headers && Object.keys(d.headers).length);
        kids.push(h("section", "rq-dsec rq-folds",
          foldBlock(t("requests.d.headers"), null, hasHeaders ? JSON.stringify(d.headers, null, 2) : ""),
          foldBlock(t("requests.d.body"), t(d.truncated ? "requests.d.bodyTrunc" : "requests.d.bodyMeta", {n: fmt.exact(d.body_len || 0)}),
            d.body ? pretty(d.body) : "")));
        Core.fill(late, kids);
        dtext = JSON.stringify({record: r, detail: d}, null, 2);
        status.textContent = t("requests.d.loaded");
        status.parentElement.classList.add("done");
        if (opts.attempts) focusAttempts();
      }).catch(err => {
        if (!current() || Api.isQuiet(err)) return;
        if (err.status === 404) {
          status.textContent = t("requests.d.expired");
        } else {
          status.textContent = t("requests.d.loadFailed", {why: Api.why(err)});
          retry.classList.remove("hide");
        }
        dtext = JSON.stringify({record: r, detail_error: err.message}, null, 2);
      }).finally(() => {
        if (!current()) return;
        loading = false;
        jsonBtn.disabled = false;
        retry.disabled = false;
        if (opts.retry && retry.classList.contains("hide")
          && document.activeElement === document.body) jsonBtn.focus({preventScroll: true});
      });
    }

    function loadObservation(r, current, box) {
      if (!M.isStream(r)) return;
      ctx.get("/usage/v1/records/" + r.request_id + "/observation", {current}).then(ob => {
        if (current() && ob) Core.fill(box, observationBlock(ob, r));
      }).catch(err => {
        if (current() && err && err.status === 404) {
          Core.fill(box, h("section", "rq-dsec", h("h3", "rq-dh", t("requests.o2.title")), h("p", "muted small", t("requests.o2.none"))));
        }
      });
    }

    function scrollParent() {
      let p = body.parentElement;
      while (p && p !== document.body) {
        const s = getComputedStyle(p).overflowY;
        if (s === "auto" || s === "scroll") return p;
        p = p.parentElement;
      }
      return document.scrollingElement || document.documentElement;
    }

    // 查看尝试 / 排查详情: bring the attempts into view and continue keyboard reading there.
    function focusAttempts() {
      const head = refs.attemptsHead;
      if (!head || !head.isConnected) return false;
      head.scrollIntoView({block: "start", behavior: "smooth"});
      head.focus({preventScroll: true});
      return true;
    }

    // Drop the shown record: late answers for it are ignored from now on.
    function reset() {
      seq++;
      record = null;
      dtext = "";
      loading = false;
      clear(body);
    }

    return {toolbar, body, show, reset, updateNav, focusAttempts, record: () => record, title: () => (record ? title(record) : ""),
      loading: () => loading, hasAttempts: () => !!(refs.attemptsHead && refs.attemptsHead.isConnected)};
  }

  return {create, pretty};
})();
