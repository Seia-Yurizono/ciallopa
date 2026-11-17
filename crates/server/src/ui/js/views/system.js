"use strict";
// System: live process state — memory (/admin/v1/memory, an in-memory 10 s sampler, last 15 min),
// request-body buffer, WebSocket connections, concurrency (/admin/v1/runtime via Shell.status) and
// the usage recorder counters (summary.recorder). Admin keys only for the process state; a usage
// key sees the recorder and an explanation. Warnings feed the "system" nav badge through
// Shell.systemAlert (also from the overview, which probes memory on refresh like the old page).
Views.system = (() => {
  const {h, fmt} = Core;
  const t = (k, v) => I18n.t(k, v);

  I18n.add("zh", {
    "system.readAt": "读取于 {time}",
    "system.memory": "内存",
    "system.rss": "进程 RSS",
    "system.rssInfo": "操作系统统计的常驻内存（/proc/self/status VmRSS）；峰值为进程启动以来的最高值（VmHWM）。",
    "system.peak": "峰值 {v}",
    "system.notProvided": "此系统不提供",
    "system.jemalloc": "jemalloc 已分配",
    "system.jemallocInfo": "已分配：程序正在使用的堆内存；驻留：分配器占用的物理页，含尚未归还系统的空闲页。",
    "system.resident": "驻留 {v}",
    "system.unavailable": "不可用",
    "system.noJemalloc": "未使用 jemalloc 分配器",
    "system.buffer": "请求体缓冲",
    "system.bufferInfo": "正在内存中的请求体和 WebSocket 消息（含重放副本）占用的预算；上限为 max_buffered_body_mb。空间不足时最多等待 1 秒，仍不足则 HTTP 请求返回 " +
      "503，WebSocket 消息收到可重试的 503 错误帧（连接保持）。",
    "system.ws": "WebSocket 连接",
    "system.wsInfo": "已建立的下游 WebSocket 连接数 / ws_max_connections（0 为不限）；缓冲为这些连接当前占用的消息预算。",
    "system.wsBuffer": "缓冲 {v}",
    "system.limit": "上限 {v}",
    "system.noLimit": "不限",
    "system.concurrency": "并发",
    "system.concurrencyInfo": "当前在途请求 / max_inflight_requests。HTTP 占用到响应结束，WS 只在 turn 进行中占用；达到上限立即返回 503，不排队。拒绝数为进程启动以来的累计。",
    "system.rejected": "启动以来拒绝 {n}",
    "system.draining": "正在关闭",
    "system.drainingBody": "进程正在关闭，不再接受新请求。",
    "system.trend": "近期趋势",
    "system.trendInfo": "每 {n} 秒采样一次，只保存在内存里（最近 15 分钟，重启后清空）。",
    "system.collecting": "采集中，稍后刷新即可看到趋势",
    "system.t.memory": "内存",
    "system.t.buffer": "缓冲",
    "system.t.ws": "WS",
    "system.s.rss": "RSS",
    "system.s.alloc": "jemalloc 已分配",
    "system.s.buffer": "请求体缓冲",
    "system.s.limit": "上限",
    "system.s.ws": "WS 连接",
    "system.warnBuffer": "缓冲占用已达上限的 {p}",
    "system.warnWs": "WS 连接已达上限",
    "system.alertTitle": "需要注意",
    "system.recorder": "用量记录",
    "system.recorderInfo": "进程启动以来的记录器计数，不受时间范围影响；用量记录是尽力写入，丢弃或写入失败的请求不会出现在统计里。",
    "system.r.accepted": "已接收",
    "system.r.written": "已写入",
    "system.r.dropped": "丢弃",
    "system.r.write_errors": "写入失败",
    "system.r.purged": "已清理",
    "system.gapTitle": "用量记录有缺口",
    "system.gapBody": "丢弃 {d} · 写入失败 {w}",
    "system.noAdmin": "当前密钥不能读取进程状态",
    "system.noAdminBody": "内存、缓冲和并发需要管理密钥（admin = true）。用量记录计数仍可查看。",
    "system.memFailed": "读取进程状态失败",
  });
  I18n.add("en", {
    "system.readAt": "Read at {time}",
    "system.memory": "Memory",
    "system.rss": "Process RSS",
    "system.rssInfo": "Resident memory as the OS counts it (/proc/self/status VmRSS); the peak is the highest since start (VmHWM).",
    "system.peak": "Peak {v}",
    "system.notProvided": "Not provided on this system",
    "system.jemalloc": "jemalloc allocated",
    "system.jemallocInfo": "Allocated: heap in use by the program. Resident: physical pages held by the allocator, including " +
      "free pages not yet returned.",
    "system.resident": "Resident {v}",
    "system.unavailable": "Unavailable",
    "system.noJemalloc": "jemalloc is not in use",
    "system.buffer": "Body buffer",
    "system.bufferInfo": "Budget held by request bodies and WebSocket messages in memory (including replay copies)" +
      "; the limit is max_buffered_body_mb. When full, a request waits up to 1 s, then HTTP gets " +
      "503 and WebSocket gets a retryable 503 error frame (the connection stays).",
    "system.ws": "WebSocket connections",
    "system.wsInfo": "Open downstream WebSocket connections / ws_max_connections (0 = no limit); the buffer is " +
      "the message budget those connections hold now.",
    "system.wsBuffer": "Buffer {v}",
    "system.limit": "Limit {v}",
    "system.noLimit": "No limit",
    "system.concurrency": "Concurrency",
    "system.concurrencyInfo": "Requests in flight / max_inflight_requests. HTTP holds a slot until the response ends, WS " +
      "only during a turn; at the limit requests get 503 at once, no queueing. Refusals are counted " +
      "since start.",
    "system.rejected": "{n} refused since start",
    "system.draining": "Shutting down",
    "system.drainingBody": "The process is shutting down and takes no new requests.",
    "system.trend": "Recent trend",
    "system.trendInfo": "Sampled every {n} s and kept in memory only (last 15 minutes, cleared on restart).",
    "system.collecting": "Collecting samples — refresh in a moment to see the trend",
    "system.t.memory": "Memory",
    "system.t.buffer": "Buffer",
    "system.t.ws": "WS",
    "system.s.rss": "RSS",
    "system.s.alloc": "jemalloc allocated",
    "system.s.buffer": "Body buffer",
    "system.s.limit": "Limit",
    "system.s.ws": "WS connections",
    "system.warnBuffer": "Body buffer at {p} of its limit",
    "system.warnWs": "WebSocket connections at the limit",
    "system.alertTitle": "Needs attention",
    "system.recorder": "Usage recorder",
    "system.recorderInfo": "Recorder counters since the process started, not range-scoped. Usage records are best effort: " +
      "dropped or failed writes are missing from the statistics.",
    "system.r.accepted": "Accepted",
    "system.r.written": "Written",
    "system.r.dropped": "Dropped",
    "system.r.write_errors": "Write errors",
    "system.r.purged": "Purged",
    "system.gapTitle": "Usage records have gaps",
    "system.gapBody": "Dropped {d} · write errors {w}",
    "system.noAdmin": "This key can't read the process state",
    "system.noAdminBody": "Memory, buffers and concurrency need an admin key (admin = true). Recorder counters are still shown.",
    "system.memFailed": "Couldn't read the process state",
  });

  // =====================================================================================
  // Model (pure; scripts/overview-check.cjs)
  // =====================================================================================
  const known = v => v !== null && v !== undefined;

  // Warnings of a memory snapshot: body buffer ≥ 80 % of a set limit, WebSocket at a set cap.
  function memAlerts(m) {
    if (!m || !m.now) return [];
    const out = [];
    const lim = m.buffered_body_limit_bytes, used = m.now.buffered_body_bytes || 0;
    if (lim > 0 && used / lim >= 0.8) out.push({id: "buffer", ratio: used / lim});
    if (m.ws_max_connections > 0 && (m.now.ws_connections || 0) >= m.ws_max_connections) out.push({id: "ws"});
    return out;
  }

  const recorderGap = r => !!r && !!(r.dropped || r.write_errors);

  // Usage of a limit: {ratio, unlimited}; 0 / missing limit = no limit (no bar).
  function usage(used, limit) {
    if (!(limit > 0)) return {ratio: null, unlimited: true};
    return {ratio: Math.max(0, (used || 0) / limit), unlimited: false};
  }

  // History + the current sample, oldest first.
  function samples(m) {
    if (!m || !m.now) return [];
    return (m.history || []).concat([m.now]);
  }

  const model = {memAlerts, recorderGap, usage, samples};

  // ---------- badge sources (also driven by the overview) ----------
  let memSeq = 0;
  function noteMemory(m) {
    Shell.systemAlert("memory", memAlerts(m).length);
  }
  function noteRecorder(r) {
    Shell.systemAlert("recorder", recorderGap(r) ? 1 : 0);
  }
  function noteRuntime(status) {
    const rt = status && status.runtime && status.runtime.runtime;
    Shell.systemAlert("draining", rt && rt.draining ? 1 : 0);
  }

  // probeMemory(ctx) — read /admin/v1/memory for the badge (no request for a usage key).
  // Resolves to the snapshot or null; a newer probe wins.
  function probeMemory(ctx) {
    if (Api.noAdmin()) {
      Shell.systemAlert("memory", 0);
      return Promise.resolve(null);
    }
    const seq = ++memSeq;
    return ctx.get("/admin/v1/memory", {timeout: 15000, current: () => seq === memSeq}).then(m => {
      if (seq === memSeq) noteMemory(m);
      return m;
    }, e => {
      if (Api.isQuiet(e)) throw e;
      return null;
    });
  }

  // =====================================================================================
  // View
  // =====================================================================================
  let S = null;

  function build(root) {
    const E = S.els;
    E.alerts = h("div", "sys-alerts");
    E.when = h("span", "card-sub sys-when");
    E.noAdmin = h("div");
    E.cards = h("div", "sys-cards", UI.skeleton.stats(4));
    E.trendSeg = UI.segmented({label: t("system.trend"), value: S.trend, className: "sys-trend-seg",
      options: ["memory", "buffer", "ws"].map(v => ({value: v, label: t("system.t." + v)})),
      onChange: v => {
        S.trend = v;
        renderTrend();
      }});
    E.trendInfo = h("span");
    E.trendBox = h("div", "sys-chart", UI.skeleton.block(180));
    E.trendTitle = h("h2", "card-title", t("system.trend"));
    E.trendCard = h("section", "card sys-trend",
      h("div", "card-head", E.trendTitle, E.trendInfo, h("span", "grow"), E.trendSeg),
      h("div", "card-body", E.trendBox));
    E.recorder = h("section", "card sys-recorder");
    root.appendChild(h("div", "sys",
      h("div", "sys-head", E.when),
      E.alerts, E.noAdmin, E.cards, E.trendCard, E.recorder));
  }

  function metricCard(o) {
    return h("section", {class: "card sys-card" + (o.hot ? " hot" : ""), "data-card": o.id},
      h("div", "stat-label", h("span", null, o.label), o.info ? UI.info(o.info, o.label) : null),
      h("div", "stat-value sys-value", o.value),
      o.meter || null,
      h("div", "sys-sub", o.sub || ""));
  }

  function renderCards() {
    const E = S.els, m = S.mem;
    const rt = Shell.status.runtime && Shell.status.runtime.runtime;
    if (!m && !rt) {
      if (!S.memLoaded) return;
      Core.clear(E.cards);
      E.cards.classList.add("hide");
      return;
    }
    E.cards.classList.remove("hide");
    const cards = [];
    if (m) {
      const n = m.now;
      const alerts = memAlerts(m).map(a => a.id);
      cards.push(metricCard({id: "rss", label: t("system.rss"), info: t("system.rssInfo"), value: fmt.bytes(n.rss_bytes),
        sub: known(m.rss_peak_bytes) ? t("system.peak", {v: fmt.bytes(m.rss_peak_bytes)}) : t("system.notProvided")}));
      cards.push(metricCard({id: "jemalloc", label: t("system.jemalloc"), info: t("system.jemallocInfo"),
        value: m.jemalloc ? fmt.bytes(n.allocated_bytes) : t("system.unavailable"),
        sub: m.jemalloc ? t("system.resident", {v: fmt.bytes(n.resident_bytes)}) : t("system.noJemalloc")}));
      const tone = (r, warn, err) => (r >= err ? "err" : r >= warn ? "warn" : "accent");
      const bu = usage(n.buffered_body_bytes, m.buffered_body_limit_bytes);
      cards.push(metricCard({id: "buffer", label: t("system.buffer"), info: t("system.bufferInfo"), hot: alerts.includes("buffer"),
        value: fmt.bytes(n.buffered_body_bytes || 0),
        meter: bu.unlimited ? null : Charts.meter({value: Math.min(1, bu.ratio), tone: tone(bu.ratio, 0.6, 0.8),
          text: fmt.pct(bu.ratio, 0), tall: true}),
        sub: bu.unlimited ? t("system.noLimit") : t("system.limit", {v: fmt.bytes(m.buffered_body_limit_bytes)})}));
      const wu = usage(n.ws_connections, m.ws_max_connections);
      cards.push(metricCard({id: "ws", label: t("system.ws"), info: t("system.wsInfo"), hot: alerts.includes("ws"),
        value: [fmt.exact(n.ws_connections || 0), wu.unlimited ? null : h("span", "unit", "/ " + fmt.exact(m.ws_max_connections))],
        meter: wu.unlimited ? null : Charts.meter({value: Math.min(1, wu.ratio), tone: tone(wu.ratio, 0.8, 1),
          text: "", tall: true}),
        sub: (wu.unlimited ? t("system.noLimit") + " · " : "") + t("system.wsBuffer", {v: fmt.bytes(n.ws_bytes || 0)})}));
    }
    if (rt) {
      const cu = usage(rt.inflight, rt.limit);
      const by = rt.rejected_by || {};
      const reasons = ["inflight", "ws", "body", "ws_msg"].filter(k => by[k]).map(k => t("overview.r." + k) + " " + fmt.exact(by[k]));
      cards.push(metricCard({id: "concurrency", label: t("system.concurrency"), info: t("system.concurrencyInfo"),
        value: rt.draining ? UI.tag(t("system.draining"), "err", {dot: true}) :
          [fmt.exact(rt.inflight || 0), cu.unlimited ? null : h("span", "unit", "/ " + fmt.exact(rt.limit))],
        meter: cu.unlimited || rt.draining ? null : Charts.meter({value: Math.min(1, cu.ratio),
          tone: cu.ratio >= 0.8 ? "warn" : "accent", text: "", tall: true}),
        sub: (cu.unlimited ? t("system.noLimit") + " · " : "") + t("system.rejected", {n: fmt.exact(rt.rejected || 0)}) +
          (reasons.length ? " · " + reasons.join(" · ") : "")}));
    }
    Core.fill(E.cards, cards);
  }

  function renderAlerts() {
    const E = S.els;
    Core.clear(E.alerts);
    const items = [];
    for (const a of memAlerts(S.mem)) {
      items.push(a.id === "buffer" ? t("system.warnBuffer", {p: fmt.pct(a.ratio, 0)}) : t("system.warnWs"));
    }
    if (items.length) E.alerts.appendChild(UI.banner({kind: "warn", title: t("system.alertTitle"), body: items.join(" · ")}));
    const rt = Shell.status.runtime && Shell.status.runtime.runtime;
    if (rt && rt.draining) E.alerts.appendChild(UI.banner({kind: "err", title: t("system.draining"), body: t("system.drainingBody")}));
    const r = S.recorder;
    if (recorderGap(r)) {
      E.alerts.appendChild(UI.banner({kind: "err", title: t("system.gapTitle"), tip: t("system.recorderInfo"),
        body: t("system.gapBody", {d: fmt.exact(r.dropped || 0), w: fmt.exact(r.write_errors || 0)})}));
    }
    if (S.memError && !Api.noAdmin()) {
      E.alerts.appendChild(UI.banner({kind: "err", title: t("system.memFailed"), body: Api.why(S.memError),
        actions: [UI.button(t("ui.retry"), {size: "sm", onClick: () => Shell.refresh()})]}));
    }
  }

  function renderNoAdmin() {
    const E = S.els;
    Core.clear(E.noAdmin);
    if (!Api.noAdmin()) return;
    const other = UI.button(t("shell.loginOther"), {onClick: () => Shell.logout()});
    E.noAdmin.appendChild(h("div", "card sys-noadmin", UI.empty({icon: "lock", title: t("system.noAdmin"),
      body: t("system.noAdminBody"), actions: [other]})));
  }

  function renderTrend() {
    const E = S.els, m = S.mem;
    E.trendCard.classList.toggle("hide", !m);
    if (!m) return;
    Core.fill(E.trendInfo, UI.info(t("system.trendInfo", {n: m.sample_every_secs || 10})));
    const hist = samples(m);
    if (hist.length < 2) {
      if (S.chart) {
        S.chart.destroy();
        S.chart = null;
      }
      Core.fill(E.trendBox, h("div", "sys-collecting", UI.icon("clock"), t("system.collecting")));
      return;
    }
    const x = hist.map(s => s.at_ms);
    let series;
    if (S.trend === "memory") {
      series = [{id: "rss", label: t("system.s.rss"), color: "s1", values: hist.map(s => s.rss_bytes)}];
      if (m.jemalloc) series.push({id: "alloc", label: t("system.s.alloc"), color: "s2", values: hist.map(s => s.allocated_bytes)});
    } else if (S.trend === "buffer") {
      series = [{id: "buf", label: t("system.s.buffer"), color: "s1", values: hist.map(s => s.buffered_body_bytes)}];
      if (m.buffered_body_limit_bytes > 0) {
        series.push({id: "lim", label: t("system.s.limit"), color: "other", dashed: true,
          values: hist.map(() => m.buffered_body_limit_bytes)});
      }
    } else {
      series = [{id: "ws", label: t("system.s.ws"), color: "s1", values: hist.map(s => s.ws_connections)}];
      if (m.ws_max_connections > 0) {
        series.push({id: "lim", label: t("system.s.limit"), color: "other", dashed: true, values: hist.map(() => m.ws_max_connections)});
      }
    }
    // Byte series are plotted in MB so the axis gets round ticks (1 MB = 1024² bytes, as fmt.bytes).
    const bytes = S.trend !== "ws", MB = 1024 * 1024;
    if (bytes) for (const sr of series) sr.values = sr.values.map(v => (known(v) ? v / MB : v));
    const opts = {x, series, kind: "line", height: UI.isPhone() ? 160 : 200, label: t("system.trend"),
      yFormat: bytes ? v => Core.fmt.exact(v) + " MB" : v => fmt.exact(v), xFormat: ms => fmt.hm(ms),
      tipTitle: i => fmt.time(x[i]), tipValue: (sr, v) => (known(v) ? (bytes ? fmt.bytes(v * MB) : fmt.exact(v)) : fmt.NONE)};
    if (!S.chart) {
      Core.clear(E.trendBox);
      S.chart = Charts.timeSeries(E.trendBox, opts);
    } else S.chart.update(opts);
  }

  function renderRecorder() {
    const E = S.els, r = S.recorder;
    E.recorder.classList.toggle("hide", !r);
    if (!r) return;
    const keys = ["accepted", "written", "dropped", "write_errors", "purged"];
    Core.fill(E.recorder,
      h("div", "card-head", h("h2", "card-title", t("system.recorder")), UI.info(t("system.recorderInfo"))),
      h("div", "card-body sys-rec", keys.map(k => h("div", "sys-rec-item",
        h("span", "sys-rec-k", t("system.r." + k)),
        h("span", "sys-rec-v num" + ((k === "dropped" || k === "write_errors") && r[k] ? " c-err" : ""), fmt.exact(r[k] || 0))))));
  }

  function renderWhen() {
    const m = S.mem;
    S.els.when.textContent = m && m.now ? t("system.readAt", {time: fmt.hms(m.now.at_ms)}) : "";
  }

  function renderAll() {
    if (!S) return;
    renderWhen();
    renderAlerts();
    renderNoAdmin();
    renderCards();
    renderTrend();
    renderRecorder();
  }

  // Like probeMemory, but keeps the error for the banner. A usage key makes no request.
  function loadMemoryStrict() {
    if (Api.noAdmin()) {
      S.mem = null;
      S.memLoaded = true;
      Shell.systemAlert("memory", 0);
      return Promise.resolve();
    }
    const ctx = S.ctx, seq = ++memSeq;
    return ctx.get("/admin/v1/memory", {timeout: 15000, current: () => seq === memSeq}).then(m => {
      if (!S || seq !== memSeq) return;
      noteMemory(m);
      S.mem = m;
      S.memError = null;
      S.memLoaded = true;
    }, e => {
      if (!S || seq !== memSeq || Api.isQuiet(e)) return;
      S.memLoaded = true;
      // a usage key: Api flags noAdmin on admin_required; show the explanation, keep no stale data
      if (Api.noAdmin()) {
        S.mem = null;
        S.memError = null;
        Shell.systemAlert("memory", 0);
      } else S.memError = e;
    });
  }

  function loadRecorder() {
    const ctx = S.ctx;
    return ctx.get("/usage/v1/summary?range=1m").then(s => {
      if (!S) return;
      S.recorder = s.recorder || null;
      noteRecorder(S.recorder);
    }, e => {
      if (Api.isQuiet(e)) throw e;
    });
  }

  function loadAll() {
    return Promise.all([loadMemoryStrict(), loadRecorder()]).then(renderAll, e => {
      if (S && !Api.isQuiet(e)) S.ctx.fail(e);
    });
  }

  return {
    title: () => I18n.t("nav.system"),
    model,
    probeMemory,
    noteMemory,
    noteRecorder,
    noteRuntime,
    mount(root, ctx) {
      S = {ctx, els: {}, mem: null, memLoaded: false, memError: null, recorder: null, trend: "memory", chart: null};
      build(root);
      ctx.onRefresh(() => loadAll());
      ctx.onStatus(st => {
        noteRuntime(st);
        if (S.memLoaded) {
          renderAlerts();
          renderCards();
        }
      });
      ctx.onCleanup(() => {
        if (S && S.chart) S.chart.destroy();
      });
      noteRuntime(Shell.status);
      ctx.busy(loadAll());
    },
    unmount() {
      S = null;
    },
    onLang() {
      if (!S) return;
      S.els.trendTitle.textContent = t("system.trend");
      S.els.trendSeg.setLabels(["memory", "buffer", "ws"].map(v => t("system.t." + v)));
      renderAll();
    },
  };
})();
