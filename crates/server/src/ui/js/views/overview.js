"use strict";
// Overview: "is it healthy, how much was used, where does it fail" for the shared scope.
// One /usage/v1/summary scan (+ the previous period when comparing) feeds the hero metrics, the
// trend, the problem breakdown and the grouped table; /usage/v1/occupancy adds peak concurrency;
// live state (runtime, quota) comes from Shell.status. Every number keeps the old population rules
// (docs/METRICS.md): unknown is "—", never 0; nothing priced is "unpriced", never $0.00.
Views.overview = (() => {
  const {h, fmt} = Core;
  const t = (k, v) => I18n.t(k, v);

  I18n.add("zh", {
    "overview.updateFailed": "更新失败",
    "overview.keepLast": "保留最近一次结果",
    "overview.retry": "重试",
    "overview.vsPrev": "较上期 {d}",
    "overview.prevWas": "上一周期 {v}",
    "overview.rpm": "{n} RPM",
    "overview.upstream": "上游 {p}",
    "overview.p90": "p90 {v}",
    "overview.samples": "{n} 个样本",
    "overview.io": "入 {i} · 出 {o}",
    "overview.cache": "缓存 {p}",
    "overview.noUsage": "{n} 条未报告",
    "overview.unpricedN": "{n} 条未定价",
    "overview.unpriced": "未定价",
    "overview.cancelled": "{n} 断开",
    "overview.incomplete": "{n} 截断",
    "overview.unknownEnd": "{n} 未确认",
    "overview.withPrewarm": "含预热",
    "overview.noPrewarm": "不含预热",
    "overview.info.requests": "已结束的业务请求，不含预热。RPM 为窗口平均，不是实时入口速率。",
    "overview.info.errorRate": "失败 / 业务请求；本地拒绝、客户端断开、上游截断和未确认结束不计入。上游失败率 = 上游返回的失败 / 业务请求，含额度用尽和限流 429。",
    "overview.info.ttft": "流请求从开始到首个文本、推理或工具参数输出的时间；{n} 个样本，无输出的不填零。",
    "overview.info.tps": "完成流的总输出 Token / 总生成时长；{n} 个完成样本。估算值，含尾部传输时间。",
    "overview.info.tokens": "仅累计上游报告了用量的请求（{n} 条），{pw}；输入含缓存命中，推理 Token 已包含在输出中。缓存 = 缓存命中的输入 / 输入 Token（{c} 条报告了缓存字段）。",
    "overview.info.cost": "按模型、档位和报告用量以 API 标价估算，{pw}；不是账单，订阅的实际费用不同。",
    "overview.info.costUnpriced": "{n} 条请求缺少价格，未计入。",
    "overview.info.compare": "与等长的上一周期对比。",
    "overview.trend": "趋势",
    "overview.metric": "指标",
    "overview.grain": "粒度",
    "overview.m.requests": "请求",
    "overview.m.tokens": "Token",
    "overview.m.cost": "费用",
    "overview.m.fail": "失败",
    "overview.m.lat": "延迟",
    "overview.m.cap": "占用",
    "overview.g.auto": "自动",
    "overview.g.hour": "按小时",
    "overview.g.day": "按天",
    "overview.compare": "对比上一周期",
    "overview.prewarm": "含预热",
    "overview.prewarmInfo": "默认不含 Codex WebSocket 预热（generate:false）。打开后 Token、费用、趋势和分组计入预热，" +
      "请求列表也显示预热行；请求数、失败率和延迟始终不含预热。保存在本浏览器。",
    "overview.trendHint": "点击查看这段时间的请求 · 拖选缩小时间范围",
    "overview.trendHintTouch": "再点一次查看这段时间的请求",
    "overview.s.ok": "成功",
    "overview.s.unfinished": "未完成",
    "overview.s.failed": "失败",
    "overview.s.prev": "上一周期",
    "overview.s.prevP90": "上一周期 p90",
    "overview.s.inflight": "并发峰值",
    "overview.s.ws": "WS 峰值",
    "overview.s.inflightLimit": "并发上限",
    "overview.s.wsLimit": "WS 上限",
    "overview.partial": "（部分时段）",
    "overview.notReported": "未报告",
    "overview.tip.counts": "失败 {f} · 取消 {c} · 截断 {i}",
    "overview.tip.unknown": "结果未知 {n}",
    "overview.tip.tokens": "已报告 {s} / {r} 条",
    "overview.tip.rates": "失败率 {r} · 上游 {u}",
    "overview.tip.duration": "耗时 p50 / p90 / p99 {v}",
    "overview.tip.refused": "已拒绝 {n}，不计入失败",
    "overview.tip.full": "满载 {n} 分钟",
    "overview.tip.avg": "均值 并发 {i} · WS {w}",
    "overview.tip.noOcc": "没有占用采样",
    "overview.problems": "问题",
    "overview.failedGroup": "失败",
    "overview.failedGroupTip": "计入失败率",
    "overview.unfinishedGroup": "未完成",
    "overview.unfinishedGroupTip": "不计入失败率",
    "overview.noFailures": "这段时间没有失败",
    "overview.fc.quota": "额度用尽",
    "overview.fc.rate_limit": "限流",
    "overview.fc.request": "请求内容",
    "overview.fc.upstream": "上游错误",
    "overview.fc.network": "网络",
    "overview.fc.stream": "流中断",
    "overview.fc.local": "本地",
    "overview.fc.other": "其他",
    "overview.u.cancelled": "客户端断开",
    "overview.u.incomplete": "上游截断",
    "overview.u.unknown": "未确认结束",
    "overview.recent": "最近报错",
    "overview.recentNone": "没有报错",
    "overview.all": "全部",
    "overview.capacity": "容量",
    "overview.quotaLeft": "额度剩余",
    "overview.quotaInfo": "每段一个启用且有该窗口的账号，填充 = 剩余额度（上游最近一次快照，不受时间范围影响）；百分比为已知账号的平均剩余。点按查看每个账号。",
    "overview.nAccounts": "{n} 个账号",
    "overview.quotaUnknown": "额度未知",
    "overview.relogin": "需要重新登录",
    "overview.left": "剩余 {p}",
    "overview.quotaNone": "暂无额度数据",
    "overview.live": "实时",
    "overview.liveInfo": "进程当前状态，不受时间范围影响。HTTP 占用到响应结束，WS 只在 turn 进行中占用；达到上限立即返回 503，不排队。上限 {limit}。",
    "overview.inflight": "并发",
    "overview.draining": "正在关闭",
    "overview.noLimit": "不限",
    "overview.liveNoAdmin": "实时并发需要管理密钥",
    "overview.peak": "区间峰值",
    "overview.peakInfo": "所选时间段的占用峰值；满载 = 并发或 WS 达到上限的分钟数（按实例累计）。点击查看占用趋势。",
    "overview.full": "满载 {n} 分钟",
    "overview.refused": "已拒绝",
    "overview.refusedInfo": "达到本地上限时返回 503，不计入失败率。只随时间范围、API Key 和实例筛选；按账号或模型筛选时没有拒绝数。",
    "overview.r.inflight": "并发上限",
    "overview.r.ws": "WS 上限",
    "overview.r.body": "请求体缓冲",
    "overview.r.ws_msg": "WS 消息缓冲",
    "overview.refusedScoped": "按账号或模型筛选时不统计",
    "overview.breakdown": "分布",
    "overview.groupBy": "分组",
    "overview.moreGroups": "更多",
    "overview.gr.account": "账号",
    "overview.gr.key": "API Key",
    "overview.gr.instance": "实例",
    "overview.gr.model": "模型",
    "overview.gr.response_model": "实际模型",
    "overview.gr.endpoint": "接口",
    "overview.gr.reasoning_effort": "推理强度",
    "overview.gr.transport": "传输",
    "overview.gr.service_tier": "档位",
    "overview.gr.error": "错误码",
    "overview.gr.err_source": "错误来源",
    "overview.col.upstream": "上游",
    "overview.col.ttft": "TTFT p50",
    "overview.col.duration": "耗时 p50",
    "overview.col.retried": "重试",
    "overview.col.downgraded": "降级",
    "overview.total": "合计",
    "overview.noneValue": "（无）",
    "overview.emptyRange": "这段时间没有请求",
    "overview.emptyRangeHint": "换一个时间范围，或清除筛选",
    "overview.filterTo": "筛选到「{v}」",
    "overview.viewRequests": "查看请求",
    "overview.noPrices": "尚无价格表，费用不显示",
    "overview.fetchPrices": "拉取价格",
    "overview.unpricedModels": "{n} 个模型未定价",
    "overview.setPrices": "设置价格",
    "overview.rowFailures": "失败 {f} · 断开 {c} · 截断 {i} · 未确认 {u}",
    "overview.rowRetry": "重试 {r} · 换号 {s}",
    "overview.rowTokens": "已报告 {s} 条 · 输入 {i}（缓存 {c}）· 输出 {o}",
    "overview.tier.priority": "Fast",
    "overview.tier.flex": "Flex",
    "overview.tier.standard": "标准",
    "overview.src.client": "客户端",
    "overview.src.local": "ciallopa",
    "overview.src.network": "网络",
    "overview.src.proxy": "出口代理",
    "overview.src.upstream": "上游",
    "overview.src.stream": "流内",
  });
  I18n.add("en", {
    "overview.updateFailed": "Update failed",
    "overview.keepLast": "showing the last result",
    "overview.retry": "Retry",
    "overview.vsPrev": "{d} vs prev.",
    "overview.prevWas": "Previous period {v}",
    "overview.rpm": "{n} RPM",
    "overview.upstream": "Upstream {p}",
    "overview.p90": "p90 {v}",
    "overview.samples": "{n} samples",
    "overview.io": "In {i} · out {o}",
    "overview.cache": "Cached {p}",
    "overview.noUsage": "{n} unreported",
    "overview.unpricedN": "{n} unpriced",
    "overview.unpriced": "Unpriced",
    "overview.cancelled": "{n} disconnected",
    "overview.incomplete": "{n} truncated",
    "overview.unknownEnd": "{n} unconfirmed",
    "overview.withPrewarm": "prewarm included",
    "overview.noPrewarm": "prewarm excluded",
    "overview.info.requests": "Finished business requests, prewarm excluded. RPM is the average over the range, not a live rate.",
    "overview.info.errorRate": "Failures / business requests. Local refusals, client disconnects, truncated and unconfirmed " +
      "requests are not counted. Upstream rate = failures returned by upstream / business requests," +
      " including quota and 429.",
    "overview.info.ttft": "Time from start to the first text, reasoning or tool-argument output of streamed requests;" +
      " {n} samples, requests without output are left out.",
    "overview.info.tps": "Total output tokens / total generation time of completed streams; {n} samples. An estimate " +
      "that includes trailing transfer time.",
    "overview.info.tokens": "Only requests whose usage upstream reported ({n}), {pw}. Input includes cached input; reasoning " +
      "tokens are part of output. Cached = cached input / input tokens ({c} requests reported the " +
      "cache field).",
    "overview.info.cost": "Estimated from list API prices by model, tier and reported usage, {pw}. Not a bill; subscription costs differ.",
    "overview.info.costUnpriced": "{n} requests have no price and are left out.",
    "overview.info.compare": "Compared with the previous period of the same length.",
    "overview.trend": "Trend",
    "overview.metric": "Metric",
    "overview.grain": "Granularity",
    "overview.m.requests": "Requests",
    "overview.m.tokens": "Tokens",
    "overview.m.cost": "Cost",
    "overview.m.fail": "Failures",
    "overview.m.lat": "Latency",
    "overview.m.cap": "Capacity",
    "overview.g.auto": "Auto",
    "overview.g.hour": "Hourly",
    "overview.g.day": "Daily",
    "overview.compare": "Compare previous period",
    "overview.prewarm": "Include prewarm",
    "overview.prewarmInfo": "Codex WebSocket prewarm (generate:false) is left out by default. When on, tokens, cost, trend and " +
      "groups include it and the request list shows prewarm rows; request counts, error rate and latency never do. Saved in this browser.",
    "overview.trendHint": "Click to see these requests · drag to zoom in",
    "overview.trendHintTouch": "Tap again to see these requests",
    "overview.s.ok": "Succeeded",
    "overview.s.unfinished": "Unfinished",
    "overview.s.failed": "Failed",
    "overview.s.prev": "Previous period",
    "overview.s.prevP90": "Previous p90",
    "overview.s.inflight": "Peak in flight",
    "overview.s.ws": "Peak WS",
    "overview.s.inflightLimit": "In-flight limit",
    "overview.s.wsLimit": "WS limit",
    "overview.partial": " (partial)",
    "overview.notReported": "Not reported",
    "overview.tip.counts": "Failed {f} · cancelled {c} · truncated {i}",
    "overview.tip.unknown": "Unknown outcome {n}",
    "overview.tip.tokens": "Reported {s} / {r}",
    "overview.tip.rates": "Error rate {r} · upstream {u}",
    "overview.tip.duration": "Duration p50 / p90 / p99 {v}",
    "overview.tip.refused": "Refused {n}, not counted as failures",
    "overview.tip.full": "Full for {n} min",
    "overview.tip.avg": "Average in flight {i} · WS {w}",
    "overview.tip.noOcc": "No occupancy samples",
    "overview.problems": "Problems",
    "overview.failedGroup": "Failed",
    "overview.failedGroupTip": "count toward the error rate",
    "overview.unfinishedGroup": "Unfinished",
    "overview.unfinishedGroupTip": "not counted",
    "overview.noFailures": "No failures in this range",
    "overview.fc.quota": "Quota exhausted",
    "overview.fc.rate_limit": "Rate limited",
    "overview.fc.request": "Bad request",
    "overview.fc.upstream": "Upstream error",
    "overview.fc.network": "Network",
    "overview.fc.stream": "Stream interrupted",
    "overview.fc.local": "Local",
    "overview.fc.other": "Other",
    "overview.u.cancelled": "Client disconnected",
    "overview.u.incomplete": "Truncated upstream",
    "overview.u.unknown": "Unconfirmed end",
    "overview.recent": "Recent errors",
    "overview.recentNone": "No errors",
    "overview.all": "All",
    "overview.capacity": "Capacity",
    "overview.quotaLeft": "Quota left",
    "overview.quotaInfo": "One segment per enabled account with that window; the fill is the remaining quota (latest " +
      "upstream snapshot, not range-scoped). The percentage averages the known accounts. Tap to " +
      "see each account.",
    "overview.nAccounts": "{n} accounts",
    "overview.quotaUnknown": "Unknown",
    "overview.relogin": "Needs re-login",
    "overview.left": "{p} left",
    "overview.quotaNone": "No quota data yet",
    "overview.live": "Live",
    "overview.liveInfo": "Current process state, not range-scoped. HTTP requests hold a slot until the response ends;" +
      " WS only during a turn. At the limit requests get 503 at once, no queueing. Limit: {limit}.",
    "overview.inflight": "In flight",
    "overview.draining": "Shutting down",
    "overview.noLimit": "No limit",
    "overview.liveNoAdmin": "Live concurrency needs an admin key",
    "overview.peak": "Peak in range",
    "overview.peakInfo": "Peak occupancy in the selected range; full = minutes at the in-flight or WS limit (summed " +
      "over instances). Click for the capacity trend.",
    "overview.full": "Full {n} min",
    "overview.refused": "Refused",
    "overview.refusedInfo": "Requests refused with 503 at a local limit; not counted in the error rate. Only the time " +
      "range, API Key and instance apply; with an account or model filter there is no refusal count.",
    "overview.r.inflight": "In-flight limit",
    "overview.r.ws": "WS limit",
    "overview.r.body": "Body buffer",
    "overview.r.ws_msg": "WS message buffer",
    "overview.refusedScoped": "Not counted under an account or model filter",
    "overview.breakdown": "Breakdown",
    "overview.groupBy": "Group by",
    "overview.moreGroups": "More",
    "overview.gr.account": "Account",
    "overview.gr.key": "API Key",
    "overview.gr.instance": "Instance",
    "overview.gr.model": "Model",
    "overview.gr.response_model": "Served model",
    "overview.gr.endpoint": "Endpoint",
    "overview.gr.reasoning_effort": "Reasoning effort",
    "overview.gr.transport": "Transport",
    "overview.gr.service_tier": "Tier",
    "overview.gr.error": "Error code",
    "overview.gr.err_source": "Error source",
    "overview.col.upstream": "Upstream",
    "overview.col.ttft": "TTFT p50",
    "overview.col.duration": "Duration p50",
    "overview.col.retried": "Retried",
    "overview.col.downgraded": "Downgraded",
    "overview.total": "Total",
    "overview.noneValue": "(none)",
    "overview.emptyRange": "No requests in this range",
    "overview.emptyRangeHint": "Try another time range or clear the filters",
    "overview.filterTo": "Filter to “{v}”",
    "overview.viewRequests": "View requests",
    "overview.noPrices": "No price table yet, costs are hidden",
    "overview.fetchPrices": "Fetch prices",
    "overview.unpricedModels": "{n} models unpriced",
    "overview.setPrices": "Set prices",
    "overview.rowFailures": "Failed {f} · disconnected {c} · truncated {i} · unconfirmed {u}",
    "overview.rowRetry": "Retried {r} · switched account {s}",
    "overview.rowTokens": "Reported {s} · input {i} (cached {c}) · output {o}",
    "overview.tier.priority": "Fast",
    "overview.tier.flex": "Flex",
    "overview.tier.standard": "Standard",
    "overview.src.client": "Client",
    "overview.src.local": "ciallopa",
    "overview.src.network": "Network",
    "overview.src.proxy": "Proxy",
    "overview.src.upstream": "Upstream",
    "overview.src.stream": "In-stream",
  });

  // =====================================================================================
  // Model: pure functions (checked by scripts/overview-check.cjs)
  // =====================================================================================
  const DAY = 864e5;
  // Failure classes in display order (first_quota_reset is quota on the server side).
  const FCLS = ["quota", "rate_limit", "request", "upstream", "network", "stream", "local", "other"];
  const GROUPS = ["account", "model", "key", "instance", "response_model", "endpoint", "reasoning_effort", "transport",
    "service_tier", "error", "err_source"];
  const MAIN_GROUPS = ["account", "model", "key", "instance"];
  // Scope groups narrow the whole scope; error / source / tier rows open the matching requests.
  const GROUP_SCOPE = {account: "account", model: "model", key: "key", instance: "instance"};
  const GROUP_DRILL = {error: "fcode", err_source: "fsrc", service_tier: "ftier"};
  const METRICS = ["requests", "tokens", "cost", "fail", "lat", "cap"];
  const GRAINS = ["auto", "hour", "day"];
  const REASONS = ["inflight", "ws", "body", "ws_msg"];
  const DEFAULTS = {tmetric: "requests", tmode: "auto", group: "account"};
  const STORE = {tmetric: "ciallopa_f_tmetric", tmode: "ciallopa_f_tmode", group: "ciallopa_f_group"};
  const COMPARE_STORE = "ciallopa_compare";
  const PREV_TTL = 60e3;

  const num = v => (Core.isNum(v) ? Number(v) : 0);
  const known = v => v !== null && v !== undefined;

  // Hourly buckets for "hour", or "auto" with a window of at most two days.
  function isHourly(mode, win) {
    return mode === "hour" || (mode !== "day" && win.to - win.from <= 2 * DAY);
  }

  // /usage/v1/summary query of the scope (the same absolute window, filters and prewarm rule the
  // request list uses) plus bucket, tz (minutes east of UTC at the window end) and group.
  function summaryParams(scopeParams, mode, group) {
    const hourly = isHourly(mode, scopeParams);
    const tz = -new Date(scopeParams.to).getTimezoneOffset();
    return Object.assign({}, scopeParams, {bucket: hourly ? "hour" : "day", tz, group});
  }

  // The previous period depends on the scope and the window length only: a rolling window reuses
  // it for a minute instead of doubling every auto-refresh scan.
  function prevSig(params, fixed) {
    const p = Object.assign({}, params);
    delete p.group;
    const span = Number(p.to) - Number(p.from);
    if (!fixed) {
      delete p.from;
      delete p.to;
    }
    p.span = span;
    return Api.qs(p);
  }

  // Occupancy over the same window and instance, one row per bucket and instance.
  function occParams(params) {
    return {from: params.from, to: params.to, instance: params.instance || "", tz: params.tz,
      step: params.bucket === "hour" ? "1h" : "1d"};
  }

  // Every bucket intersecting the exact query window, partial ones flagged (a rolling 24 h
  // usually spans 25 hourly buckets).
  function trendPoints(summary, hourly, tz) {
    const step = hourly ? 36e5 : DAY, off = tz * 60000;
    const first = Math.floor((summary.from_ms + off) / step) * step - off;
    const by = new Map((summary.buckets || []).map(b => [b.start_ms, b]));
    const pts = [];
    for (let start = first; start <= summary.to_ms; start += step) {
      const end = start + step, b = by.get(start);
      const from = Math.max(start, summary.from_ms), to = Math.min(end - 1, summary.to_ms);
      pts.push({
        start, end, from, to, partial: from !== start || to !== end - 1,
        n: b ? num(b.count) : 0, f: b ? num(b.failures) : 0, cancel: b ? num(b.cancelled) : 0,
        inc: b ? num(b.incomplete) : 0, unknown: b ? num(b.unknown) : 0,
        records: b ? num(b.count) + num(b.prewarm_count) : 0, samples: b ? num(b.usage_samples) : 0,
        tokens: b ? (b.usage_samples ? num(b.input_tokens) + num(b.output_tokens) : null) : 0,
        cost: b ? num(b.cost_usd) : 0, unpriced: b ? num(b.unknown_price) : 0,
        cls: (b && b.failure_classes) || {}, up: b ? num(b.upstream_failures) : 0,
        refused: b ? (known(b.refused) ? b.refused : null) : null, refusals: (b && b.refusals) || null,
        t50: b ? b.stream_ttft_p50_ms : null, t90: b ? b.stream_ttft_p90_ms : null, t99: b ? b.stream_ttft_p99_ms : null,
        d50: b ? b.duration_p50_ms : null, d90: b ? b.duration_p90_ms : null, d99: b ? b.duration_p99_ms : null,
      });
    }
    return pts;
  }

  // Previous-period points aligned bucket by bucket from the end (both windows have one length).
  function alignPrev(pts, prevPts) {
    if (!prevPts) return null;
    const shift = pts.length - prevPts.length;
    return pts.map((_, i) => prevPts[i - shift] || null);
  }

  // Occupancy rows folded to one value per bucket start (peaks: max; averages and full minutes: sum
  // over instances).
  function occByBucket(occ) {
    const m = new Map();
    for (const r of (occ && occ.rows) || []) {
      const o = m.get(r.minute_ms) || {inf: 0, ws: 0, li: 0, lw: 0, full: 0, ia: 0, wa: 0};
      o.ia += num(r.inflight_avg);
      o.wa += num(r.ws_avg);
      o.inf = Math.max(o.inf, num(r.inflight_peak));
      o.ws = Math.max(o.ws, num(r.ws_peak));
      o.li = Math.max(o.li, num(r.max_inflight_requests));
      o.lw = Math.max(o.lw, num(r.ws_max_connections));
      o.full += num(r.full_minutes);
      m.set(r.minute_ms, o);
    }
    return m;
  }

  function occPeak(occ) {
    const rows = (occ && occ.rows) || [];
    if (!rows.length) return null;
    const o = {inf: 0, ws: 0, li: 0, lw: 0, full: 0};
    for (const r of rows) {
      o.inf = Math.max(o.inf, num(r.inflight_peak));
      o.ws = Math.max(o.ws, num(r.ws_peak));
      o.li = Math.max(o.li, num(r.max_inflight_requests));
      o.lw = Math.max(o.lw, num(r.ws_max_connections));
      o.full += num(r.full_minutes);
    }
    return o;
  }

  // Change against the previous period: kind "rel" (percent), "pt" (percentage points), "ms".
  // better: "up" | "down" colours the direction; null stays neutral. → {text, tone} | null.
  function delta(cur, prev, kind, better) {
    if (!known(cur) || !known(prev)) return null;
    if (kind === "rel" && !prev) return null;
    const d = kind === "pt" ? (cur - prev) * 100 : kind === "ms" ? cur - prev : ((cur - prev) / prev) * 100;
    const a = Math.abs(d), zero = kind === "ms" ? a < 1 : a < 0.05;
    const size = kind === "ms" ? fmt.ms(a) : a.toFixed(a >= 10 ? 0 : 1) + (kind === "pt" ? "pt" : "%");
    const text = zero ? "±0" : (d > 0 ? "+" : "−") + size;
    const tone = zero || !better ? "" : (d > 0) === (better === "up") ? "good" : "bad";
    return {text, tone};
  }

  // Deltas of the hero metrics (only with a previous period that had requests).
  function heroDeltas(tot, prev) {
    if (!tot || !prev || !prev.count) return {};
    const sumTok = x => num(x.input_tokens) + num(x.output_tokens);
    return {
      requests: delta(tot.count, prev.count, "rel", null),
      errorRate: delta(tot.failure_rate, prev.failure_rate, "pt", "down"),
      ttft: delta(tot.stream_ttft_p50_ms, prev.stream_ttft_p50_ms, "ms", "down"),
      tps: delta(tot.generation_tps, prev.generation_tps, "rel", "up"),
      tokens: prev.usage_samples && tot.usage_samples ? delta(sumTok(tot), sumTok(prev), "rel", null) : null,
      cost: !tot.unknown_price && !prev.unknown_price ? delta(tot.cost_usd, prev.cost_usd, "rel", null) : null,
    };
  }

  // Tokens: unknown unless upstream reported usage (or there was nothing to report).
  function tokenTotal(x) {
    if (!x) return null;
    if (x.usage_samples || !(num(x.count) + num(x.prewarm_count))) return num(x.input_tokens) + num(x.output_tokens);
    return null;
  }

  // Estimated cost: the priced amount; nothing priced at all is "unpriced", never $0.00.
  function costText(c, unpriced) {
    return unpriced && !c ? t("overview.unpriced") : fmt.usd(c);
  }

  // ---- quota pool (from /usage/v1/quota) ----
  function windowLabel(m, d) {
    if (!m) return d;
    if (m === 300) return "5h";
    if (m === 10080) return t("term.windowWeek");
    if (m >= 1440) return Math.round(m / 1440) + "d";
    if (m >= 60) return Math.round(m / 60) + "h";
    return m + "m";
  }

  // Window slots by window_minutes (≤ 1 day short "s", longer "l"), not by primary / secondary: a pro
  // account whose only (weekly) window arrives as primary has no 5h slot.
  function slots(q) {
    const o = {s: null, l: null};
    if (!q) return o;
    for (const [w, d, k] of [[q.primary, "5h", "s"], [q.secondary, t("term.windowWeek"), "l"]]) {
      if (!w || !known(w.used_pct)) continue;
      let s = w.window_minutes ? (w.window_minutes > 1440 ? "l" : "s") : k;
      if (o[s]) s = s === "s" ? "l" : "s";
      if (!o[s]) o[s] = {label: windowLabel(w.window_minutes, d), w};
    }
    return o;
  }

  // Remaining percent (0..100) of a window; a window whose reset time has passed is full again.
  function remaining(w, now) {
    if (!w || !known(w.used_pct)) return null;
    if (w.resets_at && w.resets_at * 1000 <= (now === undefined ? Date.now() : now)) return 100;
    return Math.max(0, 100 - Number(w.used_pct));
  }

  // Pool of one slot over the enabled, configured accounts that have that window, plus those with no
  // quota yet (unknown). → {members: [{id, email, rem, relogin}], avg} | null when none has it.
  function pool(accounts, slot, now) {
    const list = (accounts || []).filter(a => a.configured !== false && !a.disabled && (!a.quota || slots(a.quota)[slot]));
    if (!list.length) return null;
    const members = list.map(a => {
      const x = slots(a.quota)[slot];
      return {id: a.account, email: a.email || null, rem: x ? remaining(x.w, now) : null, relogin: !!a.needs_relogin};
    });
    const vals = members.map(m => m.rem).filter(known);
    return {members, avg: vals.length ? vals.reduce((p, r) => p + r, 0) / vals.length : null};
  }

  // ---- drill-downs into the request list (old URL parameter names) ----
  function errorCause(cls) {
    if (!cls) return "";
    return {quota: "quota", rate_limit: "rate_limit", request: "request", upstream: "upstream", network: "network",
      stream: "stream"}[cls] || "gateway";
  }

  // KPI and problem links: failed → 报错 (+ cause), cancelled / unknown → 未完成 + cause,
  // incomplete (truncated and content_filter) → the incomplete outcome, refused → 本地拒绝.
  function drillParams(outcome, cls) {
    if (outcome === "failed") return {fproblem: "error", fcause: errorCause(cls)};
    if (outcome === "cancelled") return {fproblem: "unfinished", fcause: "client_closed"};
    if (outcome === "unknown") return {fproblem: "unfinished", fcause: "unconfirmed"};
    if (outcome === "refused") return {fproblem: "refused"};
    return {foutcome: outcome};
  }

  // A grouped row: {scope: patch} for scope groups, {params} for drill groups, null otherwise.
  function groupAction(group, value) {
    if (!known(value) || value === "") return null;
    if (GROUP_SCOPE[group]) return {scope: {[GROUP_SCOPE[group]]: String(value)}};
    if (GROUP_DRILL[group]) {
      const p = {[GROUP_DRILL[group]]: String(value)};
      if (group !== "service_tier") p.fproblem = "error";
      return {params: p};
    }
    return null;
  }

  function groupName(group, value) {
    if (!known(value)) return t("overview.noneValue");
    const v = String(value);
    if (group === "service_tier" && I18n.has("overview.tier." + v)) return t("overview.tier." + v);
    if (group === "err_source" && I18n.has("overview.src." + v)) return t("overview.src." + v);
    return v;
  }

  // Bucket drill: the bucket's window inside a fixed scope window.
  function bucketParams(win, p, metric) {
    const o = {from: win.from, to: win.to, rfrom: p.from, rto: p.to};
    if (metric === "fail") o.fproblem = "error";
    return o;
  }

  // View-owned preferences: URL first (old names), then this browser, then the default.
  function readPrefs(query, store) {
    const pick = (k, allowed) => {
      const u = query.get(k);
      if (allowed.includes(u)) return u;
      const s = store(STORE[k]);
      return allowed.includes(s) ? s : DEFAULTS[k];
    };
    return {metric: pick("tmetric", METRICS), mode: pick("tmode", GRAINS), group: pick("group", GROUPS),
      compare: store(COMPARE_STORE) !== "0"};
  }

  // Latest-wins loads: a slower response of an older query (or identity) never overwrites a newer
  // one, and its failure is dropped too.
  function latest() {
    let seq = 0;
    return {
      run(fetcher, apply) {
        const mine = ++seq;
        const current = () => mine === seq;
        return Promise.resolve().then(() => fetcher(current)).then(data => {
          if (current()) apply(data);
          return current();
        }, e => {
          if (current()) throw e;
          return false;
        });
      },
      bump: () => ++seq,
    };
  }

  const model = {FCLS, GROUPS, METRICS, GRAINS, REASONS, isHourly, summaryParams, prevSig, occParams, trendPoints, alignPrev,
    occByBucket, occPeak, delta, heroDeltas, tokenTotal, costText, slots, remaining, pool, errorCause, drillParams, groupAction,
    groupName, bucketParams, readPrefs, latest};

  // =====================================================================================
  // View
  // =====================================================================================
  let S = null; // per-mount state

  function freshState(ctx) {
    const prefs = readPrefs(ctx.route.query, k => Core.local.get(k));
    return {
      ctx, prefs, els: {},
      data: null, // {summary, previous, refusals, recorder, occ, sig, hourly, tz}
      prevCache: null, // {sig, at, data}
      recent: null, // records or null
      loadedSig: null,
      error: null,
      summaryLoad: latest(),
      recentLoad: latest(),
      chart: null,
    };
  }

  const currentParams = () => summaryParams(Shell.scope.params(), S.prefs.mode, S.prefs.group);
  // Signature of the scope a result answers (window end excluded for rolling windows, so the
  // auto-refresh roll does not mark the screen stale).
  function scopeSig() {
    const p = Object.assign({}, currentParams());
    if (!Shell.scope.window().fixed) {
      p.span = p.to - p.from;
      delete p.from;
      delete p.to;
    }
    delete p.tz;
    return Api.qs(p);
  }
  const isCurrent = () => !!S && !!S.data && S.loadedSig === scopeSig();

  function pwText() {
    return Shell.scope.get().prewarm ? t("overview.withPrewarm") : t("overview.noPrewarm");
  }

  // ---------- loading ----------
  function loadSummary() {
    const ctx = S.ctx;
    const params = currentParams();
    const sig = scopeSig();
    const fixed = Shell.scope.window().fixed;
    const psig = prevSig(params, fixed);
    const wantPrev = S.prefs.compare && !(S.prevCache && S.prevCache.sig === psig && Date.now() - S.prevCache.at < PREV_TTL);
    markStale();
    return S.summaryLoad.run(current => Promise.all([
      ctx.get("/usage/v1/summary?" + Api.qs(Object.assign({}, params, wantPrev ? {compare: 1} : {})), {current}),
      ctx.get("/usage/v1/occupancy?" + Api.qs(occParams(params)), {current}).catch(e => {
        if (Api.isQuiet(e)) throw e;
        return null;
      }),
    ]), ([s, occ]) => {
      if (s.previous) S.prevCache = {sig: psig, at: Date.now(), data: s.previous};
      const previous = S.prefs.compare && S.prevCache && S.prevCache.sig === psig ? S.prevCache.data : null;
      S.data = {summary: s.summary, previous, refusals: s.refusals || null, recorder: s.recorder || null, occ,
        hourly: params.bucket === "hour", tz: Number(params.tz), win: {from: params.from, to: params.to}};
      S.loadedSig = sig;
      S.error = null;
      if (Views.system && Views.system.noteRecorder) Views.system.noteRecorder(s.recorder);
      renderAll();
    });
  }

  function loadRecent() {
    const ctx = S.ctx;
    const q = Object.assign({}, Shell.scope.params(), {problem: "error", limit: 5});
    return S.recentLoad.run(current => ctx.get("/usage/v1/records?" + Api.qs(q), {current}), d => {
      S.recent = (d && d.records) || [];
      renderRecent();
    });
  }

  function loadAll(o) {
    const auto = o && o.auto;
    const jobs = [loadSummary(), loadRecent().catch(() => {
      if (S) S.recent = S.recent || [];
    })];
    if (Views.system && Views.system.probeMemory) jobs.push(Views.system.probeMemory(S.ctx));
    const all = Promise.all(jobs).catch(e => {
      if (!S || Api.isQuiet(e)) return;
      S.error = e;
      renderNotice();
      if (!auto) S.ctx.fail(e, t("overview.updateFailed"));
    }).finally(() => {
      if (S) markStale();
    });
    return all;
  }

  // ---------- skeleton & layout ----------
  function build(root) {
    const E = S.els;
    E.notice = h("div", "ov-notice");
    E.hero = h("section", {class: "card tint ov-hero", "aria-label": t("nav.overview")}, UI.skeleton.stats(6));
    E.trendCard = buildTrendCard();
    E.problems = h("div", "card-body ov-problems", UI.skeleton.lines(6));
    E.recent = h("div", "ov-recent");
    E.capacity = h("div", "card-body ov-capacity", UI.skeleton.lines(6));
    E.problemsHead = h("div", "card-head", h("h2", "card-title", t("overview.problems")));
    E.capacityHead = h("div", "card-head", h("h2", "card-title", t("overview.capacity")),
      h("span", "grow"), h("a", {class: "btn btn-ghost btn-sm", href: "/ui/accounts"}, t("nav.accounts"), UI.icon("chevron-right")));
    E.breakdownCard = buildBreakdownCard();
    root.appendChild(h("div", "ov",
      E.notice,
      E.hero,
      E.trendCard,
      h("div", "ov-grid",
        h("section", "card ov-card", E.problemsHead, E.problems, E.recent),
        h("section", "card ov-card", E.capacityHead, E.capacity)),
      E.breakdownCard));
  }

  // ---------- notice (failed refresh keeps old data) ----------
  function renderNotice() {
    const box = S.els.notice;
    Core.clear(box);
    if (!S.error) return;
    const retry = UI.button(t("overview.retry"), {size: "sm", onClick: () => Shell.refresh()});
    box.appendChild(UI.banner({kind: "err", title: t("overview.updateFailed"),
      body: Api.why(S.error) + (S.data ? " · " + t("overview.keepLast") : ""), actions: [retry]}));
  }

  function markStale() {
    if (!S) return;
    const stale = !!S.data && !isCurrent();
    for (const el of [S.els.hero, S.els.trendCard, S.els.problems, S.els.capacity, S.els.breakdownCard]) {
      if (el) el.classList.toggle("is-stale", stale);
    }
  }

  // Drill link: an anchor (Shell routes it), inert while the numbers answer an older scope.
  function drill(content, params, o) {
    o = o || {};
    const a = h("a", {class: "ov-link" + (o.className ? " " + o.className : ""), href: Shell.href("requests", params),
      title: o.title || null, "aria-label": o.label || null}, content);
    a.addEventListener("click", e => {
      if (!isCurrent()) e.preventDefault();
    });
    return a;
  }

  // ---------- hero ----------
  function tile(o) {
    const label = h("div", "stat-label", h("span", null, o.label), o.info ? UI.info(o.info, o.label) : null);
    const value = h("div", "stat-value ov-value" + (o.tone ? " " + o.tone : ""), o.value);
    const d = o.delta;
    const foot = h("div", "ov-tile-foot",
      d ? h("span", {class: "stat-delta" + (d.tone ? " " + d.tone : ""), title: o.prevText || null},
        t("overview.vsPrev", {d: d.text})) : null,
      ...(o.sub || []).filter(Boolean).map(x => h("span", "ov-sub", x)));
    const spark = o.spark && o.spark.filter(known).length >= 3 ?
      h("span", "ov-spark", Charts.sparkline(o.spark, {color: o.sparkColor || "accent", width: 56, height: 18})) : null;
    label.appendChild(spark || h("span"));
    return h("div", {class: "ov-tile", "data-tile": o.id}, label, value, foot);
  }

  function renderHero() {
    const box = S.els.hero;
    const sum = S.data.summary, tot = sum.total;
    const prev = S.data.previous ? S.data.previous.total : null;
    const D = heroDeltas(tot, prev);
    const pts = trendPoints(sum, S.data.hourly, S.data.tz);
    const span = Math.max(1, sum.to_ms - sum.from_ms);
    const rpm = num(tot.count) * 60000 / span;
    const failBad = num(tot.failure_rate) >= 0.05;
    const unp = num(tot.unknown_price);
    const tokens = tokenTotal(tot);
    const reported = tot.usage_samples || !(num(tot.count) + num(tot.prewarm_count));
    const prevText = (fn, v) => (prev && known(v) ? t("overview.prevWas", {v: fn(v)}) : null);
    const tiles = [
      tile({id: "requests", label: t("term.requests"), info: t("overview.info.requests"),
        value: drill(fmt.n(tot.count), drillParams("business"), {title: fmt.exact(tot.count)}),
        delta: D.requests, prevText: prevText(fmt.exact, prev && prev.count), spark: pts.map(p => p.n),
        sub: [t("overview.rpm", {n: rpm >= 1 ? rpm.toFixed(1) : rpm.toFixed(2)})]}),
      tile({id: "errorRate", label: t("term.errorRate"), info: t("overview.info.errorRate"), tone: failBad ? "c-err" : "",
        value: drill(fmt.pct(tot.failure_rate), drillParams("failed"), {title: fmt.exact(tot.failures)}),
        delta: D.errorRate, prevText: prevText(fmt.pct, prev && prev.failure_rate), sparkColor: "err",
        spark: pts.map(p => (p.n ? p.f / p.n : null)),
        sub: [tot.count ? t("overview.upstream", {p: fmt.pct(tot.upstream_failure_rate)}) : null]}),
      tile({id: "ttft", label: t("term.ttft") + " p50", info: t("overview.info.ttft", {n: fmt.exact(num(tot.stream_ttft_samples))}),
        value: fmt.ms(tot.stream_ttft_p50_ms), delta: D.ttft, prevText: prevText(fmt.ms, prev && prev.stream_ttft_p50_ms),
        spark: pts.map(p => p.t50), sub: [known(tot.stream_ttft_p90_ms) ? t("overview.p90", {v: fmt.ms(tot.stream_ttft_p90_ms)}) : null]}),
      tile({id: "tps", label: t("term.tps"), info: t("overview.info.tps", {n: fmt.exact(num(tot.tps_samples))}),
        value: known(tot.generation_tps) ? [Number(tot.generation_tps).toFixed(1), h("span", "unit", "tok/s")] : fmt.NONE,
        delta: D.tps, prevText: prevText(v => Number(v).toFixed(1) + " tok/s", prev && prev.generation_tps),
        sub: [t("term.estimate")]}),
      tile({id: "tokens", label: t("term.tokens"),
        info: t("overview.info.tokens", {n: fmt.exact(num(tot.usage_samples)), pw: pwText(), c: fmt.exact(num(tot.cache_samples))}),
        value: known(tokens) ? h("span", {title: fmt.exact(tokens)}, fmt.n(tokens)) : fmt.NONE, delta: D.tokens,
        spark: pts.map(p => p.tokens),
        sub: [
          reported ? t("overview.io", {i: fmt.n(tot.input_tokens), o: fmt.n(tot.output_tokens)}) : null,
          known(tot.cache_ratio) && tot.cache_samples ? t("overview.cache", {p: fmt.pct(tot.cache_ratio)}) : null,
          tot.no_usage ? h("span", "c-warn", t("overview.noUsage", {n: fmt.exact(tot.no_usage)})) : null]}),
      tile({id: "cost", label: t("term.cost"),
        info: t("overview.info.cost", {pw: pwText()}) + (unp ? " " + t("overview.info.costUnpriced", {n: fmt.exact(unp)}) : ""),
        value: h("span", {title: fmt.exactUsd(tot.cost_usd)}, costText(tot.cost_usd, unp)), delta: D.cost,
        prevText: prevText(fmt.usd, prev && prev.cost_usd), spark: pts.map(p => p.cost),
        sub: [unp ? h("a", {class: "ov-link c-warn", href: "/ui/settings#prices"}, t("overview.unpricedN", {n: fmt.exact(unp)})) :
          null]}),
    ];
    Core.fill(box, h("div", "ov-tiles", tiles));
  }

  // ---------- trend ----------
  function buildTrendCard() {
    const E = S.els;
    const metricOpts = METRICS.map(m => ({value: m, label: t("overview.m." + m)}));
    E.metricSeg = UI.segmented({label: t("overview.metric"), value: S.prefs.metric, options: metricOpts, className: "ov-metric",
      onChange: v => setPref("metric", v)});
    E.grainSeg = UI.segmented({label: t("overview.grain"), value: S.prefs.mode, className: "ov-grain",
      options: GRAINS.map(g => ({value: g, label: t("overview.g." + g)})), onChange: v => setPref("mode", v)});
    E.compare = h("input", {type: "checkbox", class: "switch", id: "ov-compare"});
    E.compare.checked = S.prefs.compare;
    E.compare.addEventListener("change", () => setPref("compare", E.compare.checked));
    E.compareLabel = h("label", {class: "check ov-compare", for: "ov-compare"}, E.compare,
      h("span", null, t("overview.compare")));
    E.prewarm = h("input", {type: "checkbox", class: "switch", id: "ov-prewarm"});
    E.prewarm.checked = !!Shell.scope.get().prewarm;
    E.prewarm.addEventListener("change", () => Shell.scope.set({prewarm: E.prewarm.checked}));
    E.prewarmLabel = h("label", {class: "check ov-compare", for: "ov-prewarm"}, E.prewarm, h("span", null, t("overview.prewarm")));
    E.prewarmInfo = h("span", "ov-info-slot", UI.info(t("overview.prewarmInfo")));
    E.trendOptsBtn = UI.button("", {kind: "ghost", size: "sm", icon: "dots", title: t("overview.grain"),
      onClick: () => openTrendOptions()});
    E.trendOptsBtn.classList.add("phone-only");
    E.trendTitle = h("h2", "card-title", t("overview.trend"));
    E.trendSub = h("span", "card-sub");
    E.trendBox = h("div", "ov-chart");
    E.trendBox.appendChild(UI.skeleton.block(200));
    return h("section", "card ov-trend",
      h("div", "card-head ov-trend-head", E.trendTitle, E.trendSub, h("span", "grow"),
        h("div", "ov-trend-ctl desk-only", E.grainSeg, E.compareLabel, h("span", "ov-pw", E.prewarmLabel, E.prewarmInfo)),
        E.trendOptsBtn),
      h("div", "ov-metric-row", E.metricSeg),
      h("div", "card-body", E.trendBox));
  }

  // Phone: granularity and comparison live in a sheet.
  function openTrendOptions() {
    const seg = UI.segmented({label: t("overview.grain"), value: S.prefs.mode, block: true,
      options: GRAINS.map(g => ({value: g, label: t("overview.g." + g)})), onChange: v => {
        S.els.grainSeg.setValue(v);
        setPref("mode", v);
      }});
    const sw = h("input", {type: "checkbox", class: "switch", id: "ov-compare-sheet"});
    sw.checked = S.prefs.compare;
    sw.addEventListener("change", () => {
      S.els.compare.checked = sw.checked;
      setPref("compare", sw.checked);
    });
    const pw = h("input", {type: "checkbox", class: "switch", id: "ov-prewarm-sheet"});
    pw.checked = !!Shell.scope.get().prewarm;
    pw.addEventListener("change", () => Shell.scope.set({prewarm: pw.checked}));
    UI.sheet({title: t("overview.trend"), body: h("div", "stack ov-sheet",
      h("div", "field", h("span", "field-label", t("overview.grain")), seg),
      h("label", {class: "check", for: "ov-compare-sheet"}, sw, h("span", "grow", t("overview.compare"))),
      h("label", {class: "check", for: "ov-prewarm-sheet"}, pw, h("span", "grow", t("overview.prewarm"))),
      h("div", "small muted", t("overview.prewarmInfo")))});
  }

  function setPref(k, v) {
    S.prefs[k] = v;
    if (k === "compare") {
      Core.local.set(COMPARE_STORE, v ? "1" : "0");
      if (!v && S.data) {
        S.data.previous = null;
        renderAll();
      }
      if (v) S.ctx.busy(loadSummary().catch(e => S.ctx.fail(e, t("overview.updateFailed"))));
      return;
    }
    const param = {metric: "tmetric", mode: "tmode", group: "group"}[k];
    Core.local.set(STORE[param], v);
    S.ctx.setParams({[param]: v === DEFAULTS[param] ? "" : v});
    if (k === "metric") renderTrend();
    else {
      if (k === "group") renderBreakdownControls();
      S.ctx.busy(loadSummary().catch(e => S.ctx.fail(e, t("overview.updateFailed"))));
    }
  }

  // Series and formatting of the selected metric.
  function trendSpec(metric, pts, prevPts, occ) {
    const prevLine = (label, get) => (prevPts ? [{id: "prev", label, color: "other", dashed: true,
      values: prevPts.map(p => (p ? get(p) : null))}] : []);
    if (metric === "requests") {
      return {kind: "bar", series: [
        {id: "f", label: t("overview.s.failed"), color: "err", values: pts.map(p => p.f)},
        {id: "u", label: t("overview.s.unfinished"), color: "neutral", values: pts.map(p => p.cancel + p.inc + p.unknown)},
        {id: "ok", label: t("overview.s.ok"), color: "s1", values: pts.map(p => Math.max(0, p.n - p.f - p.cancel - p.inc - p.unknown))},
      ], lines: prevLine(t("overview.s.prev"), p => p.n), y: v => fmt.n(v), v: v => fmt.exact(v)};
    }
    if (metric === "tokens") {
      return {kind: "bar", series: [{id: "tok", label: t("term.tokens"), color: "s1", values: pts.map(p => p.tokens)}],
        lines: prevLine(t("overview.s.prev"), p => p.tokens), y: v => fmt.n(v),
        v: v => (known(v) ? fmt.exact(v) : t("overview.notReported"))};
    }
    if (metric === "cost") {
      return {kind: "bar", series: [{id: "cost", label: t("term.cost"), color: "s1", values: pts.map(p => p.cost)}],
        lines: prevLine(t("overview.s.prev"), p => p.cost), y: v => (v < 1 ? "$" + v.toFixed(2) : "$" + fmt.n(v)),
        v: v => fmt.usd(v)};
    }
    if (metric === "fail") {
      return {kind: "bar", series: [{id: "f", label: t("overview.s.failed"), color: "err", values: pts.map(p => p.f)}],
        lines: prevLine(t("overview.s.prev"), p => p.f), y: v => fmt.n(v), v: v => fmt.exact(v)};
    }
    if (metric === "lat") {
      return {kind: "line", series: [
        {id: "p50", label: "TTFT p50", color: "s1", values: pts.map(p => p.t50)},
        {id: "p90", label: "TTFT p90", color: "s2", values: pts.map(p => p.t90)},
        {id: "p99", label: "TTFT p99", color: "s3", values: pts.map(p => p.t99)},
      ].concat(prevPts ? [{id: "prev", label: t("overview.s.prevP90"), color: "other", dashed: true,
        values: prevPts.map(p => (p ? p.t90 : null))}] : []), y: v => fmt.ms(v), v: v => fmt.ms(v)};
    }
    // cap: peaks against the limits (limits dashed); refusals go to the tooltip (no second axis)
    const o = pts.map(p => occ.get(p.start) || null);
    const anyLimit = k => o.some(x => x && x[k]);
    return {kind: "line", series: [
      {id: "inf", label: t("overview.s.inflight"), color: "s1", values: o.map(x => (x ? x.inf : null))},
      {id: "ws", label: t("overview.s.ws"), color: "s2", values: o.map(x => (x ? x.ws : null))},
    ].concat(anyLimit("li") ? [{id: "li", label: t("overview.s.inflightLimit"), color: "s1", dashed: true,
      values: o.map(x => (x && x.li ? x.li : null))}] : [])
      .concat(anyLimit("lw") ? [{id: "lw", label: t("overview.s.wsLimit"), color: "s2", dashed: true,
        values: o.map(x => (x && x.lw ? x.lw : null))}] : []), y: v => fmt.n(v), v: v => fmt.exact(v), occ: o};
  }

  function refusedText(p) {
    if (!known(p.refused)) return null;
    const parts = p.refusals ? REASONS.filter(k => p.refusals[k]).map(k => t("overview.r." + k) + " " + fmt.exact(p.refusals[k])) : [];
    return t("overview.tip.refused", {n: fmt.exact(p.refused)}) + (parts.length ? " · " + parts.join(" · ") : "");
  }

  function tipExtra(metric, p, spec, i) {
    const out = [];
    if (metric === "requests") {
      if (p.unknown) out.push(t("overview.tip.unknown", {n: fmt.exact(p.unknown)}));
      out.push(t("term.tokens") + " " + (known(p.tokens) ? fmt.exact(p.tokens) : t("overview.notReported")) + " · " +
        t("term.cost") + " " + costText(p.cost, p.unpriced));
    } else if (metric === "tokens") {
      if (p.records) out.push(t("overview.tip.tokens", {s: fmt.exact(p.samples), r: fmt.exact(p.records)}) + " · " + pwText());
    } else if (metric === "cost") {
      if (p.unpriced) out.push(t("overview.info.costUnpriced", {n: fmt.exact(p.unpriced)}));
    } else if (metric === "fail") {
      out.push(t("overview.tip.rates", {r: fmt.pct(p.n ? p.f / p.n : null), u: fmt.pct(p.n ? p.up / p.n : null)}));
      for (const c of FCLS) if (p.cls[c]) out.push(t("overview.fc." + c) + " " + fmt.exact(p.cls[c]));
    } else if (metric === "lat") {
      out.push(t("overview.tip.duration", {v: [p.d50, p.d90, p.d99].map(fmt.ms).join(" / ")}));
    } else if (metric === "cap") {
      const o = spec.occ[i];
      if (!o) out.push(t("overview.tip.noOcc"));
      else {
        out.push(t("overview.tip.avg", {i: o.ia.toFixed(1), w: o.wa.toFixed(1)}));
        if (o.full) out.push(t("overview.tip.full", {n: fmt.exact(o.full)}));
      }
    }
    if (metric === "fail" || metric === "cap") {
      const r = refusedText(p);
      if (r) out.push(r);
    }
    return out;
  }

  function renderTrend() {
    if (!S.data) return;
    const E = S.els, metric = S.prefs.metric;
    const sum = S.data.summary;
    const pts = trendPoints(sum, S.data.hourly, S.data.tz);
    const prev = S.data.previous && S.data.previous.buckets && metric !== "cap" ?
      alignPrev(pts, trendPoints({from_ms: S.data.previous.from, to_ms: S.data.previous.to, buckets: S.data.previous.buckets},
        S.data.hourly, S.data.tz)) : null;
    const spec = trendSpec(metric, pts, prev, occByBucket(S.data.occ));
    E.trendSub.textContent = t(S.data.hourly ? "overview.g.hour" : "overview.g.day");
    const win = Shell.scope.window();
    const fixedWin = win.fixed ? win : S.data.win;
    const opts = {
      x: pts.map(p => p.start), series: spec.series, lines: spec.lines, kind: spec.kind, height: UI.isPhone() ? 180 : 220,
      yFormat: spec.y, label: t("overview.trend") + " · " + t("overview.m." + metric),
      xFormat: S.data.hourly ? ms => fmt.hm(ms) : ms => fmt.day(ms),
      tipTitle: i => fmt.time(pts[i].from, true) + " – " + fmt.time(pts[i].to, true) + (pts[i].partial ? t("overview.partial") : ""),
      tipValue: (sr, v) => spec.v(v),
      tipExtra: i => tipExtra(metric, pts[i], spec, i),
      hint: UI.isPhone() ? t("overview.trendHintTouch") : t("overview.trendHint"),
      onClick: i => {
        if (!isCurrent()) return;
        Shell.go(Shell.href("requests", bucketParams(fixedWin, pts[i], metric)));
      },
      onBrush: (i0, i1) => {
        if (!isCurrent()) return;
        Shell.scope.set({from: pts[i0].from, to: pts[i1].to});
      },
    };
    if (!S.chart) {
      Core.clear(E.trendBox);
      S.chart = Charts.timeSeries(E.trendBox, opts);
    } else S.chart.update(opts);
  }

  // ---------- problems ----------
  function renderProblems() {
    const box = S.els.problems, tot = S.data.summary.total;
    const fc = tot.failure_classes || {};
    const failRows = FCLS.filter(c => fc[c]).map(c => ({key: c, label: t("overview.fc." + c), value: fc[c], color: "err",
      href: Shell.href("requests", drillParams("failed", c))}));
    const classed = FCLS.reduce((a, c) => a + num(fc[c]), 0);
    if (num(tot.failures) > classed) {
      failRows.push({key: "rest", label: t("overview.fc.other"), value: num(tot.failures) - classed, color: "err",
        href: Shell.href("requests", drillParams("failed"))});
    }
    const unRows = [["cancelled", tot.cancelled], ["incomplete", tot.incomplete], ["unknown", tot.unknown]]
      .map(([k, v]) => ({key: k, label: t("overview.u." + k), value: num(v), color: "neutral",
        href: Shell.href("requests", drillParams(k))}));
    const max = Math.max(1, ...failRows.map(r => r.value), ...unRows.map(r => r.value));
    const head = (title, tip, n, link) => h("div", "ov-group-head",
      h("span", "ov-group-title", title), h("span", "ov-group-tip", tip), h("span", "grow"),
      link ? drill(fmt.exact(n), link, {className: "num strong"}) : h("span", "num strong", fmt.exact(n)));
    Core.fill(box,
      head(t("overview.failedGroup"), t("overview.failedGroupTip"), num(tot.failures), drillParams("failed")),
      failRows.length ? bars(failRows, max) : h("div", "ov-none", UI.icon("check"), t("overview.noFailures")),
      head(t("overview.unfinishedGroup"), t("overview.unfinishedGroupTip"),
        num(tot.cancelled) + num(tot.incomplete) + num(tot.unknown), null),
      bars(unRows, max));
  }

  // Horizontal bars as drill links (fixed category order, shared scale within the card).
  function bars(rows, max) {
    return h("div", "hbars ov-bars", rows.map(r => {
      const fill = h("span", "hbar-fill sw-" + r.color + (r.value ? "" : " zero"));
      fill.style.setProperty("width", (r.value / max) * 100 + "%");
      const el = h("a", {class: "hbar ov-hbar", href: r.href}, h("span", "hbar-label", h("span", "trunc", r.label)),
        h("span", "hbar-track", fill), h("span", "hbar-value", fmt.exact(r.value)));
      el.addEventListener("click", e => {
        if (!isCurrent()) e.preventDefault();
      });
      return el;
    }));
  }

  function renderRecent() {
    const box = S.els.recent;
    if (!box || !S.recent) return;
    const head = h("div", "ov-recent-head", h("span", "ov-group-title", t("overview.recent")), h("span", "grow"),
      h("a", {class: "btn btn-ghost btn-sm", href: Shell.href("requests", {fproblem: "error"})}, t("overview.all"),
        UI.icon("chevron-right")));
    if (!S.recent.length) {
      Core.fill(box, head, h("div", "ov-none muted", t("overview.recentNone")));
      return;
    }
    const list = h("div", "ov-recent-list", S.recent.map(r => {
      // the failure class names it; the HTTP status only when it is an error status (a broken
      // stream may have answered 200 first)
      const cls = r.failure_class && I18n.has("overview.fc." + r.failure_class) ? t("overview.fc." + r.failure_class) : null;
      const status = r.status >= 400 ? String(r.status) : null;
      return h("a", {class: "ov-recent-row", href: Shell.href("requests", {fproblem: "error", record: r.request_id})},
        h("span", "ov-recent-status", UI.dot("err"), h("span", null, cls || status || fmt.NONE),
          cls && status ? h("span", "mono muted", status) : null),
        h("span", "ov-recent-main trunc", h("span", "strong", r.model || fmt.NONE),
          r.error ? h("span", "muted mono", " · " + r.error) : null),
        h("span", "ov-recent-meta muted trunc", r.account || ""),
        h("span", {class: "ov-recent-time muted num", title: fmt.time(r.ts_ms)}, fmt.ago(r.ts_ms)));
    }));
    Core.fill(box, head, list);
  }

  // ---------- capacity ----------
  function poolRow(label, p) {
    const avg = p.avg;
    const segs = h("span", "ov-pool-segs", p.members.map(m => {
      const seg = h("span", "ov-pool-seg" + (m.rem === null ? " unknown" : ""));
      if (m.rem !== null) {
        const fill = h("span", "ov-pool-fill" + (m.rem < 5 ? " err" : m.rem < 20 ? " warn" : ""));
        fill.style.setProperty("width", Math.min(100, m.rem) + "%");
        seg.appendChild(fill);
      }
      return seg;
    }));
    const btn = h("button", {type: "button", class: "ov-pool", "aria-haspopup": "dialog", "aria-expanded": "false",
      "aria-label": label + " · " + (avg === null ? fmt.NONE : t("overview.left", {p: Math.round(avg) + "%"})) + " · " +
        t("overview.nAccounts", {n: p.members.length})},
    h("span", "ov-pool-label", h("span", "strong", label), h("span", "muted", t("overview.nAccounts", {n: p.members.length}))),
    segs, h("span", "ov-pool-value num" + (avg !== null && avg < 20 ? " c-warn" : ""), avg === null ? fmt.NONE : Math.round(avg) + "%"));
    btn.addEventListener("click", () => {
      const list = h("div", "ov-pool-pop", h("div", "ov-pool-pop-title", label + " · " + t("overview.quotaLeft")),
        p.members.map(m => h("div", "ov-pool-pop-row",
          h("span", "trunc", m.email || m.id),
          Charts.meter({value: m.rem === null ? null : m.rem / 100, tone: "auto",
            text: m.rem === null ? (m.relogin ? t("overview.relogin") : t("overview.quotaUnknown")) : Math.round(m.rem) + "%"}))));
      if (UI.isPhone()) UI.sheet({title: label + " · " + t("overview.quotaLeft"), body: list});
      else UI.popover(btn, list, {className: "ov-pool-popover"});
    });
    return btn;
  }

  function renderCapacity() {
    const box = S.els.capacity;
    const q = Shell.status.quota;
    const accts = q && Array.isArray(q.accounts) ? q.accounts : null;
    const parts = [];
    // quota pool
    const pools = accts ? [["s", t("term.window5h")], ["l", t("term.windowWeek")]].map(([slot, label]) => {
      const p = pool(accts, slot);
      return p ? poolRow(label, p) : null;
    }).filter(Boolean) : [];
    parts.push(h("div", "ov-cap-block",
      h("div", "ov-cap-head", h("span", "ov-group-title", t("overview.quotaLeft")), UI.info(t("overview.quotaInfo"))),
      pools.length ? h("div", "ov-pools", pools) : h("div", "ov-none muted", t("overview.quotaNone"))));
    // live concurrency
    const rt = Shell.status.runtime && Shell.status.runtime.runtime;
    const live = h("div", "ov-cap-block");
    live.appendChild(h("div", "ov-cap-head", h("span", "ov-group-title", t("overview.live")),
      rt ? UI.info(t("overview.liveInfo", {limit: rt.limit ? fmt.exact(rt.limit) : t("overview.noLimit")})) : null));
    if (!rt) live.appendChild(h("div", "ov-none muted", Api.noAdmin() ? t("overview.liveNoAdmin") : fmt.NONE));
    else if (rt.draining) live.appendChild(h("div", "ov-live-row", UI.tag(t("overview.draining"), "err", {dot: true})));
    else {
      const near = !!rt.limit && rt.inflight / rt.limit >= 0.8;
      live.appendChild(h("div", "ov-live-row",
        h("span", "ov-live-k", t("overview.inflight")),
        rt.limit ? Charts.meter({value: rt.inflight / rt.limit, tone: near ? "warn" : "accent", text: ""}) : h("span", "grow"),
        h("span", "ov-live-v num", h("span", "strong", fmt.exact(rt.inflight)),
          h("span", "muted", " / " + (rt.limit ? fmt.exact(rt.limit) : t("overview.noLimit"))))));
      live.appendChild(h("div", "ov-live-row", h("span", "ov-live-k", "WS"), h("span", "grow"),
        h("span", "ov-live-v num", h("span", "strong", fmt.exact(rt.ws_connections)))));
    }
    const pk = occPeak(S.data.occ);
    if (pk) {
      const b = h("button", {type: "button", class: "ov-peak", title: t("overview.peakInfo")},
        h("span", "ov-live-k", t("overview.peak")),
        h("span", "ov-live-v num", t("overview.inflight") + " " + fmt.exact(pk.inf) + (pk.li ? "/" + fmt.exact(pk.li) : "") +
          " · WS " + fmt.exact(pk.ws) + (pk.lw ? "/" + fmt.exact(pk.lw) : "")),
        pk.full ? h("span", "c-warn small", t("overview.full", {n: fmt.exact(pk.full)})) : null,
        UI.icon("chevron-right"));
      b.addEventListener("click", () => {
        S.els.metricSeg.setValue("cap");
        setPref("metric", "cap");
        S.els.trendCard.scrollIntoView({block: "start", behavior: "smooth"});
      });
      live.appendChild(b);
    }
    parts.push(live);
    // refusals
    const R = S.data.refusals;
    const ref = h("div", "ov-cap-block");
    ref.appendChild(h("div", "ov-cap-head", h("span", "ov-group-title", t("overview.refused")), UI.info(t("overview.refusedInfo")),
      h("span", "grow"), R ? drill(fmt.exact(num(R.count)), drillParams("refused"), {className: "num strong" +
        (R.count ? " c-warn" : "")}) : null));
    if (!R) ref.appendChild(h("div", "ov-none muted", t("overview.refusedScoped")));
    else if (R.count) {
      ref.appendChild(h("div", "ov-reasons", REASONS.filter(k => R[k]).map(k => h("span", "ov-reason",
        h("span", "muted", t("overview.r." + k)), h("span", "num strong", fmt.exact(R[k]))))));
    }
    parts.push(ref);
    Core.fill(box, parts);
  }

  // ---------- breakdown ----------
  function buildBreakdownCard() {
    const E = S.els;
    E.groupSeg = h("div", "ov-groups");
    E.breakBody = h("div", "ov-break", UI.skeleton.rows(5, 6));
    E.breakFoot = h("div", "ov-break-foot");
    const card = h("section", "card ov-breakdown",
      h("div", "card-head ov-break-head", h("h2", "card-title", t("overview.breakdown")), h("span", "grow"), E.groupSeg),
      E.breakBody, E.breakFoot);
    renderBreakdownControls(E);
    return card;
  }

  function renderBreakdownControls(E) {
    E = E || S.els;
    const g = S.prefs.group;
    const main = MAIN_GROUPS.includes(g);
    const seg = UI.segmented({label: t("overview.groupBy"), value: main ? g : null,
      options: MAIN_GROUPS.map(x => ({value: x, label: t("overview.gr." + x)})), onChange: v => setPref("group", v)});
    const more = h("button", {type: "button", class: "btn btn-sm ov-more" + (main ? "" : " is-set"), "aria-haspopup": "menu",
      "aria-expanded": "false"}, h("span", null, main ? t("overview.moreGroups") : t("overview.gr." + g)), UI.icon("chevron-down"));
    more.addEventListener("click", () => UI.menu(more, GROUPS.filter(x => !MAIN_GROUPS.includes(x)).map(x => ({
      label: t("overview.gr." + x), checked: x === g, onSelect: () => setPref("group", x)})), {title: t("overview.groupBy")}));
    Core.fill(E.groupSeg, seg, more);
  }

  function rowAction(g, x) {
    const v = x.group ? x.group[g] : null;
    return groupAction(g, v);
  }

  function applyAction(act) {
    if (!act || !isCurrent()) return;
    if (act.scope) {
      Shell.scope.set(act.scope);
      scrollTo({top: 0, behavior: "smooth"});
    } else Shell.go(Shell.href("requests", act.params));
  }

  function nameCell(g, x, maxCount, total) {
    const v = x.group ? x.group[g] : null;
    const name = total ? t("overview.total") : groupName(g, v);
    const act = total ? null : rowAction(g, x);
    const label = act ? h("button", {type: "button", class: "ov-name-btn trunc",
      title: act.scope ? t("overview.filterTo", {v: name}) : t("overview.viewRequests") + " · " + name}, name) :
      h("span", "trunc" + (total ? " strong" : ""), name);
    if (act) label.addEventListener("click", () => applyAction(act));
    const bar = h("span", "ov-share");
    if (!total) {
      const fill = h("span", "ov-share-fill");
      fill.style.setProperty("width", (maxCount ? (num(x.count) / maxCount) * 100 : 0) + "%");
      bar.appendChild(fill);
    }
    return h("div", "ov-name", label, total ? null : bar);
  }

  function failCell(x) {
    const r = x.failure_rate;
    const tip = t("overview.rowFailures", {f: fmt.exact(num(x.failures)), c: fmt.exact(num(x.cancelled)),
      i: fmt.exact(num(x.incomplete)), u: fmt.exact(num(x.unknown))}) +
      (x.failure_classes ? FCLS.filter(c => x.failure_classes[c]).map(c => "\n" + t("overview.fc." + c) + " " +
        fmt.exact(x.failure_classes[c])).join("") : "");
    return h("span", {class: num(r) >= 0.05 ? "c-err" : "", title: tip}, fmt.pct(r));
  }

  function costCell(x) {
    const c = x.cost_usd, u = num(x.unknown_price);
    const unpricedTip = u ? t("overview.info.costUnpriced", {n: fmt.exact(u)}) : "";
    const title = [known(c) ? fmt.exactUsd(c) : "", unpricedTip].filter(Boolean).join(" · ");
    return h("span", {title: title || null, class: u && !c ? "muted" : ""}, costText(c, u));
  }

  function tokensCell(x) {
    const v = tokenTotal(x);
    return h("span", {title: t("overview.rowTokens", {s: fmt.exact(num(x.usage_samples)), i: fmt.exact(num(x.input_tokens)),
      c: fmt.exact(num(x.cached_tokens)), o: fmt.exact(num(x.output_tokens))})}, known(v) ? fmt.n(v) : fmt.NONE);
  }

  function columns(g, maxCount) {
    return [
      {id: "name", label: t("overview.gr." + g), className: "ov-col-name", render: r => nameCell(g, r.x, maxCount, r.total)},
      {id: "count", label: t("term.requests"), num: true, render: r => h("span", {title: fmt.exact(r.x.count)}, fmt.n(r.x.count))},
      {id: "fail", label: t("term.errorRate"), num: true, render: r => failCell(r.x)},
      {id: "up", label: t("overview.col.upstream"), num: true, priority: 3,
        render: r => h("span", {title: fmt.exact(num(r.x.upstream_failures))}, fmt.pct(r.x.upstream_failure_rate))},
      {id: "ttft", label: t("overview.col.ttft"), num: true, priority: 2,
        render: r => h("span", {title: t("overview.p90", {v: fmt.ms(r.x.stream_ttft_p90_ms)}) + " · " +
          t("overview.samples", {n: fmt.exact(num(r.x.stream_ttft_samples))})}, fmt.ms(r.x.stream_ttft_p50_ms))},
      {id: "dur", label: t("overview.col.duration"), num: true, priority: 3,
        render: r => h("span", {title: t("overview.p90", {v: fmt.ms(r.x.duration_p90_ms)}) + " · " +
          t("overview.samples", {n: fmt.exact(num(r.x.duration_samples))})}, fmt.ms(r.x.duration_p50_ms))},
      {id: "retry", label: t("overview.col.retried"), num: true, priority: 3,
        render: r => h("span", {title: t("overview.rowRetry", {r: fmt.exact(num(r.x.retried)), s: fmt.exact(num(r.x.switched))})},
          r.x.retried ? fmt.exact(r.x.retried) : fmt.NONE)},
      {id: "dg", label: t("overview.col.downgraded"), num: true, priority: 3,
        render: r => (r.x.downgraded ? fmt.exact(r.x.downgraded) : fmt.NONE)},
      {id: "tok", label: t("term.tokens"), num: true, render: r => tokensCell(r.x)},
      {id: "cost", label: t("term.cost"), num: true, priority: 2, render: r => costCell(r.x)},
    ];
  }

  // Phone: one card per group (name, requests, error rate); a tap shows the whole row in a sheet.
  function phoneList(g, rows, maxCount) {
    return h("div", "list ov-break-list", rows.map(r => {
      const x = r.x;
      const item = h("button", {type: "button", class: "list-item ov-break-item" + (r.total ? " total" : "")},
        h("div", "list-line", nameCell(g, x, maxCount, r.total), h("span", "grow"),
          h("span", "num strong", fmt.n(x.count))),
        h("div", "list-line sub", h("span", null, t("term.errorRate") + " "), failCell(x), h("span", null, " · " + t("term.tokens") + " "),
          tokensCell(x), h("span", null, " · "), costCell(x)));
      item.addEventListener("click", e => {
        if (e.target.closest(".ov-name-btn")) return;
        openRowSheet(g, r);
      });
      return item;
    }));
  }

  function openRowSheet(g, r) {
    const x = r.x;
    const name = r.total ? t("overview.total") : groupName(g, x.group ? x.group[g] : null);
    const act = r.total ? null : rowAction(g, x);
    const pairs = [
      [t("term.requests"), fmt.exact(x.count)],
      [t("term.errorRate"), failCell(x)],
      [t("overview.col.upstream"), fmt.pct(x.upstream_failure_rate)],
      [t("overview.col.ttft"), fmt.ms(x.stream_ttft_p50_ms) + " · " + t("overview.p90", {v: fmt.ms(x.stream_ttft_p90_ms)})],
      [t("overview.col.duration"), fmt.ms(x.duration_p50_ms) + " · " + t("overview.p90", {v: fmt.ms(x.duration_p90_ms)})],
      [t("overview.col.retried"), t("overview.rowRetry", {r: fmt.exact(num(x.retried)), s: fmt.exact(num(x.switched))})],
      [t("overview.col.downgraded"), fmt.exact(num(x.downgraded))],
      [t("term.tokens"), tokensCell(x)],
      [t("term.cost"), costCell(x)],
    ];
    let layer = null;
    const actions = act ? [UI.button(act.scope ? t("overview.filterTo", {v: name}) : t("overview.viewRequests"),
      {kind: "primary", onClick: () => {
        layer.close();
        applyAction(act);
      }})] : null;
    layer = UI.sheet({title: name, body: h("div", "stack", UI.kv(pairs),
      h("div", "small muted", t("overview.rowFailures", {f: fmt.exact(num(x.failures)), c: fmt.exact(num(x.cancelled)),
        i: fmt.exact(num(x.incomplete)), u: fmt.exact(num(x.unknown))})),
      actions ? h("div", "row", actions) : null)});
  }

  function renderBreakdown() {
    const E = S.els, s = S.data.summary;
    const g = (s.group_by && s.group_by[0]) || S.prefs.group;
    const groups = s.groups || [];
    if (!groups.length) {
      Core.fill(E.breakBody, UI.empty({icon: "inbox", title: t("overview.emptyRange"), body: t("overview.emptyRangeHint")}));
    } else {
      const maxCount = Math.max(1, ...groups.map(x => num(x.count)));
      const rows = groups.map((x, i) => ({key: "g" + i + "|" + JSON.stringify(x.group || {}), x}))
        .concat([{key: "total", x: s.total, total: true}]);
      Core.steady([E.breakBody], () => {
        if (UI.isPhone()) {
          Core.fill(E.breakBody, phoneList(g, rows, maxCount));
        } else {
          const tbl = UI.table({columns: columns(g, maxCount), keyOf: r => r.key, compact: false, label: t("overview.breakdown")});
          tbl.setRows(rows);
          const last = tbl.rowOf("total");
          if (last) last.classList.add("ov-total");
          Core.fill(E.breakBody, tbl.el);
        }
      });
    }
    // prices
    Core.clear(E.breakFoot);
    const um = s.unknown_price_models || [];
    if (s.price_models === 0) {
      E.breakFoot.appendChild(h("div", "ov-price-note", UI.icon("alert"), h("span", null, t("overview.noPrices")),
        h("a", {class: "btn btn-link", href: "/ui/settings#prices"}, t("overview.fetchPrices"))));
    } else if (um.length) {
      const names = h("span", "muted trunc", um.join(" · "));
      E.breakFoot.appendChild(h("div", {class: "ov-price-note", title: um.join("\n")}, UI.icon("alert"),
        h("span", "ov-nowrap", t("overview.unpricedModels", {n: um.length})), names,
        h("a", {class: "btn btn-link ov-nowrap", href: "/ui/settings#prices"}, t("overview.setPrices"))));
    }
  }

  // ---------- all ----------
  function renderAll() {
    if (!S || !S.data) return;
    renderHero();
    renderTrend();
    renderProblems();
    renderCapacity();
    renderBreakdown();
    renderRecent();
    renderNotice();
    markStale();
  }

  function relabel() {
    // static labels built once at mount
    const E = S.els;
    E.trendTitle.textContent = t("overview.trend");
    E.metricSeg.setLabels(METRICS.map(m => t("overview.m." + m)));
    E.grainSeg.setLabels(GRAINS.map(g => t("overview.g." + g)));
    E.compareLabel.lastChild.textContent = t("overview.compare");
    E.prewarmLabel.lastChild.textContent = t("overview.prewarm");
    Core.fill(E.prewarmInfo, UI.info(t("overview.prewarmInfo")));
    E.problemsHead.firstChild.textContent = t("overview.problems");
    E.capacityHead.firstChild.textContent = t("overview.capacity");
    E.capacityHead.lastChild.firstChild.textContent = t("nav.accounts");
    E.breakdownCard.querySelector(".card-title").textContent = t("overview.breakdown");
    renderBreakdownControls();
  }

  return {
    title: () => I18n.t("nav.overview"),
    scope: ["time", "account", "model", "key", "instance"],
    model,
    mount(root, ctx) {
      S = freshState(ctx);
      build(root);
      ctx.onRefresh(o => loadAll(o));
      ctx.onScope(sc => {
        S.els.prewarm.checked = !!sc.prewarm;
        markStale();
        ctx.busy(loadAll());
      });
      ctx.onStatus(st => {
        if (Views.system && Views.system.noteRuntime) Views.system.noteRuntime(st);
        if (S.data) renderCapacity();
      });
      ctx.onCleanup(() => {
        if (S && S.chart) S.chart.destroy();
      });
      let phone = UI.isPhone();
      ctx.listen(window, "resize", Core.debounce(() => {
        if (UI.isPhone() === phone || !S.data) return;
        phone = UI.isPhone();
        renderBreakdown();
        renderTrend();
      }, 150));
      ctx.busy(loadAll());
    },
    unmount() {
      S = null;
    },
    onRoute(route) {
      if (!S) return;
      const p = readPrefs(route.query, k => Core.local.get(k));
      const changed = p.metric !== S.prefs.metric || p.mode !== S.prefs.mode || p.group !== S.prefs.group;
      if (!changed) return;
      const reload = p.mode !== S.prefs.mode || p.group !== S.prefs.group;
      Object.assign(S.prefs, {metric: p.metric, mode: p.mode, group: p.group});
      S.els.metricSeg.setValue(p.metric);
      S.els.grainSeg.setValue(p.mode);
      renderBreakdownControls();
      if (reload) S.ctx.busy(loadSummary().catch(e => S.ctx.fail(e)));
      else renderTrend();
    },
    onLang() {
      if (!S) return;
      relabel();
      renderAll();
    },
  };
})();
