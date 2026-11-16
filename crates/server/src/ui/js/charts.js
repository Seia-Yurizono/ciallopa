"use strict";
// Charts: hand-drawn SVG / HTML charts following the dataviz rules (single y axis, hairline grid,
// bars <= 24px with a 4px rounded data end, 2px lines, a legend for 2+ series, text in text colours,
// tooltips that list every series at the hovered x). Colours are CSS classes (charts.css) keyed by
// s1..s4 | other | neutral | accent | ok | warn | err, so a theme switch needs no redraw.
// Charts draw at the container's real pixel width and redraw on resize.
const Charts = (() => {
  const {h, svg, clear} = Core;
  const SLOTS = ["s1", "s2", "s3", "s4"];
  const colorOf = (s, i) => s.color || SLOTS[i % SLOTS.length];

  // Round axis maximum and step: ~4 gridlines.
  function niceScale(max, ticks) {
    ticks = ticks || 4;
    if (!(max > 0)) return {max: 1, step: 0.25};
    const raw = max / ticks;
    const mag = Math.pow(10, Math.floor(Math.log10(raw)));
    const step = [1, 2, 2.5, 5, 10].map(m => m * mag).find(s => s >= raw) || 10 * mag;
    return {max: Math.ceil(max / step - 1e-9) * step, step};
  }

  function tickFormat(max) {
    return v => (max < 10000 ? Core.fmt.exact(Math.round(v * 100) / 100) : Core.fmt.n(v));
  }

  // Bar path: square at the baseline, 4px rounded data end (top), r clamped to the bar size.
  function barPath(x, y, w, hgt, rounded) {
    if (hgt <= 0 || w <= 0) return "";
    const r = rounded ? Math.min(4, w / 2, hgt) : 0;
    return "M" + x + "," + (y + hgt) + "V" + (y + r) + (r ? "Q" + x + "," + y + " " + (x + r) + "," + y : "") +
      "H" + (x + w - r) + (r ? "Q" + (x + w) + "," + y + " " + (x + w) + "," + (y + r) : "") + "V" + (y + hgt) + "Z";
  }

  function legend(items) {
    return h("div", "chart-legend", items.map(it => h("span", "legend-item",
      h("span", "legend-key sw-" + (it.color || "s1") + (it.line ? " line" : "")), it.label)));
  }

  // ---------- time series ----------
  // timeSeries(container, opts) → {update(opts), destroy(), el}
  // opts.x: bucket start times (ms); opts.series: [{id, label, values: (number|null)[], color?}];
  // opts.kind: "bar" (series stacked bottom → top, e.g. failed then ok) | "line" | "area";
  // opts.height (px, includes the x-axis band; default 200); opts.yFormat(v); opts.xFormat(ms, i);
  // opts.tipTitle(i); opts.tipValue(series, v, i); opts.hint (last tooltip line, e.g. "click to …");
  // opts.onClick(i) — bucket click / Enter / second tap; opts.onBrush(i0, i1) — mouse drag over
  // buckets (touch drag scrubs instead); opts.legend (default: shown for 2+ series); opts.label.
  function timeSeries(container, opts) {
    let o = opts;
    const root = h("div", {class: "chart", tabindex: "0", role: "img"});
    const tip = h("div", {class: "chart-tip hide", "aria-hidden": "true"});
    const legendBox = h("div");
    const wrap = h("div", "stack", root, legendBox);
    root.appendChild(tip);
    container.appendChild(wrap);
    let geo = null, hover = -1, brush = null, lastTap = {i: -1, at: 0}, width = 0;

    function draw() {
      width = Math.floor(root.clientWidth);
      for (const n of Array.from(root.childNodes)) if (n !== tip) n.remove();
      if (!width) return;
      const kind = o.kind || "bar";
      const n = o.x.length;
      const H = o.height || 200;
      const series = o.series;
      const stackTotals = o.x.map((_, i) => series.reduce((a, s) => a + (Number(s.values[i]) || 0), 0));
      const dataMax = kind === "bar" ? Math.max(0, ...stackTotals) :
        Math.max(0, ...series.flatMap(s => s.values.filter(v => v !== null && v !== undefined).map(Number)));
      const scale = niceScale(dataMax);
      const yFmt = o.yFormat || tickFormat(scale.max);
      const tickLabels = [];
      for (let v = 0; v <= scale.max + 1e-9; v += scale.step) tickLabels.push(yFmt(v));
      const left = Math.max(28, Math.max(...tickLabels.map(s => s.length)) * 6.6 + 10);
      const right = 4, top = 8, axisH = 22;
      const plotW = Math.max(10, width - left - right), plotH = Math.max(20, H - top - axisH);
      const band = n ? plotW / n : plotW;
      const y = v => top + plotH - (v / scale.max) * plotH;
      geo = {left, top, plotW, plotH, band, n};
      root.setAttribute("aria-label", o.label || "");
      const s = svg("svg", {width, height: H, viewBox: "0 0 " + width + " " + H, "aria-hidden": "true"});
      // grid + y ticks
      tickLabels.forEach((lab, k) => {
        const yy = Math.round(y(k * scale.step)) + 0.5;
        s.appendChild(svg("line", {class: k === 0 ? "chart-base" : "chart-grid", x1: left, x2: left + plotW, y1: yy, y2: yy}));
        s.appendChild(svg("text", {class: "chart-tick", x: left - 8, y: yy + 3.5, "text-anchor": "end"}, lab));
      });
      // x labels
      const xFmt = o.xFormat || defaultXFormat(o.x);
      const stride = Math.max(1, Math.ceil((n * 72) / plotW));
      for (let i = 0; i < n; i += stride) {
        const cx = left + band * (i + 0.5);
        s.appendChild(svg("text", {class: "chart-tick", x: cx, y: top + plotH + 16,
          "text-anchor": i === 0 && band < 36 ? "start" : "middle"}, xFmt(o.x[i], i)));
      }
      if (!n || dataMax <= 0) {
        s.appendChild(svg("text", {class: "chart-empty", x: left + plotW / 2, y: top + plotH / 2, "text-anchor": "middle"},
          o.empty || I18n.t("chart.empty")));
      }
      // marks
      const marks = svg("g");
      if (kind === "bar") {
        const bw = Math.max(1, Math.min(24, band * 0.72, band - 2));
        for (let i = 0; i < n; i++) {
          let base = 0;
          const x0 = left + band * i + (band - bw) / 2;
          const parts = series.map((sr, si) => ({sr, si, v: Number(sr.values[i]) || 0})).filter(p => p.v > 0);
          parts.forEach((p, k) => {
            const y1 = y(base), y2 = y(base + p.v);
            base += p.v;
            const gap = k > 0 ? 2 : 0;
            const hgt = Math.max(0, y1 - y2 - gap);
            if (hgt < 0.5) return;
            marks.appendChild(svg("path", {class: "mark-bar c-" + colorOf(p.sr, p.si), "data-i": i,
              d: barPath(round1(x0), round1(y2), round1(bw), round1(hgt), k === parts.length - 1)}));
          });
        }
      } else {
        series.forEach((sr, si) => {
          const pts = sr.values.map((v, i) => (v === null || v === undefined ? null : [left + band * (i + 0.5), y(Number(v))]));
          let d = "", area = "", runStart = null, prev = null;
          pts.forEach((p, i) => {
            if (!p) {
              if (prev && runStart !== null) area += "L" + prev[0] + "," + y(0) + "Z";
              runStart = null;
              prev = null;
              return;
            }
            if (!prev) {
              d += "M" + round1(p[0]) + "," + round1(p[1]);
              area += "M" + round1(p[0]) + "," + y(0) + "L" + round1(p[0]) + "," + round1(p[1]);
              runStart = i;
            } else {
              d += "L" + round1(p[0]) + "," + round1(p[1]);
              area += "L" + round1(p[0]) + "," + round1(p[1]);
            }
            prev = p;
          });
          if (prev && runStart !== null) area += "L" + round1(prev[0]) + "," + y(0) + "Z";
          if (kind === "area" && area) marks.appendChild(svg("path", {class: "mark-area c-" + colorOf(sr, si), d: area}));
          if (d) marks.appendChild(svg("path", {class: "mark-line c-" + colorOf(sr, si) + (sr.dashed ? " dashed" : ""), d}));
          // isolated points (no neighbours) would be invisible as a path: draw a dot
          pts.forEach((p, i) => {
            if (p && !pts[i - 1] && !pts[i + 1]) {
              marks.appendChild(svg("circle", {class: "mark-dot c-" + colorOf(sr, si), cx: p[0], cy: p[1], r: 3}));
            }
          });
        });
      }
      s.appendChild(marks);
      s.appendChild(svg("g", {class: "chart-overlay"}));
      root.insertBefore(s, tip);
      clear(legendBox);
      const showLegend = o.legend !== undefined ? o.legend : series.length > 1;
      if (showLegend) legendBox.appendChild(legend(series.map((sr, si) => ({label: sr.label, color: colorOf(sr, si),
        line: kind !== "bar"}))));
      if (hover >= 0 && hover < n) showHover(hover, false);
    }

    function overlay() {
      return root.querySelector(".chart-overlay");
    }

    function showHover(i, fromPointer, px) {
      const g = overlay();
      if (!g || !geo || i < 0 || i >= geo.n) return hideHover();
      hover = i;
      clear(g);
      const kind = o.kind || "bar";
      const cx = geo.left + geo.band * (i + 0.5);
      if (kind === "bar") {
        g.appendChild(svg("rect", {class: "chart-hover-band", x: geo.left + geo.band * i, y: geo.top, width: geo.band,
          height: geo.plotH}));
        root.classList.add("has-hover");
        for (const b of root.querySelectorAll(".mark-bar")) b.classList.toggle("hot", Number(b.dataset.i) === i);
      } else {
        const xx = Math.round(cx) + 0.5;
        g.appendChild(svg("line", {class: "chart-cursor", x1: xx, x2: xx, y1: geo.top, y2: geo.top + geo.plotH}));
        o.series.forEach((sr, si) => {
          const v = sr.values[i];
          if (v === null || v === undefined) return;
          const sc = niceScale(currentMax());
          const yy = geo.top + geo.plotH - (Number(v) / sc.max) * geo.plotH;
          g.appendChild(svg("circle", {class: "mark-dot c-" + colorOf(sr, si), cx, cy: yy, r: 4}));
        });
      }
      if (brush) drawBrush();
      renderTip(i, px === undefined ? cx : px);
    }

    function currentMax() {
      const kind = o.kind || "bar";
      if (kind === "bar") return Math.max(0, ...o.x.map((_, i) => o.series.reduce((a, s) => a + (Number(s.values[i]) || 0), 0)));
      return Math.max(0, ...o.series.flatMap(s => s.values.filter(v => v !== null && v !== undefined).map(Number)));
    }

    function renderTip(i, px) {
      clear(tip);
      const title = o.tipTitle ? o.tipTitle(i) : defaultTipTitle(o.x, i);
      tip.appendChild(h("div", "chart-tip-title", title));
      const rows = o.series.slice().reverse();
      rows.forEach(sr => {
        const si = o.series.indexOf(sr);
        const v = sr.values[i];
        const text = o.tipValue ? o.tipValue(sr, v, i) : v === null || v === undefined ? Core.fmt.NONE : Core.fmt.exact(v);
        tip.appendChild(h("div", "chart-tip-row", h("span", "legend-key sw-" + colorOf(sr, si)),
          h("span", "chart-tip-name", sr.label), h("span", "chart-tip-value", text)));
      });
      if (o.hint && o.onClick) tip.appendChild(h("div", "chart-tip-hint", o.hint));
      tip.classList.remove("hide");
      const tw = tip.offsetWidth;
      let x = px + 12;
      if (x + tw > width) x = px - tw - 12;
      tip.style.setProperty("left", Math.max(0, x) + "px");
      tip.style.setProperty("top", "4px");
    }

    function hideHover() {
      hover = -1;
      tip.classList.add("hide");
      root.classList.remove("has-hover");
      for (const b of root.querySelectorAll(".mark-bar.hot")) b.classList.remove("hot");
      const g = overlay();
      if (g) clear(g);
    }

    function drawBrush() {
      const g = overlay();
      if (!g || !brush) return;
      const a = Math.min(brush.a, brush.b), b = Math.max(brush.a, brush.b);
      g.appendChild(svg("rect", {class: "chart-brush", x: geo.left + geo.band * a, y: geo.top, width: geo.band * (b - a + 1),
        height: geo.plotH}));
    }

    function indexAt(clientX) {
      if (!geo) return -1;
      const r = root.getBoundingClientRect();
      const x = clientX - r.left - geo.left;
      if (x < 0 || x > geo.plotW) return -1;
      return Core.clamp(Math.floor(x / geo.band), 0, geo.n - 1);
    }

    function localX(clientX) {
      return clientX - root.getBoundingClientRect().left;
    }

    root.addEventListener("pointermove", e => {
      const i = indexAt(e.clientX);
      if (brush && e.pointerType === "mouse") {
        if (i >= 0) brush.b = i;
        showHover(brush.b, true, localX(e.clientX));
        return;
      }
      if (e.pointerType === "mouse" || (e.pointerType !== "mouse" && e.buttons)) {
        if (i < 0) hideHover();
        else showHover(i, true, localX(e.clientX));
      }
    });
    root.addEventListener("pointerleave", e => {
      if (e.pointerType === "mouse" && !brush) hideHover();
    });
    root.addEventListener("pointerdown", e => {
      const i = indexAt(e.clientX);
      if (i < 0) return;
      if (e.pointerType === "mouse") {
        if (e.button !== 0) return;
        brush = o.onBrush ? {a: i, b: i} : null;
        if (brush) root.setPointerCapture(e.pointerId);
        return;
      }
      showHover(i, true, localX(e.clientX));
    });
    root.addEventListener("pointerup", e => {
      const i = indexAt(e.clientX);
      if (e.pointerType === "mouse") {
        const b = brush;
        brush = null;
        if (b && b.a !== b.b) {
          hideHover();
          o.onBrush(Math.min(b.a, b.b), Math.max(b.a, b.b));
          return;
        }
        if (i >= 0 && o.onClick) o.onClick(i);
        if (i >= 0) showHover(i, true, localX(e.clientX));
        return;
      }
      // touch / pen: first tap shows the tooltip, a second tap on the same bucket activates it
      if (i < 0) return;
      const now = Date.now();
      if (o.onClick && lastTap.i === i && now - lastTap.at < 4000) {
        lastTap = {i: -1, at: 0};
        o.onClick(i);
      } else lastTap = {i, at: now};
    });
    root.addEventListener("keydown", e => {
      if (!geo || !geo.n) return;
      if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
        e.preventDefault();
        const step = e.key === "ArrowRight" ? 1 : -1;
        const i = hover < 0 ? (step > 0 ? 0 : geo.n - 1) : Core.clamp(hover + step, 0, geo.n - 1);
        showHover(i, false);
      } else if (e.key === "Enter" && hover >= 0 && o.onClick) {
        e.preventDefault();
        o.onClick(hover);
      } else if (e.key === "Escape" && hover >= 0) {
        e.stopPropagation();
        hideHover();
      }
    });
    root.addEventListener("blur", () => {
      if (!root.matches(":hover")) hideHover();
    });

    const ro = typeof ResizeObserver === "function" ? new ResizeObserver(() => {
      if (Math.floor(root.clientWidth) !== width) draw();
    }) : null;
    if (ro) ro.observe(root);
    const offLang = I18n.onChange(() => draw());
    draw();

    return {
      el: wrap,
      update(next) {
        o = Object.assign({}, o, next);
        draw();
      },
      setLoading(on) {
        root.classList.toggle("loading", !!on);
      },
      destroy() {
        if (ro) ro.disconnect();
        offLang();
        wrap.remove();
      },
    };
  }

  const round1 = v => Math.round(v * 10) / 10;

  function defaultXFormat(xs) {
    const span = xs.length > 1 ? xs[xs.length - 1] - xs[0] : 0;
    return span <= 2 * 864e5 ? ms => Core.fmt.hm(ms) : ms => Core.fmt.day(ms);
  }

  function defaultTipTitle(xs, i) {
    return Core.fmt.time(xs[i], true);
  }

  // ---------- sparkline ----------
  // sparkline(values, {color, width, height, area, label}) → <svg>; null values break the line.
  // The last point carries an end dot.
  function sparkline(values, o) {
    o = o || {};
    const W = o.width || 96, H = o.height || 28, pad = 3;
    const nums = values.filter(v => v !== null && v !== undefined).map(Number);
    const max = Math.max(...nums, 0), min = Math.min(...nums, 0);
    const span = max - min || 1;
    const x = i => pad + (values.length > 1 ? (i * (W - 2 * pad)) / (values.length - 1) : (W - 2 * pad) / 2);
    const y = v => H - pad - ((Number(v) - min) / span) * (H - 2 * pad);
    const color = o.color || "s1";
    const s = svg("svg", {class: "spark", width: W, height: H, viewBox: "0 0 " + W + " " + H, role: o.label ? "img" : null,
      "aria-label": o.label || null, "aria-hidden": o.label ? null : "true"});
    // One line path with gaps at nulls; the optional area wash is closed per contiguous run.
    let d = "", area = "", run = [], lastIdx = -1;
    const closeRun = () => {
      if (run.length) area += "M" + run[0][0] + "," + (H - pad) + run.map(p => "L" + p[0] + "," + p[1]).join("") +
        "L" + run[run.length - 1][0] + "," + (H - pad) + "Z";
      run = [];
    };
    values.forEach((v, i) => {
      if (v === null || v === undefined) {
        closeRun();
        return;
      }
      const pt = [round1(x(i)), round1(y(v))];
      d += (run.length ? "L" : "M") + pt[0] + "," + pt[1];
      run.push(pt);
      lastIdx = i;
    });
    closeRun();
    if (o.area && area) s.appendChild(svg("path", {class: "mark-area c-" + color, d: area}));
    if (d) s.appendChild(svg("path", {class: "mark-line c-" + color, d}));
    if (lastIdx >= 0) s.appendChild(svg("circle", {class: "mark-dot c-" + color, cx: round1(x(lastIdx)), cy: round1(y(values[lastIdx])),
      r: 2.5}));
    return s;
  }

  // ---------- horizontal bars ----------
  // hbars(rows, {format, max}) → element with .update(rows). rows: [{key, label, value, color?, sub?,
  // onClick?, title?}] — fixed order (the caller's); a row with onClick is a button.
  function hbars(rows, o) {
    o = o || {};
    const el = h("div", "hbars");
    el.update = next => {
      const max = o.max || Math.max(0, ...next.map(r => Number(r.value) || 0)) || 1;
      Core.keyed(el, next, r => r.key, r => {
        const node = h(r.onClick ? "button" : "div", {class: "hbar", type: r.onClick ? "button" : null},
          h("span", "hbar-label"), h("span", "hbar-track", h("span", "hbar-fill")), h("span", "hbar-value"));
        node.addEventListener("click", () => node._row && node._row.onClick && node._row.onClick());
        return node;
      }, () => {});
      for (const node of el.children) {
        const r = next.find(x => String(x.key) === node.dataset.key);
        node._row = r;
        const [label, track, value] = node.children;
        Core.fill(label, r.label);
        if (r.title) node.title = r.title;
        const fillEl = track.firstChild;
        fillEl.className = "hbar-fill sw-" + (r.color || "s1") + (Number(r.value) > 0 ? "" : " zero");
        fillEl.style.setProperty("width", ((Number(r.value) || 0) / max) * 100 + "%");
        Core.fill(value, (o.format || Core.fmt.exact)(r.value), r.sub ? h("span", "sub", r.sub) : null);
      }
    };
    el.update(rows);
    return el;
  }

  // ---------- segmented ratio bar ----------
  // segments(parts, {tall, legend, format, label}) — parts: [{label, value, color}]. Widths are
  // proportional; a zero part draws nothing. Thin (8px) by default; tall (28px) prints the share
  // inside a part when it fits. The legend lists names in block order.
  function segments(parts, o) {
    o = o || {};
    const total = parts.reduce((a, p) => a + (Number(p.value) || 0), 0);
    const track = h("div", {class: "segbar-track", role: "img",
      "aria-label": (o.label ? o.label + ": " : "") + parts.map(p => p.label + " " + (o.format || Core.fmt.exact)(p.value)).join(", ")});
    if (!total) track.appendChild(h("span", "segbar-empty"));
    for (const p of parts) {
      const v = Number(p.value) || 0;
      if (!v) continue;
      const share = v / total;
      const part = h("span", {class: "segbar-part sw-" + (p.color || "s1"), title: p.label + " · " + (o.format || Core.fmt.exact)(v) +
        " · " + Core.fmt.pct(share)});
      part.style.setProperty("flex-basis", "calc(" + share * 100 + "% - 2px)");
      part.style.setProperty("flex-grow", "1");
      if (o.tall && share >= 0.08) part.textContent = Core.fmt.pct(share);
      track.appendChild(part);
    }
    const el = h("div", "segbar" + (o.tall ? " tall" : ""), track);
    if (o.legend !== false) {
      el.appendChild(h("div", "chart-legend", parts.map(p => h("span", "legend-item", h("span", "legend-key sw-" + (p.color || "s1")),
        p.label, h("span", "num strong", (o.format || Core.fmt.exact)(p.value))))));
    }
    return el;
  }

  // ---------- meter ----------
  // meter({value, range: [lo, hi], open, tone, label, text, stale, title, tall}) — value and range in
  // 0..1. The solid fill is the known part (value, or lo when a range is given); the hatched part
  // is the estimate range up to hi; open: only a lower bound is known, the range fades out.
  // tone: accent (default) | ok | warn | err | neutral, or "auto" (remaining: < 5% err, < 20% warn).
  function meter(o) {
    const el = h("div", {class: "meter", title: o.title || null});
    const fillEl = h("span", "meter-fill");
    const rangeEl = h("span", "meter-range");
    const track = h("span", {class: "meter-track", role: "meter", "aria-valuemin": "0", "aria-valuemax": "100"}, rangeEl, fillEl);
    const label = h("span", "meter-label");
    const value = h("span", "meter-value");
    el.append(label, track, value);
    el.update = next => {
      o = Object.assign({}, o, next);
      const v = Core.isNum(o.value) ? Core.clamp(Number(o.value), 0, 1) : null;
      const lo = o.range ? Core.clamp(Number(o.range[0]), 0, 1) : v;
      const hi = o.range ? Core.clamp(Number(o.range[1] === null || o.range[1] === undefined ? 1 : o.range[1]), 0, 1) : null;
      let tone = o.tone || "accent";
      if (tone === "auto") tone = v === null ? "neutral" : v < 0.05 ? "err" : v < 0.2 ? "warn" : "accent";
      el.className = "meter" + (tone !== "accent" ? " " + tone : "") + (o.open ? " open" : "") + (o.stale ? " stale" : "") +
        (o.tall ? " tall" : "");
      fillEl.style.setProperty("width", (lo === null ? 0 : lo * 100) + "%");
      rangeEl.style.setProperty("left", (lo === null ? 0 : lo * 100) + "%");
      rangeEl.style.setProperty("width", hi === null || lo === null ? "0%" : Math.max(0, hi - lo) * 100 + "%");
      label.textContent = o.label || "";
      label.classList.toggle("hide", !o.label);
      value.textContent = o.text !== undefined ? o.text : v === null ? Core.fmt.NONE : Core.fmt.pct(v, 0);
      value.classList.toggle("hide", o.text === "");
      if (v === null) track.removeAttribute("aria-valuenow");
      else track.setAttribute("aria-valuenow", String(Math.round(v * 100)));
      track.setAttribute("aria-label", o.ariaLabel || o.label || "");
    };
    el.update({});
    return el;
  }

  return {SLOTS, niceScale, barPath, legend, timeSeries, sparkline, hbars, segments, meter};
})();
