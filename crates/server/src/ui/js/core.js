"use strict";
// Core: DOM helpers, storage, locale-aware formatting, keyed lists, focus/scroll keeping.
// All files share one script scope; each exposes exactly one namespace. Load-time code here only
// defines things — anything touching `document` runs from Core.init() (called by Shell.start()).

// View registry: js/views/<name>.js assigns Views.<name> = { title, mount, unmount, ... }.
const Views = {};

const Core = (() => {
  const SVG_NS = "http://www.w3.org/2000/svg";

  // ---------- DOM ----------
  const $ = (sel, root) => (root || document).querySelector(sel);
  const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));
  const byId = id => document.getElementById(id);

  // Children: nodes, strings / numbers (as text nodes), arrays (flattened); null/undefined/false skipped.
  function add(el, kids) {
    for (const k of kids) {
      if (k === null || k === undefined || k === false) continue;
      if (Array.isArray(k)) add(el, k);
      else el.appendChild(typeof k === "object" ? k : document.createTextNode(String(k)));
    }
    return el;
  }

  // Attributes: class, text (textContent), on<event> (listener), css ({prop: value} through CSSOM,
  // never a style attribute — the CSP forbids those), dataset ({k: v}); true → empty attribute;
  // null / undefined / false → omitted.
  function setAttrs(el, attrs) {
    for (const k in attrs) {
      const v = attrs[k];
      if (v === null || v === undefined || v === false) continue;
      if (k === "class") el.setAttribute("class", v);
      else if (k === "text") el.textContent = String(v);
      else if (k === "css") for (const p in v) el.style.setProperty(p, String(v[p]));
      else if (k === "dataset") for (const d in v) el.dataset[d] = String(v[d]);
      else if (k.startsWith("on") && typeof v === "function") el.addEventListener(k.slice(2), v);
      else if (k === "style") throw new Error("Core.h: use css:{} instead of a style attribute");
      else el.setAttribute(k, v === true ? "" : String(v));
    }
  }

  function h(tag, attrs, ...kids) {
    const el = document.createElement(tag);
    if (typeof attrs === "string") el.className = attrs;
    else if (attrs) setAttrs(el, attrs);
    return add(el, kids);
  }

  function svg(tag, attrs, ...kids) {
    const el = document.createElementNS(SVG_NS, tag);
    if (typeof attrs === "string") el.setAttribute("class", attrs);
    else if (attrs) setAttrs(el, attrs);
    return add(el, kids);
  }

  function clear(el) {
    while (el.firstChild) el.removeChild(el.firstChild);
    return el;
  }

  // Replace all children in one step.
  function fill(el, ...kids) {
    clear(el);
    return add(el, kids);
  }

  // ---------- storage (each access may throw in private mode / blocked storage) ----------
  function store(kind) {
    const area = () => (kind === "session" ? globalThis.sessionStorage : globalThis.localStorage);
    return {
      get(k) {
        try {
          return area().getItem(k);
        } catch (_) {
          return null;
        }
      },
      set(k, v) {
        try {
          area().setItem(k, String(v));
        } catch (_) {
          // storage unavailable: the preference lasts for this page only
        }
      },
      del(k) {
        try {
          area().removeItem(k);
        } catch (_) {
          // ignore
        }
      },
    };
  }
  const local = store("local");
  const session = store("session");

  // ---------- formatting ----------
  // Locale comes from I18n (loaded after this file); formatting reads it lazily at call time.
  const loc = () => (typeof I18n !== "undefined" ? I18n.locale() : "zh-CN");
  const tr = (k, v) => (typeof I18n !== "undefined" ? I18n.t(k, v) : k);
  const NONE = "—";
  const p2 = x => String(x).padStart(2, "0");
  const nfCache = new Map();
  function nf(opts) {
    const key = loc() + JSON.stringify(opts || {});
    if (!nfCache.has(key)) nfCache.set(key, new Intl.NumberFormat(loc(), opts));
    return nfCache.get(key);
  }
  const isNum = n => n !== null && n !== undefined && n !== "" && Number.isFinite(Number(n));

  const fmt = {
    NONE,
    // Grouped exact integer / decimal: 12,345.
    exact(n) {
      return isNum(n) ? nf().format(Number(n)) : "";
    },
    // Compact count: 950 · 1.2k · 3.40M · 1.20B (same in both languages, as in tables).
    n(n) {
      if (!isNum(n)) return NONE;
      n = Number(n);
      const a = Math.abs(n);
      if (a >= 1e9) return (n / 1e9).toFixed(2) + "B";
      if (a >= 1e6) return (n / 1e6).toFixed(2) + "M";
      if (a >= 1e3) return (n / 1e3).toFixed(1) + "k";
      return String(Math.round(n));
    },
    // Durations from milliseconds: 320ms · 1.24s · 12.3s · 4m 12s.
    ms(n) {
      if (!isNum(n)) return NONE;
      n = Number(n);
      if (n < 1000) return Math.round(n) + "ms";
      if (n < 6e4) return (n / 1e3).toFixed(n >= 1e4 ? 1 : 2) + "s";
      const m = Math.floor(n / 6e4);
      return m + "m " + Math.round((n % 6e4) / 1e3) + "s";
    },
    // Estimated USD: $0.00 · <$0.01 · $1,234.56.
    usd(c) {
      if (!isNum(c)) return NONE;
      c = Number(c);
      if (c === 0) return "$0.00";
      if (c > 0 && c < 0.01) return "<$0.01";
      return "$" + c.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2});
    },
    exactUsd(c) {
      return isNum(c) ? "$" + Number(c).toFixed(6) : "";
    },
    // Ratio 0..1 → percent text: 0.0321 → 3.2%. digits defaults to 1 below 10%, else 0.
    pct(x, digits) {
      if (!isNum(x)) return NONE;
      const v = Number(x) * 100;
      if (digits !== undefined) return v.toFixed(digits) + "%";
      // One decimal below 100 (13.8%), without a trailing ".0" (50%).
      return (Math.abs(v) < 100 ? v.toFixed(1).replace(/\.0$/, "") : v.toFixed(0)) + "%";
    },
    // Percent value already in 0..100.
    pct100(v, digits) {
      return isNum(v) ? fmt.pct(Number(v) / 100, digits) : NONE;
    },
    bytes(n) {
      if (!isNum(n)) return NONE;
      n = Number(n);
      const u = ["B", "KB", "MB", "GB", "TB"];
      let i = 0;
      while (Math.abs(n) >= 1024 && i < u.length - 1) {
        n /= 1024;
        i++;
      }
      return (i === 0 ? String(Math.round(n)) : n.toFixed(n >= 100 ? 0 : 1)) + " " + u[i];
    },
    // Tokens in → out: "1.7k → 233".
    io(input, output) {
      return fmt.n(input) + " → " + fmt.n(output);
    },
    // Local wall time "MM-DD HH:mm:ss" (year prefix when not this year).
    time(ms, noSec) {
      if (!isNum(ms)) return NONE;
      const d = new Date(Number(ms));
      const y = d.getFullYear() !== new Date().getFullYear() ? d.getFullYear() + "-" : "";
      const hm = p2(d.getHours()) + ":" + p2(d.getMinutes());
      return y + p2(d.getMonth() + 1) + "-" + p2(d.getDate()) + " " + hm + (noSec ? "" : ":" + p2(d.getSeconds()));
    },
    hms(ms) {
      const d = new Date(Number(ms));
      return p2(d.getHours()) + ":" + p2(d.getMinutes()) + ":" + p2(d.getSeconds());
    },
    // Short date for axes: "11-16" / "Nov 16".
    day(ms) {
      return new Intl.DateTimeFormat(loc(), {month: "short", day: "numeric"}).format(new Date(Number(ms)));
    },
    hm(ms) {
      const d = new Date(Number(ms));
      return p2(d.getHours()) + ":" + p2(d.getMinutes());
    },
    tzOffset(ms) {
      const d = new Date(isNum(ms) ? Number(ms) : Date.now());
      const o = -d.getTimezoneOffset(), a = Math.abs(o);
      return "UTC" + (o >= 0 ? "+" : "-") + p2(Math.floor(a / 60)) + ":" + p2(a % 60);
    },
    // ISO-8601 with the local offset: 2026-11-16T10:00:00+08:00.
    iso(ms) {
      const d = new Date(Number(ms));
      return d.getFullYear() + "-" + p2(d.getMonth() + 1) + "-" + p2(d.getDate()) + "T" + p2(d.getHours()) + ":" +
        p2(d.getMinutes()) + ":" + p2(d.getSeconds()) + fmt.tzOffset(ms).slice(3);
    },
    // Countdown from seconds: 3天 9时 / 1时 06分 / 4分 12秒 (zh) · 3d 9h / 1h 06m / 4m 12s (en).
    dur(s) {
      if (!isNum(s)) return NONE;
      s = Number(s);
      if (s <= 0) return tr("time.due");
      const d = Math.floor(s / 86400), hh = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
      if (d) return tr("time.dh", {d, h: hh});
      if (hh) return tr("time.hm", {h: hh, m: p2(m)});
      return tr("time.ms", {m, s: p2(Math.floor(s % 60))});
    },
    // Relative past time: 刚刚 / 5 分钟前 · just now / 5 min ago.
    ago(ms) {
      if (!isNum(ms)) return NONE;
      const s = Math.max(0, (Date.now() - Number(ms)) / 1000);
      if (s < 60) return tr("time.now");
      if (s < 3600) return tr("time.minAgo", {n: Math.floor(s / 60)});
      if (s < 86400) return tr("time.hourAgo", {n: Math.floor(s / 3600)});
      return tr("time.dayAgo", {n: Math.floor(s / 86400)});
    },
  };

  // ---------- keyed lists ----------
  // Reconcile `container`'s children with `items`, reusing the node of every key that is still
  // present (focus, selection and transitions survive), creating nodes for new keys and removing
  // the rest. create(item) → Node; update(node, item) refreshes it in place. Node order follows
  // `items`. Returns {created: Node[], removed: number}.
  function keyed(container, items, keyOf, create, update) {
    const old = new Map();
    for (const n of Array.from(container.children)) if (n.dataset && n.dataset.key !== undefined) old.set(n.dataset.key, n);
    const created = [];
    let ref = container.firstElementChild;
    const seen = new Set();
    for (const item of items) {
      const k = String(keyOf(item));
      if (seen.has(k)) continue;
      seen.add(k);
      let n = old.get(k);
      if (n) {
        old.delete(k);
        if (update) update(n, item);
      } else {
        n = create(item);
        n.dataset.key = k;
        created.push(n);
      }
      if (n !== ref) container.insertBefore(n, ref);
      else ref = ref.nextElementSibling;
    }
    // Focus inside a removed node moves to the row now at its place (or the last row).
    const active = document.activeElement;
    let lostAt = -1;
    for (const n of old.values()) {
      if (active && n.contains(active)) lostAt = Array.prototype.indexOf.call(container.children, n);
      n.remove();
    }
    for (const n of Array.from(container.children)) if (!n.dataset || n.dataset.key === undefined) n.remove();
    if (lostAt >= 0 && container.children.length) {
      const row = container.children[Math.min(lostAt, container.children.length - 1)];
      const target = row.matches("[tabindex],a,button") ? row : row.querySelector("[tabindex],a,button");
      if (target) target.focus({preventScroll: true});
    }
    return {created, removed: old.size};
  }

  // ---------- focus keeping across re-renders ----------
  // The last focused control carrying data-fk gets focus back after a render replaced it.
  // A pointer press elsewhere forgets it.
  let lastFk = null;
  function refocus() {
    if (!lastFk) return;
    const a = document.activeElement;
    if (a && a !== document.body && a.isConnected) return;
    const t = document.querySelector('[data-fk="' + CSS.escape(lastFk) + '"]');
    if (t && t.offsetParent !== null) t.focus({preventScroll: true});
  }

  // ---------- steady re-render ----------
  // f() replaces the content of `boxes`. Focus inside them is released first (removing a focused
  // node makes the browser lay out the half-empty page and commit a clamped scroll); the focused
  // control, or else the anchored row nearest the top of the view, keeps its viewport position.
  // Anchors carry data-fk / data-key / data-anchor, stable across renders.
  const ANCHORS = ["data-fk", "data-anchor", "data-key"];
  const ANCHOR_SEL = ANCHORS.map(a => "[" + a + "]").join(",");
  function anchorSel(e) {
    for (const a of ANCHORS) if (e.hasAttribute(a)) return "[" + a + '="' + CSS.escape(e.getAttribute(a)) + '"]';
    return null;
  }
  function topEdge() {
    const bar = document.querySelector("[data-topbar]");
    return bar ? bar.getBoundingClientRect().bottom : 0;
  }
  function pickAnchor() {
    const top = topEdge();
    const vis = e => {
      const r = e.getBoundingClientRect();
      return r.height > 0 && r.bottom > top && r.top < innerHeight;
    };
    const a = document.activeElement;
    if (a && a !== document.body && a.closest) {
      const e = a.closest(ANCHOR_SEL);
      if (e && vis(e)) return e;
    }
    for (const f of [0.5, 0.25, 0.75]) {
      for (let y = top + 8; y < innerHeight; y += 48) {
        const e = document.elementFromPoint(innerWidth * f, y);
        const t = e && e.closest && e.closest(ANCHOR_SEL);
        if (t && vis(t)) return t;
      }
    }
    return null;
  }
  function steady(boxes, f) {
    boxes = boxes.filter(Boolean);
    const a = document.activeElement;
    if (a && a !== document.body && boxes.some(b => b.contains(a))) a.blur();
    // At the top of the page there is nothing to hold: content growing above an anchor must not scroll it away.
    const e = window.scrollY > 0 ? pickAnchor() : null;
    const sel = e && anchorSel(e);
    const before = e ? e.getBoundingClientRect().top : 0;
    f();
    const n = sel && document.querySelector(sel);
    if (n && n.getBoundingClientRect().height > 0) {
      const d = n.getBoundingClientRect().top - before;
      if (Math.abs(d) >= 1) scrollBy(0, d);
    }
    refocus();
  }

  // ---------- live countdowns: elements with data-until (unix ms) ----------
  const countdowns = new Set();
  function track(e) {
    if (e.nodeType !== 1) return;
    if (e.hasAttribute("data-until")) countdowns.add(e);
    for (const n of e.querySelectorAll("[data-until]")) countdowns.add(n);
  }
  function tickCountdowns() {
    if (document.hidden || !countdowns.size) return;
    for (const e of countdowns) {
      if (!e.isConnected) {
        countdowns.delete(e);
        continue;
      }
      e.textContent = (e.dataset.pre || "") + fmt.dur((Number(e.dataset.until) - Date.now()) / 1000);
    }
  }

  // ---------- misc ----------
  function debounce(fn, ms) {
    let t = null;
    const d = (...a) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...a), ms);
    };
    d.cancel = () => clearTimeout(t);
    return d;
  }

  async function copy(text) {
    await navigator.clipboard.writeText(String(text));
  }

  const clamp = (x, lo, hi) => Math.min(hi, Math.max(lo, x));

  function init() {
    document.addEventListener("focusin", e => {
      const t = e.target.closest && e.target.closest("[data-fk]");
      if (t) lastFk = t.dataset.fk;
      else if (!e.target.closest || !e.target.closest(".toasts")) lastFk = null;
    });
    document.addEventListener("pointerdown", e => {
      if (!e.target.closest || !e.target.closest("[data-fk],.toasts")) lastFk = null;
    }, true);
    track(document.body);
    new MutationObserver(changes => {
      for (const c of changes) {
        if (c.type === "attributes") {
          if (c.target.hasAttribute("data-until")) countdowns.add(c.target);
          else countdowns.delete(c.target);
        } else for (const e of c.addedNodes) track(e);
      }
    }).observe(document.body, {subtree: true, childList: true, attributes: true, attributeFilter: ["data-until"]});
    setInterval(tickCountdowns, 1000);
  }

  return {$, $$, byId, h, svg, add, clear, fill, local, session, fmt, nf, isNum, keyed, steady, refocus, debounce, copy,
    clamp, tickCountdowns, init};
})();
