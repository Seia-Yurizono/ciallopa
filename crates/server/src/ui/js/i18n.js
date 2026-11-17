"use strict";
// I18n: dictionaries, t(), language switching, static-markup translation.
// Dictionaries: js/i18n/zh.js and en.js hold shared terms; each view adds its own keys with
// I18n.add("zh", {...}) / I18n.add("en", {...}) at the top of its file. Both languages must define
// the same key set (scripts/shell-check.cjs enforces it). A value is a string with {name}
// placeholders or a function (vars) → string (plurals and the like).
const I18n = (() => {
  const LANGS = ["zh", "en"];
  const LOCALES = {zh: "zh-CN", en: "en"};
  const STORE_KEY = "ciallopa_lang";
  const dicts = {zh: {}, en: {}};
  const listeners = new Set();
  const warned = new Set();
  let current = "zh";

  function add(lang, entries) {
    if (!dicts[lang]) throw new Error("I18n.add: unknown language " + lang);
    for (const k in entries) {
      if (Object.prototype.hasOwnProperty.call(dicts[lang], k)) throw new Error("I18n.add: duplicate key " + lang + ":" + k);
      dicts[lang][k] = entries[k];
    }
  }

  function interpolate(s, vars) {
    if (!vars) return s;
    return s.replace(/\{(\w+)\}/g, (m, name) => (vars[name] === undefined || vars[name] === null ? m : String(vars[name])));
  }

  function t(key, vars) {
    let v = dicts[current][key];
    if (v === undefined) v = dicts.zh[key];
    if (v === undefined) {
      if (!warned.has(key)) {
        warned.add(key);
        if (typeof console !== "undefined") console.warn("i18n: missing key " + key);
      }
      return key;
    }
    return typeof v === "function" ? v(vars || {}) : interpolate(v, vars);
  }

  const has = key => dicts[current][key] !== undefined;
  const lang = () => current;
  const locale = () => LOCALES[current];

  // Default: stored choice, else navigator.language (zh* → 中文, anything else → English).
  function detect(stored, navLang) {
    if (LANGS.includes(stored)) return stored;
    return /^zh\b/i.test(navLang || "") ? "zh" : "en";
  }

  function apply(root) {
    const scope = root || document;
    const pick = (attr, fn) => {
      const nodes = scope.querySelectorAll ? Array.from(scope.querySelectorAll("[" + attr + "]")) : [];
      if (scope.hasAttribute && scope.hasAttribute(attr)) nodes.unshift(scope);
      for (const el of nodes) fn(el, el.getAttribute(attr));
    };
    pick("data-i18n", (el, k) => {
      el.textContent = t(k);
    });
    for (const a of ["title", "aria-label", "placeholder"]) pick("data-i18n-" + a, (el, k) => el.setAttribute(a, t(k)));
  }

  function setLang(next, opts) {
    if (!LANGS.includes(next)) return;
    const changed = next !== current;
    current = next;
    if (!opts || opts.persist !== false) Core.local.set(STORE_KEY, next);
    if (typeof document !== "undefined" && document.documentElement) {
      document.documentElement.lang = LOCALES[next];
      apply(document);
    }
    if (changed) for (const fn of Array.from(listeners)) fn(next);
  }

  function init() {
    current = detect(Core.local.get(STORE_KEY), typeof navigator !== "undefined" ? navigator.language : "");
    document.documentElement.lang = LOCALES[current];
    apply(document);
  }

  // fn(lang) after every switch; returns an unsubscribe function.
  function onChange(fn) {
    listeners.add(fn);
    return () => listeners.delete(fn);
  }

  // Language-aware Intl helpers.
  const number = (n, opts) => Core.nf(opts).format(n);
  const date = (ms, opts) => new Intl.DateTimeFormat(locale(), opts).format(new Date(ms));

  return {LANGS, STORE_KEY, add, t, has, lang, locale, detect, apply, setLang, init, onChange, number, date,
    dicts: () => dicts};
})();
