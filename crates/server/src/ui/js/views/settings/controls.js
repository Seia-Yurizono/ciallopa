"use strict";
// Settings controls: one builder per kind of setting. Each returns {el, set(value), error(message)}
// and reports edits to the draft: draft.edit / invalidate, then draft.changed(). An unsaved control
// carries .chg (a switch also .pending); its row gets .is-dirty while any of its controls does.
const SettingsControls = (() => {
  const {h} = Core;
  const t = (k, v) => I18n.t(k, v);
  const T = SettingsText;
  const idOf = (p, k) => p + "-" + String(k).replace(/\W/g, "_");

  function rowDirty(row) {
    if (row) row.classList.toggle("is-dirty", !!row.querySelector(".chg,.pending"));
  }
  function mark(el, row, changed) {
    el.classList.toggle("chg", changed);
    if (el.classList.contains("switch")) el.classList.toggle("pending", changed);
    rowDirty(row);
  }

  // Inline error line under a control; server errors use the same line.
  function errLine(key, inputs) {
    const e = h("div", {class: "st-err hide", id: idOf("fe", key)});
    for (const i of inputs) i.setAttribute("aria-describedby", e.id);
    const show = msg => {
      e.textContent = msg || "";
      e.classList.toggle("hide", !msg);
      for (const i of inputs) i.setAttribute("aria-invalid", String(!!msg));
    };
    return {el: e, show};
  }

  const switchInput = (label, fk, on) => {
    const e = h("input", {type: "checkbox", class: "switch", role: "switch", "aria-label": label, "data-fk": fk});
    e.checked = !!on;
    return e;
  };

  // bool: a switch with its state word.
  function bool(draft, f, v, row) {
    const orig = !!v;
    const e = switchInput(T.title(f), f.key, orig);
    const state = h("span", "st-state", orig ? t("settings.isOn") : t("settings.isOff"));
    e.addEventListener("change", () => {
      state.textContent = e.checked ? t("settings.isOn") : t("settings.isOff");
      mark(e, row, draft.edit(f.key, e.checked, orig));
      draft.changed();
    });
    const set = x => {
      if (e.checked === !!x) return;
      e.checked = !!x;
      e.dispatchEvent(new Event("change"));
    };
    return {el: h("label", "st-switch", e, state), set, error() {}};
  }

  // Fixed options with an explanation each: a radio list, every option's meaning visible before choosing.
  function choices(draft, f, v, row) {
    const g = h("div", {class: "st-choices", role: "radiogroup", "aria-label": T.title(f)});
    for (const c of f.choices) {
      const id = idOf("ch", f.key + "-" + c.value);
      const text = T.choice(f, c);
      const rb = h("input", {type: "radio", name: "r-" + f.key, value: c.value, id, "data-fk": f.key + "=" + c.value,
        "aria-describedby": id + "-h"});
      rb.checked = c.value === v;
      rb.addEventListener("change", () => {
        if (!rb.checked) return;
        mark(g, row, draft.edit(f.key, c.value, v));
        draft.changed();
      });
      g.appendChild(h("label", {class: "st-choice", for: id}, rb,
        h("span", "st-choice-text",
          h("span", "st-choice-head", h("span", "st-choice-label", text.label), h("code", "st-choice-value", c.value)),
          h("span", {class: "st-choice-help", id: id + "-h"}, text.help))));
    }
    const set = x => {
      const r = Array.from(g.querySelectorAll("input[type=radio]")).find(b => b.value === x);
      if (r && !r.checked) {
        r.checked = true;
        r.dispatchEvent(new Event("change"));
      }
    };
    return {el: g, set, error() {}};
  }

  // Plain value: text, number (≥ 0, int or float, with unit) or a select for enums without choices.
  function input(draft, f, v, row) {
    let e;
    if (f.kind === "enum") {
      e = h("select", {class: "select st-select", "aria-label": T.title(f), "data-fk": f.key});
      for (const o of f.options || []) e.appendChild(h("option", {value: o}, o));
      e.value = v;
    } else {
      const num = f.kind === "int" || f.kind === "float";
      e = h("input", {class: "input" + (num ? " num st-numin" : " st-text"), "aria-label": T.title(f), "data-fk": f.key,
        type: num ? "number" : "text", step: num ? (f.kind === "int" ? "1" : "any") : null, min: num ? "0" : null,
        inputmode: num ? (f.kind === "int" ? "numeric" : "decimal") : null, spellcheck: num ? null : "false",
        autocomplete: "off", placeholder: f.kind === "duration" ? "24h" : null});
      e.value = v === null || v === undefined ? "" : String(v);
    }
    const num = f.kind === "int" || f.kind === "float";
    const orig = e.value;
    const err = errLine(f.key, [e]);
    let localBad = false;
    e.addEventListener("input", () => {
      let val = e.value;
      if (f.kind === "int") val = val === "" ? null : parseInt(val, 10);
      else if (f.kind === "float") val = val === "" ? null : parseFloat(val);
      if (num) {
        const raw = e.value;
        localBad = !!(e.validity && e.validity.badInput) || (raw !== "" && (f.kind === "int" ? !/^\d+$/.test(raw) : !(Number(raw) >= 0)));
        err.show(localBad ? t(f.kind === "int" ? "settings.errInt" : "settings.errNum") : "");
        draft.invalidate(f.key, localBad);
      }
      mark(e, row, draft.edit(f.key, val, orig, e.value));
      draft.changed();
    });
    e.addEventListener("change", () => e.dispatchEvent(new Event("input")));
    const u = num ? T.unit(f) : "";
    const el = num ? h("div", "st-num", e, u ? h("span", "st-unit", u) : null, err.el) : h("div", "st-field", e, err.el);
    const set = x => {
      e.value = x === null || x === undefined ? "" : String(x);
      e.dispatchEvent(new Event("input"));
    };
    return {el, set, error: msg => err.show(msg || (localBad ? t(f.kind === "int" ? "settings.errInt" : "settings.errNum") : ""))};
  }

  // "0 = off" number: a switch plus the value. Off writes 0 and keeps the typed value (greyed) for
  // switching back; on with an empty or 0 value is an error, not a silent off.
  function zeroOff(draft, f, v, row) {
    const offText = SettingsModel.zoOff(f.key);
    const d = draft.base && draft.base.defaults ? Number(draft.base.defaults[f.key]) : 0;
    const fb = d > 0 ? d : SettingsModel.ZO[f.key][1];
    const orig = Number(v) || 0, on0 = orig > 0, u = T.unit(f);
    const sw = switchInput(T.title(f) + " · " + t("settings.on"), f.key + "|on", on0);
    const e = h("input", {type: "number", class: "input num st-numin", step: f.kind === "int" ? "1" : "any", min: "0",
      inputmode: f.kind === "int" ? "numeric" : "decimal", "aria-label": T.title(f), "data-fk": f.key});
    e.value = String(on0 ? v : fb);
    e.disabled = !on0;
    const st = h("span", "st-unit", on0 ? u : offText);
    const err = errLine(f.key, [e]);
    const apply = () => {
      const on = sw.checked;
      e.disabled = !on;
      st.textContent = on ? u : offText;
      let val = 0, bad = false;
      if (on) {
        const s = e.value.trim();
        bad = !!(e.validity && e.validity.badInput) || s === "" || (f.kind === "int" ? !/^\d+$/.test(s) : !(Number(s) >= 0));
        val = f.kind === "int" ? parseInt(s, 10) : parseFloat(s);
        if (!bad && !(val > 0)) bad = true;
      }
      err.show(bad ? t(f.kind === "int" ? "settings.errPosInt" : "settings.errPosNum") : "");
      draft.invalidate(f.key, bad);
      const changed = draft.isChanged(val, orig);
      if (bad || !changed) delete draft.edits[f.key];
      else draft.edits[f.key] = val;
      e.classList.toggle("chg", (changed || bad) && on);
      sw.classList.toggle("pending", draft.isChanged(on, on0));
      rowDirty(row);
      draft.changed();
    };
    sw.addEventListener("change", () => {
      if (sw.checked && !(Number(e.value) > 0)) e.value = String(fb);
      apply();
      if (sw.checked) e.focus();
    });
    e.addEventListener("input", apply);
    e.addEventListener("change", apply);
    const set = x => {
      const n = Number(x) || 0;
      sw.checked = n > 0;
      if (n > 0) e.value = String(x);
      apply();
    };
    return {el: h("div", "st-zo-wrap", h("div", "st-zo", sw, e, st), err.el), set, error: msg => msg && err.show(msg)};
  }

  // Background refresh range (shortest–longest, minutes): the switch is the shortest key (0 = off).
  // Switching on fills 5–10 when the stored values do not make a range; off writes 0 to the shortest
  // only and keeps the longest.
  function range(draft, f, fx, v, vx, row) {
    const offText = SettingsModel.zoOff(f.key);
    const omin = Number(v) || 0, oraw = Number(vx) || 0, on0 = omin > 0, omax = on0 ? oraw || omin : 0;
    const ON = SettingsModel.RZ_ON;
    const sw = switchInput(T.title(f) + " · " + t("settings.on"), f.key + "|on", on0);
    const num = (k, label) => h("input", {type: "number", class: "input num st-numin", step: "1", min: "1", max: "60",
      inputmode: "numeric", "aria-label": label, "data-fk": k});
    const a = num(f.key, T.title(f) + " · " + t("settings.rangeMin"));
    const b = num(fx.key, T.title(f) + " · " + t("settings.rangeMax"));
    a.value = String(on0 ? omin : ON[0]);
    b.value = String(on0 ? omax : oraw >= ON[0] ? oraw : ON[1]);
    a.disabled = b.disabled = !on0;
    const st = h("span", "st-unit", on0 ? t("settings.unit.min") : offText);
    const err = errLine(f.key, [a, b]);
    const apply = () => {
      const on = sw.checked;
      a.disabled = b.disabled = !on;
      st.textContent = on ? t("settings.unit.min") : offText;
      let msg = "", mn = 0, mx = 0;
      if (on) {
        const ok = x => !(x.validity && x.validity.badInput) && /^\d+$/.test(x.value.trim()) && +x.value >= 1 && +x.value <= 60;
        if (!ok(a) || !ok(b)) msg = t("settings.errRange");
        else {
          mn = parseInt(a.value, 10);
          mx = parseInt(b.value, 10);
          if (mn > mx) msg = t("settings.errMinMax");
        }
      }
      const bad = !!msg;
      err.show(msg);
      draft.invalidate(f.key, bad);
      const changed = draft.isChanged(on, on0) || (on && (mn !== omin || mx !== omax));
      if (bad || !changed) {
        delete draft.edits[f.key];
        delete draft.edits[fx.key];
      } else if (on) {
        draft.edit(f.key, mn, omin);
        draft.edit(fx.key, mx, oraw);
      } else {
        draft.edit(f.key, 0, omin);
        delete draft.edits[fx.key];
      }
      a.classList.toggle("chg", on && (bad || draft.isChanged(mn, omin)));
      b.classList.toggle("chg", on && (bad || draft.isChanged(mx, omax)));
      sw.classList.toggle("pending", draft.isChanged(on, on0));
      rowDirty(row);
      draft.changed();
    };
    sw.addEventListener("change", () => {
      if (sw.checked && !(+a.value >= 1 && +b.value >= +a.value && +b.value <= 60)) {
        a.value = String(ON[0]);
        b.value = String(ON[1]);
      }
      apply();
      if (sw.checked) a.focus();
    });
    for (const x of [a, b]) {
      x.addEventListener("input", apply);
      x.addEventListener("change", apply);
    }
    const set = x => {
      const n = Number(x) || 0;
      sw.checked = n > 0;
      if (n > 0) {
        a.value = String(n);
        if (+b.value < n) b.value = String(n);
      }
      apply();
    };
    return {el: h("div", "st-zo-wrap", h("div", "st-zo st-range", sw, a, h("span", "st-dash", "–"), b, st), err.el), set,
      error: msg => msg && err.show(msg)};
  }

  // Raw state of every control under root (value / checked by data-fk, in document order), and its
  // replay onto a freshly built form: language switches rebuild the form without losing the draft.
  function capture(root) {
    return Array.from(root.querySelectorAll("input[data-fk],select[data-fk]")).map(e => ({fk: e.dataset.fk, type: e.type,
      value: e.value, checked: e.checked}));
  }
  function replay(root, states) {
    const byFk = new Map();
    for (const e of root.querySelectorAll("input[data-fk],select[data-fk]")) byFk.set(e.dataset.fk, e);
    for (const s of states) {
      const e = byFk.get(s.fk);
      if (!e) continue;
      if (s.type === "checkbox" || s.type === "radio") {
        if (e.checked !== s.checked && (s.type === "checkbox" || s.checked)) {
          e.checked = s.checked;
          e.dispatchEvent(new Event("change"));
        }
      } else if (e.value !== s.value) {
        e.value = s.value;
        e.dispatchEvent(new Event("input"));
      }
    }
  }

  return {bool, choices, input, zeroOff, range, switchInput, rowDirty, mark, capture, replay};
})();
