"use strict";
// UI: reusable components (toast, layers, popover, segmented, tabs, table, ...). Visual classes live
// in css/components.css; see ui/README.md for the class contract.
const UI = (() => {
  const {h, svg, clear} = Core;
  const t = (k, v) => I18n.t(k, v);
  const PHONE_Q = "(max-width: 767px), (pointer: coarse) and (max-height: 500px)";
  const isPhone = () => typeof matchMedia === "function" && matchMedia(PHONE_Q).matches;
  const kids = x => (x === undefined || x === null ? [] : Array.isArray(x) ? x : [x]);

  // ---------- icons (symbols in index.html: <symbol id="i-NAME">) ----------
  function icon(name, cls) {
    return svg("svg", {class: "icon" + (cls ? " " + cls : ""), "aria-hidden": "true", focusable: "false"},
      svg("use", {href: "#i-" + name}));
  }

  // button(label, {kind, size, icon, onClick, title, type, attrs}) — kind: primary | ghost | danger |
  // danger-solid | link; size: sm. An icon-only button needs a title (used as aria-label).
  function button(label, o) {
    o = o || {};
    const cls = ["btn"];
    if (o.kind) cls.push("btn-" + o.kind);
    if (o.size === "sm") cls.push("btn-sm");
    if (!label && o.icon) cls.push("btn-icon");
    if (o.className) cls.push(o.className);
    const b = h("button", Object.assign({type: o.type || "button", class: cls.join(" "), title: o.title || null,
      "aria-label": !label && o.title ? o.title : null, onclick: o.onClick || null}, o.attrs || {}),
    o.icon ? icon(o.icon) : null, label ? h("span", null, label) : null);
    return b;
  }

  // ---------- toasts ----------
  // toast(text, {kind: "ok" | "err", undo: async fn, action: {label, run}, ms}).
  // Undo / action toasts stay 10 s, errors 10 s with a close button, others 4 s. The timer pauses
  // while the toast is hovered, touched or focused, and while the tab is hidden.
  function toast(text, o) {
    o = o || {};
    const box = Core.byId("toasts");
    const isErr = o.kind === "err";
    const d = h("div", {class: "toast" + (o.kind ? " " + o.kind : ""), role: isErr ? "alert" : "status"},
      h("span", "toast-text", text));
    let timer = null, pointer = false, focus = false, gone = false;
    const bye = () => {
      if (gone) return;
      gone = true;
      clearTimeout(timer);
      d.classList.add("out");
      setTimeout(() => d.remove(), 200);
    };
    const arm = () => {
      clearTimeout(timer);
      if (pointer || focus || gone) return;
      timer = setTimeout(() => (document.hidden ? arm() : bye()), o.ms || (o.undo || o.action || isErr ? 10000 : 4000));
    };
    if (o.undo) {
      const b = button(t("ui.undo"), {size: "sm"});
      b.addEventListener("click", async () => {
        b.disabled = true;
        bye();
        try {
          await o.undo();
        } catch (e) {
          toast(t("ui.undoFailed", {why: Api.why(e)}), {kind: "err"});
        }
      });
      d.appendChild(b);
    }
    if (o.action) {
      const b = button(o.action.label, {size: "sm"});
      b.addEventListener("click", () => {
        bye();
        o.action.run();
      });
      d.appendChild(b);
    }
    if (isErr) d.appendChild(button("", {size: "sm", icon: "x", title: t("ui.close"), onClick: bye}));
    for (const [ev, v] of [["pointerenter", true], ["pointerdown", true], ["pointerleave", false], ["pointerup", false]]) {
      d.addEventListener(ev, () => {
        pointer = v;
        arm();
      });
    }
    d.addEventListener("focusin", () => {
      focus = true;
      arm();
    });
    d.addEventListener("focusout", e => {
      if (!d.contains(e.relatedTarget)) {
        focus = false;
        arm();
      }
    });
    box.appendChild(d);
    while (box.children.length > 4) box.firstElementChild.remove();
    arm();
    return {el: d, close: bye};
  }

  // ---------- layers: modal, drawer, sheet, page ----------
  // Only one sheet at a time (opening a sheet closes the previous one). Focus moves into the
  // layer, Tab is trapped, Esc / scrim click close the top layer and focus returns to the opener.
  // On phones a layer also pushes a history entry, so the system back gesture closes it.
  const layers = [];
  let seq = 0, popSkip = 0, popTimer = null;
  const afterPop = [];

  // URL writes (replaceState / pushState) must wait while a layer's own history entry is being
  // popped: history.back() is asynchronous, and a write before the pop lands would be undone.
  function afterHistory(fn) {
    if (popSkip) afterPop.push(fn);
    else fn();
  }

  function flushAfterPop() {
    clearTimeout(popTimer);
    popSkip = 0;
    for (const fn of afterPop.splice(0)) fn();
  }

  function focusables(root) {
    return Array.from(root.querySelectorAll(
      "button,a[href],input,select,textarea,summary,[tabindex]:not([tabindex='-1'])"))
      .filter(x => !x.disabled && x.offsetParent !== null && !x.closest("details:not([open]) > :not(summary)"));
  }

  // open({type, title, body, foot, head, wide, danger, label, onClose, focus, scrim, className})
  // → {el, panel, body, head, foot, close(), setTitle(text)}
  function open(o) {
    const type = o.type || "modal";
    if (type === "sheet") for (const l of layers.slice()) if (l.type === "sheet") l.close();
    const id = "layer-" + ++seq;
    const titleId = id + "-title";
    const title = h("h2", {class: "layer-title", id: titleId}, o.title || "");
    const back = (type === "page" || type === "drawer") && isPhone();
    const closeBtn = button("", {kind: "ghost", icon: back ? "chevron-left" : "x", title: back ? t("ui.back") : t("ui.close")});
    const head = h("header", "layer-head", back ? closeBtn : null, title, kids(o.head), back ? null : closeBtn);
    const body = h("div", "layer-body", kids(o.body));
    const foot = o.foot ? h("footer", "layer-foot", kids(o.foot)) : null;
    const panel = h("div", {class: "layer-panel", role: o.role || "dialog", "aria-modal": "true", "aria-labelledby": titleId,
      "aria-label": o.label || null, tabindex: "-1"},
    type === "sheet" || type === "modal" ? h("div", {class: "sheet-grip", "aria-hidden": "true"}) : null, head, body, foot);
    const scrim = h("div", "layer-scrim");
    const el = h("div", {class: "layer " + type + (o.wide ? " wide" : "") + (o.danger ? " danger" : "") +
      (o.className ? " " + o.className : "") + (o.scrim === false ? " no-scrim" : "")}, scrim, panel);
    const layer = {id, type, el, panel, body, head, foot, opener: document.activeElement, at: Date.now(),
      onClose: o.onClose, hist: false, closed: false,
      blocking: type !== "drawer" || isPhone(),
      setTitle: s => {
        title.textContent = s;
      },
      close: () => close(layer)};
    closeBtn.addEventListener("click", () => layer.close());
    scrim.addEventListener("click", () => layer.close());
    Core.byId("layers").appendChild(el);
    layers.push(layer);
    document.documentElement.classList.add("layer-open");
    if (isPhone() && typeof history !== "undefined") {
      history.pushState(Object.assign({}, history.state, {layer: id}), "", location.href);
      layer.hist = true;
    }
    const target = typeof o.focus === "function" ? o.focus(layer) : o.focus;
    // Phones: focus the panel itself so the on-screen keyboard does not pop up uninvited.
    (target || (isPhone() ? panel : focusables(panel).find(x => !head.contains(x)) || closeBtn)).focus({preventScroll: true});
    return layer;
  }

  function close(layer, fromPop) {
    if (layer.closed) return;
    layer.closed = true;
    const i = layers.indexOf(layer);
    if (i >= 0) layers.splice(i, 1);
    layer.el.classList.add("closing");
    setTimeout(() => layer.el.remove(), 200);
    if (!layers.length) document.documentElement.classList.remove("layer-open");
    if (layer.hist && !fromPop && history.state && history.state.layer === layer.id) {
      popSkip++;
      history.back();
      clearTimeout(popTimer);
      popTimer = setTimeout(flushAfterPop, 500);
    }
    if (layer.onClose) layer.onClose();
    const o = layer.opener;
    if (o && o.isConnected && document.contains(o)) o.focus({preventScroll: true});
  }

  // closeAll() also unwinds the layers' history entries; closeAll({fromPop: true}) leaves history alone.
  function closeAll(o) {
    closePopover();
    for (const l of layers.slice().reverse()) close(l, !!(o && o.fromPop));
  }

  // Shell's popstate handler asks here first: true means the event belonged to a layer.
  function handlePop() {
    if (popSkip) {
      popSkip--;
      if (!popSkip) flushAfterPop();
      return true;
    }
    const top = layers[layers.length - 1];
    if (top && top.hist) {
      close(top, true);
      return true;
    }
    return false;
  }

  const modal = o => open(Object.assign({}, o, {type: "modal"}));
  const drawer = o => open(Object.assign({}, o, {type: "drawer"}));
  const sheet = o => open(Object.assign({}, o, {type: isPhone() ? "sheet" : o.desktop || "sheet"}));
  const page = o => open(Object.assign({}, o, {type: "page"}));
  const layerCount = () => layers.length;
  // True while a modal, sheet or full-screen page is open: auto refresh pauses.
  const blocking = () => layers.some(l => l.blocking) || !!pop;

  // confirm({title, body, ok, cancel, danger}) → Promise<boolean>. Focus starts on Cancel; the
  // confirm button ignores clicks in its first 400 ms (no double tap through the opener).
  function confirm(o) {
    return new Promise(resolve => {
      let done = false;
      const finish = v => {
        if (done) return;
        done = true;
        resolve(v);
        layer.close();
      };
      const cancel = button(o.cancel || t("ui.cancel"), {onClick: () => finish(false)});
      const ok = button(o.ok || t("ui.confirm"), {kind: o.danger ? "danger-solid" : "primary"});
      ok.addEventListener("click", () => {
        if (Date.now() - layer.at < 400) return;
        finish(true);
      });
      const layer = modal({title: o.title, body: typeof o.body === "string" ? h("p", "muted", o.body) : o.body,
        foot: [cancel, ok], danger: o.danger, focus: cancel, onClose: () => finish(false)});
    });
  }

  // ---------- popover ----------
  // popover(anchor, content, {className, placement: "bottom-start" | "bottom-end", onClose, role})
  // One at a time; outside click, Esc, scroll of the page or a resize close it.
  let pop = null;
  function closePopover(restore) {
    if (!pop) return;
    const p = pop;
    pop = null;
    p.el.remove();
    if (p.anchor && p.anchor.isConnected) {
      p.anchor.setAttribute("aria-expanded", "false");
      if (restore) p.anchor.focus({preventScroll: true});
    }
    if (p.onClose) p.onClose();
  }

  function place(el, anchor, placement) {
    const r = anchor.getBoundingClientRect();
    const w = el.offsetWidth, ht = el.offsetHeight, m = 8;
    let x = placement === "bottom-end" ? r.right - w : r.left;
    x = Core.clamp(x, m, innerWidth - w - m);
    let y = r.bottom + 6;
    if (y + ht > innerHeight - m && r.top - ht - 6 > m) y = r.top - ht - 6;
    el.style.setProperty("left", Math.round(x) + "px");
    el.style.setProperty("top", Math.round(Math.max(m, y)) + "px");
  }

  function popover(anchor, content, o) {
    o = o || {};
    if (pop && pop.anchor === anchor) {
      closePopover(true);
      return null;
    }
    closePopover();
    const el = h("div", {class: "popover" + (o.className ? " " + o.className : ""), role: o.role || "dialog",
      tabindex: "-1"}, kids(content));
    Core.byId("layers").appendChild(el);
    place(el, anchor, o.placement);
    anchor.setAttribute("aria-expanded", "true");
    pop = {el, anchor, onClose: o.onClose, reposition: () => place(el, anchor, o.placement)};
    const first = o.focus === false ? null : focusables(el)[0];
    if (first) first.focus({preventScroll: true});
    return {el, close: () => closePopover(true), reposition: pop.reposition};
  }

  // ⓘ button opening a short explanation (click / tap / Enter; never hover-only).
  function info(content, label) {
    const b = h("button", {type: "button", class: "info-btn", "aria-label": label || t("ui.info"), "aria-expanded": "false"},
      icon("info"));
    b.addEventListener("click", e => {
      e.stopPropagation();
      popover(b, typeof content === "function" ? content() : content, {className: "text", role: "note", focus: false});
    });
    return b;
  }

  // menu(anchor, items) — items: {label, icon?, onSelect, danger?, checked?, meta?} | "-" | {group}.
  // A popover on desktop, a sheet on phones.
  function menu(anchor, items, o) {
    o = o || {};
    let close = () => {};
    const list = h("div", {class: "menu", role: "menu"});
    for (const it of items) {
      if (it === "-") {
        list.appendChild(h("div", {class: "menu-sep", role: "separator"}));
        continue;
      }
      if (it.group) {
        list.appendChild(h("div", "menu-label", it.group));
        continue;
      }
      const role = it.checked === undefined ? "menuitem" : "menuitemradio";
      const b = h("button", {type: "button", class: "menu-item" + (it.danger ? " danger" : ""), role,
        "aria-checked": it.checked === undefined ? null : String(!!it.checked)},
      it.icon ? icon(it.icon) : null, h("span", "grow", it.label), it.meta ? h("span", "menu-meta", it.meta) : null,
      it.checked === undefined ? null : icon("check", "check-mark"));
      b.addEventListener("click", () => {
        close();
        it.onSelect();
      });
      list.appendChild(b);
    }
    if (isPhone()) {
      const l = sheet({title: o.title || "", body: list});
      close = () => l.close();
    } else {
      const p = popover(anchor, list, {placement: o.placement || "bottom-end", role: "presentation"});
      close = () => closePopover(true);
      if (!p) return;
    }
  }

  // ---------- tooltip for [data-tip] on fine pointers (icon rail labels, truncated values) ----------
  let tipEl = null, tipFor = null;
  function showTip(target) {
    const text = target.getAttribute("data-tip");
    if (!text) return;
    hideTip();
    tipFor = target;
    tipEl = h("div", {class: "tip", role: "tooltip"}, text);
    Core.byId("layers").appendChild(tipEl);
    const r = target.getBoundingClientRect();
    const side = target.getAttribute("data-tip-side");
    let x, y;
    if (side === "right") {
      x = r.right + 8;
      y = r.top + r.height / 2 - tipEl.offsetHeight / 2;
    } else {
      x = r.left + r.width / 2 - tipEl.offsetWidth / 2;
      y = r.bottom + 6;
    }
    tipEl.style.setProperty("left", Math.round(Core.clamp(x, 8, innerWidth - tipEl.offsetWidth - 8)) + "px");
    tipEl.style.setProperty("top", Math.round(Core.clamp(y, 8, innerHeight - tipEl.offsetHeight - 8)) + "px");
  }
  function hideTip() {
    if (tipEl) tipEl.remove();
    tipEl = tipFor = null;
  }

  // ---------- click-again confirmation for inline buttons ----------
  // First click turns the button into `label` for 3 s (armed after 300 ms); a second click runs
  // act(); timeout, blur or Esc reverts.
  function disarm(b) {
    if (!b._armed) return;
    clearTimeout(b._armTimer);
    b._armed = 0;
    b.textContent = b._armText;
    b.classList.remove("armed");
  }
  function clickAgain(b, label, act) {
    if (b._armed) {
      if (Date.now() - b._armed < 300) return undefined;
      disarm(b);
      return act();
    }
    b._armText = b.textContent;
    b._armed = Date.now();
    b.textContent = label;
    b.classList.add("armed");
    b._armTimer = setTimeout(() => disarm(b), 3000);
    b.addEventListener("blur", () => disarm(b), {once: true});
    return undefined;
  }

  // ---------- segmented control ----------
  // segmented({options: [{value, label, title?}], value, onChange(value), label, block, className})
  // → element with .value and .setValue(v). Arrow keys move the selection (radio group).
  function segmented(o) {
    const el = h("div", {class: "seg" + (o.block ? " seg-block" : "") + (o.className ? " " + o.className : ""),
      role: "radiogroup", "aria-label": o.label || null});
    let value = o.value;
    const btns = o.options.map(op => {
      const b = h("button", {type: "button", role: "radio", title: op.title || null, "data-value": op.value}, op.label);
      b.addEventListener("click", () => {
        if (value === op.value) return;
        el.setValue(op.value);
        if (o.onChange) o.onChange(op.value);
      });
      b.addEventListener("keydown", e => {
        const d = e.key === "ArrowRight" || e.key === "ArrowDown" ? 1 : e.key === "ArrowLeft" || e.key === "ArrowUp" ? -1 : 0;
        if (!d) return;
        e.preventDefault();
        const i = (btns.indexOf(b) + d + btns.length) % btns.length;
        btns[i].focus();
        btns[i].click();
      });
      el.appendChild(b);
      return b;
    });
    el.setValue = v => {
      value = v;
      for (const b of btns) {
        const on = b.dataset.value === String(v);
        b.setAttribute("aria-checked", String(on));
        b.tabIndex = on ? 0 : -1;
      }
      if (!btns.some(b => b.tabIndex === 0) && btns[0]) btns[0].tabIndex = 0;
    };
    el.setLabels = labels => btns.forEach((b, i) => {
      if (labels[i] !== undefined) b.textContent = labels[i];
    });
    Object.defineProperty(el, "value", {get: () => value});
    el.setValue(value);
    return el;
  }

  // ---------- tabs ----------
  // tabs({items: [{id, label, count?, countKind?}], value, onChange(id), label}) → element with
  // .setValue(id) and .setItems(items). Roving tabindex; arrows / Home / End.
  function tabs(o) {
    const el = h("div", {class: "tabs", role: "tablist", "aria-label": o.label || null});
    let value = o.value, items = [];
    function render() {
      const focused = el.contains(document.activeElement) ? document.activeElement.dataset.id : null;
      clear(el);
      for (const it of items) {
        const on = it.id === value;
        const b = h("button", {type: "button", class: "tab", role: "tab", "aria-selected": String(on), tabindex: on ? "0" : "-1",
          "data-id": it.id, "aria-controls": o.controls || null},
        h("span", null, it.label),
        it.count === undefined || it.count === null ? null : h("span", "tab-count" + (it.countKind ? " " + it.countKind : ""),
          typeof it.count === "number" ? Core.fmt.n(it.count) : it.count));
        b.addEventListener("click", () => {
          if (value !== it.id) {
            el.setValue(it.id);
            if (o.onChange) o.onChange(it.id);
          }
        });
        b.addEventListener("keydown", e => {
          const ids = items.map(x => x.id);
          let i = ids.indexOf(it.id);
          if (e.key === "ArrowRight") i = (i + 1) % ids.length;
          else if (e.key === "ArrowLeft") i = (i - 1 + ids.length) % ids.length;
          else if (e.key === "Home") i = 0;
          else if (e.key === "End") i = ids.length - 1;
          else return;
          e.preventDefault();
          el.setValue(ids[i]);
          if (o.onChange) o.onChange(ids[i]);
          el.querySelector('[data-id="' + CSS.escape(ids[i]) + '"]').focus();
        });
        el.appendChild(b);
      }
      if (focused) {
        const f = el.querySelector('[data-id="' + CSS.escape(focused) + '"]');
        if (f) f.focus({preventScroll: true});
      }
    }
    el.setValue = v => {
      value = v;
      render();
    };
    el.setItems = next => {
      items = next;
      render();
    };
    Object.defineProperty(el, "value", {get: () => value});
    el.setItems(o.items || []);
    return el;
  }

  // ---------- skeletons, empty / error states, banners ----------
  const skeleton = {
    // n table-like rows, each with `cols` bars of varying width
    rows(n, cols) {
      const widths = ["18%", "12%", "28%", "10%", "14%", "8%"];
      return h("div", {class: "skel-rows", "aria-hidden": "true"}, Array.from({length: n || 6}, () =>
        h("div", "skel-row", Array.from({length: cols || 5}, (_, i) =>
          h("span", {class: "skel", css: {width: widths[i % widths.length]}})))));
    },
    block(height) {
      return h("span", {class: "skel block", "aria-hidden": "true", css: height ? {height: height + "px"} : null});
    },
    stats(n) {
      return h("div", {class: "placeholder-grid", "aria-hidden": "true"}, Array.from({length: n || 4}, () =>
        h("div", "stack", h("span", {class: "skel line", css: {width: "45%"}}), h("span", "skel value"))));
    },
    lines(n) {
      return h("div", {class: "stack", "aria-hidden": "true"}, Array.from({length: n || 3}, (_, i) =>
        h("span", {class: "skel", css: {width: [80, 64, 72, 50][i % 4] + "%"}})));
    },
  };

  // empty({icon, title, body, actions, kind}) — kind "err" for an error state.
  function empty(o) {
    return h("div", {class: "empty" + (o.kind ? " " + o.kind : ""), role: o.kind === "err" ? "alert" : null},
      h("span", "empty-icon", icon(o.icon || (o.kind === "err" ? "alert" : "inbox"))),
      o.title ? h("div", "empty-title", o.title) : null,
      o.body ? h("div", "empty-body", o.body) : null,
      o.actions ? h("div", "empty-actions", kids(o.actions)) : null);
  }

  // errorState({title?, error, retry?}) — keeps the reason text from Api.why().
  function errorState(o) {
    const retry = o.retry ? button(t("ui.retry"), {onClick: o.retry}) : null;
    return empty({kind: "err", title: o.title || t("ui.loadFailed"), body: o.error ? Api.why(o.error) : null,
      actions: retry ? [retry] : null});
  }

  // banner({kind: "warn" | "err" | "info", title, body, actions, icon})
  function banner(o) {
    return h("div", {class: "banner" + (o.kind && o.kind !== "warn" ? " " + o.kind : ""), role: o.kind === "err" ? "alert" : "status",
      title: o.tip || null},
    icon(o.icon || (o.kind === "info" ? "info" : "alert")),
    h("div", "banner-text", o.title ? h("div", "banner-title", o.title) : null, o.body ? h("div", "banner-body", kids(o.body)) : null),
    o.actions ? h("div", "banner-actions", kids(o.actions)) : null);
  }

  // ---------- small pieces ----------
  const badge = (text, kind) => h("span", "badge" + (kind ? " " + kind : ""), text === null || text === undefined ? "" : String(text));
  const tag = (text, kind, o) => h(o && o.href ? "a" : "span", {class: "tag" + (kind ? " " + kind : ""), href: o && o.href || null,
    title: o && o.title || null}, o && o.dot ? h("span", "dot " + (kind || "")) : null, text);
  const dot = (kind, label) => h("span", {class: "dot" + (kind ? " " + kind : ""), role: label ? "img" : null,
    "aria-label": label || null, "aria-hidden": label ? null : "true"});

  // chip({label, value, onClick, onRemove, pressed, removeLabel}) — filter chip.
  function chip(o) {
    const el = h("button", {type: "button", class: "chip" + (o.dashed ? " dashed" : ""), "aria-pressed": o.pressed === undefined ? null :
      String(!!o.pressed), title: o.title || null, "aria-label": o.ariaLabel || null},
    o.icon ? icon(o.icon) : null,
    o.label ? h("span", "chip-label", o.label) : null,
    o.value !== undefined ? h("span", "chip-value", o.value) : null,
    o.onRemove ? h("span", "chip-x", icon("x")) : null);
    el.addEventListener("click", () => (o.onRemove ? o.onRemove() : o.onClick && o.onClick()));
    return el;
  }

  // kv([[label, value], ...]) → <dl class="kv">; a null value row is skipped.
  function kv(pairs) {
    const dl = h("dl", "kv");
    for (const [k, v] of pairs) {
      if (v === null || v === undefined) continue;
      dl.append(h("dt", null, k), h("dd", null, kids(v)));
    }
    return dl;
  }

  // field({label, hint, control, error, id}) — label is bound to the control by id.
  function field(o) {
    const id = o.id || (o.control && o.control.id) || "f-" + ++seq;
    if (o.control && !o.control.id) o.control.id = id;
    return h("div", "field", o.label ? h("label", {class: "field-label", for: id}, o.label) : null, o.control,
      o.hint ? h("div", "field-hint", o.hint) : null, o.error ? h("div", {class: "field-error", role: "alert"}, o.error) : null);
  }

  // saveBar({text, onSave, onDiscard, saveLabel, discardLabel}) → {el, save, discard, update(text)}
  function saveBar(o) {
    const text = h("span", "savebar-text", o.text || "");
    const discard = button(o.discardLabel || t("ui.discard"), {size: "sm", onClick: o.onDiscard});
    const save = button(o.saveLabel || t("ui.save"), {size: "sm", kind: "primary", onClick: o.onSave});
    const el = h("div", {class: "savebar", role: "region", "aria-label": o.label || t("ui.unsaved")}, text, discard, save);
    return {el, save, discard, update: s => {
      text.textContent = s;
    }};
  }

  // table({columns, keyOf, onRowClick, compact, label, selected})
  // columns: [{id, label, num?, priority? (2 | 3), render(row) → Node | string, className?}]
  // → {el, table, tbody, setRows(rows), setSelected(key), rowOf(key)}. Rows are keyed: unchanged
  // keys keep their <tr> (focus and selection survive refreshes).
  function table(o) {
    const cellCls = c => [c.num ? "num" : "", c.priority ? "p" + c.priority : "", c.className || ""].join(" ").trim() || null;
    const thead = h("thead", null, h("tr", null, o.columns.map(c => h("th", {scope: "col", class: cellCls(c)}, c.label))));
    const tbody = h("tbody");
    const tbl = h("table", {class: "table" + (o.compact ? " compact" : ""), "aria-label": o.label || null}, thead, tbody);
    const wrap = h("div", "table-wrap", tbl);
    let selected = o.selected || null;
    const fillRow = (tr, row) => {
      clear(tr);
      for (const c of o.columns) tr.appendChild(h("td", {class: cellCls(c)}, kids(c.render(row))));
      tr.classList.toggle("is-selected", tr.dataset.key === selected);
    };
    function setRows(rows) {
      Core.keyed(tbody, rows, o.keyOf, row => {
        const tr = h("tr", {class: o.onRowClick ? "clickable" : null, tabindex: o.onRowClick ? "0" : null});
        tr.dataset.key = String(o.keyOf(row));
        if (o.onRowClick) {
          tr.addEventListener("click", e => {
            if (e.target.closest("a,button,input,select,label")) return;
            o.onRowClick(tr._row, tr);
          });
          tr.addEventListener("keydown", e => {
            if (e.key === "Enter" && e.target === tr) o.onRowClick(tr._row, tr);
          });
        }
        tr._row = row;
        fillRow(tr, row);
        return tr;
      }, (tr, row) => {
        if (tr._row !== row) {
          tr._row = row;
          fillRow(tr, row);
        }
      });
    }
    function setSelected(k) {
      selected = k === null || k === undefined ? null : String(k);
      for (const tr of tbody.children) tr.classList.toggle("is-selected", tr.dataset.key === selected);
    }
    return {el: wrap, table: tbl, tbody, setRows, setSelected,
      rowOf: k => tbody.querySelector('tr[data-key="' + CSS.escape(String(k)) + '"]')};
  }

  // Panel shown by a view whose data needs an admin key.
  function needAdmin() {
    const other = button(t("shell.loginOther"), {kind: "primary", onClick: () => Shell.logout()});
    return h("div", "card need-admin", empty({icon: "lock", title: t("shell.needAdmin"), body: t("api.adminRequired"),
      actions: [other]}));
  }

  function init() {
    document.addEventListener("keydown", e => {
      if (e.key === "Escape") {
        if (document.activeElement && document.activeElement._armed) {
          disarm(document.activeElement);
          return;
        }
        if (pop) {
          e.preventDefault();
          closePopover(true);
          return;
        }
        const top = layers[layers.length - 1];
        if (top) {
          e.preventDefault();
          top.close();
        }
        return;
      }
      if (e.key !== "Tab") return;
      const top = layers[layers.length - 1];
      if (!top || pop) return;
      const f = focusables(top.panel);
      if (!f.length) return;
      const i = f.indexOf(document.activeElement);
      e.preventDefault();
      f[i < 0 ? (e.shiftKey ? f.length - 1 : 0) : (i + (e.shiftKey ? f.length - 1 : 1)) % f.length].focus();
    });
    document.addEventListener("pointerdown", e => {
      if (pop && !pop.el.contains(e.target) && !pop.anchor.contains(e.target)) closePopover();
      hideTip();
    }, true);
    addEventListener("resize", () => {
      if (pop) pop.reposition();
      hideTip();
    });
    document.addEventListener("scroll", e => {
      if (pop && !pop.el.contains(e.target)) pop.reposition();
      hideTip();
    }, true);
    // Popover: Tab past its last control closes it and continues from the anchor.
    document.addEventListener("focusin", e => {
      if (pop && !pop.el.contains(e.target) && e.target !== pop.anchor) closePopover();
    });
    const fine = () => typeof matchMedia === "function" && matchMedia("(hover: hover) and (pointer: fine)").matches;
    document.addEventListener("pointerover", e => {
      if (!fine()) return;
      const el = e.target.closest && e.target.closest("[data-tip]");
      if (el && el !== tipFor) showTip(el);
      else if (!el && tipFor) hideTip();
    });
    document.addEventListener("focusin", e => {
      const el = e.target.closest && e.target.closest("[data-tip]");
      if (el && e.target.matches(":focus-visible")) showTip(el);
      else hideTip();
    });
  }

  return {PHONE_Q, isPhone, icon, button, toast, open, modal, drawer, sheet, page, confirm, closeAll, handlePop, afterHistory, layerCount,
    blocking, popover, closePopover, info, menu, clickAgain, disarm, segmented, tabs, skeleton, empty, errorState, banner, badge,
    tag, dot, chip, kv, field, saveBar, table, needAdmin, init};
})();
