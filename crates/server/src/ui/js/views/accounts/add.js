"use strict";
// AccAdd: adding accounts — a two-step dialog (desktop modal, phone full-screen page): pick a method
// (device code login, paste auth.json, import files, rescan the directory) → in progress. The
// device-code flow lives in the view (env.device) and outlives the dialog: closing the dialog keeps
// waiting, "stop waiting" only stops this page's polling (the browser authorization may still land).
const AccAdd = (() => {
  const {h, svg} = Core;
  const t = (k, v) => I18n.t(k, v);
  const fmt = Core.fmt;
  const T = {timeout: 45000};
  const SKIP = ["permission_denied", "read_error", "invalid_json", "missing_tokens", "unsupported_type", "invalid_name", "too_large",
    "write_failed"];
  const HINTS = ["invalid_json", "missing_tokens", "invalid_name", "unsupported_type"];

  let open = null; // {layer, step, paint}

  // Countdown ring: remaining share of the 15-minute code lifetime.
  function ring(frac) {
    const r = 15, c = 2 * Math.PI * r;
    return svg("svg", {class: "acc-ring", viewBox: "0 0 36 36", width: 36, height: 36, "aria-hidden": "true"},
      svg("circle", {class: "acc-ring-track", cx: 18, cy: 18, r}),
      svg("circle", {class: "acc-ring-fill", cx: 18, cy: 18, r, "stroke-dasharray": c.toFixed(2),
        "stroke-dashoffset": (c * (1 - Core.clamp(frac, 0, 1))).toFixed(2), transform: "rotate(-90 18 18)"}));
  }

  function option(icon, title, sub, onClick, o) {
    const b = h("button", {type: "button", class: "acc-opt", "data-fk": "add|" + (o && o.id)}, h("span", "acc-opt-icon", UI.icon(icon)),
      h("span", "acc-opt-text", h("span", "acc-opt-title", title, o && o.badge ? UI.badge(o.badge, "accent") : null), h("span",
          "acc-opt-sub", sub)),
      UI.icon("chevron-right", "acc-opt-go"));
    b.addEventListener("click", onClick);
    return b;
  }

  function pickStep(env) {
    return h("div", "acc-add-pick",
      option("key", t("accounts.add.device"), t("accounts.add.deviceSub"), () => go(env, "device", true), {id: "device",
          badge: t("accounts.add.rec")}),
      option("copy", t("accounts.add.paste"), t("accounts.add.pasteSub"), () => go(env, "paste"), {id: "paste"}),
      option("inbox", t("accounts.add.files"), t("accounts.add.filesSub"), () => env.pickFiles(), {id: "files"}),
      h("p", "small muted acc-add-foot", t("accounts.add.rescanHint")));
  }

  function deviceStep(env) {
    const st = env.device.state, box = h("div", "acc-dev");
    if (st.phase === "starting") {
      box.appendChild(h("div", "acc-dev-wait", h("span", "spinner"), t("accounts.dev.requesting")));
      return box;
    }
    if (st.phase === "waiting") {
      const link = h("a", {class: "btn", href: st.url, target: "_blank", rel: "noopener noreferrer"}, h("span", null,
          t("accounts.dev.open")),
        UI.icon("external"));
      const copy = UI.button(t("ui.copy"), {size: "sm", icon: "copy", attrs: {"data-fk": "add|copy"}});
      copy.addEventListener("click", async () => {
        try {
          await Core.copy(st.code);
          UI.toast(t("ui.copied"), {kind: "ok"});
        } catch (_) {
          UI.toast(t("ui.copyFailed"), {kind: "err"});
        }
      });
      const left = Math.max(0, st.until - Date.now());
      const stop = UI.button(t("accounts.dev.stop"), {attrs: {"data-fk": "add|stop"}});
      stop.addEventListener("click", () => env.device.stop());
      box.append(
        h("ol", "acc-dev-steps",
          h("li", null, h("span", "acc-dev-n", "1"), h("div", "grow", h("div", null, t("accounts.dev.step1")), h("div",
              "mono small muted acc-url", st.url)), link),
          h("li", null, h("span", "acc-dev-n", "2"), h("div", "grow", h("div", null, t("accounts.dev.step2")),
            h("div", "acc-code-row", h("span", {class: "acc-code", "aria-label": t("accounts.dev.codeAria")}, st.code), copy)))),
        h("div", {class: "acc-dev-status", role: "status"}, h("span", "acc-dev-ring", ring(left / 9e5)),
          h("div", "grow", h("div", null, h("span", "spinner sm"), " ", t("accounts.dev.waiting")),
            h("div", {class: "small muted num acc-dev-left", "data-until": st.until, "data-pre": t("accounts.dev.leftPre")},
              t("accounts.dev.leftPre") + fmt.dur(left / 1000))), stop),
        h("p", "small muted", t("accounts.dev.note")));
      return box;
    }
    const n = st.note;
    if (n) {
      const ok = n.kind === "done";
      const msg = ok ? t("accounts.dev.done", {account: n.account}) : n.kind === "stopped" ? t("accounts.dev.stopped") :
        n.kind === "expired" ? t("accounts.dev.expired") : n.kind === "failed" ? t("accounts.dev.failed", {msg: n.msg}) :
          n.kind === "checkFailed" ? t("accounts.dev.checkFailed", {why: n.why}) : t("accounts.dev.startFailed", {why: n.why});
      box.appendChild(UI.banner({kind: ok ? "info" : n.kind === "stopped" ? "info" : "err", title: msg}));
    }
    const again = UI.button(n ? t("accounts.dev.restart") : t("accounts.dev.start"), {kind: "primary", attrs: {"data-fk": "add|start"}});
    again.addEventListener("click", () => env.device.start());
    box.appendChild(h("div", "acc-row-btns", again));
    return box;
  }

  function pasteStep(env) {
    const ta = h("textarea", {class: "textarea mono acc-paste", rows: "8", spellcheck: "false", autocomplete: "off",
      placeholder: "{\"tokens\": {…}}", "aria-label": t("accounts.paste.aria"), "data-fk": "add|ta"});
    ta.value = env.pasteDraft || "";
    ta.addEventListener("input", () => {
      env.pasteDraft = ta.value;
    });
    const submit = UI.button(t("accounts.paste.import"), {kind: "primary", attrs: {"data-fk": "add|imp"}});
    submit.addEventListener("click", () => importPaste(env, ta, submit));
    return h("div", "acc-paste-box", h("p", "small muted", t("accounts.paste.hint")), ta, h("div", "acc-row-btns", submit));
  }

  async function importPaste(env, ta, btn) {
    let doc;
    try {
      doc = JSON.parse(ta.value);
    } catch (_) {
      UI.toast(t("accounts.paste.badJson"), {kind: "err"});
      ta.focus();
      return;
    }
    const run = async () => {
      const j = await env.ctx.post("/admin/v1/accounts/import", {auth_json: doc}, T);
      env.pasteDraft = "";
      if (ta.isConnected) ta.value = "";
      UI.toast(t("accounts.toast.imported", {account: j.account}), {kind: "ok"});
      close();
      await env.reload();
    };
    await Api.busyRun(btn, t("accounts.paste.import"), async () => {
      const dry = await env.ctx.post("/admin/v1/accounts/import", {auth_json: doc, dry_run: true}, T);
      if (!dry.exists) return run();
      // Same account already there: importing replaces its tokens (the current ones are not kept).
      const em = typeof doc.email === "string" && doc.email ? doc.email : null;
      AccDetail.dialog({title: t("accounts.paste.replaceTitle", {name: em || dry.account}), ok: t("accounts.paste.replace"), danger: true,
        body: [h("p", "mono small", em ? em + " (" + dry.account + ")" : dry.account), h("p", null, t("accounts.paste.replaceBody")),
          h("div", "acc-warnbox", h("b", null, t("accounts.irreversible")), " ", t("accounts.paste.replaceWarn")),
          h("p", "muted small", t("accounts.paste.replaceNote"))],
        run});
    }, t("accounts.busy"));
  }

  const STEPS = {pick: pickStep, device: deviceStep, paste: pasteStep};

  function title(step) {
    return step === "device" ? t("accounts.add.device") : step === "paste" ? t("accounts.add.paste") : t("accounts.add.title");
  }

  function paint() {
    if (!open) return;
    const {layer, env} = open;
    layer.setTitle(title(open.step));
    open.back.classList.toggle("hide", open.step === "pick");
    // Phone: the page header already holds the page's own back arrow, so the way back to the
    // method list is a button under the step instead of a second arrow.
    const other = open.phone && open.step !== "pick" ? UI.button(t("accounts.add.other"), {kind: "ghost", icon: "chevron-left",
      className: "acc-add-other", onClick: () => go(env, "pick")}) : null;
    Core.steady([layer.body], () => Core.fill(layer.body, STEPS[open.step](env), other));
  }

  function go(env, step, autostart) {
    if (!open) return;
    open.step = step;
    if (step === "device" && autostart && env.device.state.phase === "idle" && !env.device.state.note) env.device.start();
    else paint();
    const f = open.layer.body.querySelector("textarea,[data-fk='add|stop'],[data-fk='add|start'],a.btn");
    if (f && !UI.isPhone()) f.focus({preventScroll: true});
  }

  // openDialog(env, step?) — the device flow in progress reopens on its own step.
  function openDialog(env, step) {
    if (open) return;
    step = step || (env.device.busy() ? "device" : "pick");
    const back = UI.button("", {kind: "ghost", icon: "chevron-left", title: t("ui.back"), className: "hide acc-add-back"});
    const phone = UI.isPhone();
    const make = phone ? UI.page : UI.modal;
    const layer = make({title: title(step), head: phone ? null : back, body: h("div"), className: "acc-add", onClose: () => {
      open = null;
      env.renderToolbar();
    }});
    if (!phone) layer.head.insertBefore(back, layer.head.firstChild);
    open = {layer, env, step, back, phone};
    back.addEventListener("click", () => go(env, "pick"));
    const tick = setInterval(() => {
      if (!open || open.layer !== layer) return clearInterval(tick);
      const st = env.device.state;
      const r = layer.body.querySelector(".acc-dev-ring");
      if (r && st.phase === "waiting") Core.fill(r, ring((st.until - Date.now()) / 9e5));
    }, 1000);
    paint();
    if (step === "device" && env.device.state.phase === "idle" && !env.device.state.note) env.device.start();
  }

  function close() {
    if (open) open.layer.close();
  }

  // ---------- import files (CPA codex account files or Codex CLI auth.json), checked server side ----------
  async function importFiles(env, files, btn) {
    const run = async () => {
      const {send, big} = await AccModel.importPayload(files);
      const j = send.length ? await env.ctx.post("/admin/v1/accounts/import-files", {files: send}, T) : {added: 0, exists: 0, skipped: 0,
          results: []};
      j.results = (j.results || []).concat(big);
      j.skipped = (j.skipped || 0) + big.length;
      close();
      await env.reload();
      if (!j.exists && !j.skipped) UI.toast(t("accounts.toast.filesImported", {n: j.added}), {kind: "ok"});
      else importResult(j);
    };
    if (btn) await Api.busyRun(btn, t("accounts.tb.import"), run, t("accounts.busyImport"));
    else await run().catch(e => env.ctx.fail(e, t("accounts.tb.import")));
  }

  function skipLine(f, upload) {
    const reason = SKIP.includes(f.reason) ? t("accounts.skip." + f.reason) : f.reason;
    const detail = (f.reason === "unsupported_type" || f.reason === "read_error") && f.detail ? " (" + f.detail + ")" : "";
    let extra = null;
    if (f.reason === "permission_denied" && f.uid !== undefined) {
      extra = t("accounts.skip.owner", {uid: f.uid, gid: f.gid, mode: f.mode === undefined || f.mode === null ? "?" :
          f.mode.toString(8).padStart(4, "0")});
    } else if (upload && f.reason === "missing_tokens") extra = t("accounts.skip.apiKeyAuth");
    else if (upload && f.reason === "write_failed") extra = t("accounts.skip.writeFailed", {detail: f.detail ? " (" + f.detail + ")" : ""});
    else if (HINTS.includes(f.reason)) extra = t("accounts.skipHint." + f.reason);
    return h("li", null, h("span", "mono", f.file || f.name), h("b", null, reason + detail), extra ? h("span", "muted", extra) : null,
      f.kept ? h("span", "muted", t("accounts.skip.kept")) : null);
  }

  function importResult(j) {
    const grp = (st, title) => {
      const L = (j.results || []).filter(r => r.status === st);
      if (!L.length) return null;
      return h("div", "acc-imr-grp", h("div", "small muted", title), h("ul", "acc-files", L.map(r => st === "skipped" ? skipLine(r, true) :
        h("li", null, h("span", "mono", r.name), h("b", {class: st === "added" ? "c-ok" : "muted"}, st === "added" ?
            t("accounts.imr.added") :
          t("accounts.imr.exists")), h("span", "mono muted", r.account)))));
    };
    const ok = UI.button(t("ui.close"), {kind: "primary"});
    const layer = UI.modal({title: t("accounts.imr.title", {n: j.added}) + (j.exists ? t("accounts.imr.existsN", {n: j.exists}) : "") +
      (j.skipped ? t("accounts.imr.skippedN", {n: j.skipped}) : ""), wide: true, body: [grp("added", t("accounts.imr.gAdded")),
      grp("exists", t("accounts.imr.gExists")), grp("skipped", t("accounts.imr.gSkipped"))], foot: [ok], focus: ok});
    ok.addEventListener("click", () => layer.close());
  }

  // ---------- rescan the directory ----------
  async function rescan(env, btn) {
    await Api.busyRun(btn, t("accounts.tb.rescan"), async () => {
      const j = await env.ctx.post("/admin/v1/accounts/reload", {}, T);
      await env.reload();
      const bad = ((j.auth_files && j.auth_files.skipped) || []).filter(f => f.reason !== "unsupported_type").length;
      const msg = t("accounts.rescan.done", {what: j.added && j.added.length ? t("accounts.rescan.added", {n: j.added.length}) :
          t("accounts.rescan.none"),
        removed: j.removed && j.removed.length ? t("accounts.rescan.removed", {n: j.removed.length}) : "", n: j.accounts});
      if (bad) UI.toast(msg + "\n" + t("accounts.rescan.bad", {n: bad}), {kind: "err"});
      else UI.toast(msg, {kind: "ok"});
    }, t("accounts.busyScan"));
  }

  return {open: openDialog, close, paint: () => paint(), isOpen: () => !!open, importFiles, rescan, skipLine, ring};
})();
