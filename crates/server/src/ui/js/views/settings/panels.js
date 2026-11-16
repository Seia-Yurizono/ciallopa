"use strict";
// Settings panels that are not schema rows: model prices (/admin/v1/prices*), API Keys
// (/admin/v1/config/keys) and the change log (/admin/v1/config/audit). Each panel is created per
// mount with an env: {ctx, draft, fail(err), reload() (re-read the config), onChange() (summaries)}.
const SettingsPanels = (() => {
  const {h, fmt} = Core;
  const t = (k, v) => I18n.t(k, v);

  const host = u => {
    try {
      return new URL(u).host;
    } catch (_) {
      return u;
    }
  };

  // ---------- prices ----------
  function prices(env) {
    const {ctx} = env;
    let st = null, filter = "all";
    const info = h("div", "st-pr-info");
    const note = h("div", "st-pr-note");
    const check = UI.button(t("settings.pr.check"), {kind: "primary", attrs: {"data-fk": "pcheck"}});
    const status = h("div", "card st-pr-status", h("div", "st-pr-line", info, check), note);
    const seg = h("div", "st-pr-filter");
    const list = h("div", {class: "st-pr-list", role: "list"});
    const models = h("div", "st-sub st-pr-models hide",
      h("div", "st-sub-head", h("h3", "st-sub-title", t("settings.pr.models")),
        UI.info(t("settings.pr.legend"), t("settings.pr.models")), h("span", "grow"), seg),
      h("div", "card", list));
    const el = h("div", "st-pr", status, models);

    async function load() {
      const p = await ctx.get("/admin/v1/prices");
      st = p;
      render();
      env.onChange();
      return p;
    }

    function render() {
      const p = st;
      Core.clear(info);
      Core.clear(note);
      check.textContent = t("settings.pr.check");
      if (!p) {
        info.appendChild(UI.skeleton.lines(1));
        return;
      }
      if (!p.usage_enabled) {
        info.appendChild(h("span", "st-pr-count", t("settings.info.usageOff")));
        check.disabled = true;
        models.classList.add("hide");
        return;
      }
      info.append(h("span", "st-pr-count", p.models ? t("settings.info.models", {n: p.models}) : t("settings.info.noTable")),
        h("span", "st-pr-meta",
          p.updated_ms ? " · " + t("settings.pr.updated", {t: fmt.time(p.updated_ms, true)}) : "",
          p.enabled ? [" · " + t("settings.pr.source") + " ", h("span", {class: "mono", title: p.source}, host(p.source))] :
            " · " + t("settings.pr.noSource")));
      check.disabled = !p.enabled;
      check.title = p.enabled ? t("settings.pr.checkTip") : t("settings.pr.noSourceTip");
      const un = p.unpriced || [];
      if (!p.models && !un.length && !(p.ignored || []).length) note.textContent = t("settings.pr.emptyNote");
      else if (un.length) note.textContent = t("settings.pr.unpricedNote", {n: un.length});
      else note.append(t("settings.pr.file") + " ", h("span", "mono", p.path || ""), " · " + t("settings.pr.fileNote"));
      renderModels(p);
    }

    function renderModels(p) {
      const un = p.unpriced || [], ig = p.ignored || [], cu = p.custom || [];
      const kinds = [["all", un.length + cu.length + ig.length], ["un", un.length], ["cu", cu.length], ["ig", ig.length]];
      if (!kinds.find(k => k[0] === filter && k[1])) filter = "all";
      Core.clear(seg);
      const shown = kinds.filter(k => k[0] === "all" || k[1]);
      if (shown.length > 2) {
        seg.appendChild(UI.segmented({label: t("settings.pr.models"), value: filter,
          options: shown.map(([k, n]) => ({value: k, label: t("settings.pr." + k) + " " + n})),
          onChange: v => {
            filter = v;
            renderModels(st);
          }}));
      }
      const rows = [];
      const btn = (m, act, kind) => {
        const b = UI.button(t("settings.pr." + act), {size: "sm", kind, attrs: {"data-fk": "pm|" + m + "|" + act}});
        b.addEventListener("click", () => onAction(m, act, b));
        return b;
      };
      if (filter === "all" || filter === "un") {
        for (const m of un) rows.push(row(m, UI.tag(t("settings.pr.un"), "warn"), t("settings.pr.usedRecently"),
          [btn(m, "set"), btn(m, "ignore", "ghost")]));
      }
      if (filter === "all" || filter === "cu") {
        for (const c of cu) {
          rows.push(row(c.model, UI.tag(t("settings.pr.cu"), "accent"), t("settings.pr.customLine",
            {i: c.in, c: c.cache === null || c.cache === undefined ? t("settings.pr.sameAsInput") : c.cache, o: c.out}),
          [btn(c.model, "edit"), btn(c.model, "unset", "ghost")]));
        }
      }
      if (filter === "all" || filter === "ig") {
        for (const m of ig) rows.push(row(m, UI.tag(t("settings.pr.ig")), t("settings.pr.ignoredLine"), [btn(m, "unignore", "ghost")]));
      }
      Core.fill(list, rows);
      models.classList.toggle("hide", !(un.length || ig.length || cu.length));
      Core.refocus();
    }

    const row = (model, tag, detail, actions) => h("div", {class: "st-pr-row", role: "listitem"},
      h("span", "st-pr-model mono", model), h("span", "st-pr-state", tag, h("span", "st-pr-detail", detail)),
      h("span", "st-pr-actions", actions));

    // Writes go through here; the change log is re-read afterwards either way.
    async function pmodel(body) {
      try {
        return await ctx.post("/admin/v1/prices/models", body);
      } finally {
        env.afterWrite();
      }
    }

    function onAction(m, act, b) {
      const cur = (st && st.custom || []).find(c => c.model === m);
      if (act === "set" || act === "edit") {
        priceDialog(m, cur);
        return;
      }
      Api.busyRun(b, b.textContent, async () => {
        if (act === "ignore") {
          await pmodel({model: m, action: "ignore"});
          UI.toast(t("settings.pr.ignored", {m}), {kind: "ok", undo: async () => {
            await pmodel({model: m, action: "unignore"});
            UI.toast(t("settings.pr.unignored", {m}));
            await load();
          }});
        } else if (act === "unignore") {
          await pmodel({model: m, action: "unignore"});
          UI.toast(t("settings.pr.unignored", {m}), {kind: "ok", undo: async () => {
            await pmodel({model: m, action: "ignore"});
            UI.toast(t("settings.pr.reIgnored", {m}));
            await load();
          }});
        } else if (act === "unset") {
          await pmodel({model: m, action: "unset"});
          UI.toast(t("settings.pr.unsetDone", {m}), {kind: "ok", undo: cur ? async () => {
            await pmodel({model: m, action: "set", in: cur.in, cache: cur.cache, out: cur.out});
            UI.toast(t("settings.pr.restored", {m}));
            await load();
          } : null});
        }
        await load();
      }, t("settings.pr.working"));
    }

    // Price dialog: input and output required, cached input optional (empty = same as input).
    function priceDialog(m, cur) {
      const inp = (v, ph) => h("input", {type: "number", class: "input num", min: "0", max: "10000", step: "any", inputmode: "decimal",
        value: v === null || v === undefined ? "" : String(v), placeholder: ph || null});
      const fin = inp(cur && cur.in), fca = inp(cur && cur.cache, t("settings.pr.cacheHint")), fout = inp(cur && cur.out);
      const err = h("div", {class: "field-error hide", role: "alert"});
      const form = h("form", {class: "st-pr-form", novalidate: true},
        h("p", "muted small", t("settings.pr.dlgUnit")),
        h("div", "st-pr-grid", UI.field({label: t("settings.pr.in"), control: fin}),
          UI.field({label: t("settings.pr.cache"), control: fca}), UI.field({label: t("settings.pr.out"), control: fout})),
        err, h("p", "muted small", t("settings.pr.dlgNote")));
      const cancel = UI.button(t("ui.cancel"), {onClick: () => layer.close()});
      const save = UI.button(t("settings.pr.save"), {kind: "primary", type: "submit", attrs: {form: "st-pr-form-" + m.length}});
      form.id = "st-pr-form-" + m.length;
      const layer = UI.modal({title: t(cur ? "settings.pr.dlgEdit" : "settings.pr.dlgSet", {m}), body: form, foot: [cancel, save],
        focus: fin});
      form.addEventListener("submit", e => {
        e.preventDefault();
        const num = x => {
          const s = x.value.trim();
          if (s === "") return null;
          const v = Number(s);
          return Number.isFinite(v) && v >= 0 && v <= 10000 ? v : NaN;
        };
        const i = num(fin), c = num(fca), o = num(fout);
        const bad = i === null || Number.isNaN(i) ? fin : o === null || Number.isNaN(o) ? fout : Number.isNaN(c) ? fca : null;
        for (const x of [fin, fca, fout]) x.setAttribute("aria-invalid", String(x === bad));
        if (bad) {
          err.textContent = t("settings.pr.dlgErr");
          err.classList.remove("hide");
          bad.focus();
          return;
        }
        Api.busyRun(save, t("settings.pr.save"), async () => {
          await pmodel({model: m, action: "set", in: i, cache: c, out: o});
          layer.close();
          UI.toast(t("settings.pr.setDone", {m}), {kind: "ok"});
          await load();
        }, t("settings.pr.working"));
      });
    }

    check.addEventListener("click", () => Api.busyRun(check, t("settings.pr.check"), async () => {
      try {
        const j = await ctx.post("/admin/v1/prices/check", {});
        const changes = j.changes || [];
        const act = changes.filter(c => c.kind === "add" || c.kind === "update");
        const rest = changes.filter(c => /^skip_/.test(c.kind));
        if (!j.token) {
          UI.toast(t("settings.pr.upToDate", {n: j.remote_models}), {kind: "ok"});
          return;
        }
        const tbl = UI.table({columns: [
          {id: "m", label: t("settings.pr.colModel"), render: c => h("span", "mono", (c.kind === "add" ? "+ " : "~ ") + c.model)},
          {id: "o", label: t("settings.pr.colOld"), render: c => h("span", "mono muted", c.old || t("settings.pr.added"))},
          {id: "n", label: t("settings.pr.colNew"), render: c => h("span", "mono", c.new)}], keyOf: c => c.model, compact: true});
        tbl.setRows(act);
        const body = h("div", "stack", tbl.el,
          rest.length ? h("p", "muted small",
            t("settings.pr.skipped") + rest.map(c => c.model).join(I18n.lang() === "zh" ? "、" : ", ")) : null,
          h("p", "muted small", t("settings.pr.backupNote", {p: st && st.path || ""})));
        const cancel = UI.button(t("ui.cancel"), {onClick: () => layer.close()});
        const apply = UI.button(t("settings.pr.apply"), {kind: "primary"});
        const layer = UI.modal({title: t("settings.pr.changesTitle", {a: j.added, u: j.updated}), body, foot: [cancel, apply], wide: true,
          focus: apply});
        apply.addEventListener("click", () => Api.busyRun(apply, t("settings.pr.apply"), async () => {
          try {
            const r = await ctx.post("/admin/v1/prices/apply", {token: j.token});
            layer.close();
            UI.toast(t("settings.pr.applied", {n: r.applied, b: r.backup || ""}), {kind: "ok"});
          } finally {
            env.afterWrite();
            load().catch(() => {});
          }
        }, t("settings.pr.working")));
      } finally {
        load().catch(() => {});
      }
    }, t("settings.pr.checking")));

    return {el, load, render, state: () => st};
  }

  // ---------- API Keys ----------
  function keys(env) {
    const {ctx} = env;
    let cfg = null;
    const add = UI.button(t("settings.k.new"), {kind: "primary", icon: "plus", attrs: {"data-fk": "knew"}});
    const count = h("span", "st-k-count");
    const head = h("div", "st-k-head", count, h("span", "st-k-note muted small", t("settings.k.adminNote")), h("span", "grow"), add);
    const tbl = UI.table({label: t("settings.p.keys"), keyOf: k => k.label, columns: [
      {id: "label", label: t("settings.k.colLabel"), className: "st-k-label", render: k => h("span", "strong", k.label)},
      {id: "hint", label: t("settings.k.colKey"), className: "st-k-hint", render: k => h("span", "mono", k.hint)},
      {id: "role", label: t("settings.k.colRole"), className: "st-k-role", render: k => (k.admin ?
        h("span", {class: "tag accent", title: t("settings.k.adminTip")}, t("settings.k.admin")) :
        h("span", {class: "tag", title: t("settings.k.clientTip")}, t("settings.k.client")))},
      {id: "act", label: "", className: "st-k-act", render: k => (cfg && cfg.write_enabled ? delButton(k) : null)}]});
    const none = h("div", "st-k-none hide", UI.empty({icon: "key", title: t("settings.k.none")}));
    const el = h("div", "st-keys", head, h("div", "card card-flush", tbl.el, none));
    add.addEventListener("click", openCreate);

    function render(c) {
      cfg = c;
      const list = c && c.running && c.running.api_keys || [];
      count.textContent = t("settings.k.count", {n: list.length});
      add.classList.toggle("hide", !(c && c.write_enabled));
      // Rows are keyed by label; a write mode change re-creates them.
      tbl.setRows([]);
      tbl.setRows(list);
      tbl.el.classList.toggle("hide", !list.length);
      none.classList.toggle("hide", !!list.length);
    }

    function delButton(k) {
      const b = UI.button("", {kind: "ghost", size: "sm", icon: "trash", title: t("settings.k.delLabel", {l: k.label}),
        attrs: {"data-fk": "key|" + k.label, "aria-haspopup": "dialog"}});
      b.classList.add("st-k-del");
      b.addEventListener("click", async () => {
        const ok = await UI.confirm({title: t("settings.k.delTitle", {l: k.label}), body: t("settings.k.delBody"),
          ok: t("settings.k.del"), danger: true});
        if (!ok || !ctx.alive()) return;
        await Api.busyRun(b, t("settings.k.del"), async () => {
          try {
            await ctx.req("DELETE", "/admin/v1/config/keys/" + encodeURIComponent(k.label), {}, {"If-Match": env.draft.etag});
          } catch (e) {
            env.fail(e);
            return;
          }
          await env.reload();
          UI.toast(t("settings.k.deleted", {l: k.label}), {kind: "ok"});
        });
      });
      return b;
    }

    // Create: label → server-generated key, shown once in the same dialog with copy.
    function openCreate() {
      const input = h("input", {class: "input", maxlength: "64", required: true, autocomplete: "off", spellcheck: "false",
        placeholder: t("settings.k.labelPh")});
      const form = h("form", {class: "stack", id: "st-k-form", novalidate: true},
        UI.field({label: t("settings.k.label"), control: input}), h("p", "muted small", t("settings.k.labelHint")));
      const cancel = UI.button(t("ui.cancel"), {onClick: () => layer.close()});
      const go = UI.button(t("settings.k.create"), {kind: "primary", type: "submit", attrs: {form: "st-k-form"}});
      const layer = UI.modal({title: t("settings.k.new"), body: form, foot: [cancel, go], focus: input,
        onClose: () => Core.clear(layer.body)});
      form.addEventListener("submit", e => {
        e.preventDefault();
        const label = input.value.trim();
        if (!label) {
          input.setAttribute("aria-invalid", "true");
          input.focus();
          return;
        }
        Api.busyRun(go, t("settings.k.create"), async () => {
          let j;
          try {
            ({j} = await ctx.req("POST", "/admin/v1/config/keys", {label}, {"If-Match": env.draft.etag}));
          } catch (err) {
            if (!Api.isQuiet(err)) env.fail(err);
            return;
          }
          // Re-read after the write; the secret appears only for the identity that asked for it.
          await env.reload().catch(() => {});
          if (!ctx.alive() || layer.closed) return;
          showSecret(layer, j.key);
        }, t("settings.k.creating"));
      });
    }

    function showSecret(layer, secret) {
      layer.setTitle(t("settings.k.createdTitle"));
      const code = h("div", {class: "code-block st-k-secret", tabindex: "0"}, secret);
      const copy = UI.button(t("ui.copy"), {icon: "copy", onClick: () => Core.copy(secret).then(
        () => UI.toast(t("settings.k.copied"), {kind: "ok"}), () => UI.toast(t("settings.k.copyFail"), {kind: "err"}))});
      const done = UI.button(t("settings.k.saved"), {kind: "primary", onClick: () => layer.close()});
      Core.fill(layer.body, UI.banner({kind: "warn", title: t("settings.k.once")}), code, h("div", "row", copy));
      Core.fill(layer.foot, done);
      copy.focus();
    }

    return {el, render};
  }

  // ---------- change log ----------
  function audit(env) {
    const {ctx} = env;
    let entries = null;
    const tbl = UI.table({label: t("settings.p.audit"), keyOf: e => e.ts_ms + "|" + e.action + "|" + (e.actor || ""), columns: [
      {id: "t", label: t("settings.a.time"), className: "st-a-time", render: e => h("span", {class: "num", title: fmt.iso(e.ts_ms)},
        fmt.time(e.ts_ms))},
      {id: "actor", label: t("settings.a.actor"), className: "st-a-actor", render: e => (e.actor ? h("span", null, e.actor) :
        h("span", "muted", t("settings.a.fileSignal")))},
      {id: "act", label: t("settings.a.action"), className: "st-a-act", render: e => h("span", null, e.action,
        h("span", "muted", " · " + e.source))},
      {id: "c", label: t("settings.a.changes"), className: "st-a-changes", render: e => h("div", "st-a-list",
        (e.changes || []).map(c => h("div", "st-a-change", h("code", null, c.key), " ",
          h("span", "muted", SettingsModel.fmtValue(c.old)), " → ", h("span", null, SettingsModel.fmtValue(c.new)))))}]});
    const none = h("div", "hide", UI.empty({icon: "clock", title: t("settings.a.none")}));
    const skel = UI.skeleton.rows(4, 4);
    const el = h("div", "card card-flush st-audit", skel, tbl.el, none);

    async function load() {
      const j = await ctx.get("/admin/v1/config/audit?limit=20");
      entries = j.entries || [];
      render();
      env.onChange();
    }
    function render() {
      skel.classList.toggle("hide", !!entries);
      if (!entries) return;
      tbl.setRows(entries);
      tbl.el.classList.toggle("hide", !entries.length);
      none.classList.toggle("hide", !!entries.length);
    }
    const last = () => (entries && entries.length ? entries[0].ts_ms : null);
    return {el, load, render, last};
  }

  return {prices, keys, audit};
})();
