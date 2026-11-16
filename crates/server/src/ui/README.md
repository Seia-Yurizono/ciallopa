# Management UI (`crates/server/src/ui`)

One static single-page document, served by `ui.rs` at `/ui`, `/ui/requests`, `/ui/accounts`,
`/ui/settings` and `/ui/system`. Plain HTML/CSS/JS: no framework, bundler, npm or external resource.

## Layout and load order

```
index.html                 skeleton (login, sidebar, top bar, tab bar, icon sprite); <style>/*@css*/</style>, <script>/*@js*/</script>
css/tokens.css             colour / type / space / radius / motion tokens, light + dark
css/base.css               reset, typography, utilities (.hide .num .muted .trunc .row .stack .phone-only .desk-only)
css/components.css         buttons, inputs, seg, tabs, chip/badge/tag/dot, card, table, kv, meter, skeleton, empty,
                           banner, toast, layers (modal/drawer/sheet/page), popover, tip, menu, savebar
css/charts.css             chart marks and colour classes
css/shell.css              app frame, sidebar / rail, top scope bar, tab bar, "more" panel, login
css/<view>.css             one per view, owned by that view
js/core.js                 Core (+ the `Views` registry)
js/i18n.js, i18n/zh.js, i18n/en.js   I18n and the shared dictionary
js/api.js                  Api
js/components.js           UI
js/charts.js               Charts
js/shell.js                Shell
js/views/<view>.js         Views.overview / requests / accounts / settings / system
js/main.js                 Shell.start()
```

The order is the `include_str!` list in `../ui.rs` — the only manifest. `scripts/lib/page.cjs` parses it,
so the dev server and Node checks assemble exactly what the server ships. **A new file must be added to
`ui.rs`** (`scripts/shell-check.cjs` fails if `ui/` and the manifest differ).

All scripts share one scope. Each file defines one namespace: `const X = (() => { … return {…}; })();`,
view files `Views.name = (() => { … })();`. Load-time code only defines things; anything touching the DOM
runs from `Shell.start()` (Node checks load the real files into a `vm` without a DOM).

## CSP rules

The page CSP allows only the hashed inline `<script>` and `<style>`. Therefore:

- No `style="…"` attributes, no `onclick=` attributes, no `eval` / `new Function`, no `innerHTML` with data.
  Dynamic styles: `el.style.setProperty()` (or `Core.h(tag, {css: {...}})`) and SVG attributes.
  Dynamic text: `textContent` (`Core.h` children). Demo data contains XSS probes; `window.__xss` must stay unset.
- No external fonts, images or scripts. Icons are `<symbol>`s in `index.html` (`UI.icon(name)`).
- The bundle must not contain the text `</script` (checked).

## View contract

```js
Views.example = (() => {
  I18n.add("zh", {"example.title": "示例"});
  I18n.add("en", {"example.title": "Example"});
  return {
    title: () => I18n.t("example.title"),          // top bar + document title
    scope: ["time", "account", "model", "key", "instance"], // shared scope fields shown in the bar (optional)
    mount(root, ctx) { … },                         // build into root
    unmount() { … },                                // module-level state reset (ctx cleans itself)
    onRoute(route) { … },                           // optional: same view, new query/hash (back/forward, Shell.go)
    onLang() { … },                                 // optional: re-render data-driven text after a language switch
  };
})();
```

`ctx` (one per mount, everything is released on unmount / logout):

| member | |
| --- | --- |
| `root`, `name`, `route` (`{view, path, query: URLSearchParams, hash}`) | mount target and the URL that mounted it |
| `signal`, `alive()` | aborted / false after unmount or identity change |
| `get / post / put / del(path, …opts)`, `req(method, path, body, headers, opts)` | `Api` calls bound to the mount: aborted on unmount, a late success rejects with kind `"stale"`, 30 s default timeout |
| `onRefresh(fn({auto}))` | manual / automatic refresh; return a promise to drive the progress line |
| `onScope(fn(scope))`, `onLang(fn)`, `onStatus(fn(status))` | auto-unsubscribed listeners |
| `listen(target, type, fn, opts)`, `interval(fn, ms)`, `timeout(fn, ms)`, `onCleanup(fn)` | auto-released |
| `params()`, `setParams({k: v}, {push})`, `setHash(h, {push})` | view-owned URL state (`""`/`null` removes); scope params are kept |
| `busy(promise)`, `fail(err, name?)` | progress line; default error toast (silent for aborted / stale / 401) |

Auto refresh pauses while the tab is hidden or a blocking layer (modal, sheet, phone page) is open. Keep old
data on screen while refreshing; show skeletons only for the first load.

## Namespaces

**Core** — `$(sel, root)`, `$$()`, `byId(id)`; `h(tag, attrs | class, ...children)` (attrs: `class`, `text`,
`css: {}`, `dataset: {}`, `on<event>`; `true` → empty attribute; null/false skipped), `svg(tag, …)`, `add`, `clear`,
`fill(el, ...children)`; `local` / `session` (`get/set/del`, never throw); `fmt.*` (below); `nf(opts)` cached
`Intl.NumberFormat`; `isNum`; `keyed(container, items, keyOf, create, update)` keyed reconciliation (nodes of kept
keys survive, focus moves to the neighbour of a removed row); `steady(boxes, fn)` re-render keeping the focused /
anchored row (`data-fk`, `data-anchor`, `data-key`) at its viewport position and restoring `data-fk` focus;
`refocus()`; `debounce`; `copy(text)`; `clamp`. Elements with `data-until="<ms>"` (and optional `data-pre`)
show a live countdown.

`Core.fmt` (unknown → `—`): `n` 1.2k / 3.40M · `exact` grouped · `ms` 320ms / 1.24s / 4m 12s · `usd` $0.00 / <$0.01 ·
`exactUsd` · `pct(ratio, digits?)` · `pct100` · `bytes` · `io(in, out)` "1.7k → 233" · `time(ms, noSec)` MM-DD HH:mm:ss ·
`hms` · `hm` · `day` · `iso` · `tzOffset` · `dur(seconds)` countdown · `ago(ms)`. `dur`/`ago`/`day` follow the language.

**I18n** — `t(key, vars)`; `add(lang, dict)` (duplicate keys throw); `lang()`, `locale()` (`zh-CN` | `en`),
`setLang(l)`, `onChange(fn)` → unsubscribe; `apply(root)` fills `data-i18n`, `data-i18n-title`,
`data-i18n-aria-label`, `data-i18n-placeholder`; `number(n, opts)`, `date(ms, opts)`.

**Api** — `req(method, path, body?, headers?, opts?)` → `{j, r}`; `get/post/put/del`; `qs(obj)`; `why(err)`;
`busyRun(button, name, fn, busyLabel?)`; `key/setKey/clearKey`, `bump()` (new identity), `epoch()`, `noAdmin()`,
`isAdminError(e)`, `adminMsg(json)`, `isQuiet(e)`, `T` (45 s timeout opts). Options: `timeout`, `current()`,
`signal`, `strict`. Errors carry `kind` (`network` | `timeout` | `access` | `aborted` | `stale`) or `status` + `body`.
Semantics kept from the old pages: Bearer key from `sessionStorage["ciallopa_key"]`; a 401 of the *current* identity
clears the key and returns to login (message "401"); a 403 `admin_required` sets `noAdmin()` and explains;
Cloudflare Access (opaque redirect, HTML answer, `cf-access-*` 401/403) asks for a reload and keeps the key;
nothing from an older identity (key + epoch) may act.

**UI** — `icon(name)`, `button(label, {kind, size, icon, onClick, title})`; `toast(text, {kind: "ok"|"err", undo,
action: {label, run}})`; layers: `modal / drawer / sheet / page({title, body, foot, head, wide, danger, focus,
onClose})` → `{el, body, close(), setTitle()}` (focus trap, Esc, return focus, phone back gesture; one sheet at
a time; drawer → full-screen page on phones), `confirm({title, body, ok, cancel, danger})` → `Promise<bool>`,
`closeAll()`, `blocking()`, `afterHistory(fn)`; `popover(anchor, content, {placement})`, `info(content)` (ⓘ),
`menu(anchor, items)` (popover / phone sheet); `clickAgain(btn, label, act)`, `disarm`; `segmented({options,
value, onChange})` → `.setValue`; `tabs({items: [{id, label, count, countKind}], value, onChange})` → `.setValue
/.setItems`; `skeleton.rows(n, cols) / block(h) / stats(n) / lines(n)`; `empty({icon, title, body, actions,
kind})`, `errorState({error, retry})`, `banner({kind, title, body, actions, tip})`; `badge`, `tag(text, kind,
{dot, href})`, `dot`, `chip({label, value, onRemove, onClick, pressed})`, `kv(pairs)`, `field({label, hint, control,
error})`, `saveBar({text, onSave, onDiscard})`, `table({columns, keyOf, onRowClick, compact})` → `{el, setRows,
setSelected, rowOf}`, `needAdmin()`; `isPhone()`. Any element with `data-tip` gets a hover/focus tooltip.

**Charts** — colours are keys: `s1..s4` (categorical, fixed order — colour follows the entity), `other`,
`neutral`, `accent`, `ok`, `warn`, `err` (status only). Failures are always `err`.
- `timeSeries(container, {x, series: [{id, label, values, color}], kind: "bar"|"line"|"area", height, yFormat,
  xFormat, tipTitle, tipValue, hint, onClick(i), onBrush(i0, i1), legend, label})` → `{update, setLoading,
  destroy}`. Bars stack bottom → top; drawn at real pixel width, redrawn on resize. Mouse: hover tooltip, click,
  drag to brush. Touch: tap shows the tooltip, drag scrubs, a second tap on the same bucket = click.
  Keyboard: ←/→ move, Enter = click.
- `sparkline(values, {color, width, height, area, label})` → `<svg>`.
- `hbars(rows: [{key, label, value, color, sub, onClick}], {format, max})` → element with `.update(rows)`.
- `segments(parts: [{label, value, color}], {tall, legend, format, label})` — one whole split into parts.
- `meter({value, range: [lo, hi], open, tone: "auto"|…, label, text, stale})` → element with `.update()`.
  Quota: solid = known remaining, hatched = estimate range, `open` fades a range that has only a lower bound.
  Never draw an estimate as a precise solid bar.
- `legend(items)`, `niceScale(max)`.

**Shell** — `scope.get()`, `scope.window()` → `{from, to, fixed}`, `scope.params()` (API params incl.
`exclude_prewarm`), `scope.set(patch)` (`range | from | to | account | model | key | instance | prewarm`),
`scope.fix() / unfix() / clear(fields)`, `scope.query()`, `scope.onChange(fn)`; `href(view, params?, hash?)`
URL carrying the scope — **use it for drill-downs** (overview → requests keeps the investigation);
`go(url, {replace})`; `refresh()`, `refreshStatus()`, `status` (`quota` from `/usage/v1/quota`, `runtime` from
`/admin/v1/runtime`); `banner(id, specFn | null)` global banner (specFn re-runs on language switch);
`setBadge(name, n, kind)` (`accounts`, `system`); `busy(p)`; `setTheme`, `theme`; `setAuto(on, secs)`; `logout(msg?)`;
pure helpers `parseRoute`, `scopeFromQuery`, `scopeToQuery`, `scopeWindow`, `attentionCount`, `usableCount`.

URL: scope params keep the old names `range`, `from`, `to`, `facct`, `fmodel`, `fkey`, `finstance`. Old `/ui?…`
links carrying request parameters (`fproblem`, `fcause`, `foutcome`, `fclass`, `ftier`, `fsrc`, `fcode`, `fthread`,
`ferr`, `fretry`, `rfrom`, `rto`, `record`, `rid`) redirect to `/ui/requests` with the same query; `/ui#system`
→ `/ui/system`. Settings anchors stay hashes (`/ui/settings#keys`, `#network`, `#prices`).
Stored preferences: `ciallopa_theme`, `ciallopa_lang`, `ciallopa_side`, `ciallopa_auto`, `ciallopa_auto_secs`,
`ciallopa_f_range`, `ciallopa_prewarm` (localStorage); the key in `sessionStorage`.

Shortcuts: `g o / r / a / s / y`, `/` (focuses `[data-search]` in the view, else the scope filter), `?`, `Esc`,
`[` or Ctrl/⌘+B (sidebar). Views add `j`/`k`/`Enter` themselves.

## Component conventions

- **One solid button per screen** (`.btn-primary`, near-black). Everything else is `.btn` (bordered),
  `.btn-ghost` (icon / quiet), `.btn-danger` (red outline; solid red only inside a confirm), `.btn-link`.
  Toggles show state with `aria-pressed` / `aria-expanded`.
- Status colour only for status (`ok` / `warn` / `err` tags, dots, meters); accent only for focus, selection,
  links, the first series and quota fill. Hero metrics sit on `.card.tint`.
- Explanations go behind `UI.info()` (ⓘ) or into details, not standing paragraphs.
- Tables: `.table` inside `.table-wrap`, numbers in `td.num`, column priority `.p2` (≥1024px) / `.p3` (≥1280px);
  phones use `.list` / `.list-item` cards instead of wide tables.
- Phones (<768px or coarse-pointer landscape): touch targets ≥44px (compact controls get an invisible hit area),
  inputs ≥16px, safe-area insets, long details as `UI.page`/`drawer` (full screen), short choices as `UI.sheet`.
- Type scale 11/12/13/14/16/20/28; desktop body 13px, phone 14px; spacing on the 4px grid (`--s1`…`--s8`).

## i18n

Keys are `area.name`; shared keys live in `js/i18n/zh.js` / `en.js` (`time.*`, `api.*`, `ui.*`, `nav.*`,
`scope.*`, `term.*` …); a view adds its keys (prefixed with the view name) at the top of its file. zh and en
must have the same key set (checked). Values may be functions for plurals. Chinese puts a space between Chinese
and Latin text / numbers and uses full-width punctuation; English uses sentence case. Server text (settings
`desc`/`help`, error messages) is shown as given; settings may provide English per key with the server text as
fallback.

Glossary (both languages): Token · API Key · TTFT · TPS (tok/s) · SSE · WebSocket (WS) · HTTP · RPM · p50/p90 ·
Codex · Cloudflare · auth.json · OAuth · Proxy (中文「出口代理」) · Session (「会话」) · strategy names such as
Round-robin stay English · Plan names as given · credits = 额度 / Quota · 点数 = Purchased credits ·
Reset card (「重置卡」) · Cooldown (「冷却」) · Prewarm (「预热」) · Est. cost (「参考费用」) · Recovered (「已救回」).

## Develop and test

```sh
cargo run -p ciallopa --example ui_demo -- 127.0.0.1:8399   # demo backend, key sk-demo-0123456789abcdef
node scripts/ui-dev.cjs [8398] [http://127.0.0.1:8399]      # page from disk + same CSP, proxies /usage /admin
for f in scripts/*-check.cjs; do node "$f"; done             # Node checks (shell-check + per-view checks)
cargo test --locked -p ciallopa --lib ui && cargo test --locked -p ciallopa --test it http::
```

The dev server re-reads the files on every request; reload the page after an edit. Verify layouts in a real
browser at 1440×900 and 390×844 (touch), both languages and themes, with no console errors or CSP violations.
