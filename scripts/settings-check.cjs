// Settings view regression checks. Run from any directory: node scripts/settings-check.cjs
// Loads the real ui/js sources into a Node vm (a tiny DOM stub for the controls) and covers: English
// text for every config key in crates/core/src/settings.rs, the draft / load / validate / save /
// discard races of the old settings page (raw invalid drafts survive refreshes, conflict preview and
// version, saved-but-reread-failed, edits during review / save, explicit discard, late loads and
// logins), refresh ranges and "0 = off" switches, error localisation, audit polling, and the
// language-switch replay of raw control state.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {loadScripts, read, SRC, manifest} = require('./lib/page.cjs');

// ---------- minimal DOM ----------
function matchOne(el, sel) {
  const m = /^([a-z]*)((?:\[[^\]]+\]|\.[\w-]+)*)$/i.exec(sel.trim());
  if (!m) throw new Error('selector not supported by the stub: ' + sel);
  if (m[1] && el.tagName.toLowerCase() !== m[1].toLowerCase()) return false;
  for (const part of m[2].match(/\[[^\]]+\]|\.[\w-]+/g) || []) {
    if (part[0] === '.') {
      if (!el.classList.contains(part.slice(1))) return false;
      continue;
    }
    const a = /^\[([\w-]+)(?:=([^\]]*))?\]$/.exec(part);
    if (!el.hasAttribute(a[1])) return false;
    if (a[2] !== undefined && el.getAttribute(a[1]) !== a[2].replace(/^"|"$/g, '')) return false;
  }
  return true;
}
class Node {
  constructor() {
    this.childNodes = [];
    this.parentNode = null;
  }
  get children() {
    return this.childNodes.filter(n => n instanceof Element);
  }
  get firstChild() {
    return this.childNodes[0] || null;
  }
  appendChild(n) {
    if (n.parentNode) n.parentNode.removeChild(n);
    n.parentNode = this;
    this.childNodes.push(n);
    return n;
  }
  removeChild(n) {
    this.childNodes.splice(this.childNodes.indexOf(n), 1);
    n.parentNode = null;
    return n;
  }
  get textContent() {
    return this.childNodes.map(n => n.textContent).join('');
  }
  set textContent(v) {
    this.childNodes = [];
    if (v !== '') this.appendChild(new Text(String(v)));
  }
}
class Text extends Node {
  constructor(s) {
    super();
    this.data = s;
  }
  get textContent() {
    return this.data;
  }
}
class Element extends Node {
  constructor(tag) {
    super();
    this.tagName = tag.toUpperCase();
    this.attrs = new Map();
    this.listeners = {};
    this.dataset = {};
    this.style = {setProperty() {}};
    this.validity = {badInput: false};
    this.value = '';
    this.checked = false;
    this.disabled = false;
    const cls = new Set();
    this.classList = {
      add: (...c) => c.forEach(x => cls.add(x)), remove: (...c) => c.forEach(x => cls.delete(x)), contains: c => cls.has(c),
      toggle: (c, on) => {
        on = on === undefined ? !cls.has(c) : !!on;
        if (on) cls.add(c);
        else cls.delete(c);
        return on;
      },
      _set: cls,
    };
  }
  set className(v) {
    this.classList._set.clear();
    String(v).split(/\s+/).filter(Boolean).forEach(c => this.classList.add(c));
  }
  get className() {
    return Array.from(this.classList._set).join(' ');
  }
  setAttribute(k, v) {
    if (k === 'class') this.className = v;
    else if (k === 'type') this.type = v;
    else if (k.startsWith('data-')) this.dataset[k.slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())] = String(v);
    this.attrs.set(k, String(v));
  }
  getAttribute(k) {
    if (k === 'class') return this.className;
    if (k.startsWith('data-')) {
      const d = this.dataset[k.slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())];
      return d === undefined ? null : d;
    }
    return this.attrs.has(k) ? this.attrs.get(k) : null;
  }
  hasAttribute(k) {
    return this.getAttribute(k) !== null;
  }
  removeAttribute(k) {
    this.attrs.delete(k);
  }
  addEventListener(type, fn) {
    (this.listeners[type] = this.listeners[type] || []).push(fn);
  }
  removeEventListener() {}
  dispatchEvent(e) {
    for (const fn of this.listeners[e.type] || []) fn(e);
    return true;
  }
  focus() {}
  blur() {}
  insertBefore(n, ref) {
    if (!ref) return this.appendChild(n);
    if (n.parentNode) n.parentNode.removeChild(n);
    n.parentNode = this;
    this.childNodes.splice(this.childNodes.indexOf(ref), 0, n);
    return n;
  }
  remove() {
    if (this.parentNode) this.parentNode.removeChild(this);
  }
  querySelectorAll(sel) {
    const out = [], parts = sel.split(',');
    const walk = n => {
      for (const c of n.children) {
        if (parts.some(p => matchOne(c, p))) out.push(c);
        walk(c);
      }
    };
    walk(this);
    return out;
  }
  querySelector(sel) {
    return this.querySelectorAll(sel)[0] || null;
  }
}
const document = {createElement: t => new Element(t), createTextNode: s => new Text(s), createElementNS: (_, t) => new Element(t)};
class Event {
  constructor(type) {
    this.type = type;
  }
}

const env = {document, Event, Intl, navigator: {language: 'zh-CN'}};
// The shared layer plus the settings files only, so other views' work in progress cannot break this check.
const files = manifest().js.filter(f => !f.endsWith('/main.js') && (!f.includes('/views/') || /\/views\/settings(\/|\.js)/.test(f)));
const ctx = loadScripts(env, files);
const {I18n, SettingsModel: M, SettingsControls: C, SettingsText: T} = (() => {
  const vm = require('node:vm');
  return vm.runInContext('({I18n, SettingsModel, SettingsControls, SettingsText})', ctx);
})();
I18n.setLang('zh', {persist: false});

const tests = [];
const test = (name, fn) => tests.push({name, fn});
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((a, b) => {
    resolve = a;
    reject = b;
  });
  return {promise, resolve, reject};
};

// ---------- English text for every schema key ----------
function schemaFromRust() {
  const rs = fs.readFileSync(path.join(SRC, '../../core/src/settings.rs'), 'utf8');
  const body = rs.slice(rs.indexOf('pub const SCHEMA'), rs.indexOf('];', rs.indexOf('pub const SCHEMA')));
  const keys = [], choices = {};
  for (const line of body.split('\n')) {
    const k = /(?:^|[\s.])f\("([\w.]+)"/.exec(line);
    if (!k) continue;
    keys.push(k[1]);
    const cs = [...line.matchAll(/\bc\("([\w-]+)"/g)].map(m => m[1]);
    if (cs.length) choices[k[1]] = cs;
  }
  return {keys, choices};
}

test('English title + summary for every SCHEMA key in settings.rs, and for every choice', () => {
  const {keys, choices} = schemaFromRust();
  assert.ok(keys.length >= 60, 'parsed ' + keys.length + ' keys from settings.rs');
  const en = Object.keys(T.SCHEMA_EN);
  assert.deepEqual(keys.filter(k => !en.includes(k)), [], 'keys without English text');
  assert.deepEqual(en.filter(k => !keys.includes(k)), [], 'English text for keys that no longer exist');
  for (const k of keys) {
    const [title, sum] = T.SCHEMA_EN[k];
    assert.ok(title && sum, k);
    assert.ok(!/[一-鿿]/.test(title + sum + (T.SCHEMA_EN[k][2] || '')), 'Chinese in English text of ' + k);
  }
  for (const k in choices) {
    assert.deepEqual(Object.keys(T.CHOICES_EN[k] || {}).sort(), choices[k].slice().sort(), 'English choices of ' + k);
  }
  // every key also has a section (or lands in General › Advanced)
  const placed = new Set(keys.filter(k => !M.unplaced(keys.map(key => ({key}))).includes(k)));
  assert.ok(placed.size > 50);
});

test('every literal t("settings.…") key exists in both languages', () => {
  const files = ['ui/js/views/settings.js', 'ui/js/views/settings/model.js', 'ui/js/views/settings/controls.js',
    'ui/js/views/settings/panels.js', 'ui/js/views/settings/text.js'];
  const dicts = I18n.dicts();
  for (const f of files) {
    for (const m of read(f).matchAll(/\bt\("(settings\.[\w.]*\w)"/g)) {
      assert.ok(m[1] in dicts.zh && m[1] in dicts.en, f + ': ' + m[1]);
    }
  }
  for (const id of M.SECTIONS.map(s => s.id)) {
    for (const k of ['settings.p.' + id, 'settings.p.' + id + '.sub']) assert.ok(k in dicts.en, k);
  }
});

test('Chinese summaries collapse long server help to its first clause; English uses the dictionary', () => {
  const f = {key: 'max_inflight_requests', desc: '并发上限', help: '同时转发的 HTTP 请求与进行中的 WebSocket turn 合计上限：HTTP 请求占用到响应结束；更多说明。'};
  I18n.setLang('zh', {persist: false});
  assert.equal(T.summary(f), '同时转发的 HTTP 请求与进行中的 WebSocket turn 合计上限');
  assert.equal(T.details(f), f.help);
  assert.equal(T.title({key: 'x', desc: '请求体上限（MiB）'}), '请求体上限');
  assert.equal(T.unit({key: 'x', desc: '停止等待（秒）'}), '秒');
  I18n.setLang('en', {persist: false});
  assert.equal(T.title(f), 'Concurrency limit');
  assert.equal(T.unit({key: 'x', desc: '停止等待（秒）'}), 's');
  assert.equal(T.title({key: 'not.in.dictionary', desc: '服务端文本'}), '服务端文本', 'falls back to the server text');
  assert.ok(T.searchText(f).includes('concurrency') && T.searchText(f).includes('并发'), 'search matches both languages');
  I18n.setLang('zh', {persist: false});
});

test('server range errors are localised and mapped to their key', () => {
  const schema = [{key: 'max_body_mb', desc: '请求体上限（MiB）'}];
  const [e] = M.parseErrors([{message: 'invalid: max_body_mb must be in 1..=4096'}], schema);
  assert.equal(e.key, 'max_body_mb');
  assert.equal(e.message, '需要在 1 到 4096 之间');
  I18n.setLang('en', {persist: false});
  assert.equal(M.parseErrors([{key: 'max_body_mb', message: 'max_body_mb must be in 1..=4096'}], schema)[0].message,
    'Must be between 1 and 4096');
  I18n.setLang('zh', {persist: false});
  const [other] = M.parseErrors([{message: 'must be 0 or in max_body_mb..=65536'}], schema);
  assert.equal(other.key, null);
  assert.equal(other.message, 'must be 0 or in max_body_mb..=65536', 'unknown forms stay as the server wrote them');
});

// ---------- draft / controller ----------
const config = (version, extra) => Object.assign({version, write_enabled: true, running: {api_keys: []}, values: {}, defaults: {},
  schema: []}, extra || {});
const reply = (version, extra) => ({j: config(version, extra), r: {headers: {get: k => (k === 'etag' ? `"${version}"` : null)}}});

function harness() {
  const h = {auth: 'synthetic-admin', calls: [], events: [], handler: async () => reply('v1')};
  h.draft = M.createDraft({identity: () => h.auth});
  h.ctl = M.createController(h.draft, async (...args) => {
    h.calls.push(args);
    return h.handler(...args);
  }, {
    rebased: (c, unchanged) => h.events.push(['rebased', c.version, unchanged]),
    preserved: (c, moved) => h.events.push(['preserved', c.version, moved]),
    loaded: () => {},
    clearServerErrors: () => h.events.push(['clearServerErrors']),
    showErrors: list => h.events.push(['errors', list]),
    review: o => {
      h.review = o;
      h.events.push(['review', o]);
    },
    saved: (r, remaining) => h.events.push(['saved', remaining]),
    savedStale: e => h.events.push(['savedStale', e.message]),
    toast: text => h.events.push(['toast', text]),
  });
  h.last = type => h.events.filter(e => e[0] === type).at(-1);
  h.count = type => h.events.filter(e => e[0] === type).length;
  return h;
}
const intField = {key: 'timeout', kind: 'int', desc: '超时（秒）'};

test('raw invalid drafts survive refreshes; an external change makes every value a draft', async () => {
  const h = harness();
  await h.ctl.load();
  assert.equal(h.draft.phase, 'clean');
  assert.deepEqual(h.last('rebased'), ['rebased', 'v1', false]);
  const row = document.createElement('div');
  const field = C.input(h.draft, intField, 100, row);
  row.appendChild(field.el);
  const control = field.el.children[0];
  control.value = '12.5';
  control.dispatchEvent(new Event('input'));
  assert.equal(h.draft.invalid.has('timeout'), true);
  assert.equal(h.draft.phase, 'dirty');
  assert.equal(row.classList.contains('is-dirty'), true);
  h.handler = async () => reply('v2');
  assert.equal(await h.ctl.load(), false, 'a refresh with a draft keeps it');
  assert.equal(h.count('rebased'), 1, 'the form is not rebuilt');
  assert.deepEqual(h.last('preserved'), ['preserved', 'v2', true]);
  assert.equal(control.value, '12.5');
  assert.equal(h.draft.edits.timeout, 12);
  assert.equal(h.draft.etag, '"v2"', 'writes (keys, switches) use the newest version');
  assert.equal(h.draft.latest.version, 'v2');
  assert.equal(h.draft.base.version, 'v1', 'the form keeps the snapshot it was built from');
  control.value = '100';
  control.dispatchEvent(new Event('input'));
  assert.equal(h.draft.edits.timeout, 100, 'after an external change, returning to the old value is still a draft');
});

test('a 412 is a conflict that keeps the draft and fetches nothing', () => {
  const f = M.failure({status: 412, message: 'stale version'}, 2);
  assert.equal(f.conflict, true);
  assert.match(f.body, /未保存的修改已保留/);
  assert.match(M.failure({status: 412}, 0).body, /刷新后重试/);
  assert.equal(M.failure({status: 500, message: 'boom'}, 1).conflict, false);
});

test('review shows the latest values and saves against the reviewed version', async () => {
  const h = harness();
  await h.ctl.load();
  h.draft.edits = {timeout: 7};
  h.draft.changed();
  h.handler = async method => (method === 'POST' ? {j: {ok: true, version: 'v3', diff: [{key: 'timeout', old: 200, new: 7,
    effect: 'hot'}]}} :
    method === 'PUT' ? {j: {applied: ['timeout'], pending_restart: []}} : reply('v4'));
  await h.ctl.review();
  assert.equal(h.review.versionChanged, true, 'the dialog says the file changed and old values are the latest');
  assert.deepEqual(h.review.diff[0], {key: 'timeout', old: 200, new: 7, effect: 'hot'});
  await h.review.save();
  const put = h.calls.find(x => x[0] === 'PUT');
  assert.deepEqual(JSON.parse(JSON.stringify(put[2])), {changes: {timeout: 7}});
  assert.equal(put[3]['If-Match'], '"v3"', 'save the version shown by validation, not the previous GET');
  assert.equal(h.draft.count(), 0);
  assert.deepEqual(h.last('saved'), ['saved', 0]);
  assert.equal(h.draft.base.version, 'v4');
});

test('validation errors are reported per key without opening the review', async () => {
  const h = harness();
  await h.ctl.load();
  h.draft.edits = {max_body_mb: 99999};
  h.draft.changed();
  h.handler = async () => ({j: {ok: false, version: 'v1', errors: [{message: 'invalid: max_body_mb must be in 1..=4096'}], diff: []}});
  h.draft.base.schema = [{key: 'max_body_mb', desc: '请求体上限（MiB）'}];
  await h.ctl.review();
  assert.equal(h.count('review'), 0);
  assert.equal(h.last('errors')[1][0].key, 'max_body_mb');
  assert.equal(h.draft.phase, 'dirty');
});

test('a write that lands but cannot be re-read is reported apart from a failed save', async () => {
  const h = harness();
  await h.ctl.load();
  const row = document.createElement('div');
  const field = C.input(h.draft, intField, 100, row);
  const control = field.el.children[0];
  control.value = '8';
  control.dispatchEvent(new Event('input'));
  h.handler = async method => {
    if (method === 'POST') return {j: {ok: true, version: 'v1', diff: [{key: 'timeout', old: 100, new: 8, effect: 'hot'}]}};
    if (method === 'PUT') return {j: {applied: ['timeout'], pending_restart: []}};
    throw new Error('offline');
  };
  await h.ctl.review();
  await h.review.save();
  assert.deepEqual(h.last('savedStale'), ['savedStale', 'offline']);
  assert.equal(h.count('saved'), 0);
  assert.equal(h.draft.edits.timeout, 8);
  assert.equal(control.value, '8');
  control.value = '100';
  control.dispatchEvent(new Event('input'));
  assert.equal(h.draft.edits.timeout, 100, 'reverting a saved value must remain submitable while the baseline is stale');
  h.handler = async () => reply('v5');
  assert.equal(await h.ctl.load(true), true);
  assert.equal(h.draft.count(), 0);
  assert.equal(h.draft.stale, false);
});

test('edits during validation cancel the preview; edits during a save are kept', async () => {
  const h = harness();
  await h.ctl.load();
  h.draft.edits = {timeout: 8};
  h.draft.changed();
  const validation = deferred();
  h.handler = async () => validation.promise;
  const reviewing = h.ctl.review();
  h.draft.edits.timeout = 9;
  h.draft.changed();
  validation.resolve({j: {ok: true, version: 'v1', diff: []}});
  await reviewing;
  assert.equal(h.count('review'), 0);
  assert.match(h.last('toast')[1], /编辑已变化/);
  h.handler = async () => ({j: {ok: true, version: 'v1', diff: []}});
  await h.ctl.review();
  const saving = deferred();
  h.handler = async method => (method === 'PUT' ? saving.promise : reply('v2'));
  const saved = h.review.save();
  h.draft.edits.timeout = 10;
  h.draft.changed();
  saving.resolve({j: {applied: ['timeout'], pending_restart: []}});
  await saved;
  assert.equal(h.draft.edits.timeout, 10);
  assert.deepEqual(h.last('saved'), ['saved', 1], 'the toast says later edits were kept');
});

test('explicit discard applies only when its fresh read succeeds, and protects newer typing', async () => {
  const h = harness();
  await h.ctl.load();
  h.draft.edits = {timeout: 10};
  h.draft.changed();
  const loading = deferred();
  h.handler = async () => loading.promise;
  const discarding = h.ctl.discard();
  h.draft.edits.timeout = 11;
  h.draft.changed();
  loading.resolve(reply('v6'));
  assert.equal(await discarding, false);
  assert.equal(h.draft.edits.timeout, 11);
  h.handler = async () => {
    throw new Error('offline');
  };
  await assert.rejects(h.ctl.discard(), /offline/);
  assert.equal(h.draft.edits.timeout, 11);
  h.handler = async () => reply('v6');
  assert.equal(await h.ctl.discard(), true);
  assert.equal(h.draft.count(), 0);
  assert.equal(h.draft.base.version, 'v6');
});

test('late loads, previous logins and logout cannot replace the accepted snapshot', async () => {
  const h = harness();
  await h.ctl.load();
  const old = deferred();
  h.handler = async () => old.promise;
  const oldLoad = h.ctl.load();
  h.handler = async () => reply('v8');
  await h.ctl.load();
  old.resolve(reply('v7'));
  await oldLoad;
  assert.equal(h.draft.base.version, 'v8');
  const previous = deferred();
  h.handler = async () => previous.promise;
  const previousLoad = h.ctl.load();
  h.auth = 'another-synthetic-admin';
  previous.resolve(reply('old-login'));
  await previousLoad;
  assert.equal(h.draft.base.version, 'v8');
  const guard = h.draft.guard();
  h.draft.retire();
  assert.equal(guard(false), false, 'logout / unmount invalidates even a same-key guard');
});

test('an unchanged refresh does not rebuild the form; a changed one does', async () => {
  const h = harness();
  await h.ctl.load();
  await h.ctl.load();
  assert.deepEqual(h.last('rebased'), ['rebased', 'v1', true]);
  h.handler = async () => reply('v1', {pending_restart: [{key: 'listen', old: 'a', new: 'b'}]});
  await h.ctl.load();
  assert.deepEqual(h.last('rebased'), ['rebased', 'v1', false]);
  h.handler = async () => reply('v1', {pending_restart: [{key: 'listen', old: 'a', new: 'b'}], runtime: {inflight: 3}});
  await h.ctl.load();
  assert.deepEqual(h.last('rebased'), ['rebased', 'v1', true], 'runtime numbers alone do not rebuild');
});

test('immediate switches write one key against the newest version, then re-read', async () => {
  const h = harness();
  await h.ctl.load();
  h.handler = async (method) => (method === 'PUT' ? {j: {applied: ['ipv6']}} : reply('v2'));
  await h.ctl.writeNow('ipv6', true);
  const put = h.calls.find(x => x[0] === 'PUT');
  assert.deepEqual(JSON.parse(JSON.stringify(put[2])), {changes: {ipv6: true}});
  assert.equal(put[3]['If-Match'], '"v1"');
  assert.equal(h.draft.base.version, 'v2');
});

// ---------- controls ----------
test('refresh ranges: on fills 5–10, min > max and 1–60 are errors, off writes 0 to the shortest only', () => {
  const draft = M.createDraft({identity: () => 'x'});
  const F = {key: 'ui.quota_refresh_mins', kind: 'int', desc: '额度自动刷新间隔（分钟）'};
  const FX = {key: 'ui.quota_refresh_max_mins', kind: 'int', desc: '额度自动刷新最长间隔（分钟）'};
  let rz, sw, a, b, st, fe;
  const mk = (v, vx) => {
    draft.edits = {};
    draft.invalid.clear();
    draft.stale = false;
    rz = C.range(draft, F, FX, v, vx, document.createElement('div'));
    [sw, a, , b, st] = rz.el.children[0].children;
    fe = rz.el.children[1];
  };
  const edits = () => JSON.parse(JSON.stringify(draft.edits));
  const set = (e, v) => {
    e.value = String(v);
    e.dispatchEvent(new Event('input'));
  };
  mk(0, 0);
  assert.equal(sw.checked || !a.disabled || !b.disabled, false);
  assert.equal(a.value + '-' + b.value + ' ' + st.textContent, '5-10 只手动刷新');
  assert.deepEqual(edits(), {});
  sw.checked = true;
  sw.dispatchEvent(new Event('change'));
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 5, 'ui.quota_refresh_max_mins': 10});
  assert.equal(st.textContent, '分钟');
  set(b, 3);
  assert.equal(draft.invalid.has('ui.quota_refresh_mins'), true);
  assert.deepEqual(edits(), {});
  assert.equal(fe.textContent, '最短不能大于最长');
  set(b, 61);
  assert.match(fe.textContent, /1–60/);
  set(b, '');
  assert.match(fe.textContent, /1–60/);
  set(b, 5);
  assert.equal(draft.invalid.size, 0);
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 5, 'ui.quota_refresh_max_mins': 5}, 'equal values = fixed interval');
  sw.checked = false;
  sw.dispatchEvent(new Event('change'));
  assert.deepEqual(edits(), {}, 'back off: nothing to save');
  mk(5, 10);
  assert.equal(sw.checked && a.value + '-' + b.value, '5-10');
  sw.checked = false;
  sw.dispatchEvent(new Event('change'));
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 0});
  mk(15, 0);
  assert.equal(sw.checked && a.value + '-' + b.value, '15-15', 'a config from before the range shows a fixed interval');
  set(a, 15);
  assert.deepEqual(edits(), {});
  set(a, 10);
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 10, 'ui.quota_refresh_max_mins': 15});
  rz.set(0);
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 0}, 'reset to the default (off)');
  assert.equal(M.rzText(M.rzRange('ui.quota_refresh_mins', k => ({'ui.quota_refresh_mins': 5, 'ui.quota_refresh_max_mins': 10})[k])),
    '每 5–10 分钟');
  assert.equal(M.rzText(M.rzRange('ui.quota_refresh_mins', k => ({'ui.quota_refresh_mins': 15})[k])), '每 15 分钟');
  assert.equal(M.rzText(M.rzRange('ui.reset_credits_refresh_mins', k => ({'ui.reset_credits_refresh_max_mins': 10})[k])), '只手动刷新');
});

test('"0 = off" numbers: off writes 0 and keeps the typed value; on with 0 is an error', () => {
  const draft = M.createDraft({identity: () => 'x'});
  draft.base = {defaults: {max_inflight_requests: 64}};
  const F = {key: 'max_inflight_requests', kind: 'int', desc: '并发上限'};
  const z = C.zeroOff(draft, F, 32, document.createElement('div'));
  const [sw, e, st] = z.el.children[0].children;
  assert.equal(sw.checked && e.value, '32');
  sw.checked = false;
  sw.dispatchEvent(new Event('change'));
  assert.deepEqual(JSON.parse(JSON.stringify(draft.edits)), {max_inflight_requests: 0});
  assert.equal(e.disabled, true);
  assert.equal(e.value, '32', 'the typed value stays for switching back');
  assert.equal(st.textContent, '不限');
  sw.checked = true;
  sw.dispatchEvent(new Event('change'));
  assert.deepEqual(JSON.parse(JSON.stringify(draft.edits)), {});
  e.value = '0';
  e.dispatchEvent(new Event('input'));
  assert.equal(draft.invalid.has('max_inflight_requests'), true, 'on with 0 is an error, not a silent off');
  const off = C.zeroOff(M.createDraft({identity: () => 'x'}), F, 0, document.createElement('div'));
  const [sw2, e2] = off.el.children[0].children;
  sw2.checked = true;
  sw2.dispatchEvent(new Event('change'));
  assert.equal(e2.value, '64', 'switching on fills the default');
});

test('language switch: raw control state replays onto a rebuilt form', () => {
  const draft = M.createDraft({identity: () => 'x'});
  const build = () => {
    const root = document.createElement('div');
    root.appendChild(C.input(draft, intField, 100, document.createElement('div')).el);
    root.appendChild(C.bool(draft, {key: 'ipv6x', kind: 'bool', desc: 'x'}, false, document.createElement('div')).el);
    root.appendChild(C.choices(draft, {key: 'routing.strategy', kind: 'enum', desc: 'x', choices: [{value: 'a', label: 'A', help: ''},
      {value: 'b', label: 'B', help: ''}]}, 'a', document.createElement('div')).el);
    return root;
  };
  const first = build();
  const num = first.querySelector('input[data-fk=timeout]');
  num.value = '1.5';
  num.dispatchEvent(new Event('input'));
  const sw = first.querySelector('input[data-fk=ipv6x]');
  sw.checked = true;
  sw.dispatchEvent(new Event('change'));
  const rb = first.querySelector('input[data-fk=routing.strategy=b]');
  rb.checked = true;
  rb.dispatchEvent(new Event('change'));
  const states = C.capture(first);
  draft.edits = {};
  draft.invalid.clear();
  const second = build();
  C.replay(second, states);
  assert.equal(second.querySelector('input[data-fk=timeout]').value, '1.5');
  assert.equal(draft.invalid.has('timeout'), true);
  assert.deepEqual(JSON.parse(JSON.stringify(draft.edits)), {timeout: 1, ipv6x: true, 'routing.strategy': 'b'});
});

test('audit polling exists only while its section is shown', () => {
  const timers = new Map();
  let next = 0;
  const p = M.createPoller(() => {}, 15000, {set: fn => {
    timers.set(++next, fn);
    return next;
  }, clear: id => timers.delete(id)});
  p.sync(true);
  assert.equal(timers.size, 1);
  p.sync(true);
  assert.equal(timers.size, 1, 're-entering keeps one poll');
  p.sync(false);
  assert.equal(timers.size, 0);
});

test('section anchors: current names and the old single-page aliases', () => {
  assert.equal(M.sectionOf('#keys'), 'keys');
  assert.equal(M.sectionOf('#network'), 'network');
  assert.equal(M.sectionOf('#prices'), 'prices');
  assert.equal(M.sectionOf('#keysec'), 'keys');
  assert.equal(M.sectionOf('#g-upstream_opts'), 'network');
  assert.equal(M.sectionOf('#nope'), null);
  assert.equal(M.sectionOf(''), null);
});

(async () => {
  let failed = 0;
  const watchdog = setTimeout(() => {
    console.error('FAIL: settings checks did not settle within 10 seconds');
    process.exit(1);
  }, 10000);
  for (const t of tests) {
    try {
      await t.fn();
      console.log('ok   ' + t.name);
    } catch (e) {
      failed++;
      console.log('FAIL ' + t.name + '\n     ' + String(e.stack || e).split('\n').slice(0, 6).join('\n     '));
    }
  }
  clearTimeout(watchdog);
  console.log((tests.length - failed) + '/' + tests.length + ' passed');
  if (failed) process.exitCode = 1;
})();
