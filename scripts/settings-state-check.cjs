// Draft and revision regressions against the actual settings page functions. No dependencies.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../crates/server/src/settings.html'), 'utf8');
const start = html.indexOf('// ---- settings\n');
const end = html.lastIndexOf('})();');
assert.ok(start >= 0 && end > start, 'settings source anchors must exist');

class Node {
  constructor() {
    this.children = [];
    this.listeners = {};
    this.value = '';
    this.validity = {badInput: false};
    this.classes = new Set();
    this.classList = {
      add: (...names) => names.forEach(n => this.classes.add(n)),
      remove: (...names) => names.forEach(n => this.classes.delete(n)),
      contains: n => this.classes.has(n),
      toggle: (n, on = !this.classes.has(n)) => on ? this.classes.add(n) : this.classes.delete(n),
    };
  }
  set textContent(v) { this.text = String(v); this.children = []; }
  get textContent() { return (this.text || '') + this.children.map(x => x.textContent ?? String(x)).join(''); }
  append(...nodes) { this.children.push(...nodes.flat().filter(x => x !== null && x !== undefined)); }
  appendChild(node) { this.append(node); return node; }
  prepend(node) { this.children.unshift(node); }
  addEventListener(type, fn) { this.listeners[type] = fn; }
  dispatchEvent(event) { return this.listeners[event.type]?.(event); }
  setAttribute(name, value) { this[name] = value; }
  querySelector() { return null; }
  focus() {}
}
const nodes = new Map();
const node = id => {
  if (!nodes.has(id)) nodes.set(id, new Node());
  return nodes.get(id);
};
const h = (_tag, attrs, ...children) => {
  const n = new Node();
  if (attrs && typeof attrs === 'object') Object.assign(n, attrs);
  n.append(...children);
  return n;
};
const deferred = () => {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return {promise, resolve};
};
const config = version => ({version, write_enabled: true, running: {api_keys: []}});
const reply = version => ({j: config(version), r: {headers: {get: () => `"${version}"`}}});
let auth = 'synthetic-admin';
let handler = async () => reply('v1');
const calls = [];
const c = vm.createContext({
  $: node, h, clear: n => {n.textContent = '';}, key: () => auth,
  req: async (...args) => {calls.push(args); return handler(...args);},
  get: async () => ({accounts: []}), egressTag() {}, setTabCount() {},
  addEventListener() {}, onLogout() {}, login() {}, disarm() {},
  adminMsg: () => null, why: e => e.message, inView: () => true, toast: text => c.toasts.push(text), toasts: [],
  busyRun: (_button, _name, fn) => {const job = fn(); c.busyJobs.push(job); return job;}, busyJobs: [],
  document: {createTextNode: text => ({textContent: text})}, Event: class {constructor(type) {this.type = type;}},
  navigator: {}, renderCount: 0, keyRenders: [], dialogs: [],
});
vm.runInContext(html.slice(start, end), c);
vm.runInContext(`
  // Rendering is observed separately; the draft/load/validation handlers are the page's own.
  render=()=>{renderCount++;EDITS={};BAD.clear();BASE_STALE=false;upd()};
  renderKeys=c=>keyRenders.push(c.version);
  loadPrices=async()=>{};loadAudit=async()=>{};
  dialog=(title,body,yes,cls,save)=>dialogs.push({title,body,save});
`, c);
const run = source => vm.runInContext(source, c);
const watchdog = setTimeout(() => {
  console.error('FAIL: settings checks did not settle within 5 seconds');
  process.exitCode = 1;
}, 5000);

(async () => {
  await run('load()');
  assert.equal(c.renderCount, 1);

  // Reload/key-management refreshes preserve the actual invalid input, not its parsed value.
  run('row=h("tr");field=input({key:"timeout",kind:"int",desc:"超时"},100,row);control=field.children[0];control.value="12.5";control.dispatchEvent(new Event("input"))');
  assert.equal(run('BAD.has("timeout")'), true);
  handler = async () => reply('v2');
  await run('load()');
  assert.equal(c.renderCount, 1);
  assert.equal(run('control.value'), '12.5');
  assert.equal(run('EDITS.timeout'), 12);
  assert.equal(run('BAD.has("timeout")'), true);
  assert.equal(run('ETAG'), '"v2"');
  assert.deepEqual(Array.from(c.keyRenders), ['v2']);
  assert.match(node('draft').textContent, /编辑已保留/);
  run('control.value="100";control.dispatchEvent(new Event("input"))');
  assert.equal(run('EDITS.timeout'), 100, 'after an external change, returning to the old value is still a draft');
  run('control.value="12.5";control.dispatchEvent(new Event("input"))');

  // A conflict does not fetch or erase anything. Retrying validates against the current file.
  const before = calls.length;
  run('fail({status:412,message:"stale version"})');
  assert.equal(calls.length, before);
  assert.equal(run('control.value'), '12.5');
  assert.match(node('msg').textContent, /未保存的编辑已保留/);
  run('control.value="7";control.dispatchEvent(new Event("input"))');
  handler = async method => method === 'POST'
    ? {j: {ok: true, version: 'v3', diff: [{key: 'timeout', old: 200, new: 7, effect: 'hot'}]}}
    : method === 'PUT' ? {j: {applied: ['timeout'], pending_restart: []}} : reply('v4');
  await run('reviewEdits()');
  assert.match(c.dialogs.at(-1).body.textContent, /最新配置/);
  assert.match(c.dialogs.at(-1).body.textContent, /timeout2007/);
  await c.dialogs.at(-1).save();
  const put = calls.find(x => x[0] === 'PUT');
  assert.deepEqual(JSON.parse(JSON.stringify(put[2])), {changes: {timeout: 7}});
  assert.equal(put[3]['If-Match'], '"v3"', 'save the version shown by validation, not the previous GET');
  assert.equal(run('nEdits()'), 0);

  // A successful write followed by a failed read must not leave a silently stale baseline.
  run('field=input({key:"timeout",kind:"int",desc:"超时"},100,row);control=field.children[0];control.value="8";control.dispatchEvent(new Event("input"))');
  handler = async method => {
    if (method === 'POST') return {j: {ok: true, version: 'v4', diff: [{key: 'timeout', old: 100, new: 8, effect: 'hot'}]}};
    if (method === 'PUT') return {j: {applied: ['timeout'], pending_restart: []}};
    throw new Error('offline');
  };
  await run('reviewEdits()');
  await c.dialogs.at(-1).save();
  assert.equal(run('EDITS.timeout'), 8);
  assert.equal(run('control.value'), '8');
  assert.match(node('msg').textContent, /已保存.*重新读取.*失败.*草稿已保留/);
  run('control.value="100";control.dispatchEvent(new Event("input"))');
  assert.equal(run('EDITS.timeout'), 100, 'reverting a saved value must remain submitable while the baseline is stale');
  handler = async () => reply('v4');
  await run('load(true)');
  assert.equal(run('nEdits()'), 0);
  assert.equal(run('BASE_STALE'), false);

  // Validation cannot offer a stale preview; editing while a save runs cannot lose the draft.
  run('EDITS={timeout:8};upd()');
  const validation = deferred();
  handler = async () => validation.promise;
  const count = c.dialogs.length;
  const reviewing = run('reviewEdits()');
  run('EDITS.timeout=9;upd()');
  validation.resolve({j: {ok: true, version: 'v4', diff: []}});
  await reviewing;
  assert.equal(c.dialogs.length, count);
  handler = async () => ({j: {ok: true, version: 'v4', diff: []}});
  await run('reviewEdits()');
  const saving = deferred();
  handler = async method => method === 'PUT' ? saving.promise : reply('v5');
  const saved = c.dialogs.at(-1).save();
  run('EDITS.timeout=10;upd()');
  saving.resolve({j: {applied: ['timeout'], pending_restart: []}});
  await saved;
  assert.equal(run('EDITS.timeout'), 10);

  // Explicit discard only applies once its fresh read succeeds, and protects newer typing.
  const loading = deferred();
  handler = async () => loading.promise;
  const discarding = run('load(true)');
  run('EDITS.timeout=11;upd()');
  loading.resolve(reply('v6'));
  assert.equal(await discarding, false);
  assert.equal(run('EDITS.timeout'), 11);
  handler = async () => {throw new Error('offline');};
  await assert.rejects(run('load(true)'), /offline/);
  assert.equal(run('EDITS.timeout'), 11);
  handler = async () => reply('v6');
  assert.equal(await run('load(true)'), true);
  assert.equal(run('nEdits()'), 0);
  assert.equal(run('CFG.version'), 'v6');

  // A late GET or a response from the previous login cannot replace the accepted snapshot.
  const old = deferred();
  handler = async () => old.promise;
  const oldLoad = run('load()');
  handler = async () => reply('v8');
  await run('load()');
  old.resolve(reply('v7'));
  await oldLoad;
  assert.equal(run('CFG.version'), 'v8');
  const previousLogin = deferred();
  handler = async () => previousLogin.promise;
  const previousLoad = run('load()');
  auth = 'another-synthetic-admin';
  previousLogin.resolve(reply('old-login'));
  await previousLoad;
  assert.equal(run('CFG.version'), 'v8');

  // Refresh ranges (最短–最长): switching on fills 5–10 and writes both keys; min > max and
  // values outside 1–60 are errors; off writes 0 to the shortest only; an old single value
  // shows as a fixed interval and is not rewritten while untouched.
  const RQ = '{key:"ui.quota_refresh_mins",kind:"int",desc:"额度自动刷新间隔（分钟）"},{key:"ui.quota_refresh_max_mins",kind:"int",desc:"额度自动刷新最长间隔（分钟）"}';
  const mk = (v, vx) => run(`EDITS={};BAD.clear();BASE_STALE=false;rrow=h("div");rz=rinput(${RQ},${v},${vx},rrow);[rsw,ra,,rb,rst]=rz.el.children[0].children;rfe=rz.el.children[1]`);
  const edits = () => JSON.parse(run('JSON.stringify(EDITS)'));
  const set = (e, v) => run(`${e}.value="${v}";${e}.dispatchEvent(new Event("input"))`);
  mk(0, 0);
  assert.equal(run('rsw.checked||!ra.disabled||!rb.disabled'), false);
  assert.equal(run('ra.value+"-"+rb.value+" "+rst.textContent'), '5-10 只手动刷新');
  assert.deepEqual(edits(), {});
  run('rsw.checked=true;rsw.dispatchEvent(new Event("change"))');
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 5, 'ui.quota_refresh_max_mins': 10});
  assert.equal(run('rst.textContent'), '分钟');
  set('rb', 3);
  assert.equal(run('BAD.has("ui.quota_refresh_mins")'), true);
  assert.deepEqual(edits(), {});
  assert.equal(run('rfe.textContent'), '最短不能大于最长');
  set('rb', 61);
  assert.match(run('rfe.textContent'), /1–60/);
  set('rb', '');
  assert.match(run('rfe.textContent'), /1–60/);
  set('rb', 5);
  assert.equal(run('BAD.size'), 0);
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 5, 'ui.quota_refresh_max_mins': 5}, 'equal values = fixed interval');
  run('rsw.checked=false;rsw.dispatchEvent(new Event("change"))');
  assert.deepEqual(edits(), {}, 'back off: nothing to save');
  // stored 5–10, switched off: only the shortest is written (0); the longest is kept
  mk(5, 10);
  assert.equal(run('rsw.checked&&ra.value+"-"+rb.value'), '5-10');
  run('rsw.checked=false;rsw.dispatchEvent(new Event("change"))');
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 0});
  // v0.2.1 config: one value (15), no longest → shows 15–15, untouched = no edit
  mk(15, 0);
  assert.equal(run('rsw.checked&&ra.value+"-"+rb.value'), '15-15');
  set('ra', 15);
  assert.deepEqual(edits(), {});
  set('ra', 10);
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 10, 'ui.quota_refresh_max_mins': 15});
  run('rz.set(0)');
  assert.deepEqual(edits(), {'ui.quota_refresh_mins': 0}, '恢复 = default off');
  // summaries / read-only text
  assert.equal(run('rzText(rzRange("ui.quota_refresh_mins",k=>({"ui.quota_refresh_mins":5,"ui.quota_refresh_max_mins":10})[k]))'), '每 5–10 分钟');
  assert.equal(run('rzText(rzRange("ui.quota_refresh_mins",k=>({"ui.quota_refresh_mins":15})[k]))'), '每 15 分钟');
  assert.equal(run('rzText(rzRange("ui.reset_credits_refresh_mins",k=>({"ui.reset_credits_refresh_max_mins":10})[k]))'), '只手动刷新');
  run('EDITS={};BAD.clear();upd()');

  // Logout clears the one-time secret, including a key creation that finishes afterwards.
  node('nkv').textContent = 'synthetic-generated-secret';
  node('newkey').classList.remove('hide');
  const creation = deferred();
  handler = async () => creation.promise;
  node('kf').dispatchEvent({type: 'submit', preventDefault() {}});
  auth = '';
  run('onLogout()');
  assert.equal(node('nkv').textContent, '');
  assert.equal(node('newkey').classList.contains('hide'), true);
  creation.resolve({j: {key: 'late-synthetic-secret'}});
  await c.busyJobs.at(-1);
  assert.equal(node('nkv').textContent, '');
  assert.equal(node('newkey').classList.contains('hide'), true);
  console.log('PASS: raw drafts, conflict preview/version, saved-write refresh failure, in-flight edits, explicit discard, stale loads, refresh ranges, logout secret');
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => clearTimeout(watchdog));
