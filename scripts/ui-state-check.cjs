// Dependency-free scheduling regression checks against the actual embedded UI functions.
// Run from any directory: node /path/to/ciallopa/scripts/ui-state-check.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../crates/server/src/ui.html'), 'utf8');
const section = (start, end) => {
  const a = html.indexOf(start);
  assert.notEqual(a, -1, `UI source anchor missing: ${start}`);
  const b = html.indexOf(end, a + start.length);
  assert.notEqual(b, -1, `UI source end anchor missing after ${start}: ${end}`);
  return html.slice(a, b);
};
const deferred = () => {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
};
const nodes = new Map();
const node = id => {
  if (!nodes.has(id)) nodes.set(id, {textContent: '', setAttribute() {}, appendChild(x) {this.textContent += x.textContent;}});
  return nodes.get(id);
};
const quota = deferred();
let auth = 'synthetic-key';
const staleError = Object.assign(new Error('old-summary-failure'), {status: 503, body: {error: {message: 'old-summary-failure'}}});
const context = vm.createContext({
  $: node, key: () => auth, clear: n => {n.textContent = '';},
  h: (_tag, _attrs, text) => ({textContent: text}), document: {createTextNode: text => ({textContent: text})},
  inView: () => false, toast: text => context.toasts.push(text), toasts: [],
  get: async () => ({}), usageGet: async () => ({}), renderFp() {}, renderKpis() {}, renderRuntime() {}, renderTrend() {}, loadQuota: () => quota.promise,
  loadTrend: async () => {}, egressTag: async () => ({}), loadMem: async () => {}, loadRecords: async () => {},
  recState: {rows: []}, recordWindow: null, scopeWindow: {fixed: true}, showScope() {}, rollWindow() {}, PAGE: 50, hms: () => 'now', Date, Promise,
  query: 'A', staleError, rendered: [],
});
vm.runInContext(section('function fail(e)', 'async function busy(') + '\n' + section('const loadSeq={},loading={};', 'function emptyRow(') + '\n' + section('let allPending=0;', 'function stopAuto('), context);
vm.runInContext('async function loadSummary(){return currentLoad("ss",()=>query,()=>query==="A"?Promise.reject(staleError):Promise.resolve(query),data=>rendered.push(data))}', context);
// Query contracts and cursor behavior, using the actual query/load functions.
async function queryAndPaging() {
  const inputs = new Map();
  const input = id => {
    if (!inputs.has(id)) inputs.set(id, {value: '', checked: false, textContent: '', children: [],
      style: {}, classList: {toggle() {}}, setAttribute() {}, scrollIntoView() {}, focus() {},
      querySelectorAll() {return this.children.filter(e => e.tag === 'button');},
      appendChild(child) {this.children.push(child);}, append(...children) {this.children.push(...children);}});
    return inputs.get(id);
  };
  for (const [id, value] of Object.entries({facct: 'alice', fmodel: 'model-a', fkey: 'laptop', finstance: 'home', tmode: 'auto', group: 'account'})) input(id).value = value;
  const calls = [];
  let reply = async () => ({records: [{request_id: '0000000000000003'}], total: 3, next_before_id: '0000000000000003'});
  let rolls = 0;
  const c = vm.createContext({
    $: input, key: () => 'demo', URLSearchParams, Map, Date, Math, Number, Promise,
    document: {querySelector: () => ({offsetHeight: 49})}, getComputedStyle: () => ({top: '48px'}),
    scopeWindow: {from: 9000000, to: 95400000, fixed: false}, recordWindow: null,
    recState: {rows: [], cursor: null, total: 0},
    usageGet: async url => {
      if (url.startsWith('/usage/v1/records?')) {calls.push(new URL(url, 'http://local')); return reply();}
      return {};
    },
    renderRecords() {}, renderKpis() {}, renderFp() {}, renderTrend() {}, renderRuntime() {}, showScope() {}, saveF() {}, fmtN: String, fail(error) {throw error;},
    loadQuota: async () => {}, egressTag: async () => ({}), loadMem: async () => {}, hms: () => 'now',
    rollWindow() {rolls++; c.scopeWindow = {...c.scopeWindow, from: c.scopeWindow.from + 1000, to: c.scopeWindow.to + 1000};},
  });
  vm.runInContext([
    section('function scopeFilters()', 'function scopeLabel('),
    section('const loadSeq={},loading={};', 'function emptyRow('),
    section('const PAGE=50;', '$("rmore").addEventListener'),
    section('function summaryQuery()', 'async function loadSummary('),
    section('function trendQuery()', 'function renderRuntime('),
    section('function trendPoints(', 'function selectBucket('),
    section('function summaryCurrent()', 'function statLink('),
    section('function drillOutcome(', '// ---- reset-card popover:'),
    section('function selectBucket(', 'function renderTrend('),
    section('let allPending=0;', 'function stopAuto('),
  ].join('\n'), c);
  // The same absolute scope is used by summary, chart and records. A grain never changes it.
  await vm.runInContext('loadRecords(false)', c);
  const recordParams = calls.at(-1).searchParams;
  for (const query of ['summaryQuery()', 'trendQuery()']) {
    const params = new URLSearchParams(vm.runInContext(query, c));
    for (const name of ['from', 'to', 'account', 'model', 'key', 'instance']) assert.equal(params.get(name), recordParams.get(name), `${query}: ${name}`);
  }
  c.scopeWindow.to = c.scopeWindow.from + 7 * 86400000;
  input('tmode').value = 'hour';
  const week = new URLSearchParams(vm.runInContext('trendQuery()', c));
  assert.equal(Number(week.get('to')) - Number(week.get('from')), 7 * 86400000);
  assert.equal(week.get('bucket'), 'hour');

  // A full rolling 24 hours usually covers 25 partial/whole buckets, not 24 calendar slots.
  const pts = JSON.parse(vm.runInContext('JSON.stringify(trendPoints({from_ms: 1800000, to_ms: 88200000, buckets: []},true,0))', c));
  assert.equal(pts.length, 25);
  assert.equal(pts[0].from, 1800000);
  assert.equal(pts[0].to, 3599999);
  assert.equal(pts.at(-1).from, 86400000);
  assert.equal(pts.at(-1).to, 88200000);
  assert.equal(pts[0].partial, true);
  assert.equal(pts[1].partial, false);

  // Page two passes the full ID (including the sequence bits), and never an offset.
  await vm.runInContext('loadRecords(false)', c);
  reply = async () => ({records: [{request_id: '0000000000000002'}], total: 4, next_before_id: '0000000000000002'});
  await vm.runInContext('loadRecords(true)', c);
  assert.equal(calls.at(-1).searchParams.get('before_id'), '0000000000000003');
  assert.equal(calls.at(-1).searchParams.has('offset'), false);
  assert.equal(c.recState.rows.length, 2);
  assert.equal(c.recState.total, 4, 'late arrivals may change total without changing the cursor');

  // A late page must not append to a newer model query, even if it started while current.
  const page = deferred();
  reply = () => page.promise;
  const pendingPage = vm.runInContext('loadRecords(true)', c);
  input('fmodel').value = 'model-b';
  reply = async () => ({records: [{request_id: '0000000000000008'}], total: 1, next_before_id: null});
  await vm.runInContext('loadRecords(false)', c);
  page.resolve({records: [{request_id: '0000000000000001'}], total: 4, next_before_id: null});
  await pendingPage;
  assert.equal(c.recState.rows.length, 1);
  assert.equal(c.recState.rows[0].request_id, '0000000000000008');
  assert.equal(c.recState.cursor, null);

  const scopeCalls = [];
  c.loadSummary = async () => scopeCalls.push(new URLSearchParams(vm.runInContext('summaryQuery()', c)));
  c.loadTrend = async () => scopeCalls.push(new URLSearchParams(vm.runInContext('trendQuery()', c)));
  // Browsing history or a selected chart period preserves every panel's time context.
  for (const selectedPeriod of [null, {from: 9000000, to: 10000000}]) {
    c.recordWindow = selectedPeriod;
    c.recState.rows = Array(selectedPeriod ? 1 : 51).fill({});
    const before = calls.length;
    await vm.runInContext('loadAll(true)', c);
    assert.equal(calls.length, before);
    assert.equal(scopeCalls.length, 0);
    assert.equal(rolls, 0);
  }
  c.recordWindow = null;
  c.recState.rows = [];
  await vm.runInContext('loadAll(true)', c);
  assert.equal(rolls, 1);
  assert.equal(scopeCalls.length, 1, 'one summary query also provides the chart buckets');
  for (const params of scopeCalls) {
    assert.equal(params.get('from'), calls.at(-1).searchParams.get('from'));
    assert.equal(params.get('to'), calls.at(-1).searchParams.get('to'));
  }

  // Old chart/cards cannot drill into a new scope, including the input debounce gap.
  c.SUM_SCOPE = vm.runInContext('qs(scopeFilters())', c);
  input('fmodel').value = 'model-c';
  const before = calls.length;
  vm.runInContext('selectBucket({from: 1000, to: 2000}); drillOutcome("failed")', c);
  assert.equal(c.recordWindow, null);
  assert.equal(calls.length, before);
  assert.equal(input('foutcome').value, '');
  // A global card resets the selected bucket, matching its global denominator.
  c.SUM_SCOPE = vm.runInContext('qs(scopeFilters())', c);
  c.recordWindow = {from: c.scopeWindow.from, to: c.scopeWindow.from + 1000};
  vm.runInContext('drillOutcome("failed")', c);
  assert.equal(c.recordWindow, null);
  assert.equal(calls.at(-1).searchParams.get('outcome'), 'failed');
  assert.equal(Number(calls.at(-1).searchParams.get('to')), c.scopeWindow.to);
  assert.equal(vm.runInContext('tokenValue({count:3,prewarm_count:0,usage_samples:0})', c), '–');
  assert.equal(vm.runInContext('tokenValue({count:0,prewarm_count:0,usage_samples:0})', c), '0');
  assert.equal(vm.runInContext('tokenValue({count:3,prewarm_count:0,usage_samples:1,input_tokens:20,output_tokens:5})', c), '25');

  // Exercise the actual request-count card, its URL persistence and its next page.
  const links = [];
  c.h = (tag, attrs, ...children) => {
    const element = {tag, attrs, children, events: {}, addEventListener(type, fn) {this.events[type] = fn;}};
    if (tag === 'button') links.push(element);
    return element;
  };
  c.clear = element => {element.children = [];};
  c.exact = c.fmtUsd = c.fmtMs = String;
  c.RECORDER = null;
  c.activeRecord = null;
  c.location = {pathname: '/', search: ''};
  c.history = {replaceState(_state, _title, url) {c.location.search = new URL(url, 'http://local').search;}};
  const storage = new Map();
  c.localStorage = {setItem: (key, value) => storage.set(key, value), getItem: key => storage.get(key)};
  const outcome = input('foutcome');
  outcome.tagName = 'SELECT';
  outcome.options = [...html.match(/<select id="foutcome"[^>]*>(.*?)<\/select>/s)[1].matchAll(/<option value="([^"]*)"[^>]*>([^<]*)/g)].map(match => ({value: match[1], textContent: match[2]}));
  Object.defineProperty(outcome, 'selectedIndex', {get() {return this.options.findIndex(o => o.value === this.value);}});
  vm.runInContext(section('const SF=', '$("fclear").addEventListener'), c);
  vm.runInContext(section('function pct(', 'function drillOutcome('), c);
  c.SUM = {from_ms: c.scopeWindow.from, to_ms: c.scopeWindow.to, total: {count: 7, prewarm_count: 1, failures: 1, cancelled: 1, incomplete: 1, usage_samples: 1}};
  c.scopeWindow.fixed = true;
  reply = async () => ({records: [{request_id: '0000000000000007'}], total: 7, next_before_id: '0000000000000007'});
  vm.runInContext('renderKpis()', c);
  // The Token card carries the input cache rate (with its formula); the note below no longer repeats it.
  {
    const flat = n => n === null || n === undefined ? '' : typeof n !== 'object' ? String(n) : Array.isArray(n) ? n.map(flat).join('') : (n.children || []).map(flat).join('');
    const tile = name => input('strip').children.find(k => flat(k.children[0]) === name);
    const cache = () => tile('Token').children[1].children.flat().find(e => e && e.attrs && e.attrs.class === 'cr');
    assert.equal(flat(cache()), '缓存 –', 'no cache samples shows a dash, not 0%');
    assert.match(cache().attrs.title, /缓存命中的输入 Token \/ 输入 Token/);
    assert.match(cache().attrs.title, /（0 条），不含预热/);
    assert.equal(flat(input('metricnote').children).includes('缓存'), false, 'cache rate is not duplicated under the cards');
    const saved = c.SUM.total;
    c.SUM.total = {...saved, usage_samples: 4, input_tokens: 1000, output_tokens: 50, cache_ratio: 0.462, cache_samples: 3};
    vm.runInContext('renderKpis()', c);
    assert.equal(flat(cache()), '缓存 46.2%');
    assert.match(cache().attrs.title, /（3 条），不含预热/);
    assert.equal(flat(tile('Token').children[1]), '1050缓存 46.2%');
    input('fprewarm').checked = true;
    vm.runInContext('renderKpis()', c);
    assert.match(cache().attrs.title, /，含预热，/, 'the formula names the 含预热 population');
    input('fprewarm').checked = false;
    c.SUM.total = {...saved, cache_ratio: null, cache_samples: 0};
    vm.runInContext('renderKpis()', c);
    assert.equal(flat(cache()), '缓存 –');
    const sum = c.SUM;c.SUM = null;
    vm.runInContext('renderKpis()', c);
    assert.equal(flat(tile('Token').children[1]), '–', 'no summary yet: the card is a bare dash');
    c.SUM = sum;c.SUM.total = saved;links.length = 0;
    vm.runInContext('renderKpis()', c);
  }
  links[0].events.click();
  await new Promise(setImmediate);
  assert.equal(outcome.value, 'business', 'request-count card selects visible business scope');
  assert.equal(calls.at(-1).searchParams.get('exclude_prewarm'), '1');
  assert.equal(calls.at(-1).searchParams.has('outcome'), false);
  assert.equal(new URLSearchParams(vm.runInContext('summaryQuery()', c)).get('exclude_prewarm'), '1', 'record investigation keeps the scope denominator (business only by default)');
  // 「含预热」 widens every figure and the 全部结果 list; the 业务请求 and 预热 outcomes stay explicit.
  {
    const params = q => new URLSearchParams(vm.runInContext(q, c));
    const saved = outcome.value;
    input('fprewarm').checked = true;
    assert.equal(params('summaryQuery()').has('exclude_prewarm'), false, 'toggle includes prewarm in the summary');
    assert.equal(params('trendQuery()').has('exclude_prewarm'), false, 'toggle includes prewarm in the chart');
    outcome.value = '';
    assert.equal(params('qs(recordFilters())').has('exclude_prewarm'), false, 'toggle shows prewarm rows in 全部结果');
    outcome.value = 'business';
    assert.equal(params('qs(recordFilters())').get('exclude_prewarm'), '1', '业务请求 always excludes prewarm');
    input('fprewarm').checked = false;
    assert.equal(params('trendQuery()').get('exclude_prewarm'), '1', 'chart excludes prewarm by default');
    outcome.value = '';
    assert.equal(params('qs(recordFilters())').get('exclude_prewarm'), '1', '全部结果 hides prewarm rows by default');
    outcome.value = 'prewarm';
    const pw = params('qs(recordFilters())');
    assert.equal(pw.get('outcome'), 'prewarm', 'prewarm rows stay reachable through the filter');
    assert.equal(pw.has('exclude_prewarm'), false, 'the prewarm outcome is never self-excluded');
    outcome.value = saved;
  }
  assert.equal(new URLSearchParams(c.location.search).get('foutcome'), 'business');
  outcome.value = '';
  vm.runInContext('loadF()', c);
  assert.equal(outcome.value, 'business', 'URL reload keeps business scope');
  await vm.runInContext('loadRecords(true)', c);
  assert.equal(calls.at(-1).searchParams.get('before_id'), '0000000000000007');
  assert.equal(calls.at(-1).searchParams.get('exclude_prewarm'), '1');
  assert.equal(c.recState.total, 7);
  outcome.value = '';
  input('fprewarm').checked = true;
  await vm.runInContext('loadRecords(false)', c);
  assert.equal(calls.at(-1).searchParams.has('exclude_prewarm'), false, '全部结果 + 含预热 includes prewarm');
  assert.equal(calls.at(-1).searchParams.has('before_id'), false, 'changing population resets pagination');
  input('fprewarm').checked = false;
  c.location.search = '?foutcome=old-unknown-value';
  vm.runInContext('loadF()', c);
  assert.equal(outcome.value, '', 'unknown old URL values keep the default');
}
// 筛选范围 suggestions: most-used first, substring filter, keyboard pick, free text, one cached fetch.
async function scopeSuggestions() {
  const els = new Map(), document = {activeElement: null};
  const el = (tag, attrs = {}) => {
    const e = {tag, attrs: {...attrs}, children: [], hidden: false, value: '', dataset: {}, events: {},
      classList: {add(c) {if (c === 'hide') e.hidden = true;}, toggle(c, on) {if (c === 'hide') e.hidden = on;}},
      setAttribute(k, v) {e.attrs[k] = String(v); if (k === 'id') e.id = String(v); if (k.startsWith('data-')) e.dataset[k.slice(5)] = String(v);},
      getAttribute(k) {return k in e.attrs ? e.attrs[k] : null;}, removeAttribute(k) {delete e.attrs[k];},
      addEventListener(t, fn) {(e.events[t] ||= []).push(fn);}, fire(t, ev = {}) {for (const fn of e.events[t] || []) fn({preventDefault() {ev.prevented = true;}, stopPropagation() {}, ...ev}); return ev;},
      appendChild(c) {e.children.push(c);}, scrollIntoView() {},
      get text() {const t = x => typeof x === 'string' ? x : (x.children || []).map(t).join(''); return t(e);}};
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    return e;
  };
  const $ = id => {if (!els.has(id)) els.set(id, el(id.startsWith('l') ? 'ul' : 'input')); return els.get(id);};
  const h = (tag, a, ...kids) => {const e = el(tag, typeof a === 'string' ? {class: a} : a || {}); for (const k of kids.flat()) if (k != null) e.children.push(k); return e;};
  let fetches = 0, picked = 0;
  const now = 1_000_000_000;
  const c = vm.createContext({$, h, document, clear: e => {e.children = [];}, exact: String, ago: () => '刚刚', setTimeout: fn => fn(),
    Date: {now: () => now}, Promise, Array,
    changeScope() {picked++;},
    usageGet: async url => {fetches++; assert.equal(url, '/usage/v1/scope-values'); return {
      model: [{value: 'gpt-6.1', count: 90, last_ms: now - 1}, {value: 'gpt-6.1-sol', count: 12, last_ms: now - 2}, {value: 'o9-mini', count: 3, last_ms: now - 3}],
      key: [{value: 'laptop', count: 5, last_ms: now}, {value: 'unused', count: 0, last_ms: null, configured: true}],
      instance: [{value: 'box-a', count: 0, last_ms: null, configured: true}]};}});
  vm.runInContext(section('// ---- Scope suggestions:', 'let MEM=null;'), c);
  const model = $('fmodel'), list = $('lfmodel');
  const tick = () => new Promise(setImmediate);
  document.activeElement = model; model.fire('focus'); await tick();
  assert.equal(model.getAttribute('aria-expanded'), 'true');
  assert.deepEqual(list.children.map(li => li.dataset.value), ['gpt-6.1', 'gpt-6.1-sol', 'o9-mini'], 'most-used first');
  assert.equal(list.children[0].attrs.role, 'option');
  assert.match(list.children[0].text, /90 次/);
  model.value = 'SOL'; model.fire('input');
  assert.deepEqual(list.children.map(li => li.dataset.value), ['gpt-6.1-sol'], 'case-insensitive substring');
  model.value = 'gpt-6.1'; model.fire('input');
  assert.deepEqual(list.children.map(li => li.dataset.value), ['gpt-6.1-sol'], 'an exact match is not offered again');
  model.value = 'my-own-model'; model.fire('input');
  assert.equal(list.hidden, true, 'free text with no match just closes the list');
  assert.equal(model.getAttribute('aria-expanded'), 'false');
  model.value = '';
  model.fire('keydown', {key: 'ArrowDown'}); model.fire('keydown', {key: 'ArrowDown'});
  assert.equal(model.getAttribute('aria-activedescendant'), 'lfmodel-1');
  assert.equal(list.children[1].attrs['aria-selected'], 'true');
  model.fire('keydown', {key: 'ArrowUp'}); model.fire('keydown', {key: 'ArrowUp'});
  assert.equal(model.getAttribute('aria-activedescendant'), 'lfmodel-2', 'arrow keys wrap');
  const enter = model.fire('keydown', {key: 'Enter'});
  assert.equal(enter.prevented, true);
  assert.equal(model.value, 'o9-mini');
  assert.equal(picked, 1, 'a pick applies the scope at once');
  assert.equal(list.hidden, true);
  // Pointer pick keeps focus (mousedown/pointerdown are cancelled) and works on touch.
  model.value = ''; model.fire('input');
  const down = list.children[0].fire('pointerdown');
  assert.equal(down.prevented, true);
  list.children[0].fire('click');
  assert.equal(model.value, 'gpt-6.1');
  assert.equal(picked, 2);
  // Escape closes without changing the value; blur closes too.
  model.value = ''; model.fire('input');
  model.fire('keydown', {key: 'Escape'});
  assert.equal(list.hidden, true);
  assert.equal(model.value, '');
  const key = $('fkey'); document.activeElement = key; key.fire('focus'); await tick();
  assert.match($('lfkey').children[1].text, /已配置/, 'configured keys without traffic are suggested');
  document.activeElement = null; key.fire('blur');
  assert.equal($('lfkey').hidden, true);
  assert.equal(fetches, 1, 'suggestions are fetched once and cached for 60 s');
  // Logout clears the cache, so a different key never sees the old values.
  assert.ok(section('onLogout=()=>{stopAuto', '};').includes('SV=null;svAt=0'));
}
const watchdog = setTimeout(() => {
  console.error('FAIL: UI scheduling checks did not settle within 5 seconds');
  process.exitCode = 1;
}, 5000);

async function sharedAuthResponses() {
  let auth = 'old-key', answer = deferred();
  const c = vm.createContext({
    key: () => auth, KS: 'key', NOADMIN: false, authEpoch: 0,
    fetch: () => answer.promise, clearTimeout,
    sessionStorage: {removeItem() {auth = '';}},
    showApp() {c.logouts++;}, logouts: 0,
    adminMsg: () => 'admin required',
  });
  vm.runInContext(section('async function req(', 'async function get('), c);
  const response = status => ({status, ok: false, json: async () => ({error: {type: 'admin_required'}})});
  for (const status of [401, 403]) {
    auth = 'old-key';
    answer = deferred();
    const pending = vm.runInContext('req("GET","/test")', c);
    auth = 'new-key';
    answer.resolve(response(status));
    await assert.rejects(pending);
    assert.equal(auth, 'new-key');
    assert.equal(c.logouts, 0);
    assert.equal(c.NOADMIN, false);
  }
  // Logging out and back in with the same key still creates a different login session.
  for (const status of [401, 403]) {
    answer = deferred();
    const pending = vm.runInContext('req("GET","/test")', c);
    c.authEpoch++;
    answer.resolve(response(status));
    await assert.rejects(pending);
    assert.equal(auth, 'new-key');
    assert.equal(c.logouts, 0);
    assert.equal(c.NOADMIN, false);
  }
  for (const status of [401, 403]) {
    answer = deferred();
    const pending = vm.runInContext('req("GET","/test",undefined,undefined,{current:()=>false})', c);
    answer.resolve(response(status));
    await assert.rejects(pending);
    assert.equal(auth, 'new-key');
    assert.equal(c.logouts, 0, 'superseded reads do not revoke the current login');
    assert.equal(c.NOADMIN, false);
  }
  answer = deferred();
  const current = vm.runInContext('req("GET","/test")', c);
  answer.resolve(response(401));
  await assert.rejects(current, /401/);
  assert.equal(auth, '');
  assert.equal(c.logouts, 1, 'a current unauthorized response still clears the invalid login');
}

// Behind Cloudflare Access: an expired login is not a network error and not a bad key.
async function accessGateResponses() {
  let auth = 'key-1', reply;
  const inits = [];
  const c = vm.createContext({
    key: () => auth, KS: 'key', NOADMIN: false, authEpoch: 0,
    fetch: async (_path, init) => {inits.push(init); return reply();}, clearTimeout,
    sessionStorage: {removeItem() {auth = '';}},
    showApp() {c.logouts++;}, logouts: 0, adminMsg: () => null,
  });
  vm.runInContext(section('async function req(', 'async function get('), c);
  const headers = map => ({get: n => map[n.toLowerCase()] ?? null, keys: () => Object.keys(map)[Symbol.iterator]()});
  const expired = 'Cloudflare Access 登录已过期，请刷新页面重新登录';
  for (const r of [
    {type: 'opaqueredirect', status: 0, ok: false, headers: headers({})},
    {type: 'basic', status: 200, ok: true, redirected: true, headers: headers({'content-type': 'application/json'})},
    {type: 'basic', status: 200, ok: true, headers: headers({'content-type': 'text/html'})},
    {type: 'basic', status: 401, ok: false, headers: headers({'content-type': 'text/plain', 'cf-access-aud': 'x'})},
  ]) {
    reply = async () => ({...r, json: async () => {throw new Error('not json');}});
    await assert.rejects(vm.runInContext('req("GET","/usage/v1/summary")', c), e => e.kind === 'access' && e.message === expired);
    assert.equal(auth, 'key-1', 'an expired Access login keeps the key');
    assert.equal(c.logouts, 0);
  }
  assert.ok(inits.every(i => i.credentials === 'same-origin' && i.redirect === 'manual' && i.cache === 'no-store' && i.referrerPolicy === 'no-referrer'));
  // a real network failure stays a network error
  reply = async () => {throw new TypeError('Failed to fetch');};
  await assert.rejects(vm.runInContext('req("GET","/usage/v1/summary")', c), e => e.kind === 'network' && e.message === '网络错误，连不上 ciallopa（Failed to fetch）');
  // ciallopa's own 401 still clears an invalid key
  reply = async () => ({type: 'basic', status: 401, ok: false, headers: headers({'content-type': 'text/plain; charset=utf-8'}), json: async () => ({error: 'invalid api key'})});
  await assert.rejects(vm.runInContext('req("GET","/usage/v1/summary")', c), /401/);
  assert.equal(auth, '');
  assert.equal(c.logouts, 1);
}

async function overviewSnapshots() {
  let auth = 'demo-admin', now = 1000;
  const controls = new Map();
  const control = id => {
    if (!controls.has(id)) controls.set(id, {textContent: '', tBodies: [{}], setAttribute() {}, classList: {toggle() {}}});
    return controls.get(id);
  };
  let reply = async () => ({accounts: [{id: 'alice', account: 'alice'}], version: 'v1'});
  const c = vm.createContext({
    $: control, key: () => auth, Q: null, RC: null, AC: null, NOADMIN: false,
    usageGet: path => reply(path), renderQuota() {},
    stopAuto() {}, closePop() {}, renderKpis() {}, renderRuntime() {}, renderMem() {}, renderRecords() {}, clear() {},
    why: e => e.message, fmtTime: value => 't' + value,
    Date: class extends Date {static now() {return now;}},
  });
  vm.runInContext(section('const loadSeq={},loading={};', 'function emptyRow(') + '\n' +
    section('async function loadQuota()', 'function summaryQuery()') + '\n' +
    section('onLogout=()=>{stopAuto()', 'for(const id of ["auto","mauto"])'), c);
  const load = () => vm.runInContext('loadQuota()', c);
  await load();
  assert.equal(c.Q.version, 'v1');
  now = 2000;
  reply = async path => {
    if (path.startsWith('/usage/')) throw Object.assign(new Error('synthetic 503'), {status: 503});
    return {accounts: [{id: 'alice'}], version: 'v2'};
  };
  await load();
  assert.equal(c.Q.version, 'v1');
  assert.equal(c.RC.version, 'v1');
  assert.equal(c.AC.version, 'v2');
  assert.match(control('qnote').textContent, /额度.*保留.*t1000/);
  assert.match(control('qnote').textContent, /重置卡.*保留.*t1000/);
  reply = async path => {throw Object.assign(new Error('unavailable'), {status: path.includes('reset') ? 404 : 403});};
  await load();
  assert.equal(c.Q, null, 'permission loss clears the quota snapshot');
  assert.equal(c.RC, null, 'unavailable cards do not keep looking current');
  assert.equal(c.AC, null, 'permission loss clears admin metadata');
  assert.match(control('qnote').textContent, /额度.*403/);
  assert.match(control('qnote').textContent, /重置卡.*404/);
  reply = async () => {throw Object.assign(new Error('synthetic 503'), {status: 503});};
  await load();
  assert.match(control('qnote').textContent, /暂无成功数据/);
  assert.doesNotMatch(control('qnote').textContent, /保留/);
  reply = async () => ({accounts: [], version: 'recovered'});
  await load();
  assert.equal(c.Q.version, 'recovered');
  assert.doesNotMatch(control('qnote').textContent, /失败|403|404/);
  const slow = deferred();
  reply = () => slow.promise;
  const old = load();
  auth = 'new-admin';
  reply = async () => ({accounts: [], version: 'new-login'});
  await load();
  slow.resolve({accounts: [], version: 'old-login'});
  await old;
  assert.equal(c.Q.version, 'new-login');
  const logoutRead = deferred();
  reply = () => logoutRead.promise;
  const pendingLogout = load();
  vm.runInContext('onLogout()', c);
  // Keep the same key to exercise logout invalidation rather than just string inequality.
  logoutRead.resolve({accounts: [], version: 'before-logout'});
  await pendingLogout;
  assert.equal(c.Q, null);
  reply = async () => {throw Object.assign(new Error('synthetic 503'), {status: 503});};
  await load();
  assert.match(control('qnote').textContent, /暂无成功数据/);
  assert.doesNotMatch(control('qnote').textContent, /保留/);
}

async function egressResponses() {
  const controls = new Map();
  const control = id => {
    if (!controls.has(id)) controls.set(id, {textContent: '', title: '', className: 'hide', remove() {}, classList: {toggle() {}, remove() {}}});
    return controls.get(id);
  };
  let auth = 'old-admin', handler = async () => ({egress: {direct: true}});
  const c = vm.createContext({
    $: control, key: () => auth, NOADMIN: false, authEpoch: 0, onLogout() {}, closeAllModals() {},
    get: path => handler(path),
  });
  vm.runInContext(section('function showApp(', 'function login(') + '\n' +
    section('// ---- shell: account count', '// Accounts / settings need an admin key.'), c);
  const load = () => vm.runInContext('egressTag()', c);
  const direct = {egress: {direct: true}};
  const proxy = {egress: {direct: false, proxy: 'http://synthetic-proxy:8080'}};
  for (const sameIdentity of [false, true]) {
    const first = deferred();
    auth = 'old-admin';
    handler = path => path === '/admin/v1/config' ? first.promise : Promise.resolve({total: 0});
    const old = load();
    if (!sameIdentity) auth = 'new-admin';
    handler = async () => direct;
    await load();
    first.resolve(proxy);
    await old;
    assert.equal(control('egress').textContent, '直连', 'a stale success cannot overwrite the current egress');
  }
  const fail = deferred();
  handler = async () => {await fail.promise; throw Error('old failure');};
  const oldFailure = load();
  handler = async () => direct;
  await load();
  fail.resolve();
  await oldFailure;
  assert.equal(control('egress').className, 'tag line', 'a stale failure cannot hide a current success');
  const closing = deferred();
  handler = () => closing.promise;
  const pending = load();
  vm.runInContext('showApp(false)', c);
  assert.equal(control('egress').textContent, '', 'logout clears the old egress');
  assert.equal(control('egress').title, '');
  // The same key can log in again: invalidation cannot rely on comparing key strings alone.
  vm.runInContext('showApp(true)', c);
  closing.resolve(direct);
  await pending;
  assert.equal(control('egress').className, 'hide');
}

async function currentReadsProtectAuthorization() {
  for (const status of [401, 403]) {
    let auth = 'same-admin';
    const first = deferred(), controls = new Map();
    let adminCalls = 0;
    const control = id => {
      if (!controls.has(id)) controls.set(id, {textContent: '', setAttribute() {}, classList: {toggle() {}}});
      return controls.get(id);
    };
    const response = (body, code = 200) => ({ok: code < 400, status: code, json: async () => body});
    const c = vm.createContext({
      $: control, key: () => auth, authEpoch: 0, KS: 'key', NOADMIN: false,
      Q: null, RC: null, AC: null, renderQuota() {},
      why: e => e.message, fmtTime: String, adminMsg: () => 'admin required',
      AbortController, setTimeout, clearTimeout,
      sessionStorage: {removeItem() {auth = '';}},
      showApp() {c.logouts++;}, logouts: 0,
      fetch: async path => path === '/admin/v1/accounts' && ++adminCalls === 1
        ? first.promise : response({accounts: [], version: 'current'}),
    });
    vm.runInContext([
      section('async function req(', 'async function get('),
      section('async function usageGet(', 'const narrow='),
      section('const loadSeq={},loading={};', 'function emptyRow('),
      section('async function loadQuota()', 'function summaryQuery()'),
    ].join('\n'), c);
    const old = vm.runInContext('loadQuota()', c);
    await vm.runInContext('loadQuota()', c);
    first.resolve(response({error: {type: 'admin_required'}}, status));
    await old;
    assert.equal(auth, 'same-admin', `old HTTP ${status} must not clear the login`);
    assert.equal(c.logouts, 0);
    assert.equal(c.NOADMIN, false, `old HTTP ${status} must not revoke admin state`);
    await vm.runInContext('loadQuota()', c);
    assert.equal(c.AC.version, 'current');
    assert.equal(adminCalls, 3, 'the next refresh still fetches account metadata');
  }
}

function bucketSamplesAndUnknownOutcomes() {
  const c = vm.createContext({Map, Math});
  vm.runInContext(section('function trendPoints(', 'function selectBucket(') + '\n' +
    section('function requestOutcome(', 'function streamRecord('), c);
  c.summary = {from_ms: 0, to_ms: 5 * 3600000 - 1, buckets: [
    {start_ms: 3600000, count: 1, prewarm_count: 0, usage_samples: 0, input_tokens: 0, output_tokens: 0},
    {start_ms: 7200000, count: 1, prewarm_count: 0, usage_samples: 1, input_tokens: 0, output_tokens: 0},
    {start_ms: 10800000, count: 2, prewarm_count: 1, usage_samples: 2, input_tokens: 20, output_tokens: 5},
    {start_ms: 14400000, count: 0, prewarm_count: 1, usage_samples: 0, input_tokens: 0, output_tokens: 0},
  ]};
  const points = vm.runInContext('trendPoints(summary,true,0)', c);
  assert.deepEqual(Array.from(points, p => p.tokens), [0, null, 0, 25, null], 'no records, missing usage, explicit zero and partial usage stay distinct');
  assert.equal(points[3].samples, 2, 'usage samples include prewarm');
  assert.equal(points[3].records, 3, 'the usage population includes prewarm');
  const controls = new Map();
  const control = id => {
    if (!controls.has(id)) controls.set(id, {value: 'tokens', children: [], textContent: '',
      classList: {add() {}, toggle() {}}, setAttribute() {}, appendChild(child) {this.children.push(child);},
      append(...kids) {this.children.push(...kids);}});
    return controls.get(id);
  };
  Object.assign(c, {
    $: control, TREND: {summary: c.summary, hourly: true, tz: 0}, recordWindow: null,
    narrow: () => false, summaryCurrent: () => true, clear: e => {e.children = [];},
    sv: (tag, attrs, text) => ({tag, attrs, text, style: {}, addEventListener() {}}),
    h: (tag, attrs, ...kids) => ({tag, attrs, kids}),
    p2: String, fmtN: String, fmtTime: String, exact: String, fmtUsd: String, pwText: () => '含预热', // these buckets were fetched with 「含预热」 on
  });
  vm.runInContext(section('function renderTrend()', 'let lastNarrow='), c);
  vm.runInContext('renderTrend()', c);
  const labels = control('svg').children.filter(e => e.attrs?.role === 'button').map(e => e.attrs['aria-label']);
  assert.match(labels[0], /Token 0 ·/);
  assert.match(labels[1], /Token 未报告/);
  assert.match(labels[2], /Token 0（1 \/ 1 条已报告/);
  assert.match(labels[3], /Token 25（2 \/ 3 条已报告，含预热）/);
  assert.match(labels[4], /Token 未报告/);
  assert.equal(control('svg').children.filter(e => e.tag === 'text' && e.text === '–').length, 2, 'unreported buckets get an unknown marker, not a zero bar');
  for (const [record, expected] of [
    [{status: 200}, 'completed'], [{status: 200, error: 'upstream_error_event'}, 'failed'],
    [{error: 'client_closed'}, 'cancelled'], [{incomplete: 'max_output_tokens'}, 'incomplete'],
    [{status: 200, error: 'observation_incomplete', err_source: 'local'}, 'unknown'],
    [{error: 'observation_incomplete', err_source: 'local'}, 'unknown'],
    [{status: 400, error: 'observation_incomplete', err_source: 'upstream'}, 'failed'],
    [{status: 200, error: 'observation_incomplete', err_source: 'upstream'}, 'failed'],
    [{status: 500, error: 'observation_incomplete', err_source: 'local'}, 'failed'],
    [{endpoint: 'prewarm', error: 'observation_incomplete'}, 'prewarm'],
  ]) {
    c.record = record;
    assert.equal(vm.runInContext('requestOutcome(record)', c), expected);
  }
}

// Status chips: 2xx success green, 429 amber, other 4xx error, 5xx red; cancelled, unknown and
// prewarm rows stay neutral. Every coloured chip pair meets WCAG AA (4.5:1) in both themes.
function statusChipColours() {
  const c = vm.createContext({});
  vm.runInContext(section('function requestOutcome(', 'function streamRecord('), c);
  const kind = (outcome, status) => (c.o = outcome, c.s = status, vm.runInContext('(statusKind(o,s)||[null])[0]', c));
  assert.equal(kind('completed', 200), 's2');
  assert.equal(kind('incomplete', 200), 's2');
  assert.equal(kind('failed', 429), 's429');
  assert.equal(kind('failed', 401), 's4');
  assert.equal(kind('failed', 502), 's5');
  assert.equal(kind('failed', 200), null, 'a stream that failed after 200 is not green');
  for (const outcome of ['cancelled', 'unknown', 'prewarm']) for (const s of [200, 499, 502])
    assert.equal(kind(outcome, s), null, `${outcome} ${s} stays neutral`);
  assert.equal(kind('completed', 0), null);
  const vars = block => Object.fromEntries([...block.matchAll(/--([\w-]+):(#[0-9a-f]{6})/g)].map(m => [m[1], m[2]]));
  const lum = hex => {
    const [r, g, b] = [1, 3, 5].map(i => parseInt(hex.slice(i, i + 2), 16) / 255)
      .map(x => x <= 0.03928 ? x / 12.92 : ((x + 0.055) / 1.055) ** 2.4);
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const ratio = (a, b) => { const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p); return (x + 0.05) / (y + 0.05); };
  const light = vars(section(':root{color-scheme:light;', '}')), dark = vars(section(':root[data-theme=dark]{color-scheme:dark;', '}'));
  Object.assign(light, vars(section(':root{--st5:', '}')));
  Object.assign(dark, vars(section(':root[data-theme=dark]{--st5:', '}')));
  assert.equal(section('@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--st5:', '}'), '@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--st5:' + section(':root[data-theme=dark]{--st5:', '}').slice(':root[data-theme=dark]{--st5:'.length), 'dark --st5 tokens agree');
  for (const [theme, v] of [['light', light], ['dark', dark]])
    for (const [fg, bg] of [['ok', 'ok-bg'], ['cool', 'cool-bg'], ['err', 'err-bg'], ['st5', 'st5-bg'], ['fg-2', 'off-bg']])
      assert.ok(ratio(v[fg], v[bg]) >= 4.5, `${theme} ${fg} on ${bg}: ${ratio(v[fg], v[bg]).toFixed(2)}`);
}

async function observationErrorDetails() {
  const controls = new Map();
  const control = id => {
    if (!controls.has(id)) controls.set(id, {textContent: '', setAttribute() {}, classList: {toggle() {}, add() {}, contains: () => false}});
    return controls.get(id);
  };
  const c = vm.createContext({
    $: control, key: () => 'demo', clear() {}, field() {}, detailStats() {}, renderDetailNav() {}, saveF() {},
    tierName: () => '', normTier: () => '', fmtTime: String,
    lookupSeq: 0, detailSeq: 0, activeRecord: null, authEpoch: 0, hideDetailRetry() {},
  });
  vm.runInContext(section('const SRC=', 'function recRow(') + '\n' + section('async function openDetail(', 'async function lookupRecord('), c);
  for (const [status, source, outcome] of [[200, 'local', 'unknown'], [400, 'upstream', 'failed'], [500, 'local', 'failed']]) {
    c.record = {request_id: '0000000000000001', status, err_source: source, error: 'observation_incomplete'};
    await vm.runInContext('openDetail(record)', c);
    if (outcome === 'unknown') {
      assert.match(control('dmsg').textContent, /响应已透传.*不计为失败/);
    } else {
      assert.doesNotMatch(control('dmsg').textContent, /观察不完整|响应已透传|不计为失败/, 'same-named HTTP errors must not claim unknown-result semantics');
      assert.match(control('dmsg').textContent, /observation_incomplete/);
    }
  }
}

// Only the DOM operations used by the chip and inspector flows; no browser/layout claims.
function interactionDom() {
  const controls = new Map(), handlers = {};
  const document = {activeElement: null, contains: () => true,
    addEventListener(type, fn) {(handlers[type] ||= []).push(fn);}};
  const element = (tag = 'div', attrs = {}, ...children) => {
    const classes = new Set((attrs.class || '').split(' '));
    const e = {tagName: tag.toUpperCase(), value: '', checked: false, children, dataset: {}, events: {}, textContent: '',
      classList: {contains: n => classes.has(n), add: n => classes.add(n), remove: n => classes.delete(n),
        toggle(n, on) {if (on) classes.add(n); else classes.delete(n);if (n === 'hide' && on && e.children.includes(document.activeElement)) document.activeElement = null;}},
      setAttribute(n, v) {attrs[n] = String(v); if (n === 'data-clear-filter') this.dataset.clearFilter = v;},
      getAttribute: n => attrs[n], addEventListener(type, fn) {this.events[type] = fn;},
      appendChild(child) {this.children.push(child);}, append(...items) {this.children.push(...items);},
      querySelectorAll() {return this.children.filter(child => child.tagName === 'BUTTON');},
      focus() {document.activeElement = this;},
      get selectedIndex() {return this.options?.findIndex(o => o.value === this.value);},
    };
    let disabled = false;
    Object.defineProperty(e, 'disabled', {get: () => disabled, set(on) {disabled = on;if (on && document.activeElement === e) document.activeElement = null;}});
    for (const [n, v] of Object.entries(attrs)) e.setAttribute(n, v);
    return e;
  };
  const control = id => {
    if (!controls.has(id)) {
      const select = html.match(new RegExp('<select id="' + id + '"[^>]*>(.*?)</select>', 's'));
      const e = element(select ? 'select' : 'div');
      if (select) e.options = [...select[1].matchAll(/<option value="([^"]*)"[^>]*>([^<]*)/g)].map(m => ({value: m[1], textContent: m[2]}));
      if (id === 'ferr' || id === 'fretry') e.type = 'checkbox';
      controls.set(id, e);
    }
    return controls.get(id);
  };
  return {$: control, document, clear: e => {e.children = [];e.textContent = '';},
    h: (tag, attrs, ...children) => element(tag, typeof attrs === 'string' ? {class: attrs} : attrs || {}, ...children),
    keydown(key, modifiers = {}) {const event = {key, ...modifiers, preventDefault() {this.defaultPrevented = true;}};for (const fn of handlers.keydown || []) fn(event);return event;}};
}

async function filterChipScopeAndFocus() {
  const dom = interactionDom(), control = dom.$, calls = [], replaced = [];
  const c = vm.createContext({...dom, URLSearchParams, clearTimeout, kt: 123,
    scopeWindow: {from: 1000, to: 9000, fixed: true}, recordWindow: {from: 2000, to: 3000}, activeRecord: '0000000000000001',
    location: {pathname: '/ui', search: ''}, history: {replaceState(_state, _title, url) {replaced.push(url);c.location.search = new URL(url, 'http://local').search;}},
    localStorage: {setItem() {}, getItem() {return null;}}, fmtTime: String, renderKpis() {}, renderTrend() {},
    fail(e) {throw e;}, loadSummary: async () => calls.push(['summary', vm.runInContext('scopeFilters()', c)]),
    loadRecords: async append => calls.push(['records', vm.runInContext('recordFilters()', c), append]),
  });
  vm.runInContext([
    section('function scopeFilters()', 'async function usageGet('),
    section('function recordFilters()', 'async function loadRecords('),
    section('function changeScope()', '$("range").addEventListener'),
    section('const SF=', 'loadF();renderKpis();'),
  ].join('\n'), c);
  for (const [id, v] of Object.entries({range: '24h', group: 'account', tmode: 'auto', tmetric: 'requests', facct: '', fmodel: '<model>', fkey: 'laptop', finstance: 'home', foutcome: 'failed', fsrc: 'network', ftier: 'dg', fcode: 'timeout'})) control(id).value = v;
  control('ferr').checked = control('fretry').checked = true;
  vm.runInContext('saveF();showScope()', c);
  const chips = box => control(box).children;
  assert.equal(chips('scopechips').length, 3);
  assert.equal(chips('recordchips').length, 6, 'includes the visible outcome and legacy URL checkbox');
  const model = chips('scopechips')[0];model.focus();
  assert.match(model.getAttribute('aria-label'), /清除范围筛选：模型：<model>/);
  vm.runInContext('showScope();saveF()', c);
  assert.equal(chips('scopechips')[0], model, 'periodic renders keep unchanged focused chips');
  model.events.click();
  assert.deepEqual(calls.map(c => c[0]), ['summary', 'records']);
  assert.equal(calls[0][1].model, '');assert.equal(calls[1][1].source, 'network');
  assert.equal(calls[1][1].from, 2000, 'clearing a non-time global filter keeps the selected bucket');
  assert.equal(calls[1][2], false, 'clearing resets record pagination');
  assert.equal(dom.document.activeElement.dataset.clearFilter, 'fkey');
  let params = new URLSearchParams(c.location.search);
  assert.equal(params.has('fmodel'), false);assert.equal(params.get('fkey'), 'laptop');
  assert.equal(params.get('record'), c.activeRecord);assert.equal(params.get('from'), '1000');assert.equal(params.get('rto'), '3000');
  calls.length = 0;
  // Clear every list chip through its actual button; focus follows surviving chips then the result select.
  while (chips('recordchips').length) {const b = chips('recordchips')[0];b.focus();b.events.click();}
  assert.ok(calls.every(call => call[0] === 'records'), 'request chips never reload the global summary');
  assert.equal(control('fkey').value, 'laptop');assert.equal(control('fretry').checked, false);assert.equal(control('ferr').checked, false);
  assert.equal(dom.document.activeElement, control('foutcome'));
  assert.equal(control('recordchips').classList.contains('hide'), true);
  params = new URLSearchParams(c.location.search);assert.equal(params.has('foutcome'), false);assert.equal(params.has('ferr'), false);
  while (chips('scopechips').length) {const b = chips('scopechips')[0];b.focus();b.events.click();}
  assert.equal(dom.document.activeElement, control('scopetoggle'));
  assert.equal(control('scopechips').classList.contains('hide'), true);
  assert.ok(replaced.length > 1, 'existing replaceState history semantics are retained');
  // A shared URL rehydrates the same chips even with both filter editors collapsed.
  control('scopefilters').open = control('recordextra').open = false;
  c.location.search = '?from=1000&to=9000&rfrom=2000&rto=3000&fmodel=shared&foutcome=cancelled&fretry=1';
  c.rollWindow = () => {};
  vm.runInContext('loadF()', c);
  assert.equal(chips('scopechips').length, 1);assert.equal(chips('recordchips').length, 2);
  assert.equal(control('scopefilters').open, false);assert.equal(control('recordextra').open, false);
}

async function inspectorRetryAndNavigation() {
  const dom = interactionDom(), control = dom.$, calls = [];
  let auth = 'demo', reply = async () => response({error: {message: 'temporary'}}, 503);
  const response = (body, status = 200) => ({status, ok: status < 400, json: async () => body});
  const records = [1, 2].map(n => ({request_id: String(n).padStart(16, '0'), status: 500, has_detail: true}));
  const c = vm.createContext({...dom, AbortController, setTimeout, clearTimeout, authEpoch: 0, NOADMIN: false, KS: 'key',
    key: () => auth, sessionStorage: {removeItem() {auth = '';}}, showApp() {c.authEpoch++;vm.runInContext('closeAllModals()', c);},
    fetch: path => {calls.push(path);return reply(path);}, adminMsg: () => 'admin required', why: e => e.message,
    fmtTime: String, fmtMs: String, fmtN: String, fmtUsd: String, tierName: () => '', normTier: () => '',
    activeRecord: null, recState: {rows: records}, saveF() {}, records,
  });
  vm.runInContext([
    section('async function req(', 'async function get('), section('async function usageGet(', 'const narrow='),
    section('const MODALS=[];', '// ---- inline click-again:'), section('const SRC=', 'function recRow('),
    section('let dtext=', '// ---- active quota query'),
  ].join('\n'), c);
  vm.runInContext('recState.rows=records', c);
  control('dp').classList.add('hide');control('dretry').classList.add('hide');
  const tick = () => new Promise(setImmediate), open = n => vm.runInContext(`openDetail(records[${n}],$("recordid"))`, c);
  await open(0);
  assert.match(control('dstatus').textContent, /详情加载失败/);assert.equal(control('dretry').classList.contains('hide'), false);
  assert.equal(control('dcopy').disabled, false, 'summary/error JSON remains available after failure');
  const retry = deferred();reply = () => retry.promise;
  control('dretry').focus();control('dretry').events.click();const count = calls.length;
  control('dretry').events.click();assert.equal(calls.length, count, 'repeated Enter/click does not duplicate an in-flight retry');
  assert.equal(control('dcopy').disabled, true);assert.equal(control('dretry').getAttribute('aria-disabled'), 'true');
  assert.equal(dom.document.activeElement, control('dretry'), 'pending retry stays keyboard reachable');
  retry.resolve(response({body: 'recovered', headers: {}, body_len: 9}));await tick();
  assert.equal(control('dbody').textContent, 'recovered');assert.equal(control('dcopy').disabled, false);
  assert.equal(control('dretry').classList.contains('hide'), true);assert.equal(dom.document.activeElement, control('dclose'));
  reply = async () => response({}, 404);await open(0);
  assert.match(control('dstatus').textContent, /详情已过保留期.*概要仍可查看/);
  assert.equal(control('dretry').classList.contains('hide'), true, 'expired details do not offer a misleading retry');
  assert.equal(JSON.parse(vm.runInContext('dtext', c)).record.request_id, records[0].request_id);
  // Superseded success, errors, and authorization responses must not touch the next record or login.
  for (const status of [200, 503, 401, 403]) {
    const late = deferred();reply = () => late.promise;const old = open(0);
    reply = async () => response({body: 'next record', headers: {}, body_len: 11});await open(1);
    late.resolve(response(status === 200 ? {body: 'stale', headers: {}} : {error: {type: 'admin_required'}}, status));await old;
    assert.equal(control('dbody').textContent, 'next record');assert.equal(auth, 'demo');assert.equal(c.NOADMIN, false);
  }
  reply = async () => response({}, 503);await open(0);
  const staleRetry = deferred();reply = () => staleRetry.promise;control('dretry').events.click();
  reply = async () => response({body: 'new selection', headers: {}});await open(1);
  staleRetry.resolve(response({body: 'old retry'}));await tick();assert.equal(control('dbody').textContent, 'new selection');
  for (const status of [401, 403]) {
    const staleLookup = deferred();reply = () => staleLookup.promise;
    const old = vm.runInContext('lookupRecord("0000000000000001",$("recordid"))', c);
    reply = async () => response({body: 'direct selection', headers: {}});await open(1);
    staleLookup.resolve(response({error: {type: 'admin_required'}}, status));await old;
    assert.equal(auth, 'demo');assert.equal(c.NOADMIN, false);assert.equal(control('dbody').textContent, 'direct selection');
  }
  const late = deferred();reply = () => late.promise;const closing = open(0);
  dom.keydown('Escape');assert.equal(dom.document.activeElement, control('recordid'));
  late.resolve(response({body: 'closed'}));await closing;assert.equal(vm.runInContext('dtext', c), '');
  for (const sameKey of [false, true]) {
    const oldLogin = deferred();reply = () => oldLogin.promise;const pending = open(0);
    if (sameKey) c.authEpoch++;else auth = 'new-demo';
    const before = control('dstatus').textContent;oldLogin.resolve(response({body: 'old login'}));await pending;
    assert.equal(control('dstatus').textContent, before);assert.notEqual(control('dbody').textContent, 'old login');
  }
  reply = async () => response({body: 'current', headers: {}});await open(0);
  control('dnext').focus();control('dnext').events.click();await tick();
  assert.equal(c.activeRecord, records[1].request_id);assert.equal(dom.document.activeElement, control('dprev'), 'last row leaves focus on an enabled action');
  assert.equal(dom.keydown('ArrowUp').defaultPrevented, undefined, 'ordinary arrows still scroll the dialog');
  dom.keydown('ArrowUp', {altKey: true});await tick();assert.equal(c.activeRecord, records[0].request_id);
  c.activeRecord = '0000000000000099';dom.keydown('ArrowDown', {altKey: true});assert.equal(c.activeRecord, '0000000000000099', 'a deep link outside the list has no adjacent record');
  dom.keydown('Escape');assert.equal(dom.document.activeElement, control('recordid'));
  dom.keydown('ArrowDown', {altKey: true});assert.equal(c.activeRecord, null, 'closed dialogs ignore navigation');
}

function recordRenderingAndInspectorFocus() {
  let focused;
  let now=Date.parse('2026-10-23T12:00:00Z');
  const controls = new Map();
  const control = id => {
    if (!controls.has(id)) {
      const classes = new Set();
      controls.set(id, {value: '', classList: {
        contains: name => classes.has(name), add: name => classes.add(name),
        toggle(name, on) {if (on) classes.add(name); else classes.delete(name);},
      }, focus() {focused = this;}});
    }
    return controls.get(id);
  };
  const tb = {children: [], get firstElementChild() {return this.children[0] || null;},
    appendChild(row) {return this.insertBefore(row, null);},
    insertBefore(row, before) {
      if (row === before) return row;
      row.remove();
      const index = before === null ? this.children.length : this.children.indexOf(before);
      assert.notEqual(index, -1, 'insertBefore anchor belongs to tbody');
      this.children.splice(index, 0, row);row.parentNode = this;return row;
    },
    contains(button) {return this.children.some(row => row.button === button);},
    querySelector(selector) {
      const anchor = selector.match(/^\[data-ak="([^"]+)"\] button$/)?.[1];
      return this.children.find(row => row.dataset.ak === anchor)?.button || null;
    }};
  const row = record => {
    const element = {dataset: {ak: 'rec|' + record.request_id}, record, parentNode: null,
      get nextElementSibling() {return this.parentNode?.children[this.parentNode.children.indexOf(this) + 1] || null;},
      remove() {if(this.parentNode){this.parentNode.children.splice(this.parentNode.children.indexOf(this),1);this.parentNode=null;}},
      replaceWith(next) {const parent=this.parentNode,index=parent.children.indexOf(this);this.remove();next.remove();parent.children.splice(index,0,next);next.parentNode=parent;},
    };
    element.button = {closest: () => element, focus() {focused = this;}};
    return element;
  };
  control('rec').tBodies = [tb];
  const modal = control('dp');
  const c = vm.createContext({
    $: control, recRow: row, clear: node => {for(const child of [...node.children])child.remove();},
    keepScroll: (_id, fn) => fn(), steady: (_boxes, fn) => fn(),
    emptyRow(body) {const empty=row({request_id:'empty'});empty.empty=true;body.appendChild(empty);}, anyF: () => false, renderDetailNav() {}, exact: String, pwText: () => '不含预热', PAGE: 50,
    CSS: {escape: value => value},
    Date: class extends Date {constructor(...args){super(...(args.length?args:[now]));}},
    document: {contains: node => tb.contains(node) || [...controls.values()].includes(node)},
    recState: {rows: [{request_id: '0000000000000001'}, {request_id: '0000000000000002'}], total: 2},
  });
  vm.runInContext(section('const MODALS=[];', 'document.addEventListener("keydown"') + '\n' + section('const recordRows=new Map();', 'const PAGE=50;'), c);
  vm.runInContext('renderRecords()', c);
  const initial = [...tb.children];
  c.recState.rows = c.recState.rows.map(record => ({...record}));
  vm.runInContext('renderRecords()', c);
  assert.deepEqual(tb.children, initial, 'an unchanged API refresh preserves row nodes');
  c.recState.rows.push({request_id:'0000000000000003'});
  vm.runInContext('renderRecords()', c);
  assert.equal(tb.children[0], initial[0], 'append preserves the first existing row');
  assert.equal(tb.children[1], initial[1], 'append preserves the other existing row');
  const third=tb.children[2];
  c.recState.rows = [{...c.recState.rows[0],duration_ms:250},c.recState.rows[1],c.recState.rows[2]];
  vm.runInContext('renderRecords()', c);
  assert.notEqual(tb.children[0],initial[0], 'a changed record replaces only its row');
  assert.equal(tb.children[1],initial[1]);assert.equal(tb.children[2],third);
  assert.equal(tb.children[0].record.duration_ms,250);
  const changed=tb.children[0];
  c.recState.rows=[c.recState.rows[2],c.recState.rows[0],c.recState.rows[1]];
  vm.runInContext('renderRecords()', c);
  assert.deepEqual(tb.children,[third,changed,initial[1]],'reordering moves the same nodes');
  c.recState.rows=[c.recState.rows[1],c.recState.rows[2]];
  vm.runInContext('renderRecords()', c);
  assert.deepEqual(tb.children,[changed,initial[1]],'filtering removes only the missing row');
  assert.equal(third.parentNode,null);
  modal._opener = tb.children[0].button;
  const original = modal._opener;
  c.activeRecord = '0000000000000002'; // Adjacent-detail navigation must not change the return target.
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.equal(focused, original, 'unchanged refresh returns focus to the preserved opener');

  modal.classList.toggle('hide', false);
  modal._opener = tb.children[0].button;
  c.recState.rows=[{...c.recState.rows[0],duration_ms:500},c.recState.rows[1]];
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.notEqual(focused, original, 'a changed opener row was replaced');
  assert.equal(focused, tb.children[0].button, 'closing returns to the same record after its row was replaced');

  modal.classList.toggle('hide', false);
  modal._opener = tb.children[0].button;
  c.recState.rows = [{request_id: '0000000000000002'}];
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.equal(focused, control('foutcome'), 'a record that left the list returns to the request filter');

  modal.classList.toggle('hide', false);
  modal._opener = control('recordid');
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.equal(focused, control('recordid'), 'ID lookup keeps its own return target');

  c.recState.rows=[];
  vm.runInContext('renderRecords()',c);
  assert.equal(tb.children.length,1);assert.equal(tb.children[0].empty,true,'empty result displays the empty state');
  c.recState.rows=[{request_id:'0000000000000003'}];
  vm.runInContext('renderRecords()',c);
  assert.equal(tb.children.length,1);assert.notEqual(tb.children[0],third,'removed rows are not retained in the cache');
  assert.equal(tb.children[0].empty,undefined,'a recovered list replaces the empty state');
  const previousDay=tb.children[0];now+=86400000;
  vm.runInContext('renderRecords()',c);
  assert.notEqual(tb.children[0],previousDay,'a new local day refreshes time-only labels');

  Object.assign(c,{stopAuto(){},closePop(){},loadSeq:{},resetQuotaReads(){},renderKpis(){},renderRuntime(){},renderMem(){}});
  control('svg').children=[];control('sum').tBodies=[{children:[]}];control('quota').tBodies=[{children:[]}];
  vm.runInContext(section('onLogout=()=>{stopAuto()', 'for(const id of ["auto","mauto"])')+'\nonLogout()',c);
  assert.equal(vm.runInContext('recordRows.size',c),0,'logout releases cached records and row nodes');
  assert.equal(tb.children.length,1);assert.equal(tb.children[0].empty,true);
}

// Auto-refresh interval: stored choice, fallback to 15 s, and a running timer restarts at once.
function autoRefreshInterval() {
  const store = new Map();
  const els = new Map();
  const el = id => {
    if (!els.has(id)) els.set(id, {id, value: '', title: '', checked: false, listeners: {}, attrs: {},
      addEventListener(type, fn) {this.listeners[type] = fn;}, setAttribute(k, v) {this.attrs[k] = v;}});
    return els.get(id);
  };
  const intervals = [];
  const c = vm.createContext({
    $: el, Number, String, document: {hidden: false}, MODALS: [], loadAll() {}, timer: null,
    localStorage: {getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v))},
    setInterval: (fn, ms) => {intervals.push(ms); return intervals.length;}, clearInterval() {},
  });
  vm.runInContext('var timer=null;' + section('function stopAuto(', 'onLogout=()=>{stopAuto()') + '\n' +
    section('// Auto-refresh interval', '\nshowAutoSecs();') + '\nshowAutoSecs();', c);
  assert.equal(el('ivl').value, '15', 'default interval is 15 s');
  store.set('ciallopa_auto_secs', '7');
  assert.equal(vm.runInContext('autoSecs()', c), 15, 'an unknown stored value falls back to 15 s');
  vm.runInContext('startAuto()', c);
  assert.equal(intervals.at(-1), 15000);
  el('mivl').value = '5';
  el('mivl').listeners.change({target: el('mivl')});
  assert.equal(store.get('ciallopa_auto_secs'), '5');
  assert.equal(intervals.at(-1), 5000, 'a running timer restarts with the new interval');
  assert.equal(el('ivl').value, '5', 'both pickers stay in sync');
  assert.equal(el('mauto').attrs['aria-label'], '每 5 秒自动刷新');
  vm.runInContext('stopAuto()', c);
  const before = intervals.length;
  el('ivl').value = '60';
  el('ivl').listeners.change({target: el('ivl')});
  assert.equal(intervals.length, before, 'a stopped timer stays stopped');
  assert.equal(vm.runInContext('autoSecs()', c), 60);
}

// Cloudflare egress block banner: shown only while the block is active, no Cloudflare body.
function cloudflareEgressBanner() {
  const el = {textContent: '', hidden: null, title: '', classList: {toggle(_c, on) {el.hidden = on;}},
    appendChild(x) {el.textContent += x.textContent;}, removeAttribute(a) {if (a === 'title') el.title = '';}};
  const c = vm.createContext({
    $: () => el, clear: n => {n.textContent = '';}, h: (_t, _a, text) => ({textContent: text}),
    document: {createTextNode: text => ({textContent: text})}, fmtTime: () => '10-24 12:00:00', ago: () => '刚刚',
    exact: n => String(n), Q: null,
  });
  vm.runInContext(section('// Cloudflare challenge / block page', 'function renderQuota(){'), c);
  c.renderEgressBlock();
  assert.equal(el.hidden, true, 'no snapshot: banner hidden');
  c.Q = {egress_block: {code: 'cf_blocked', cf_code: 1010, upstream_status: 403, cf_ray: 'ray-1', account: 'a', first_at_ms: 1, last_at_ms: 2, count: 3, active: true}};
  c.renderEgressBlock();
  assert.equal(el.hidden, false);
  assert.match(el.textContent, /^上游 Cloudflare 拦截本机出口错误 1010 · .*共 3 次 · .*不冷却账号/);
  assert.ok(!/[。.]$/.test(el.textContent), 'single-sentence banner has no trailing period');
  assert.match(el.title, /cf-ray ray-1/);
  c.Q.egress_block = Object.assign({}, c.Q.egress_block, {code: 'challenge', cf_code: undefined});
  c.renderEgressBlock();
  assert.match(el.textContent, /^上游 Cloudflare 质询本机出口最近/);
  c.Q.egress_block.active = false;
  c.renderEgressBlock();
  assert.equal(el.hidden, true, 'inactive block: banner hidden');
  assert.equal(el.textContent, '');
}

// Memory panel: values, budget/WS cap warnings, missing RSS/jemalloc, sparkline, admin-only.
async function memoryPanel() {
  const els = new Map();
  const el = id => {
    if (!els.has(id)) els.set(id, {id, textContent: '', hidden: null, children: [],
      classList: {toggle(_c, on) {els.get(id).hidden = on;}},
      appendChild(x) {this.children.push(x);}});
    return els.get(id);
  };
  const text = n => typeof n === 'string' ? n : (n.text || '') + (n.kids || []).map(text).join('');
  let reply = null, calls = 0;
  const c = vm.createContext({
    $: el, key: () => 'k', NOADMIN: false, exact: n => String(n), hms: () => '12:00:00', Math, Number,
    clear: n => {n.children = [];},
    h: (tag, attrs, ...kids) => ({tag, cls: typeof attrs === 'string' ? attrs : attrs && attrs.class, title: attrs && attrs.title, kids: kids.filter(k => k !== null && k !== undefined)}),
    sv: (tag, attrs) => ({tag, attrs, kids: [], appendChild(x) {this.kids.push(x);}}),
    get: async p => {calls++; assert.equal(p, '/admin/v1/memory'); if (reply instanceof Error) throw reply; return reply;},
  });
  vm.runInContext(section('let MEM=null;', 'let allPending=0;'), c);
  const MiB = 1 << 20;
  const s = (t, rss, alloc) => ({at_ms: t, rss_bytes: rss, allocated_bytes: alloc, resident_bytes: alloc && alloc * 2, buffered_body_bytes: 0, ws_connections: 0, ws_bytes: 0});
  reply = {now: {...s(600000, 150 * MiB, 40 * MiB), buffered_body_bytes: 220 * MiB, ws_connections: 24, ws_bytes: 3 * MiB},
    rss_peak_bytes: 300 * MiB, jemalloc: true, buffered_body_limit_bytes: 256 * MiB, ws_max_connections: 24,
    ws_max_message_bytes: 16 * MiB, sample_every_secs: 10, history: [s(0, 100 * MiB, 30 * MiB), s(300000, 120 * MiB, 35 * MiB)]};
  await vm.runInContext('loadMem()', c);
  assert.equal(el('mem').hidden, false);
  const cells = el('memstrip').children;
  assert.equal(cells.length, 5);
  assert.deepEqual(cells.slice(0, 4).map(text), [
    '进程 RSS150 MiB峰值 300 MiB', 'jemalloc 已分配40.0 MiB驻留 80.0 MiB',
    '缓冲请求体220 MiB上限 256 MiB · 86%', 'WebSocket 连接24 / 24缓冲 3.0 MiB']);
  assert.deepEqual(cells.map(x => x.cls), ['kpi', 'kpi', 'kpi hot', 'kpi hot', 'kpi spark'], 'budget ≥ 80% and WS at cap are highlighted');
  for (const x of cells) assert.ok(x.title && !/[。.]$/.test(x.title), 'every cell has help without a trailing period');
  assert.equal(text(cells[4].kids[0]), '内存趋势 · 近 10 分钟');
  const svg = cells[4].kids[1];
  assert.deepEqual(svg.kids.map(p => p.attrs.class), ['ln2', 'ln'], 'RSS line over the jemalloc line');
  assert.match(svg.kids[1].attrs.d, /^M0\.0 .*L300\.0 /);
  assert.equal(el('memwhen').textContent, '读取于 12:00:00');
  // No /proc (not Linux) and no jemalloc: placeholders, no broken numbers, chart falls back.
  reply = {...reply, now: s(10000, null, null), rss_peak_bytes: null, jemalloc: false, ws_max_connections: 0, history: []};
  await vm.runInContext('loadMem()', c);
  assert.deepEqual(el('memstrip').children.slice(0, 4).map(text), [
    '进程 RSS–此系统不提供', 'jemalloc 已分配不可用未使用 jemalloc 分配器', '缓冲请求体0 B上限 256 MiB · 0%', 'WebSocket 连接0缓冲 0 B']);
  assert.equal(text(el('memstrip').children[4].kids[0]), '内存趋势 · 采集中');
  // Failure or a usage-only key hides the panel; NOADMIN skips the request.
  reply = new Error('down');
  await vm.runInContext('loadMem()', c);
  assert.equal(el('mem').hidden, true);
  c.NOADMIN = true; const before = calls;
  await vm.runInContext('loadMem()', c);
  assert.equal(calls, before);
  assert.equal(el('mem').hidden, true);
}

(async () => {
  // A rejects while current; loadAll cannot present it until the slow quota job settles.
  const oldRound = vm.runInContext('loadAll()', context);
  await new Promise(setImmediate);
  context.query = 'B';
  await vm.runInContext('loadSummary()', context);
  quota.resolve();
  await oldRound;
  assert.deepEqual(Array.from(context.rendered), ['B']);
  assert.equal(node('msg').textContent, '', 'a superseded round must not show its delayed error');
  assert.deepEqual(context.toasts, [], 'a superseded round must not toast its delayed error');

  // Direct catch(fail) reports current failures without changing their type or metadata.
  const activeError = Object.assign(new TypeError('current-failure'), {status: 502, body: {error: {message: 'current-failure'}}});
  context.activeError = activeError;
  let caught;
  try {await vm.runInContext('currentLoad("ss",()=>query,()=>Promise.reject(activeError),()=>{})', context);} catch (error) {caught = error;}
  assert.equal(caught, activeError);
  assert.ok(caught instanceof TypeError);
  assert.equal(caught.status, 502);
  assert.equal(caught.body.error.message, 'current-failure');
  await vm.runInContext('currentLoad("ss",()=>query,()=>Promise.reject(activeError),()=>{}).catch(fail)', context);
  assert.match(node('msg').textContent, /current-failure/);
  assert.equal(context.toasts.length, 1);

  // A filter change invalidates a rejection even before the debounced query starts.
  node('msg').textContent = '';
  context.toasts.length = 0;
  context.query = 'C';
  vm.runInContext('fail(activeError)', context);
  assert.equal(node('msg').textContent, '');
  assert.equal(context.toasts.length, 0);
  vm.runInContext('fail(new Error("401"))', context);
  assert.equal(node('msg').textContent, '');
  vm.runInContext('fail(new Error("ordinary-failure"))', context);
  assert.match(node('msg').textContent, /ordinary-failure/);

  // Newer success wins whether a refresh repeats the query or changes its signature.
  for (const nextQuery of ['D', 'E']) {
    const slow = deferred();
    context.slow = slow.promise;
    context.query = 'D';
    context.rendered.length = 0;
    const older = vm.runInContext('currentLoad("ss",()=>query,()=>slow,data=>rendered.push(data))', context);
    context.query = nextQuery;
    await vm.runInContext('currentLoad("ss",()=>query,()=>Promise.resolve("newer"),data=>rendered.push(data))', context);
    slow.resolve('older');
    await older;
    assert.deepEqual(Array.from(context.rendered), ['newer'], `late success must not replace query ${nextQuery}`);
  }

  // An auth change invalidates in-flight data even without a new query or sequence number.
  const previousLogin = deferred();
  context.slow = previousLogin.promise;
  context.rendered.length = 0;
  const oldLogin = vm.runInContext('currentLoad("ss",()=>query,()=>slow,data=>rendered.push(data))', context);
  auth = 'another-synthetic-key';
  previousLogin.resolve('previous-login-data');
  await oldLogin;
  assert.deepEqual(Array.from(context.rendered), [], 'data from the previous login must be discarded');
  await queryAndPaging();
  await memoryPanel();
  await scopeSuggestions();
  await sharedAuthResponses();
  await accessGateResponses();
  await overviewSnapshots();
  await currentReadsProtectAuthorization();
  await egressResponses();
  bucketSamplesAndUnknownOutcomes();
  statusChipColours();
  await observationErrorDetails();
  await filterChipScopeAndFocus();
  await inspectorRetryAndNavigation();
  recordRenderingAndInspectorFocus();
  autoRefreshInterval();
  cloudflareEgressBanner();
  console.log('PASS: status chip colours/contrast, auto-refresh interval, Cloudflare egress banner, query/auth races, Cloudflare Access expiry, shared scope and buckets, ID paging, history/drill-down, usage samples and unknown outcomes, overview snapshots, egress identity/order/logout, scoped filter chips/URL/focus, inspector retry/expiry/stale authorization/keyboard, keyed record updates and inspector focus, memory panel, scope suggestions, Token cache rate');
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => clearTimeout(watchdog));
