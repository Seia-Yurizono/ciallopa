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
  loadTrend: async () => {}, egressTag: async () => ({}), loadRecords: async () => {},
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
    loadQuota: async () => {}, egressTag: async () => ({}), hms: () => 'now',
    rollWindow() {rolls++; c.scopeWindow = {...c.scopeWindow, from: c.scopeWindow.from + 1000, to: c.scopeWindow.to + 1000};},
  });
  vm.runInContext([
    section('function scopeFilters()', 'function scopeLabel('),
    section('const loadSeq={},loading={};', 'function emptyRow('),
    section('const PAGE=50;', '$("rmore").addEventListener'),
    section('function summaryQuery()', 'async function loadSummary('),
    section('function trendQuery()', 'function renderFp('),
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
  outcome.options = [...html.match(/<select id="foutcome"[^>]*>(.*?)<\/select>/s)[1].matchAll(/<option value="([^"]*)">/g)].map(match => ({value: match[1]}));
  vm.runInContext(section('const SF=', '$("fclear").addEventListener'), c);
  vm.runInContext(section('function pct(', 'function drillOutcome('), c);
  c.SUM = {from_ms: c.scopeWindow.from, to_ms: c.scopeWindow.to, total: {count: 7, prewarm_count: 1, failures: 1, cancelled: 1, incomplete: 1, usage_samples: 1}};
  c.scopeWindow.fixed = true;
  reply = async () => ({records: [{request_id: '0000000000000007'}], total: 7, next_before_id: '0000000000000007'});
  vm.runInContext('renderKpis()', c);
  links[0].events.click();
  await new Promise(setImmediate);
  assert.equal(outcome.value, 'business', 'request-count card selects visible business scope');
  assert.equal(calls.at(-1).searchParams.get('exclude_prewarm'), '1');
  assert.equal(calls.at(-1).searchParams.has('outcome'), false);
  assert.equal(new URLSearchParams(vm.runInContext('summaryQuery()', c)).has('exclude_prewarm'), false, 'record investigation keeps the global denominator');
  assert.equal(new URLSearchParams(c.location.search).get('foutcome'), 'business');
  outcome.value = '';
  vm.runInContext('loadF()', c);
  assert.equal(outcome.value, 'business', 'URL reload keeps business scope');
  await vm.runInContext('loadRecords(true)', c);
  assert.equal(calls.at(-1).searchParams.get('before_id'), '0000000000000007');
  assert.equal(calls.at(-1).searchParams.get('exclude_prewarm'), '1');
  assert.equal(c.recState.total, 7);
  outcome.value = '';
  await vm.runInContext('loadRecords(false)', c);
  assert.equal(calls.at(-1).searchParams.has('exclude_prewarm'), false, 'all records still includes prewarm');
  assert.equal(calls.at(-1).searchParams.has('before_id'), false, 'changing population resets pagination');
  c.location.search = '?foutcome=old-unknown-value';
  vm.runInContext('loadF()', c);
  assert.equal(outcome.value, '', 'unknown old URL values keep the default');
}
const watchdog = setTimeout(() => {
  console.error('FAIL: UI scheduling checks did not settle within 5 seconds');
  process.exitCode = 1;
}, 5000);

async function sharedAuthResponses() {
  let auth = 'old-key', answer = deferred();
  const c = vm.createContext({
    key: () => auth, KS: 'key', NOADMIN: false,
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
  answer = deferred();
  const current = vm.runInContext('req("GET","/test")', c);
  answer.resolve(response(401));
  await assert.rejects(current, /401/);
  assert.equal(auth, '');
  assert.equal(c.logouts, 1, 'a current unauthorized response still clears the invalid login');
}

function inspectorFocusAfterRefresh() {
  let focused;
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
  const tb = {children: [], appendChild(row) {this.children.push(row);},
    contains(button) {return this.children.some(row => row.button === button);},
    querySelector(selector) {
      const anchor = selector.match(/^\[data-ak="([^"]+)"\] button$/)?.[1];
      return this.children.find(row => row.dataset.ak === anchor)?.button || null;
    }};
  const row = record => {
    const element = {dataset: {ak: 'rec|' + record.request_id}};
    element.button = {closest: () => element, focus() {focused = this;}};
    return element;
  };
  control('rec').tBodies = [tb];
  const modal = control('dp');
  const c = vm.createContext({
    $: control, recRow: row, clear: node => {node.children = [];},
    keepScroll: (_id, fn) => fn(), steady: (_boxes, fn) => fn(),
    emptyRow() {}, anyF: () => false, renderDetailNav() {}, exact: String, PAGE: 50,
    CSS: {escape: value => value},
    document: {contains: node => tb.contains(node) || [...controls.values()].includes(node)},
    recState: {rows: [{request_id: '0000000000000001'}, {request_id: '0000000000000002'}], total: 2},
  });
  vm.runInContext(section('const MODALS=[];', 'document.addEventListener("keydown"') + '\n' + section('function renderRecords()', 'const PAGE=50;'), c);
  vm.runInContext('renderRecords()', c);
  modal._opener = tb.children[0].button;
  const original = modal._opener;
  c.activeRecord = '0000000000000002'; // Adjacent-detail navigation must not change the return target.
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.notEqual(focused, original, 'the old list button was removed');
  assert.equal(focused, tb.children[0].button, 'closing returns to the original record after its row was rebuilt');

  modal.classList.toggle('hide', false);
  modal._opener = tb.children[0].button;
  c.recState.rows = [{request_id: '0000000000000002'}];
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.equal(focused, control('foutcome'), 'a record that left the list returns to the request filter');

  modal.classList.toggle('hide', false);
  modal._opener = control('recordid');
  vm.runInContext('renderRecords(); closeModal($("dp"))', c);
  assert.equal(focused, control('recordid'), 'ID lookup keeps its own return target');
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
  await sharedAuthResponses();
  inspectorFocusAfterRefresh();
  console.log('PASS: query races/auth, shared scope and partial buckets, ID cursor, late-page isolation, history refresh, scoped drill-down, unknown usage, request-count population, URL restore and inspector focus after refresh');
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => clearTimeout(watchdog));
