// Accounts view regression checks. Run from any directory: node scripts/accounts-check.cjs
// Loads the real ui/js sources (ui.rs manifest order) into a Node vm and drives the accounts model
// (js/views/accounts/model.js, history.js) with fake API functions. Protects what the old
// accounts-state-check protected: partial reads keep the last success with its time, 403/404 clear
// their source, superseded / old-login answers change nothing, the device-code flow ignores late
// answers after stop / logout, switches trust the server's reply, card-use retries reuse the request id,
// windows come from window_minutes (pro: weekly only), and the one-read 30-day history is throttled.
const assert = require('node:assert/strict');
const {manifest, read, vm} = require('./lib/page.cjs');

// Like lib/page.cjs loadScripts, but also exposes the accounts view's own namespaces.
function loadScripts(globals) {
  const list = manifest().js.filter(f => !f.endsWith('/main.js'));
  const ctx = vm.createContext(Object.assign({console, URLSearchParams, URL, AbortController, setTimeout, clearTimeout,
    setInterval, clearInterval, Promise}, globals || {}));
  const names = ['Views', 'Core', 'I18n', 'Api', 'UI', 'Charts', 'Shell', 'AccModel', 'AccHistory', 'AccDetail', 'AccAdd'];
  const trailer = '\n;' + names.map(n => `if (typeof ${n} !== "undefined") globalThis.${n} = ${n};`).join('');
  vm.runInContext(list.map(read).join('\n') + trailer, ctx, {filename: 'ui-bundle.js'});
  return ctx;
}

const tests = [];
const test = (name, fn) => tests.push({name, fn});

function storage() {
  const m = new Map();
  return {getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k)};
}

function sandbox(lang) {
  const env = {localStorage: storage(), sessionStorage: storage(), navigator: {language: 'zh-CN'}, Intl,
    fetch: async () => { throw new Error('no fetch in accounts checks'); }};
  const c = loadScripts(env);
  c.I18n.setLang(lang || 'zh', {persist: false});
  return c;
}

const deferred = () => {
  let resolve, reject;
  const promise = new Promise((a, b) => { resolve = a; reject = b; });
  return {promise, resolve, reject};
};
const fail = status => Object.assign(new Error('synthetic ' + status), {status, body: {}});
const tick = () => new Promise(r => setImmediate(r));

// A store over a scriptable `get`: replies(fn(url) → promise).
function harness(c) {
  let now = 1000, id = 'admin#1', handler = null;
  const calls = [];
  const counts = {change: 0, account: []};
  const deps = {
    get: (url, o) => { calls.push(url); return handler(url, o); },
    post: async () => ({}),
    identity: () => id,
    now: () => now,
    onChange: () => counts.change++,
    onAccount: x => counts.account.push(x),
  };
  const store = c.AccModel.createStore(deps);
  const payload = (url, v) => {
    if (url.startsWith('/admin/v1/accounts')) return {version: v, usage_enabled: true, write_enabled: true, accounts: [{id: 'alice'}]};
    if (url === '/usage/v1/quota') return {accounts: [{account: 'alice', quota: {version: v}}]};
    if (url === '/usage/v1/reset-credits') {
      return {can_consume: true, is_admin: true, attempts: [], auto_available: true, auto_lead_mins: 10,
        accounts: [{account: 'alice', auto: true, quota_auto: false, reset_credits: {version: v, available_count: 1}}]};
    }
    if (url.startsWith('/usage/v1/quota/history')) {
      return {from_ms: 1, to_ms: 2, accounts: [{account: 'alice', cycles: [{window: 'primary'}], events: []}]};
    }
    throw new Error('unexpected ' + url);
  };
  const h = {
    c, store, s: store.state, calls, counts, deps,
    version: v => { handler = async url => payload(url, v); },
    replies: fn => { handler = fn; },
    time: v => { now = v; },
    login: v => { id = v; },
  };
  h.version('v1');
  return h;
}

// ---------- pure helpers ----------
test('windows follow window_minutes: pro has weekly only, swapped slots stay ordered', () => {
  const c = sandbox('zh'), M = c.AccModel;
  const w = (used, mins) => ({used_pct: used, window_minutes: mins});
  const pro = M.wslots({primary: w(67, 10080), secondary: {used_pct: null}});
  assert.equal(pro.s, null);
  assert.equal(pro.l[0], '周');
  const plus = M.wslots({primary: w(62, 300), secondary: w(42, 10080)});
  assert.deepEqual([plus.s[0], plus.l[0]], ['5h', '周']);
  const swapped = M.wslots({primary: w(5, 10080), secondary: w(9, 300)});
  assert.equal(swapped.s[1].used_pct, 9);
  assert.equal(M.wslots({primary: w(17, 43200)}).l[0], '30 天', 'free plan: a 30-day long window');
  assert.deepEqual(JSON.parse(JSON.stringify(M.wslots(null))), {s: null, l: null});
  c.I18n.setLang('en', {persist: false});
  assert.equal(M.wslots({primary: w(67, 10080)}).l[0], 'Weekly');
});

test('remaining percent: unknown stays null, a passed reset reads as full', () => {
  const M = sandbox().AccModel;
  assert.equal(M.rem(null), null);
  assert.equal(M.rem({used_pct: null}), null);
  assert.equal(M.rem({used_pct: 62, resets_at: 2000}, 1000e3), 38);
  assert.equal(M.rem({used_pct: 62, resets_at: 500}, 1000e3), 100);
  assert.equal(M.rem({used_pct: 120}, 0), 0);
  assert.equal(M.staleQuota({observed_at_ms: 1}, 36e5 + 2), true);
  assert.equal(M.staleQuota({observed_at_ms: 1}, 36e5 + 1), false);
});

test('purchased credits label (Codex /status style) and estimated capacity', () => {
  const M = sandbox('zh').AccModel;
  assert.deepEqual(JSON.parse(JSON.stringify(M.crText({has_credits: true, unlimited: false, balance: 812.5}))),
    {text: '点数 812.5', none: false});
  assert.equal(M.crText({has_credits: true, unlimited: true, balance: null}).text, '点数 不限');
  assert.deepEqual(JSON.parse(JSON.stringify(M.crText({has_credits: false, unlimited: false, balance: 0}))), {text: '无点数', none: true});
  assert.equal(M.crText(null), null, 'never reported: nothing shown');
  assert.equal(M.crBal(1204.456), '1204.46');
  assert.equal(M.crBal(null), '?');
  assert.equal(M.capacity({estimate: {capacity_credits: 500, capacity_lo: 480, capacity_hi: 520}}).hi, 520);
  assert.equal(M.capacity({estimate: {capacity_lo: 300}}).lower, true, 'only a lower bound');
  assert.equal(M.capacity({estimate: {status: 'window_reset'}}), null);
});

test('reset cards: the newer source wins; limit_reached yields to a newer observation', () => {
  const M = sandbox().AccModel;
  assert.equal(M.cardCount(undefined), null);
  assert.equal(M.cardCount({available_count: 2, fetched_ms: 10}), 2);
  assert.equal(M.cardCount({available_count: 2, fetched_ms: 10, usage_available_count: 1, usage_observed_ms: 20}), 1);
  assert.equal(M.cardCount({available_count: 2, fetched_ms: 30, usage_available_count: 1, usage_observed_ms: 20}), 2);
  assert.equal(M.cardCount({usage_available_count: 0, usage_observed_ms: 20}), 0);
  assert.equal(M.limitHit({limit_reached: true, usage_observed_ms: 1e6}, {observed_at_ms: 1e6}), true);
  assert.equal(M.limitHit({limit_reached: true, usage_observed_ms: 1e6}, null), true);
  assert.equal(M.limitHit({limit_reached: true, usage_observed_ms: 1e6}, {observed_at_ms: 1e6 + 6e3}), false);
  assert.equal(M.limitHit({limit_reached: false, usage_observed_ms: 1e6}, null), false);
  const {available, used} = M.cardLists({credits: [
    {id: 'b', status: 'available', expires_at: '2030-01-02T00:00:00Z'},
    {id: 'a', status: 'available', expires_at: '2030-01-01T00:00:00Z'},
    {id: 'n', status: 'available', expires_at: null},
    {id: 'x', status: 'redeemed', expires_at: '2030-01-01T00:00:00Z'},
    {id: 'e', status: 'available', expires_at: '2000-01-01T00:00:00Z'}]}, Date.parse('2026-01-01'));
  assert.deepEqual(available.map(x => x.id), ['a', 'b', 'n'], 'soonest first, never-expiring last');
  assert.deepEqual(used.map(x => x.id).sort(), ['e', 'x'], 'used and expired cards are listed apart');
});

test('quota refresh results: skipped is neutral, summaries count what happened', () => {
  const c = sandbox('zh'), M = c.AccModel;
  assert.equal(M.qskip({ok: false, error: {kind: 'rate_limited'}}), true);
  assert.equal(M.qerr({kind: 'rate_limited', retry_after_s: 12}), '刚查询过，12 秒后重试');
  assert.equal(M.qerr({kind: 'http', status: 502, message: 'bad'}), '上游 HTTP 502 · bad');
  assert.equal(M.qerr({kind: 'network', message: 'send: refused'}), '无法连接上游（refused）');
  assert.equal(M.qsummary({ok: 2, results: [{ok: true, cooldown_cleared: true}, {ok: true}, {ok: false, error: {kind: 'rate_limited'}}]}),
    '已刷新 2 个账号的额度，1 个已解除额度冷却，1 个 30 秒内刚查询过，已跳过');
  assert.equal(M.qsummary({ok: 0, results: []}), '没有可刷新的账号');
  c.I18n.setLang('en', {persist: false});
  assert.equal(M.qsummary({ok: 1, skipped: 2, results: []}), 'Refreshed quota of 1 account, 2 skipped (queried within 30 s)');
});

test('last usable account: disabling or deleting it is called out', () => {
  const M = sandbox().AccModel;
  const a = {id: 'a', status: 'normal'}, b = {id: 'b', status: 'cooling'}, d = {id: 'd', status: 'disabled'};
  assert.equal(M.lastOne(a, [a, b, d]), true);
  assert.equal(M.lastOne(a, [a, {id: 'x', status: 'normal'}]), false);
  assert.equal(M.lastOne(b, [b, d]), true, 'no live account: the only usable cooling one');
  assert.equal(M.lastOne(b, [a, b]), false);
});

// ---------- the store ----------
test('partial reads: failures keep the last success and its time; 403 / 404 clear their source', async () => {
  const t = harness(sandbox('zh'));
  await t.store.load();
  assert.equal(t.s.quota.alice.quota.version, 'v1');
  assert.equal(t.s.credits.alice.version, 'v1');
  t.time(2000);
  t.replies(async url => (url.startsWith('/usage/') ? Promise.reject(fail(503)) : ({version: 'v2', accounts: [{id: 'alice'}]})));
  await t.store.load();
  assert.equal(t.s.data.version, 'v2');
  assert.equal(t.s.readAt, 2000);
  assert.equal(t.s.quota.alice.quota.version, 'v1', 'a failed quota read does not become empty data');
  assert.equal(t.s.credits.alice.version, 'v1');
  assert.deepEqual([t.s.reads.quota.note.kind, t.s.reads.quota.note.at], ['fail', 1000], 'note says when the kept data was read');
  assert.equal(t.s.reads.credits.note.kind, 'fail');
  t.time(3000);
  t.replies(async url => (url.endsWith('/reset-credits') ? Promise.reject(fail(503)) : (url === '/usage/v1/quota' ?
    {accounts: [{account: 'alice', quota: {version: 'v3'}}]} : {version: 'v3', accounts: [{id: 'alice'}]})));
  await t.store.load();
  assert.equal(t.s.quota.alice.quota.version, 'v3', 'recovery is per source');
  assert.equal(t.s.reads.quota.note, null);
  assert.equal(t.s.credits.alice.version, 'v1');
  t.replies(async url => (url.endsWith('/quota') ? Promise.reject(fail(403)) : url.endsWith('/reset-credits') ? Promise.reject(fail(404)) :
    {version: 'v4', usage_enabled: true, accounts: [{id: 'alice'}]}));
  await t.store.load();
  assert.equal(Object.keys(t.s.quota).length, 0, '403: no pretending with old data');
  assert.equal(Object.keys(t.s.credits).length, 0);
  assert.deepEqual([t.s.reads.quota.note.kind, t.s.reads.credits.note.kind], ['denied', 'unavailable']);
  assert.equal(t.s.cardTop.can, false, 'no card permission without the listing');
  // first read failing: nothing kept, and the note says so
  const first = harness(sandbox());
  first.replies(async url => (url.startsWith('/usage/') ? Promise.reject(fail(503)) : {accounts: [{id: 'a'}]}));
  await first.store.load();
  assert.equal(first.s.reads.quota.note.at, 0);
  // usage log off: the quota 404 says so
  const off = harness(sandbox());
  off.replies(async url => (url === '/usage/v1/quota' ? Promise.reject(fail(404)) : url.startsWith('/admin') ?
    {usage_enabled: false, accounts: []} : {accounts: []}));
  await off.store.load();
  assert.equal(off.s.reads.quota.note.kind, 'off');
});

test('a failed account list keeps the snapshot and its time; admin_required resets', async () => {
  const c = sandbox();
  const t = harness(c);
  t.version('v5');
  await t.store.load();
  t.time(4000);
  t.replies(async url => (url.startsWith('/admin/') ? Promise.reject(fail(503)) : {accounts: []}));
  await assert.rejects(t.store.load(), /synthetic 503/);
  assert.equal(t.s.data.version, 'v5');
  assert.equal(t.s.quota.alice.quota.version, 'v5');
  assert.equal(t.s.readAt, 1000);
  assert.ok(t.s.error, 'the failure is shown next to the kept data');
  t.replies(async url => (url.startsWith('/admin/') ? Promise.reject(Object.assign(fail(403), {body: {error: {type: 'admin_required'}}})) :
    {accounts: []}));
  assert.equal(await t.store.load(), false);
  assert.equal(t.s.needAdmin, true);
  assert.equal(t.s.data, null);
  t.replies(async url => (url.startsWith('/admin/') ? {version: 'bad'} : {accounts: []}));
  await assert.rejects(t.store.load(), /格式无效/, 'a list without accounts[] is an error, not an empty page');
});

test('superseded loads and old-login answers change nothing', async () => {
  const t = harness(sandbox());
  const old = new Map();
  t.replies(url => { const d = deferred(); old.set(url, d); return d.promise; });
  const pending = t.store.load();
  t.version('new');
  await t.store.load();
  const changes = t.counts.change;
  for (const [url, d] of old) d.reject(url.endsWith('/quota') ? fail(403) : url.endsWith('/reset-credits') ? fail(401) : fail(503));
  assert.equal(await pending, false);
  assert.equal(t.s.data.version, 'new');
  assert.equal(t.s.quota.alice.quota.version, 'new');
  assert.equal(t.s.credits.alice.version, 'new');
  assert.equal(t.s.reads.quota.note, null);
  assert.equal(t.counts.change, changes, 'no render for a superseded answer');
  // an older load finishing late does not clear the newer load's busy state
  const a = new Map(), b = new Map();
  t.replies(url => { const d = deferred(); a.set(url, d); return d.promise; });
  const pa = t.store.load();
  t.replies(url => { const d = deferred(); b.set(url, d); return d.promise; });
  const pb = t.store.load();
  for (const [url, d] of a) d.resolve(url.startsWith('/admin') ? {version: 'old', accounts: []} : {accounts: []});
  await pa;
  assert.equal(t.s.loading, true);
  for (const [url, d] of b) d.resolve(url.startsWith('/admin') ? {version: 'latest', accounts: []} : {accounts: []});
  await pb;
  assert.equal(t.s.loading, false);
  assert.equal(t.s.data.version, 'latest');
  // login change: an answer for the previous identity is dropped, the new identity starts clean
  const late = deferred();
  t.replies(url => (url.startsWith('/admin') ? late.promise : Promise.resolve({accounts: []})));
  const p2 = t.store.load();
  t.login('other#2');
  t.version('other');
  await t.store.load();
  late.resolve({version: 'stale', accounts: [{id: 'old'}]});
  assert.equal(await p2, false);
  assert.equal(t.s.data.version, 'other');
});

test('24 h usage follows the prewarm preference', async () => {
  const t = harness(sandbox());
  await t.store.load({prewarm: false});
  assert.ok(t.calls.includes('/admin/v1/accounts'));
  await t.store.load({prewarm: true});
  assert.ok(t.calls.includes('/admin/v1/accounts?include_prewarm=1'));
});

test('card listing flags and auto switches: a failed listing keeps the last answer', async () => {
  const t = harness(sandbox());
  t.store.readCardTop({status: 'fulfilled', value: {can_consume: true, is_admin: true, attempts: [{account: 'alice', code: 'reset'}]}});
  assert.deepEqual(JSON.parse(JSON.stringify(t.s.cardTop)), {can: true, adm: true, attempts: [{account: 'alice', code: 'reset'}]});
  t.store.readCardTop({status: 'rejected', reason: new Error('x')});
  assert.equal(t.s.cardTop.can, true);
  t.store.readCardTop({status: 'rejected', reason: fail(403)});
  assert.equal(t.s.cardTop.can || t.s.cardTop.adm, false);
  t.store.readAuto({status: 'fulfilled', value: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 5,
    accounts: [{account: 'a', auto: true, auto_next_ms: 5, quota_auto: true}, {account: 'b'}]}});
  assert.deepEqual(JSON.parse(JSON.stringify([t.s.auto, t.s.quotaAuto, t.s.autoTop])),
    [{a: {on: true, next: 5}, b: {on: false, next: null}}, {a: true, b: false}, {lead: 15, ok: true, qmins: 5, qmax: 5}]);
  t.store.readAuto({status: 'rejected', reason: new Error('x')});
  assert.equal(t.s.autoTop.lead, 15);
  t.store.readAuto({status: 'fulfilled', value: {auto_available: true, quota_refresh_mins: 5, quota_refresh_max_mins: 10, accounts: []}});
  assert.deepEqual([t.s.autoTop.qmins, t.s.autoTop.qmax], [5, 10]);
  t.store.readAuto({status: 'fulfilled', value: {auto_available: true, quota_refresh_mins: 0, quota_refresh_max_mins: 10, accounts: []}});
  assert.deepEqual([t.s.autoTop.qmins, t.s.autoTop.qmax], [0, 0], 'interval 0 = manual only');
});

test('switches: the server reply wins, only the row re-renders, a superseded switch leaves no trace', async () => {
  const t = harness(sandbox());
  await t.store.load();
  const posts = [];
  t.deps.post = async (path, body) => { posts.push([path, body]); return {account: 'alice', quota_auto: false}; };
  t.counts.account.length = 0;
  const changes = t.counts.change;
  assert.equal(await t.store.setSwitch({id: 'alice'}, 'quota', true), false);
  assert.equal(t.s.quotaAuto.alice, false);
  assert.deepEqual(JSON.parse(JSON.stringify(posts[0])), ['/admin/v1/accounts/alice/quota-refresh/auto', {enabled: true}]);
  assert.deepEqual(t.counts.account, ['alice']);
  assert.equal(t.counts.change, changes, 'no full re-render');
  const next = deferred();
  t.replies(url => { assert.equal(url, '/usage/v1/reset-credits'); return next.promise; });
  t.deps.post = async () => ({account: 'alice', auto: true});
  assert.equal(await t.store.setSwitch({id: 'alice'}, 'auto', true), true, 'feedback does not wait for the next-time read');
  assert.equal(t.s.auto.alice.on, true);
  next.resolve({accounts: [{account: 'alice', auto_next_ms: 12345}]});
  await tick();
  assert.equal(t.s.auto.alice.next, 12345);
  // a switch answered after logout / re-login (same key) is ignored
  const stale = deferred();
  t.deps.post = () => stale.promise;
  const pending = t.store.setSwitch({id: 'alice'}, 'quota', true);
  t.login('admin#2');
  stale.resolve({account: 'alice', quota_auto: true});
  assert.equal(await pending, undefined);
  assert.equal(t.s.quotaAuto.alice, false);
  // a load that started before a switch does not overwrite it with its older listing
  t.login('admin#1');
  const slow = new Map();
  t.replies(url => { const d = deferred(); slow.set(url, d); return d.promise; });
  const lp = t.store.load();
  t.deps.post = async () => ({account: 'alice', quota_auto: true});
  await t.store.setSwitch({id: 'alice'}, 'quota', true);
  for (const [url, d] of slow) {
    d.resolve(url.startsWith('/admin') ? {accounts: [{id: 'alice'}]} : url.endsWith('/reset-credits') ?
      {accounts: [{account: 'alice', quota_auto: false}], auto_available: true} : {accounts: []});
  }
  await lp;
  assert.equal(t.s.quotaAuto.alice, true);
});

test('optimistic edits roll back only what still shows the optimistic value', async () => {
  const t = harness(sandbox());
  await t.store.load();
  t.s.data.accounts[0].weight = 1;
  const revert = t.store.patchLocal('alice', {weight: 5, disabled: true});
  assert.equal(t.s.data.accounts[0].weight, 5);
  t.s.data.accounts[0].disabled = false; // a newer snapshot already changed this field
  revert();
  assert.equal(t.s.data.accounts[0].weight, 1);
  assert.equal(t.s.data.accounts[0].disabled, false);
});

test('active quota query patches the snapshot; failures stay on the row, "asked moments ago" does not', () => {
  const t = harness(sandbox());
  t.store.applyQuota({ok: true, account: 'a', observed_at_ms: 9, quota: {plan: 'plus', primary: {used_pct: 3}}, usage_available_count: 2,
    limit_reached: true});
  assert.equal(t.s.quota.a.quota.primary.used_pct, 3);
  assert.equal(t.s.quota.a.quota.observed_by, 'manual');
  assert.deepEqual([t.s.credits.a.usage_available_count, t.s.credits.a.usage_observed_ms], [2, 9]);
  t.store.applyQuota({ok: false, account: 'a', error: {kind: 'rate_limited'}});
  assert.equal(t.s.qerr.a, undefined);
  t.store.applyQuota({ok: false, account: 'a', error: {kind: 'timeout'}});
  assert.equal(t.s.qerr.a.kind, 'timeout');
  t.store.applyQuota({ok: true, account: 'a', quota: {}});
  assert.equal(t.s.qerr.a, undefined);
});

test('30-day history: one read for every account, throttled, forced after an action', async () => {
  const t = harness(sandbox());
  await t.store.loadHist(false);
  assert.deepEqual(t.calls, ['/usage/v1/quota/history?days=30']);
  assert.equal(t.store.histOf('alice').cycles.length, 1);
  assert.equal(t.store.histOf('bob').cycles.length, 0, 'accounts without history get an empty row');
  t.time(31000);
  await t.store.loadHist(false);
  assert.equal(t.calls.length, 1, 'no second read within 60 s');
  await t.store.loadHist(true);
  assert.equal(t.calls.length, 2);
  const p1 = t.store.loadHist(true), p2 = t.store.loadHist(true);
  assert.equal(p1, p2, 'a read in flight is shared');
  await p1;
  t.replies(async () => Promise.reject(fail(404)));
  await t.store.loadHist(true);
  assert.equal(t.store.histOf('alice').error.kind, 'off');
  const late = deferred();
  t.replies(() => late.promise);
  const p = t.store.loadHist(true);
  t.login('someone-else');
  late.resolve({accounts: [{account: 'alice', cycles: [1, 2, 3]}]});
  await p;
  assert.equal(t.store.histOf('alice').error.kind, 'off', 'an answer for another login is dropped');
});

// ---------- device-code login ----------
function device(c) {
  let id = 'admin#1', handler = null, now = 0;
  const intervals = new Map();
  let n = 0;
  const done = [];
  const dev = c.AccModel.createDevice({
    request: (method, path, o) => handler(method, path, o),
    identity: () => id,
    now: () => now,
    onDone: a => done.push(a),
    setInterval: fn => { intervals.set(++n, fn); return n; },
    clearInterval: k => intervals.delete(k),
  });
  return {dev, st: dev.state, done, replies: fn => { handler = fn; }, login: v => { id = v; }, time: v => { now = v; },
    tick: () => Promise.all([...intervals.values()].map(fn => fn())), intervals: () => intervals.size};
}
const code = name => ({login_id: name, url: 'https://example.invalid/device', code: name});

test('device code: a start answered after logout never shows its code', async () => {
  const d = device(sandbox()), late = deferred();
  d.replies(() => late.promise);
  const p = d.dev.start();
  assert.equal(d.st.phase, 'starting');
  d.dev.end(); // logout / unmount
  d.login('other#2');
  late.resolve(code('old-code'));
  await p;
  assert.equal(d.st.code, '');
  assert.equal(d.intervals(), 0);
});

test('device code: stop, then start again; late poll answers of the first flow are ignored', async () => {
  const d = device(sandbox());
  d.replies(async () => code('first'));
  await d.dev.start();
  assert.deepEqual([d.st.phase, d.st.code], ['waiting', 'first']);
  const latePoll = deferred();
  d.replies(() => latePoll.promise);
  const pendingPoll = d.tick();
  d.dev.stop();
  assert.equal(d.st.code, '');
  assert.equal(d.st.note.kind, 'stopped', 'stopping says the browser sign-in may still land');
  d.replies(async () => code('second'));
  await d.dev.start();
  latePoll.resolve({status: 'done', account: 'old-account'});
  await pendingPoll;
  assert.equal(d.st.code, 'second');
  assert.deepEqual(d.done, []);
  assert.equal(d.intervals(), 1);
});

test('device code: a stale failure cannot end the newer flow; the current one reports done / expiry', async () => {
  const d = device(sandbox()), a = deferred(), b = deferred();
  d.replies(() => a.promise);
  const pa = d.dev.start();
  d.dev.stop();
  d.replies(() => b.promise);
  const pb = d.dev.start();
  a.reject(Object.assign(new Error('401'), {status: 401}));
  await pa;
  assert.equal(d.st.phase, 'starting', 'the old 401 did not touch the new flow');
  b.resolve(code('newest'));
  await pb;
  assert.equal(d.st.code, 'newest');
  d.replies(async () => ({status: 'pending'}));
  await d.tick();
  assert.equal(d.st.phase, 'waiting');
  d.replies(async () => ({status: 'done', account: 'acc-1'}));
  await d.tick();
  assert.deepEqual([d.st.phase, d.st.note.kind, d.st.note.account], ['idle', 'done', 'acc-1']);
  assert.deepEqual(d.done, ['acc-1']);
  assert.equal(d.intervals(), 0);
  d.replies(async () => code('x'));
  await d.dev.start();
  d.time(15 * 60000 + 1);
  await d.tick();
  assert.equal(d.st.note.kind, 'expired');
  d.replies(async () => Promise.reject(Object.assign(new Error('写入已关闭'), {status: 403, body: {error: {type: 'write_disabled'}}})));
  await d.dev.start();
  assert.equal(d.st.note.kind, 'startFailed');
  assert.match(d.st.note.why, /写入已关闭/, 'a write-disabled 403 is explained, not treated as a bad key');
  d.replies(async () => ({url: 'u'}));
  await d.dev.start();
  assert.equal(d.st.note.kind, 'startFailed', 'an incomplete answer is an error');
});

// ---------- reset card use ----------
test('card use: confirm body, 409 pending retries the same request, unknown results reuse the id', () => {
  const M = sandbox('zh').AccModel;
  assert.deepEqual(JSON.parse(JSON.stringify(M.consumeBody({account: 'alice', credit: null, rid: null}))), {confirm: 'alice'});
  assert.deepEqual(JSON.parse(JSON.stringify(M.consumeBody({account: 'alice', credit: 'c1', rid: 'r1'}))),
    {confirm: 'alice', credit_id: 'c1', redeem_request_id: 'r1'});
  const f = M.consumeFailure({account: 'alice', credit: 'first', rid: null},
    {status: 409, body: {pending: {credit_id: 'pending-card', redeem_request_id: 'pending-id'}}});
  assert.equal(f.retry, 'pending');
  assert.deepEqual(JSON.parse(JSON.stringify(f.next)), {account: 'alice', credit: 'pending-card', rid: 'pending-id'});
  const g = M.consumeFailure({account: 'alice', credit: null, rid: 'r9'}, {status: 502, body: {status: 'pending'}});
  assert.deepEqual([g.retry, g.next.rid], ['same', 'r9']);
  assert.equal(M.consumeFailure({account: 'a'}, {status: 500}).retry, null);
  const now = 1e9;
  assert.equal(M.consumeHint({primary: {used_pct: 50, window_minutes: 300}}, now).kind, 'notLow');
  assert.equal(M.consumeHint({primary: {used_pct: 90, window_minutes: 300, resets_at: now / 1000 + 600}}, now).kind, 'resetsSoon');
  assert.equal(M.consumeHint({primary: {used_pct: 90, window_minutes: 300, resets_at: now / 1000 + 7200}}, now), null);
  assert.equal(M.consumeHint(null, now), null);
});

test('card use, delete, token refresh and estimate reset stay behind confirmations', () => {
  const src = read('ui/js/views/accounts/detail.js');
  assert.match(src, /reset-credits\/consume",\s*M\.consumeBody\(attempt\)/);
  assert.match(src, /if \(consumeCur !== attempt\) return;/, 'a closed dialog cannot write into its replacement');
  assert.match(src, /role: "alertdialog"/);
  assert.match(src, /req\("DELETE", M\.pathOf\(a\), \{confirm: a\.id\}/);
  assert.match(src, /\/refresh", \{confirm: a\.id\}/);
  assert.match(src, /\/quota-estimate\/reset", \{window: choice, confirm: a\.id\}/);
  assert.match(src, /Date\.now\(\) - layer\.at < 400/, 'no double tap through the opener');
});

// ---------- quota history ----------
test('history windows come from the account, not the slot; pro shows weekly only', () => {
  const c = sandbox('zh'), H = c.AccHistory;
  const w = (u, m) => ({used_pct: u, window_minutes: m});
  const pro = H.wins({primary: w(5, 10080), secondary: {used_pct: null}});
  const plus = H.wins({primary: w(5, 300), secondary: w(9, 10080)});
  assert.equal(pro.primary.l, '周');
  assert.equal(pro.secondary, null);
  assert.equal(H.winLabel('secondary', pro), '其他窗口', 'history of a slot the account no longer has is not called 5h');
  assert.equal(H.winLabel('primary', null), '5h', 'no quota yet: classic layout');
  assert.deepEqual(Array.from(H.order(H.wins({primary: w(5, 10080), secondary: w(9, 300)}))), ['secondary', 'primary']);
  assert.equal(H.mainWin(pro, []), 'primary');
  assert.equal(H.mainWin(plus, []), 'secondary');
  assert.deepEqual(Array.from(H.winList(pro, [])), ['primary'], 'pro: no window toggle');
  assert.deepEqual(Array.from(H.winList(plus, [])), ['primary', 'secondary']);
  assert.deepEqual(Array.from(H.winList(null, [{window: 'secondary'}])), ['secondary'], 'no quota: slots with history');
});

test('history scales: robust fence, nice ticks, reset events within 10 minutes, gaps as bridges', () => {
  const H = sandbox().AccHistory;
  assert.equal(H.fence([1, 2, 3]), Infinity);
  assert.ok(H.fence([46, 48, 50, 52, 54, 600]) < 600, 'a far-off cycle is an outlier');
  assert.ok(H.fence([50, 50, 50, 50, 55]) >= 65, 'a flat series keeps ordinary noise in range');
  assert.ok(H.domain([{capacity_credits: 46}, {capacity_credits: 48}, {capacity_credits: 50}, {capacity_credits: 52},
    {capacity_credits: 600}]).max < 600, 'the outlier does not flatten the rest');
  assert.equal(H.tick(146.66), '147');
  assert.equal(H.tick(1500), '1.5k');
  assert.equal(H.pos(50, 0, 100), 0.5);
  assert.equal(H.pos(-10, 0, 100), 0);
  assert.equal(H.pos(250, 0, 100), 1);
  assert.equal(H.eventAt([{window: 'primary', kind: 'early_reset', at_ms: 1e6 + 3e5}, {window: 'secondary', kind: 'natural', at_ms: 1e6}],
    {window: 'primary', ended_ms: 1e6}).kind, 'early_reset');
  assert.equal(H.eventAt([{window: 'primary', kind: 'early_reset', at_ms: 1e6 + 7e5}], {window: 'primary', ended_ms: 1e6}), null);
  assert.equal(H.link(10, 12), 'join');
  assert.equal(H.link(10, 13), 'gap');
  assert.equal(H.cap({capacity_credits: null, capacity_lo: 30}), 30, 'lower bound when no estimate');
  assert.equal(H.rowKind({open: true}, []), 'open');
  assert.equal(H.rowKind({close_kind: 'manual_card', window: 'primary', ended_ms: 5}, []), 'manual_card');
  const now = 1e6;
  const ep = H.epoch({epochs: [{id: 1, active: true, window: 'primary', undo_until_ms: now + 1, created_ms: 1},
    {id: 2, active: true, window: 'primary', undo_until_ms: now + 1, created_ms: 2}, {id: 3, active: false, window: 'primary',
      undo_until_ms: now + 1, created_ms: 3}, {id: 4, active: true, window: 'primary', undo_until_ms: now - 1, created_ms: 4}]},
    'primary', now);
  assert.equal(ep.id, 2, 'the latest active reset that can still be undone');
});

test('accounts strings: both languages, same keys, English plurals', () => {
  const c = sandbox('en');
  const d = c.I18n.dicts ? c.I18n.dicts() : null;
  if (d) {
    const zk = Object.keys(d.zh).filter(k => k.startsWith('accounts.')).sort();
    const ek = Object.keys(d.en).filter(k => k.startsWith('accounts.')).sort();
    assert.deepEqual(zk, ek);
    assert.ok(zk.length > 300);
  }
  assert.equal(c.I18n.t('accounts.cool.title', {n: 1}), '1 account cooling down');
  assert.equal(c.I18n.t('accounts.cool.title', {n: 3}), '3 accounts cooling down');
  c.I18n.setLang('zh', {persist: false});
  assert.equal(c.I18n.t('accounts.cool.title', {n: 3}), '3 个账号冷却中');
});

test('view sources: no style attributes, data as text, every file in the manifest', () => {
  const files = ['ui/js/views/accounts.js', 'ui/js/views/accounts/model.js', 'ui/js/views/accounts/history.js',
    'ui/js/views/accounts/detail.js', 'ui/js/views/accounts/add.js', 'ui/js/views/accounts/i18n.js'];
  const rs = read('ui.rs');
  for (const f of files) {
    const src = read(f);
    assert.ok(rs.includes('include_str!("' + f + '")'), f + ' missing from ui.rs');
    assert.doesNotMatch(src, /innerHTML|outerHTML|insertAdjacentHTML|\bstyle\s*=|setAttribute\("style"/, f);
  }
  const view = read('ui/js/views/accounts.js');
  assert.match(view, /#\?acc=\(\[\^&\]\+\)/, 'old deep link #acc=<id> still opens the account');
  const c = sandbox();
  assert.deepEqual(JSON.parse(JSON.stringify(c.Views.accounts.parseHash('#acc=a%40b.com&act=quota'))), {id: 'a@b.com', act: 'quota'});
  assert.deepEqual(JSON.parse(JSON.stringify(c.Views.accounts.parseHash('#acc=alice'))), {id: 'alice', act: null});
  assert.equal(c.Views.accounts.parseHash('#keys'), null);
});

(async () => {
  let passed = 0;
  const watchdog = setTimeout(() => { console.error('FAIL: accounts checks did not settle within 10 seconds'); process.exit(1); }, 10000);
  for (const {name, fn} of tests) {
    try {
      await fn();
      passed++;
      console.log('ok   ' + name);
    } catch (e) {
      process.exitCode = 1;
      console.log('FAIL ' + name + '\n     ' + String(e.stack || e).split('\n').slice(0, 4).join('\n     '));
    }
  }
  clearTimeout(watchdog);
  console.log('\n' + passed + '/' + tests.length + ' passed');
})();
