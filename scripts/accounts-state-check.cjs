// Partial reads and login/query races against the actual embedded accounts-page functions.
// Run from any directory: node /path/to/ciallopa/scripts/accounts-state-check.cjs
const assert = require('node:assert/strict');
const {loadPage, vm, createContext, createDom} = require('./lib/page.cjs');
const {html, section} = loadPage('accounts');
const source = [
  section('accounts-state'),
  section('accounts-section-72'),
  section('accounts-fail-73'),
  section('accounts-section-74'),
  section('accounts-section-75'),
  section('accounts-section-76'),
  section('accounts-section-77'),
  section('accounts-section-78'),
].join('\n');
const deferred = () => {
  let resolve;
  const promise = new Promise(done => {resolve = done;});
  return {promise, resolve};
};
const response = (body, status = 200) => ({ok: status >= 200 && status < 300, status, json: async () => body});
const failure = status => response({error: {message: `synthetic ${status}`}}, status);
const payload = (url, version) => url.endsWith('/quota')
  ? {accounts: [{account: 'alice', quota: {version}}]}
  : url.endsWith('/reset-credits')
    ? {accounts: [{account: 'alice', reset_credits: {version}}]}
    : {version, usage_enabled: true, accounts: [{id: 'alice'}]};

function harness() {
  let auth = 'demo-admin', now = 1000, handler = async url => response(payload(url, 'v1'));
  const {$: lookup, h} = createDom(), intervals = new Map();
  let nextInterval = 0;
  const node = id => {const e = lookup(id); e.focus = () => {c.focused = id;}; return e;};
  const c = createContext({
    $: node, clear: n => {n.textContent = '';}, h,
    document: {createTextNode: text => ({textContent: text})},
    W: false, T: {timeout: 45000}, QERR: {}, OPEN: new Set(), poll: null, NOADMIN: false,
    key: () => auth, KS: 'test-key',
    sessionStorage: {removeItem() {auth = '';}},
    showApp() {c.logoutCount++; c.onLogout();}, logoutCount: 0,
    needAdmin() {c.adminRequired++; return true;}, adminRequired: 0,
    adminMsg: body => ({admin_required: '管理权限不足', write_disabled: '已关闭写入'})[body?.error?.type] ?? null,
    why: e => e.status ? `HTTP ${e.status} · ${e.message}` : e.message,
    inView: () => true, toast: text => c.toasts.push(text), toasts: [],
    hms: date => `t${date.getTime()}`, fmtTime: time => `t${time}`,
    Date: class extends Date {static now() {return now;}},
    fetch: (...args) => handler(...args), AbortController, setTimeout, clearTimeout,
    setInterval(fn) {const id = ++nextInterval; intervals.set(id, fn); return id;},
    clearInterval: id => intervals.delete(id),
    dur: seconds => `${seconds}s`,
    setTabCount() {}, renderCount: 0,
    render() {c.renderCount++; c.showReadState();},
    loadHistAll() {c.histLoads++;}, histLoads: 0, focused: null, closeModal() {}, matchMedia: () => ({matches: false, addEventListener() {}}),
  });
  vm.runInContext(source, c);
  c.accountsState = vm.runInContext("accountsState", c);
  return {
    c, node,
    run: expression => vm.runInContext(expression, c),
    version(v) {handler = async url => response(payload(url, v));},
    replies(fn) {handler = fn;},
    time(value) {now = value;},
    auth(value) {auth = value;},
    currentAuth: () => auth,
    tick: () => Promise.all([...intervals.values()].map(fn => fn())),
    intervalCount: () => intervals.size,
  };
}

async function partialReads() {
  const t = harness();
  await t.run('load()');
  assert.equal(t.c.accountsState.quota.alice.quota.version, 'v1');
  assert.equal(t.c.accountsState.credits.alice.version, 'v1');

  // A successful account response does not turn failed quota/card reads into empty data.
  t.time(2000);
  t.replies(async url => url.startsWith('/usage/') ? failure(503) : response(payload(url, 'v2')));
  await t.run('load()');
  assert.equal(t.c.accountsState.data.version, 'v2');
  assert.equal(t.c.accountsState.quota.alice.quota.version, 'v1');
  assert.equal(t.c.accountsState.credits.alice.version, 'v1');
  assert.match(t.node('readnote').textContent, /额度读取失败.*保留已有数据.*t1000/);
  assert.match(t.node('readnote').textContent, /重置卡读取失败.*保留已有数据.*t1000/);
  assert.equal(t.node('upd').textContent, '账号更新于 t2000');

  // Recovery is per source; the still-failing card snapshot remains intact.
  t.time(3000);
  t.replies(async url => url.endsWith('/reset-credits') ? failure(503) : response(payload(url, 'v3')));
  await t.run('load()');
  assert.equal(t.c.accountsState.quota.alice.quota.version, 'v3');
  assert.equal(t.c.accountsState.credits.alice.version, 'v1');
  assert.doesNotMatch(t.node('readnote').textContent, /额度读取失败/);
  assert.match(t.node('readnote').textContent, /重置卡读取失败/);

  // Permission/unavailable responses clear only their source instead of retaining authorized data.
  t.replies(async url => url.endsWith('/quota') ? failure(403) : url.endsWith('/reset-credits') ? failure(404) : response(payload(url, 'v4')));
  await t.run('load()');
  assert.equal(Object.keys(t.c.accountsState.quota).length, 0);
  assert.equal(Object.keys(t.c.accountsState.credits).length, 0);
  assert.match(t.node('readnote').textContent, /额度无读取权限（HTTP 403）/);
  assert.match(t.node('readnote').textContent, /重置卡接口不可用（HTTP 404）/);
  assert.doesNotMatch(t.node('readnote').textContent, /保留/);

  const first = harness();
  first.replies(async url => url.startsWith('/usage/') ? failure(503) : response(payload(url, 'first')));
  await first.run('load()');
  assert.match(first.node('readnote').textContent, /暂无成功数据/);
  assert.doesNotMatch(first.node('readnote').textContent, /保留/);

  // Failure of the required account response preserves its previous timestamp and data.
  t.version('v5');
  await t.run('load()');
  const before = t.node('upd').textContent;
  t.time(4000);
  t.replies(async url => url.startsWith('/admin/') ? failure(503) : response(payload(url, 'unused')));
  await assert.rejects(t.run('load()'), /synthetic 503/);
  assert.equal(t.c.accountsState.data.version, 'v5');
  assert.equal(t.c.accountsState.quota.alice.quota.version, 'v5');
  assert.equal(t.node('upd').textContent, before);
}

async function supersededReads() {
  const t = harness(), old = new Map();
  t.replies(url => {const d = deferred(); old.set(url, d); return d.promise;});
  const pending = t.run('load()');
  t.version('new');
  await t.run('load()');
  // Neither an old 403 nor an old 401 may clear the fresh snapshot or the current identity.
  for (const [url, d] of old) d.resolve(url.endsWith('/quota') ? failure(403) : url.endsWith('/reset-credits') ? failure(401) : failure(503));
  assert.equal(await pending, false);
  assert.equal(t.c.accountsState.data.version, 'new');
  assert.equal(t.c.accountsState.quota.alice.quota.version, 'new');
  assert.equal(t.c.accountsState.credits.alice.version, 'new');
  assert.equal(t.c.renderCount, 1);
  assert.equal(t.c.logoutCount, 0);
  assert.equal(t.currentAuth(), 'demo-admin');
  assert.equal(t.node('readnote').textContent, '');

  // A failure that is presented later still checks its load generation.
  t.replies(async url => url.startsWith('/admin/') ? failure(503) : response(payload(url, 'unused')));
  const stale = await t.run('load()').catch(error => error);
  t.version('newer');
  await t.run('load()');
  t.c.stale = stale;
  t.run('fail(stale)');
  assert.equal(t.node('msg').textContent, '');

  // An older load's finally must not clear the newer load's busy state.
  const a = new Map(), b = new Map();
  t.replies(url => {const d = deferred(); a.set(url, d); return d.promise;});
  const pa = t.run('load()');
  t.replies(url => {const d = deferred(); b.set(url, d); return d.promise;});
  const pb = t.run('load()');
  for (const [url, d] of a) d.resolve(response(payload(url, 'old')));
  await pa;
  assert.equal(t.node('tbl')['aria-busy'], 'true');
  for (const [url, d] of b) d.resolve(response(payload(url, 'latest')));
  await pb;
  assert.equal(t.node('tbl')['aria-busy'], 'false');
  assert.equal(t.c.accountsState.data.version, 'latest');
}

async function loginChanges() {
  const t = harness();
  await t.run('load()');
  const old = new Map();
  t.replies(url => {const d = deferred(); old.set(url, d); return d.promise;});
  const pending = t.run('load()');
  t.run('onLogout()');
  assert.equal(t.c.accountsState.data, null);
  assert.equal(Object.keys(t.c.accountsState.quota).length, 0);
  assert.equal(Object.keys(t.c.accountsState.credits).length, 0);
  assert.equal(t.node('upd').textContent, '');
  t.auth('another-admin');
  t.version('other-login');
  await t.run('load()');
  for (const d of old.values()) d.resolve(failure(401));
  await pending;
  assert.equal(t.currentAuth(), 'another-admin');
  assert.equal(t.c.logoutCount, 0);
  assert.equal(t.c.accountsState.data.version, 'other-login');

  // A current 401 retains the normal logout behavior and clears previous identity data.
  t.replies(async () => failure(401));
  assert.equal(await t.run('load()'), false);
  assert.equal(t.currentAuth(), '');
  assert.equal(t.c.logoutCount, 1);
  assert.equal(t.c.accountsState.data, null);
  assert.equal(t.node('tbl')['aria-busy'], 'false');
}

async function deviceLoginIsolation() {
  const code = name => response({login_id: name, url: 'https://example.invalid/device', code: name});

  // A device-code start that finishes after logout must not expose its code in the next login.
  const start = harness(), lateStart = deferred();
  start.replies(() => lateStart.promise);
  const pendingStart = start.node('devbtn').click();
  start.node('imt').value = 'private pasted credentials';
  start.run('onLogout()');
  start.auth('other-admin');
  lateStart.resolve(code('old-code'));
  await pendingStart;
  assert.equal(start.node('devcode').textContent, '', 'late device start leaked across logout');
  assert.equal(start.node('imt').value, '', 'logout retained pasted credentials');
  assert.equal(start.intervalCount(), 0);

  // Stopping, then starting again, keeps late status replies from replacing the new flow.
  const poll = harness(), latePoll = deferred();
  poll.replies(async () => code('first'));
  await poll.node('devbtn').click();
  poll.replies(() => latePoll.promise);
  const pendingPoll = poll.tick();
  poll.node('devx').click();
  assert.equal(poll.node('devcode').textContent, '');
  assert.equal(poll.node('devurl').href, undefined);
  poll.replies(async () => code('second'));
  await poll.node('devbtn').click();
  latePoll.resolve(response({status: 'done', account: 'old-account'}));
  await pendingPoll;
  assert.equal(poll.node('devcode').textContent, 'second');
  assert.doesNotMatch(poll.node('devst').textContent, /old-account/);
  assert.equal(poll.intervalCount(), 1);
  assert.equal(poll.c.renderCount, 0);

  // Even a late 401 from a stopped start must not log out or unlock the newer flow.
  const newer = harness(), a = deferred(), b = deferred();
  newer.replies(() => a.promise);
  const pa = newer.node('devbtn').click();
  newer.node('devx').click();
  newer.replies(() => b.promise);
  const pb = newer.node('devbtn').click();
  a.resolve(failure(401));
  await pa;
  assert.equal(newer.node('devbtn')['aria-disabled'], 'true');
  assert.equal(newer.currentAuth(), 'demo-admin');
  assert.equal(newer.c.logoutCount, 0);
  assert.deepEqual(newer.c.toasts, []);
  b.resolve(code('newest'));
  await pb;
  assert.equal(newer.node('devcode').textContent, 'newest');
  newer.replies(async () => failure(401));
  await newer.tick();
  assert.equal(newer.currentAuth(), '');
  assert.equal(newer.c.logoutCount, 1);
  assert.equal(newer.node('devcode').textContent, '');
  assert.equal(newer.intervalCount(), 0);

  // A localized 403 can describe disabled writes without revoking this key's admin status.
  const permission = harness();
  permission.replies(async () => response({error: {type: 'write_disabled'}}, 403));
  await permission.node('devbtn').click();
  assert.equal(permission.c.NOADMIN, false);
  assert.match(permission.node('devst').textContent, /已关闭写入/);
  permission.replies(async () => response({error: {type: 'admin_required'}}, 403));
  await permission.node('devbtn').click();
  assert.equal(permission.c.NOADMIN, true);
}

// Behind Cloudflare Access: an expired login (redirect to the sign-in page, an HTML page, or an
// Access 401/403) is reported as such; the key stays, nobody is logged out.
async function accessGate() {
  const h = harness(), inits = [];
  const headers = map => ({get: n => map[n.toLowerCase()] ?? null, keys: () => Object.keys(map)[Symbol.iterator]()});
  const replies = [
    {type: 'opaqueredirect', status: 0, ok: false, headers: headers({}), json: async () => {throw new Error('opaque');}},
    {type: 'basic', status: 200, ok: true, headers: headers({'content-type': 'text/html; charset=utf-8'}), json: async () => {throw new Error('html');}},
    {type: 'basic', status: 403, ok: false, headers: headers({'content-type': 'application/json', 'cf-access-domain': 'x'}), json: async () => ({})},
  ];
  for (const reply of replies) {
    h.replies(async (url, init) => {inits.push(init); return reply;});
    await assert.rejects(h.run('load()'), e => e.kind === 'access' && e.message === 'Cloudflare Access 登录已过期，请刷新页面重新登录');
    assert.equal(h.currentAuth(), 'demo-admin');
    assert.equal(h.c.logoutCount, 0);
  }
  for (const init of inits) {
    assert.equal(init.credentials, 'same-origin');
    assert.equal(init.redirect, 'manual');
    assert.equal(init.referrerPolicy, 'no-referrer');
  }
  // ciallopa's own 401 still means a bad key
  h.replies(async () => ({type: 'basic', status: 401, ok: false, headers: headers({'content-type': 'text/plain; charset=utf-8'}), json: async () => ({error: 'invalid api key'})}));
  await h.run('load()').catch(() => {});
  assert.equal(h.c.logoutCount, 1);
  assert.equal(h.currentAuth(), '');
}

const watchdog = setTimeout(() => {
  console.error('FAIL: accounts checks did not settle within 5 seconds');
  process.exitCode = 1;
}, 5000);

function resetCardDisplay() {
  const {run} = harness();
  assert.equal(run('cardCount(undefined)'), null);
  assert.equal(run('cardCount({available_count: 2, fetched_ms: 10})'), 2);
  assert.equal(run('cardCount({available_count: 2, fetched_ms: 10, usage_available_count: 1, usage_observed_ms: 20})'), 1);
  assert.equal(run('cardCount({available_count: 2, fetched_ms: 30, usage_available_count: 1, usage_observed_ms: 20})'), 2);
  assert.equal(run('cardCount({usage_available_count: 0, usage_observed_ms: 20})'), 0);
  assert.equal(run('limitHit({limit_reached: true, usage_observed_ms: 1e6}, {observed_at_ms: 1e6})'), true);
  assert.equal(run('limitHit({limit_reached: true, usage_observed_ms: 1e6}, null)'), true);
  assert.equal(run('limitHit({limit_reached: true, usage_observed_ms: 1e6}, {observed_at_ms: 1e6 + 6e3})'), false, 'a newer quota observation wins');
  assert.equal(run('limitHit({limit_reached: false, usage_observed_ms: 1e6}, null)'), false);
  // 快到期时自动使用 + 自动刷新额度: switches per account; a failed listing keeps the last answer
  run('accountsState.auto = {}; accountsState.quotaAuto = {}; accountsState.autoTop = null; readAuto({status: "fulfilled", value: {j: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 5, accounts: [{account: "a", auto: true, auto_next_ms: 5, quota_auto: true}, {account: "b", quota_auto: false}]}}})');
  assert.equal(run('JSON.stringify([accountsState.auto, accountsState.quotaAuto, accountsState.autoTop])'), JSON.stringify([{a: {on: true, next: 5}, b: {on: false, next: null}}, {a: true, b: false}, {lead: 15, ok: true, qmins: 5, qmax: 5}]));
  run('readAuto({status: "rejected", reason: new Error("x")})');
  assert.equal(run('accountsState.auto.a.on && accountsState.quotaAuto.a && accountsState.autoTop.lead === 15 && accountsState.autoTop.qmins'), 5);
  // a refresh range (5–10 min); an old single value reads as fixed (max = min); off = 0
  run('readAuto({status: "fulfilled", value: {j: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 5, quota_refresh_max_mins: 10, accounts: [{account: "a", quota_auto: true}]}}})');
  assert.equal(run('JSON.stringify([accountsState.autoTop.qmins, accountsState.autoTop.qmax])'), '[5,10]');
  run('readAuto({status: "fulfilled", value: {j: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 0, quota_refresh_max_mins: 10, accounts: []}}})');
  assert.equal(run('JSON.stringify([accountsState.autoTop.qmins, accountsState.autoTop.qmax])'), '[0,0]');
}

// 24 h account usage follows the overview's 「含预热」 preference (business only by default).
async function prewarmFollowsOverview() {
  const t = harness(), urls = [];
  t.replies(async url => {urls.push(url); return response(payload(url, 'v1'));});
  await t.run('load()');
  assert.ok(urls.includes('/admin/v1/accounts'), 'no localStorage: business only');
  t.c.localStorage = {getItem: k => k === 'ciallopa_prewarm' ? '1' : null};
  await t.run('load()');
  assert.ok(urls.includes('/admin/v1/accounts?include_prewarm=1'), 'toggle on: prewarm tokens included');
  t.c.localStorage = {getItem: () => '0'};
  urls.length = 0;
  await t.run('load()');
  assert.ok(urls.includes('/admin/v1/accounts'));
}


// One history read for every account (no account parameter), throttled; card listing flags kept.
async function historyAndCards() {
  const t = harness();
  await t.run('load()');
  assert.equal(t.c.histLoads, 1, 'load() also refreshes the 30-day history (throttled inside)');
  t.run('readCardTop({status: "fulfilled", value: {j: {can_consume: true, is_admin: true, attempts: [{account: "alice", code: "reset"}]}}})');
  assert.equal(t.run('JSON.stringify(accountsState.cardTop)'), JSON.stringify({can: true, adm: true, attempts: [{account: 'alice', code: 'reset'}]}));
  t.run('readCardTop({status: "rejected", reason: new Error("x")})');
  assert.equal(t.run('accountsState.cardTop.can'), true, 'a failed read keeps the last answer');
  t.run('readCardTop({status: "rejected", reason: Object.assign(new Error("x"), {status: 403})})');
  assert.equal(t.run('accountsState.cardTop.can || accountsState.cardTop.adm'), false, 'no permission: no card actions');
  let now = 1000;const paths = [];
  const c = createContext({key: () => 'k', accountsState: {data: {accounts: []}}, renders: 0, render() {c.renders++;}, why: e => e.message,
    Date: class extends Date {static now() {return now;}},
    req: async (method, p) => {paths.push(method + ' ' + p); return {j: {from_ms: 1, to_ms: 2, accounts: [{account: 'alice', events: [], cycles: [{window: 'primary'}]}]}};}});
  vm.runInContext(section('accounts-QHA-79'), c);
  await vm.runInContext('loadHistAll(false)', c);
  assert.deepEqual(paths, ['GET /usage/v1/quota/history?days=30']);
  assert.equal(vm.runInContext('qhOf("alice").cycles.length', c), 1);
  assert.equal(vm.runInContext('qhOf("bob").cycles.length', c), 0, 'accounts without history get an empty row');
  now += 30e3;await vm.runInContext('loadHistAll(false)', c);
  assert.equal(paths.length, 1, 'no second read within 60 s');
  await vm.runInContext('loadHistAll(true)', c);
  assert.equal(paths.length, 2, 'forced after an action');
  // Manual card use stays behind the confirm dialog, with the pending-retry path.
  assert.match(html, /reset-credits\/consume",Object\.assign\(\{confirm:acct\}/);
  assert.match(html, /e\.status===409&&e\.body&&e\.body\.pending/);
  assert.match(html, /<div id="cp" class="modal hide" role="alertdialog"/);
}

// 添加账号 is a real disclosure toggle; Esc and 收起 close it; every panel state shows exactly one solid button.
function addPanel() {
  const t = harness();
  const btn = t.node('addbtn'), add = t.node('add');
  const isOpen = () => !add.classList.contains('hide');
  t.run('W=true');
  btn.click();
  assert.ok(isOpen());
  assert.equal(btn['aria-expanded'], 'true');
  assert.equal(t.c.focused, 'addbtn', 'toggle keeps focus so Tab reaches 收起 next');
  btn.click();
  assert.ok(!isOpen(), 'clicking 添加账号 again collapses the panel');
  assert.equal(btn['aria-expanded'], 'false');
  btn.click();
  t.c.focused = 'imt';
  let prevented = false;
  const esc = extra => ({key: 'Escape', defaultPrevented: false, preventDefault() {prevented = true;}, ...extra});
  add.fire('keydown', esc({defaultPrevented: true}));
  assert.ok(isOpen(), 'an Escape already handled inside the panel is left alone');
  add.fire('keydown', {key: 'Enter', defaultPrevented: false, preventDefault() {prevented = true;}});
  assert.ok(isOpen() && !prevented);
  add.fire('keydown', esc());
  assert.ok(!isOpen() && prevented, 'Escape closes the panel');
  assert.equal(btn['aria-expanded'], 'false');
  assert.equal(t.c.focused, 'addbtn', 'Escape returns focus to 添加账号');
  prevented = false;
  btn.fire('keydown', esc());
  assert.ok(!prevented, 'Escape with the panel closed does nothing');
  btn.click();
  t.node('addclose').click();
  assert.ok(!isOpen());
  assert.equal(t.c.focused, 'addbtn');
  t.run('setAdd(true,true)');
  assert.equal(t.c.focused, 'devbtn', 'the empty-state button jumps straight to 开始设备码登录');
  t.run('W=false;setAdd(false)');
  btn.click();
  assert.ok(!isOpen(), 'cannot open without write access');

  // one solid button per state: toolbar 添加账号 while closed; 开始设备码登录 while open (toolbar steps back)
  const tb = html.match(/<div class="sh"><h2>全部账号<\/h2>[\s\S]*?<\/div>\n/)[0];
  assert.equal((tb.match(/class="pri/g) || []).length, 1);
  assert.match(tb, /<button id="addbtn" class="pri xs hide" type="button" aria-expanded="false" aria-controls="add">添加账号<span class="car" aria-hidden="true"><\/span><\/button>/);
  const panel = html.slice(html.indexOf('<div id="add" class="hide">'), html.indexOf('<div id="readnote"'));
  assert.equal((panel.match(/class="pri"/g) || []).length, 1);
  assert.match(panel, /<button id="devbtn" class="pri" type="button">开始设备码登录<\/button>/);
  assert.match(panel, /<button type="submit">导入<\/button>/, 'paste 导入 is a default bordered button');
  assert.match(panel, /<button id="devx" class="xs" type="button">停止等待<\/button>/);
  assert.match(html, /h\("button",\{id:"emptyadd",type:"button",/, 'empty-state 添加账号 is bordered; the toolbar one stays primary');
  assert.match(html, /#addbtn\[aria-expanded=true\]\{background:var\(--surface-2\);border-color:var\(--fg\);color:var\(--fg\);box-shadow:inset 0 0 0 1px var\(--fg\)\}/, 'open: pressed, not solid');
  assert.match(html, /#addbtn \.car\{display:none\}#addbtn\[aria-expanded=true\] \.car\{display:inline\}#addbtn \.car:before\{content:"\\25B4"/, '▴ only while open');
}

(async () => {

async function quotaHistoryRender() {
  function h(tag, a, ...kids) {
    const e = {tag, className: '', style: {}, children: [], attrs: {}};
    e.appendChild = c => { e.children.push(c); return c; };
    e.setAttribute = (k, v) => { e.attrs[k] = v; };
    e.on = {};
    e.addEventListener = (t, f) => { e.on[t] = f; };
    Object.defineProperty(e, 'firstChild', {get() { return e.children[0] || null; }});
    e.removeChild = c => { e.children.splice(e.children.indexOf(c), 1); };
    e.contains = () => false;
    e.after = n => { e._after = n; };
    Object.defineProperty(e, 'textContent', {get() { return e.text || ''; }, set(v) { e.text = String(v); }});
    if (typeof a === 'string') e.className = a;
    else if (a) {
      for (const k of Object.keys(a)) {
        const v = a[k];
        if (v == null || v === false) continue;
        if (k === 'class') e.className = v;
        else if (k === 'text') e.text = String(v);
        else if (k === 'style') e.attrs.style = String(v);
        else e.attrs[k] = v === true ? '' : String(v);
      }
    }
    for (const k of kids) if (k != null && k !== false) e.children.push(k);
    return e;
  }
  function walk(e, acc = []) {
    if (e == null) return acc;
    if (Array.isArray(e)) { for (const k of e) walk(k, acc); return acc; }
    if (typeof e === 'string' || typeof e === 'number') { acc.push(String(e)); return acc; }
    if (e.text) acc.push(e.text);
    for (const k of e.children || []) walk(k, acc);
    return acc;
  }
  function find(e, pred, out = []) {
    if (!e || typeof e !== 'object') return out;
    if (Array.isArray(e)) { for (const k of e) find(k, pred, out); return out; }
    if (pred(e)) out.push(e);
    for (const k of e.children || []) find(k, pred, out);
    return out;
  }
  function makeEl(tag) {
    const e = {tag, className: '', style: {}, children: [], attrs: {}};
    e.appendChild = c => { e.children.push(c); return c; };
    e.setAttribute = (k, v) => { e.attrs[k] = String(v); if (k === 'class') e.className = String(v); };
    e.getAttribute = k => e.attrs[k];
    Object.defineProperty(e, 'textContent', {
      get() { return e.text || ''; },
      set(v) { e.text = String(v); },
    });
    return e;
  }
  const document = {
    createElementNS: (_ns, tag) => makeEl(tag),
    addEventListener() {},
    createTextNode: s => String(s),
  };
  const store = {};
  const c = createContext({
    fmtN: n => String(Math.round(Number(n))),
    fmtTime: (ms) => 't' + ms,
    fmtUsd: n => n == null ? '–' : '$' + Number(n).toFixed(2),
    h,
    document,
    clear: e => { while (e.firstChild) e.removeChild(e.firstChild); },
    sessionStorage: {getItem: k => store[k] ?? null, setItem: (k, v) => { store[k] = String(v); }},
  });
  vm.runInContext(section('accounts-wlab-80') + section('accounts-qhKindLabel-81'), c);
  assert.equal(c.qhKindLabel('early_reset'), '提前重置');
  assert.equal(c.qhKindLabel('manual_card'), '手动卡');
  assert.equal(c.qhKindLabel('auto_card'), '自动用卡');
  assert.equal(c.qhKindLabel('external_card'), '外部用卡');
  assert.equal(c.qhKindLabel('natural'), '自然重置');
  assert.equal(c.qhKindLabel('limit_change'), '额度变化');
  assert.equal(c.qhWinLabel('secondary'), '周');
  assert.equal(c.qhPos(50, 0, 100), 0.5);
  assert.equal(c.qhPos(-10, 0, 100), 0);
  assert.equal(c.qhPos(250, 0, 100), 1);
  const day = 86_400_000;
  const from = 1_791_776_716_136, to = from + 30 * day;
  assert.ok(Math.abs(c.qhPos(from + 3 * day, from, to) - 0.1) < 1e-9);
  assert.equal(c.qhEventAt([{window: 'primary', kind: 'early_reset', at_ms: 1e6 + 3e5}, {window: 'secondary', kind: 'natural', at_ms: 1e6}], {window: 'primary', ended_ms: 1e6}).kind, 'early_reset');
  assert.equal(c.qhEventAt([{window: 'primary', kind: 'early_reset', at_ms: 1e6 + 7e5}], {window: 'primary', ended_ms: 1e6}), null, 'more than 10 min apart: not this reset');
  const cycles = [
    {window: 'primary', started_ms: from + 1 * day, ended_ms: from + 3 * day, capacity_credits: 50, capacity_lo: 40, capacity_hi: 60, api_cost_usd: 1.44, close_kind: 'natural', confidence: 'ok', coverage_gap: false},
    {window: 'primary', started_ms: from + 3 * day, ended_ms: from + 15 * day, capacity_credits: 80, capacity_lo: 70, capacity_hi: 90, api_cost_usd: 2, close_kind: 'manual_card', confidence: 'fallback', coverage_gap: true, trend: 'up'},
  ];
  const ev = [
    {window: 'primary', kind: 'natural', at_ms: from + 3 * day},
    {window: 'primary', kind: 'manual_card', at_ms: from + 15 * day},
    {window: 'secondary', kind: 'early_reset', at_ms: from + 9 * day},
  ];
  const all = n => find(n, () => true);
  // Gaps: no observations between two cycles → one dashed bridge from the last known value to the next;
  // a cycle without an estimate is skipped (no invented value) and named in the next point's popover.
  {
    const H = 3600e3;
    const gc = [
      {window: 'primary', started_ms: from + 2 * day, ended_ms: from + 2 * day + 3 * H, capacity_credits: 50, close_kind: 'natural', confidence: 'ok'},
      {window: 'primary', started_ms: from + 5 * day, ended_ms: from + 5 * day + 3 * H, capacity_credits: 40, close_kind: 'natural', confidence: 'ok'},
      {window: 'primary', started_ms: from + 5 * day + 3 * H, ended_ms: from + 5 * day + 8 * H, capacity_credits: 45, close_kind: 'natural', confidence: 'ok'},
      {window: 'primary', started_ms: from + 8 * day, ended_ms: from + 8 * day + 4 * H, capacity_credits: null, capacity_lo: null, close_kind: 'natural', confidence: 'insufficient'},
      {window: 'primary', started_ms: from + 9 * day, ended_ms: from + 9 * day + 5 * H, capacity_credits: 55, close_kind: 'natural', confidence: 'ok'},
    ];
    const mg = c.qhMini({cycles: gc.map(x => Object.assign({}, x, {window: 'secondary'})), events: []}, from, to, 320);
    assert.equal(find(mg, n => n.className === 'qh-step gap').length, 1, 'row sparkline: one dashed path for the bridges');
    assert.equal((find(mg, n => n.className === 'qh-step gap')[0].attrs.d.match(/M/g) || []).length, 2);
    assert.equal((find(mg, n => n.className === 'qh-step')[0].attrs.d.match(/M/g) || []).length, 3, 'solid path restarts after each bridge');
  }
  assert.match(html, /\.qh-svg \.qh-step\.gap\{stroke-width:1\.2;stroke-dasharray:3 3;opacity:\.55\}/);
  // Robust y-domain: far-off cycles no longer flatten the rest; they are pinned to the top edge with a
  // triangle that carries the real value. The axis keeps 0 (credits are ratio data), so low values show as-is.
  assert.equal(c.qhFence([1, 2, 3]), Infinity, 'fewer than 4 values: no outliers');
  assert.ok(c.qhFence([46, 48, 50, 52, 54, 600]) < 600);
  assert.ok(c.qhFence([50, 50, 50, 50, 55]) >= 65, 'IQR floor: a flat series keeps ordinary noise in range');
  assert.equal(c.qhTick(146.66), '147');
  const vals = [52, 48, 55, 50, 47, 53, 600, 51, 49, 3, 54, 50, 46, 52];
  const outl = vals.map((v, i) => ({window: 'primary', started_ms: from + (2 * i + 0.4) * day, ended_ms: from + (2 * i + 2) * day,
    capacity_credits: v, capacity_lo: v * 0.85, capacity_hi: v * 1.15, api_cost_usd: v * 0.06, close_kind: 'natural', confidence: 'ok',
    trend: v > 100 ? 'up' : v < 10 ? 'down' : null}));
  // Windows come from upstream window_minutes, not the slot: pro sends its only (weekly) window as
  // primary, so primary history is labelled 周 and there is no 5h lane, chart or table label.
  const w = (pct, m) => ({used_pct: pct, window_minutes: m});
  const proQ = {primary: w(30, 10080), secondary: {used_pct: null, window_minutes: null}};
  const plusQ = {primary: w(60, 300), secondary: w(20, 10080)};
  const pro = c.qhWins(proQ), plus = c.qhWins(plusQ);
  assert.equal(pro.primary.l, '周');assert.equal(pro.secondary, null);
  assert.equal(plus.primary.l, '5h');assert.equal(plus.secondary.l, '周');
  assert.equal(c.qhWins(null), null, 'no quota yet: classic layout');
  assert.equal(c.qhWinLabel('primary', pro), '周');
  assert.equal(c.qhWinLabel('secondary', pro), '其他窗口', 'history from a slot the account no longer has is not called 5h');
  assert.equal(c.qhWinLabel('primary', null), '5h');
  // a slot order that puts the short window first even when upstream swaps them
  assert.deepEqual(Array.from(c.qhOrder(c.qhWins({primary: w(5, 10080), secondary: w(9, 300)}))), ['secondary', 'primary']);
  const weekly = [0, 1, 2, 3].map(i => ({window: 'primary', started_ms: from + 7 * i * day, ended_ms: from + 7 * (i + 1) * day, capacity_credits: 2000 + 100 * i, close_kind: 'natural', confidence: 'ok'}));
  // Row sparkline: the main (long) window only, no 5h lane.
  assert.equal(c.qhMainWin(pro, weekly), 'primary');assert.equal(c.qhMainWin(plus, []), 'secondary');assert.equal(c.qhMainWin(null, cycles), 'primary');
  assert.equal(c.qhMainWin(c.qhWins({primary: w(5, 10080), secondary: w(9, 300)}), []), 'primary', 'swapped slots: still the weekly one');
  const proMini = c.qhMini({cycles: weekly, events: []}, from, to, 320, pro);
  assert.deepEqual(find(proMini, n => n.className === 'qh-ml').map(n => n.text), ['周']);
  assert.equal(proMini.attrs.viewBox, '0 0 320 22');
  assert.deepEqual(find(proMini, n => n.className === 'qh-pt').map(n => n.attrs.r), ['2.2', '2.2', '2.2', '2.2'], 'one hollow dot per closed weekly cycle');
  const plusCycles = outl.concat(weekly.map(x => Object.assign({}, x, {window: 'secondary'})));
  const plusMini = c.qhMini({cycles: plusCycles, events: []}, from, to, 320, plus);
  assert.deepEqual(find(plusMini, n => n.className === 'qh-ml').map(n => n.text), ['周'], 'plus sparkline: 周 only, 5h is not in the row');
  assert.equal(find(plusMini, n => n.className === 'qh-step').length, 1);
  const miniO = c.qhMini({cycles: outl.map(x => Object.assign({}, x, {window: 'secondary'})), events: []}, from, to, 320);
  const mo = find(miniO, n => n.className === 'qh-over');
  assert.equal(mo.length, 1, 'row sparkline marks the same overflow');
  assert.equal(mo[0].children.length, 0, 'no title clutter on the sparkline');
  const miniY = find(miniO, n => n.className === 'qh-step')[0].attrs.d.match(/,([\d.]+)/g).map(t => Number(t.slice(1)));
  assert.ok(Math.max(...miniY) - Math.min(...miniY.filter(y => y > 1.5)) > 2, 'sparkline inliers are not flattened');
  assert.equal(c.qhTick(1500), '1.5k');
  // Cycle rows (one bullet bar per cycle, newest first, one shared credits scale); every number is
  // beside the bar or in the row's details, nothing tooltip-only.
  Object.assign(c, {W: true, fullDate: ms => 'd' + ms, nm: a => a.id, path: a => '/admin/v1/accounts/' + a.id, posts: [],
    apost: async (p, b) => { c.posts.push([p, b]); return {epoch_id: 77}; }, busyRun: (b, n, f) => f(),
    ok: (m, o) => { c.okMsg = m; c.okUndo = o && o.undo; }, toast() {}, loadHistAll: async () => {}, load: async () => {},
    confirmDlg: (title, nodes, yes, run) => { c.dlg = {title, nodes, yes, run}; }});
  const pc = [
    {window: 'primary', started_ms: from + day, ended_ms: from + 8 * day, capacity_credits: 2000, capacity_lo: 1800, capacity_hi: 2200, credits_used: 1980, delta_pct: 99, api_cost_usd: 50, close_kind: 'natural', confidence: 'ok', snapshots: 20, requests: 100},
    {window: 'primary', started_ms: from + 8 * day, ended_ms: from + 15 * day, capacity_credits: 1500, capacity_lo: 1480, capacity_hi: 1520, credits_used: 1500, delta_pct: 100, api_cost_usd: 70, over_credits: 300, over_api_usd: 12, close_kind: 'manual_card', confidence: 'ok'},
    {window: 'primary', started_ms: from + 15 * day, ended_ms: from + 16 * day, cycle_reset_at: (from + 22 * day) / 1000, open: true, capacity_credits: null, credits_used: 120, delta_pct: 4, api_cost_usd: 3, close_kind: 'open', confidence: 'insufficient'},
  ];
  const pdat = extra => Object.assign({from_ms: from, to_ms: to, events: [], cycles: pc, epochs: []}, extra || {});
  const proPanel = c.histPanel(pdat(), pro, 'b|x', {id: 'b'});
  assert.equal(find(proPanel, n => n.className === 'seg').length, 0, 'weekly-only: no toggle');
  assert.ok(walk(proPanel).join(' ').includes('额度周期 · 30 天 · 周'));
  assert.ok(!/5h/.test(walk(proPanel).join(' ')), 'no 5h anywhere in a pro account panel');
  const rowsOf = p => find(p, n => /^cy\b/.test(n.className));
  const pr = rowsOf(proPanel);
  assert.equal(pr.length, 3, 'one row per cycle');
  assert.ok(walk(pr[0]).join(' ').includes('进行中'), 'newest (open) cycle first');
  assert.ok(walk(pr[0]).join('').includes('/ 估算中'), 'open cycle without a capacity says 估算中, not 0');
  assert.match(pr[0].children[1].children[0].className, /\bopen\b/, 'open cycle: dashed box');
  const r1 = walk(pr[1]).join('');
  assert.ok(r1.includes('1500 / 1500'), r1);
  const nums1 = walk(pr[1].children[2]).join('');
  assert.ok(nums1.endsWith('$70.00') && !nums1.includes('点数'), 'right side: 额度 used / capacity and 额度 API price only: ' + nums1);
  const part = (r, cls) => find(r, n => n.className === cls);
  assert.equal(find(pr[1], n => /\bover\b/.test(n.className || '')).length, 0, '点数 is never drawn on the 额度 bar');
  const chip = part(pr[1], 'cpts');
  assert.equal(chip.length, 1);assert.equal(walk(chip[0]).join(''), '点数 300 · $12.00', '点数 amount and its own API price, in its own chip');
  assert.notEqual(chip[0].parent || null, pr[1].children[1].children[0], 'chip is not inside the bar');
  assert.equal(part(pr[2], 'cpts').length, 0, 'no 点数, no chip');
  assert.ok(parseFloat(part(pr[2], 'capbox')[0].style.width) > parseFloat(part(pr[1], 'capbox')[0].style.width), 'shared scale: bigger capacity, longer box');
  assert.equal(part(pr[1], 'rng').length, 1);assert.equal(part(pr[1], 'tick').length, 0, 'the blue block itself is the estimate');
  assert.deepEqual(pr[1].children[1].children.map(n => n.className), ['cbar', 'cpts']);
  assert.deepEqual(pr[1].children[1].children[0].children.map(n => n.className), ['capbox', 'rng', 'used'], 'bar: capacity, range, used on top; nothing else');
  assert.ok(parseFloat(part(pr[1], 'used')[0].style.width) <= parseFloat(part(pr[1], 'capbox')[0].style.width) + 1e-9, 'used inside the capacity block');
  assert.equal(part(pr[0], 'capbox').length, 1, 'open cycle without estimate: dashed outline at the usual size');
  assert.ok(all(proPanel).every(n => !n.attrs || n.attrs.style === undefined), 'geometry via CSSOM, no style attribute (CSP)');
  assert.deepEqual([pr[1].attrs.role, pr[1].attrs.tabindex, pr[1].attrs['aria-expanded']], ['button', '0', 'false']);
  pr[1].on.click();
  assert.equal(pr[1].attrs['aria-expanded'], 'true');assert.equal(pr[1]._after.className, 'cdet', 'click opens the details under the row');
  pr[1].on.click();assert.equal(pr[1].attrs['aria-expanded'], 'false');
  const leg = find(proPanel, n => n.className === 'qh-leg')[0];
  assert.deepEqual(leg.children.map(x => walk(x).join('')), ['估算容量', '估算范围', '已用额度', '估算中', '点数（单列，不计入额度）']);
  const dt = walk(c.qhDetail(pc[1], 'manual_card', [])).join('|');
  for (const x of ['额度', '容量', '1500 credits', '范围', '1480–1520', '本周期已用额度', '额度 API 价格', '$70.00', '不计入上面的已用、容量和 API 价格', '点数|300', '点数 API 价格|$12.00', '周期', '可信度', '正常', '结束', '手动卡']) assert.ok(dt.includes(x), x);
  const dt2 = walk(c.qhDetail(pc[2], 'natural', [])).join('|');
  assert.ok(!dt2.includes('点数') && dt2.includes('API 价格'), 'no 点数 group when none was used');
  assert.match(walk(c.qhDetail(Object.assign({}, pc[0], {coverage_gap: true, confidence: 'fallback'}), 'natural', [])).join('|'), /低 · 可能有其他端用量/);
  assert.match(walk(c.qhDetail(Object.assign({}, pc[0], {archived: true}), 'natural', [])).join('|'), /不参与趋势比较/);
  // Rows written before v0.5.4: limit-change prefixes and a second close of the same cycle share
  // its started_ms. One row per start: the open cycle, else the first reset that closed it.
  const S0 = from + 9 * day, card = {window: 'primary', started_ms: from + 2 * day, ended_ms: S0, capacity_credits: 841, credits_used: 825, delta_pct: 94, close_kind: 'manual_card', confidence: 'ok'};
  const dup = Object.assign({}, card, {ended_ms: S0 + 50e3, capacity_credits: 825, credits_used: 891});
  const lc = (h, used) => ({window: 'primary', started_ms: S0, ended_ms: S0 + h * 3600e3, capacity_credits: null, credits_used: used, delta_pct: 1, close_kind: 'limit_change', confidence: 'insufficient'});
  const openRow = {window: 'primary', started_ms: S0, ended_ms: S0 + 30 * 3600e3, open: true, capacity_credits: 1100, credits_used: 594, delta_pct: 42, close_kind: 'open', confidence: 'ok'};
  const dd = c.qhRows([openRow, lc(4, 198), dup, lc(1, 66), card, lc(3, 165)], 'primary');
  assert.deepEqual(dd.map(x => [x.close_kind, x.credits_used]), [['manual_card', 825], ['open', 594]], 'one row per cycle start');
  assert.equal(JSON.stringify(dd.map(x => x.merged)), JSON.stringify([{lc: 0, dup: 1}, {lc: 3, dup: 0}]));
  const dtm = walk(c.qhDetail(dd[1], 'open', [])).join('|');
  assert.ok(dtm.includes('3 次额度变化（窗口没有重置）') && dtm.includes('同一周期只显示一行'), dtm);
  assert.ok(walk(c.qhDetail(dd[0], 'manual_card', [])).join('|').includes('1 条重复关闭记录'));
  assert.equal(c.qhRows([card, openRow], 'primary').every(x => !x.merged), true, 'distinct starts untouched');
  assert.match(walk(c.qhDetail(pc[2], 'open', [])).join('|'), /估算中（变化还不够）/);
  // 重置估算…: manual, behind the confirm dialog; pro has one window, so there is nothing to choose
  const rb = find(proPanel, n => /\brst\b/.test(n.className))[0];
  assert.equal(walk(rb).join(''), '重置估算…');assert.equal(rb.attrs['aria-haspopup'], 'dialog');
  rb.on.click();
  assert.equal(c.dlg.title, '重置 b 的额度估算？');assert.equal(c.dlg.yes, '重置估算');
  const dlgText = () => walk({children: c.dlg.nodes}).join('');
  assert.ok(dlgText().includes('之前的估算不再参与计算，从现在起重新统计。请求记录和费用不受影响。'));
  assert.ok(dlgText().includes('周窗口') && dlgText().includes('2 个已结束的周期 + 进行中的估算'), dlgText());
  assert.ok(dlgText().includes('旧估算会归档，不删除 · 30 天内可以撤销'));
  assert.equal(find({children: c.dlg.nodes}, n => n.attrs && n.attrs.type === 'radio').length, 0, 'one window: no radio');
  await c.dlg.run();
  assert.equal(JSON.stringify(c.posts[0]), JSON.stringify(['/admin/v1/accounts/b/quota-estimate/reset', {window: 'primary', confirm: 'b'}]));
  assert.match(c.okMsg, /已重置 b 的周额度估算/);
  await c.okUndo();
  assert.equal(JSON.stringify(c.posts[1]), JSON.stringify(['/admin/v1/accounts/b/quota-estimate/reset/77/undo', {}]));
  // after a reset: banner with 撤销 (not only a toast), older cycles greyed as archived
  const ep = {id: 5, window: 'primary', from_ms: 123, created_ms: 1, undo_until_ms: Date.now() + day, active: true};
  const pe = c.histPanel(pdat({cycles: pc.map(x => Object.assign({}, x, {archived: !x.open})), epochs: [ep, Object.assign({}, ep, {id: 4, window: 'secondary'})]}), pro, 'b|x', {id: 'b'});
  const bn = find(pe, n => n.className === 'qh-ep')[0];
  assert.equal(bn.attrs.role, 'status');
  assert.ok(walk(bn).join('').startsWith('已重置 · t123 起重新统计撤销'), walk(bn).join(''));
  await find(bn, n => n.tag === 'button')[0].on.click();
  assert.equal(JSON.stringify(c.posts[2]), JSON.stringify(['/admin/v1/accounts/b/quota-estimate/reset/5/undo', {}]));
  assert.equal(rowsOf(pe).filter(r => /\barch\b/.test(r.className)).length, 2);
  assert.ok(walk(rowsOf(pe)[1]).join(' ').includes('已归档'));
  for (const gone of [Object.assign({}, ep, {active: false}), Object.assign({}, ep, {undo_until_ms: Date.now() - 1})])
    assert.equal(find(c.histPanel(pdat({epochs: [gone]}), pro, 'b|x', {id: 'b'}), n => n.className === 'qh-ep').length, 0, 'undone or expired: no banner');
  c.W = false;
  assert.equal(find(c.histPanel(pdat(), pro, 'b|x', {id: 'b'}), n => /\brst\b/.test(n.className)).length, 1, 'config read-only: reset still offered (usage log only)');
  assert.equal(find(c.histPanel(pdat(), pro, 'b|x'), n => /\brst\b/.test(n.className)).length, 0, 'no account: no reset button');
  c.W = true;
  // Plus panel: 周 | 5h toggle (button group, aria-pressed), default 周, choice kept for the session
  const pdata = {from_ms: from, to_ms: to, events: [], cycles: plusCycles};
  const panel = c.histPanel(pdata, plus, 'a|x', {id: 'a'});
  const seg = find(panel, n => n.className === 'seg')[0];
  assert.equal(seg.attrs.role, 'group');
  assert.deepEqual(seg.children.map(b => [b.tag, b.text || b.children.join(''), b.attrs['aria-pressed'], b.attrs.type]), [['button', '周', 'true', 'button'], ['button', '5h', 'false', 'button']]);
  assert.deepEqual(seg.children.map(b => b.attrs['data-fk']), ['a|x|qwl', 'a|x|qws']);
  assert.equal(rowsOf(panel).length, weekly.length, '周 rows only');
  seg.children[1].on.click();
  assert.deepEqual(seg.children.map(b => b.attrs['aria-pressed']), ['false', 'true']);
  assert.equal(rowsOf(panel).length, 12, '5h: the newest 12 cycles');
  assert.ok(walk(panel).join('').includes('再显示 ' + (outl.length - 12) + ' 个周期'));
  assert.equal(store.ciallopa_qh_win, 's');
  assert.equal(rowsOf(c.histPanel(pdata, plus)).length, 12, 'the choice carries to the next panel');
  assert.equal(rowsOf(c.histPanel(pdat(), pro)).length, 3, 'pro ignores a 5h choice');
  find(panel, n => /\brst\b/.test(n.className))[0].on.click();
  assert.deepEqual(find({children: c.dlg.nodes}, n => n.attrs && n.attrs.type === 'radio').map(n => n.attrs.value), ['primary', 'all'], 'plus: this window or all windows');
  assert.ok(dlgText().includes('5h窗口') && dlgText().includes('这个账号的所有窗口') && dlgText().includes('5h + 周'), dlgText());
  seg.children[0].on.click();assert.equal(store.ciallopa_qh_win, 'l');
  // plus account with only weekly history so far: toggle still there, 5h says there is nothing yet
  const p2 = c.histPanel({from_ms: from, to_ms: to, events: [], cycles: weekly.map(x => Object.assign({}, x, {window: 'secondary'}))}, plus);
  find(p2, n => n.className === 'seg')[0].children[1].on.click();
  assert.ok(walk(p2).join(' ').includes('暂无已记录的周期'));
  // no quota read yet: slots with history decide; only one → no toggle
  assert.equal(find(c.histPanel({from_ms: from, to_ms: to, events: ev, cycles}), n => n.className === 'seg').length, 0);
  assert.match(html, /@media \(pointer:coarse\)\{\.qh-hd \.seg button,\.qh-hd \.rst,\.qh-ep button,\.qh-more\{min-height:44px;min-width:56px\}/, '44 px targets on touch');
  assert.match(html, /#dlg\{z-index:41\}/, 'confirm dialog above the phone sheet');
  assert.match(walk(c.histPanel(null)).join(''), /加载中/);
  assert.match(walk(c.histPanel({error: 'x'})).join(''), /读取失败：x/);
  // 点数 next to the quota (like Codex /status "Credits")
  const crSrc = html.match(/function crBal\(b\)[^\n]*\nfunction crText\(cr\)[^\n]*/)[0];
  const cr = new Function(crSrc + ';return {crBal, crText};')();
  assert.deepEqual(cr.crText({has_credits: true, unlimited: false, balance: 812.5}), ['点数 812.5', '']);
  assert.deepEqual(cr.crText({has_credits: true, unlimited: true, balance: null}), ['点数 不限', '']);
  assert.deepEqual(cr.crText({has_credits: false, unlimited: false, balance: 0}), ['无点数', 'none']);
  assert.equal(cr.crText(null), null);assert.equal(cr.crBal(1204.456), '1204.46');assert.equal(cr.crBal(null), '?');
  assert.match(html, /if\(cr\)\(\/\\bnone\\b\/\.test\(b\.className\)\?a:b\)\.appendChild\(cr\)/, '点数 under the long window cell, or the only one');
  assert.doesNotMatch(html, /preserveAspectRatio:"none"/);
  assert.match(html, /\.qh-fit>svg\{position:absolute/, 'svg out of flow so a table cell can shrink');
  assert.match(html, /if\(r===null&&known\)return h\("td","qc none "\+cls\);/, 'missing window: empty cell, no placeholder');
  assert.ok(!html.includes('不适用'));
  assert.match(html, /\.counts \.tag\{height:24px;padding:0 4px;border-radius:0;background:transparent;box-shadow:none/, 'count summary is not a pill (not clickable)');
  assert.match(html, /\.sh \.counts\{flex-basis:100%\}#rescan,#qall,#impbtn,#addbtn\{flex:1 1 auto\}#impbtn,#rescan,#qall,#addbtn,#addclose,#devx,#emptyadd\{height:44px\}/, 'phone toolbar: one row of 44 px buttons');
  assert.match(html, /<button id="rescan" class="ghost xs"/);
  assert.match(html, /button\.ghost\{color:var\(--fg-2\)\}/);
  assert.match(html, /#paste summary\{min-height:44px/);
  assert.match(html, /const mw=qhMainWin\(wins,d\.cycles\);\n  if\(!qhRows\(d\.cycles,mw\)\.some\(c=>qhCap\(c\)!=null\)\)\{td\.appendChild\(h\("span","faint xs","暂无记录"\)\)/, 'no weekly history yet: 暂无记录, not an empty lane');
  assert.match(html, /#acc td\.qc\.none\{display:none\}/);
}

  addPanel();
  await quotaHistoryRender();
  resetCardDisplay();
  await switchReplies();
  await consumeDialog();
  await partialReads();
  await prewarmFollowsOverview();
  await historyAndCards();
  await supersededReads();
  await loginChanges();
  await deviceLoginIsolation();
  await accessGate();
  console.log('PASS: add-panel toggle/Esc/one solid button, reset-card display, auto-use and quota-auto switch state, account snapshots, prewarm preference, read/login races, device start/poll cancellation and identity isolation, Cloudflare Access expiry, quota cycle rows, estimate reset/undo, 点数 balance, single history read, card flags');
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => clearTimeout(watchdog));

// Switching updates only the affected row and trusts the server's accepted value.
async function switchReplies() {
  const t = harness(), calls = [], rows = [];
  t.c.path = a => '/admin/v1/accounts/' + a.id;
  t.c.renderAccount = id => rows.push(id);
  t.c.apost = async (path, body) => {calls.push([path, body]); return {account: 'alice', quota_auto: false};};
  assert.equal(await t.run('setAccountSwitch({id:"alice"},"quota",true)'), false);
  assert.equal(t.c.accountsState.quotaAuto.alice, false, 'POST reply wins over the requested value');
  assert.deepEqual(rows, ['alice']);
  assert.equal(t.c.renderCount, 0);
  assert.equal(t.c.histLoads, 0);
  const next = deferred();
  t.c.get = url => {calls.push([url]); return next.promise;};
  t.c.apost = async (path, body) => {calls.push([path, body]); return {account: 'alice', auto: true};};
  const enabling = t.run('setAccountSwitch({id:"alice"},"auto",true)');
  await new Promise(setImmediate);
  assert.equal(t.c.accountsState.auto.alice.on, true);
  assert.equal(await enabling, true, 'switch feedback does not wait for the next-time read');
  next.resolve({accounts: [{account: 'alice', auto_next_ms: 12345}]});
  await new Promise(setImmediate);
  assert.equal(t.c.accountsState.auto.alice.next, 12345);
  assert.equal(calls.filter(([url]) => url === '/usage/v1/reset-credits').length, 1);
  assert.equal(calls.length, 3, 'two POSTs, one card list; no account/quota/history reloads');
  const stale = deferred();
  t.c.apost = () => stale.promise;
  const pending = t.run('setAccountSwitch({id:"alice"},"quota",true)');
  t.run('onLogout()');
  t.auth('demo-admin'); // Same-key relogin must also invalidate the old mutation.
  stale.resolve({account: 'alice', quota_auto: true});
  assert.equal(await pending, undefined);
  assert.equal(t.c.accountsState.quotaAuto.alice, undefined);
}
async function consumeDialog() {
  const t = harness(), posts = [];
  t.c.CODEN = {reset: '已重置'};
  t.run = expression => vm.runInContext(expression, t.c);
  vm.runInContext(section('consume-dialog'), t.c);
  t.run('cp={account:"alice",credit:"first",rid:null}');
  t.c.post = async (url, body) => {posts.push(body); throw Object.assign(new Error('pending'), {status: 409, body: {pending: {credit_id: 'pending-card', redeem_request_id: 'pending-id'}}});};
  await t.node('cpyes').click();
  assert.equal(t.run('JSON.stringify(cp)'), JSON.stringify({account: 'alice', credit: 'pending-card', rid: 'pending-id'}));
  assert.equal(t.node('cpyes').textContent, '重试上一次');
  t.c.post = async (url, body) => {posts.push(body); return {code: 'reset', windows_reset: 1, redeem_request_id: 'pending-id'};};
  await t.node('cpyes').click();
  assert.equal(JSON.stringify(posts[1]), JSON.stringify({confirm: 'alice', credit_id: 'pending-card', redeem_request_id: 'pending-id'}));
  t.node('cp')._onclose();
  assert.equal(t.run('cp'), null);
  const late = deferred();
  t.c.post = () => late.promise;
  t.run('cp={account:"alice",credit:null,rid:null}');
  const pending = t.node('cpyes').click();
  t.node('cp')._onclose();
  t.run('cp={account:"bob",credit:"new-card",rid:null}');
  t.node('cpres').textContent = 'new dialog';
  late.resolve({code: 'reset', windows_reset: 1});
  await pending;
  assert.equal(t.node('cpres').textContent, 'new dialog', 'a closed dialog cannot overwrite its replacement');
}
