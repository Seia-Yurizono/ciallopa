// Partial reads and login/query races against the actual embedded accounts-page functions.
// Run from any directory: node /path/to/ciallopa/scripts/accounts-state-check.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../crates/server/src/accounts.html'), 'utf8');
function section(start, end) {
  const a = html.indexOf(start), b = html.indexOf(end, a + start.length);
  assert.ok(a >= 0 && b > a, `Accounts source anchors missing: ${start} / ${end}`);
  return html.slice(a, b);
}
const source = [
  section('// Behind Cloudflare Access an expired', 'async function get('),
  section('function fail(e)', 'function ok('),
  section('// ---- add panel:', '// ---- account reads:'),
  section('// ---- account reads:', '$("refresh").addEventListener'),
  section('// Device-code login:', 'onLogout='),
  section('\nonLogout=()=>{', '\nlogin('),
  section('// Reset cards: the newer', 'function row('),
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
  const nodes = new Map(), intervals = new Map();
  let nextInterval = 0;
  function node(id) {
    if (!nodes.has(id)) {
      const classes = new Set(), events = {};
      nodes.set(id, {
        textContent: '', value: '', dataset: {}, isConnected: true,
        classList: {
          add: x => classes.add(x),
          remove: x => classes.delete(x),
          toggle: (x, on) => on ? classes.add(x) : classes.delete(x),
          contains: x => classes.has(x),
        },
        setAttribute(name, value) {this[name] = value;},
        removeAttribute(name) {delete this[name]; if (name === 'data-until') delete this.dataset.until;},
        addEventListener(type, fn) {events[type] = fn;},
        click() {return events.click?.();},
        fire(type, e) {return events[type]?.(e);},
        focus() {c.focused = id;},
        scrollIntoView() {},
        appendChild(child) {this.textContent += child.textContent;},
      });
    }
    return nodes.get(id);
  }
  const h = (_tag, _attrs, ...kids) => ({textContent: kids.flat().map(k => k?.textContent ?? k ?? '').join('')});
  const c = vm.createContext({
    $: node, clear: n => {n.textContent = '';}, h,
    document: {createTextNode: text => ({textContent: text})},
    W: false, DATA: null, QM: {}, RCM: {}, QERR: {}, OPEN: new Set(), poll: null, NOADMIN: false,
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
  assert.equal(t.c.QM.alice.quota.version, 'v1');
  assert.equal(t.c.RCM.alice.version, 'v1');

  // A successful account response does not turn failed quota/card reads into empty data.
  t.time(2000);
  t.replies(async url => url.startsWith('/usage/') ? failure(503) : response(payload(url, 'v2')));
  await t.run('load()');
  assert.equal(t.c.DATA.version, 'v2');
  assert.equal(t.c.QM.alice.quota.version, 'v1');
  assert.equal(t.c.RCM.alice.version, 'v1');
  assert.match(t.node('readnote').textContent, /额度读取失败.*保留已有数据.*t1000/);
  assert.match(t.node('readnote').textContent, /重置卡读取失败.*保留已有数据.*t1000/);
  assert.equal(t.node('upd').textContent, '账号更新于 t2000');

  // Recovery is per source; the still-failing card snapshot remains intact.
  t.time(3000);
  t.replies(async url => url.endsWith('/reset-credits') ? failure(503) : response(payload(url, 'v3')));
  await t.run('load()');
  assert.equal(t.c.QM.alice.quota.version, 'v3');
  assert.equal(t.c.RCM.alice.version, 'v1');
  assert.doesNotMatch(t.node('readnote').textContent, /额度读取失败/);
  assert.match(t.node('readnote').textContent, /重置卡读取失败/);

  // Permission/unavailable responses clear only their source instead of retaining authorized data.
  t.replies(async url => url.endsWith('/quota') ? failure(403) : url.endsWith('/reset-credits') ? failure(404) : response(payload(url, 'v4')));
  await t.run('load()');
  assert.equal(Object.keys(t.c.QM).length, 0);
  assert.equal(Object.keys(t.c.RCM).length, 0);
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
  assert.equal(t.c.DATA.version, 'v5');
  assert.equal(t.c.QM.alice.quota.version, 'v5');
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
  assert.equal(t.c.DATA.version, 'new');
  assert.equal(t.c.QM.alice.quota.version, 'new');
  assert.equal(t.c.RCM.alice.version, 'new');
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
  assert.equal(t.c.DATA.version, 'latest');
}

async function loginChanges() {
  const t = harness();
  await t.run('load()');
  const old = new Map();
  t.replies(url => {const d = deferred(); old.set(url, d); return d.promise;});
  const pending = t.run('load()');
  t.run('onLogout()');
  assert.equal(t.c.DATA, null);
  assert.equal(Object.keys(t.c.QM).length, 0);
  assert.equal(Object.keys(t.c.RCM).length, 0);
  assert.equal(t.node('upd').textContent, '');
  t.auth('another-admin');
  t.version('other-login');
  await t.run('load()');
  for (const d of old.values()) d.resolve(failure(401));
  await pending;
  assert.equal(t.currentAuth(), 'another-admin');
  assert.equal(t.c.logoutCount, 0);
  assert.equal(t.c.DATA.version, 'other-login');

  // A current 401 retains the normal logout behavior and clears previous identity data.
  t.replies(async () => failure(401));
  assert.equal(await t.run('load()'), false);
  assert.equal(t.currentAuth(), '');
  assert.equal(t.c.logoutCount, 1);
  assert.equal(t.c.DATA, null);
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
  run('AUTO = {}; QAUTO = {}; AUTOTOP = null; readAuto({status: "fulfilled", value: {j: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 5, accounts: [{account: "a", auto: true, auto_next_ms: 5, quota_auto: true}, {account: "b", quota_auto: false}]}}})');
  assert.equal(run('JSON.stringify([AUTO, QAUTO, AUTOTOP])'), JSON.stringify([{a: {on: true, next: 5}, b: {on: false, next: null}}, {a: true, b: false}, {lead: 15, ok: true, qmins: 5, qmax: 5}]));
  run('readAuto({status: "rejected", reason: new Error("x")})');
  assert.equal(run('AUTO.a.on && QAUTO.a && AUTOTOP.lead === 15 && AUTOTOP.qmins'), 5);
  // a refresh range (5–10 min); an old single value reads as fixed (max = min); off = 0
  run('readAuto({status: "fulfilled", value: {j: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 5, quota_refresh_max_mins: 10, accounts: [{account: "a", quota_auto: true}]}}})');
  assert.equal(run('JSON.stringify([AUTOTOP.qmins, AUTOTOP.qmax])'), '[5,10]');
  run('readAuto({status: "fulfilled", value: {j: {auto_lead_mins: 15, auto_available: true, quota_refresh_mins: 0, quota_refresh_max_mins: 10, accounts: []}}})');
  assert.equal(run('JSON.stringify([AUTOTOP.qmins, AUTOTOP.qmax])'), '[0,0]');
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
  assert.equal(t.run('JSON.stringify(RCTOP)'), JSON.stringify({can: true, adm: true, attempts: [{account: 'alice', code: 'reset'}]}));
  t.run('readCardTop({status: "rejected", reason: new Error("x")})');
  assert.equal(t.run('RCTOP.can'), true, 'a failed read keeps the last answer');
  t.run('readCardTop({status: "rejected", reason: Object.assign(new Error("x"), {status: 403})})');
  assert.equal(t.run('RCTOP.can || RCTOP.adm'), false, 'no permission: no card actions');
  let now = 1000;const paths = [];
  const c = vm.createContext({key: () => 'k', DATA: {accounts: []}, renders: 0, render() {c.renders++;}, why: e => e.message,
    Date: class extends Date {static now() {return now;}},
    req: async (method, p) => {paths.push(method + ' ' + p); return {j: {from_ms: 1, to_ms: 2, accounts: [{account: 'alice', events: [], cycles: [{window: 'primary'}]}]}};}});
  vm.runInContext(section('let QHA=null', 'function hashAcc('), c);
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

function quotaHistoryRender() {
  function h(tag, a, ...kids) {
    const e = {tag, className: '', style: {}, children: [], attrs: {}};
    e.appendChild = c => { e.children.push(c); return c; };
    e.setAttribute = (k, v) => { e.attrs[k] = v; };
    e.on = {};
    e.addEventListener = (t, f) => { e.on[t] = f; };
    Object.defineProperty(e, 'firstChild', {get() { return e.children[0] || null; }});
    e.removeChild = c => { e.children.splice(e.children.indexOf(c), 1); };
    e.contains = () => false;
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
    if (typeof e === 'string' || typeof e === 'number') { acc.push(String(e)); return acc; }
    if (e.text) acc.push(e.text);
    for (const k of e.children || []) walk(k, acc);
    return acc;
  }
  function find(e, pred, out = []) {
    if (!e || typeof e !== 'object') return out;
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
  const c = vm.createContext({
    fmtN: n => String(Math.round(Number(n))),
    fmtTime: (ms) => 't' + ms,
    fmtUsd: n => n == null ? '–' : '$' + Number(n).toFixed(2),
    h,
    document,
    clear: e => { while (e.firstChild) e.removeChild(e.firstChild); },
    sessionStorage: {getItem: k => store[k] ?? null, setItem: (k, v) => { store[k] = String(v); }},
  });
  vm.runInContext(section('function wlab(', 'function whead(') + section('function qhKindLabel', 'let QHA=null'), c);
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
  const tip = c.qhCapTip({capacity_credits: 50, capacity_lo: 800, capacity_hi: 1200, api_cost_usd: 8.7, coverage_gap: true, trend: 'up', close_kind: 'natural', ended_ms: 9});
  assert.match(tip, /可能有其他端用量/);
  assert.match(tip, /高于历史中位/);
  assert.match(tip, /API 价格 \$8\.70/);
  // Popover lines: reset kind + time first, then only facts that exist; card use and caveats live here.
  const tl = c.qhTipLines({window: 'primary', ended_ms: 9, capacity_credits: 80, capacity_lo: 70, capacity_hi: 90, api_cost_usd: 2, close_kind: 'manual_card', confidence: 'fallback', coverage_gap: true, trend: 'up'},
    {kind: 'manual_card', card_source: 'manual', prev_pct: 40, next_pct: 1, at_ms: 9}, 50, false);
  assert.equal(tl[0], '手动卡 · t9');
  assert.deepEqual(Array.from(tl.slice(1, 5)), ['容量 ≈ 80 credits（70–90）', 'API 价格 $2.00', '手动使用重置卡', '已用 40% → 1%']);
  assert.equal(tl[5], '高于历史中位 50 · 可能有其他端用量 · 估算可信度低（相邻斜率回退）');
  assert.deepEqual(Array.from(c.qhTipLines({window: 'primary', ended_ms: 9, capacity_credits: 50, close_kind: 'natural', confidence: 'ok'}, null, null, true)), ['自然重置 · t9', '容量 ≈ 50 credits', '超出图表上沿']);
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
  // Drawn at the measured px width: viewBox == width/height attributes, never stretched, no style="" (CSP).
  for (const W of [360, 800]) {
    const g = c.qhGeom(W), svg = c.qhFull(cycles, 'primary', from, to, W, null, ev);
    const vb = svg.attrs.viewBox.split(' ').map(Number);
    assert.equal(vb[2], W);assert.equal(Number(svg.attrs.width), W);assert.equal(Number(svg.attrs.height), vb[3]);
    assert.equal(svg.attrs.preserveAspectRatio, undefined, 'no non-uniform scaling');
    assert.ok(all(svg).every(n => !n.attrs || n.attrs.style === undefined), 'no style attribute (CSP blocks it)');
    const pts = find(svg, n => /\bqh-pt\b/.test(n.className));
    assert.equal(pts.length, 2);
    assert.ok(Math.abs(parseFloat(pts[0].attrs.cx) - c.qhX(from + 3 * day, from, to, g)) < 0.06, 'marker at the cycle end');
    assert.ok(pts.every(p => p.className === 'qh-pt' && p.tag === 'circle' && p.attrs.r === '3.5'), 'every reset is the same hollow dot');
    assert.equal(find(svg, n => n.tag === 'title').length, 0, 'no native titles: details are in the popover');
    const steps = find(svg, n => /\bqh-step\b/.test(n.className) && !/\b(join|gap)\b/.test(n.className));
    assert.equal(steps.length, 2, 'one flat segment per cycle');
    assert.ok(Math.abs(parseFloat(steps[0].attrs.x1) - c.qhX(from + day, from, to, g)) < 0.06, 'segment starts at started_ms');
    assert.ok(Math.abs(parseFloat(steps[0].attrs.x2) - c.qhX(from + 3 * day, from, to, g)) < 0.06, 'segment ends at ended_ms');
    assert.ok(steps.every(st => st.className === 'qh-step'), 'low confidence is not drawn (it is in the popover)');
    assert.equal(find(svg, n => /\bjoin\b/.test(n.className)).length, 1, 'adjacent cycles joined by a riser');
    assert.equal(find(svg, n => /\bgap\b/.test(n.className)).length, 0, 'touching cycles: no dashed bridge');
    assert.equal(find(svg, n => /qh-cost-bar|qh-band|qh-median|qh-wl/.test(n.className)).length, 0, 'one line only: no API price bars, band, median or window label');
    assert.equal(find(svg, n => n.tag === 'pattern' || /qh-ring|qh-med-lbl|qh-hatch|gap/.test(n.className)).length, 0, 'no rings, hatching or labels in the plot');
    assert.deepEqual(find(svg, n => n.className === 'qh-unit').map(n => n.text), ['credits']);
    assert.ok(!walk(svg).join(' ').includes('API 价格'), 'API price only in the popover');
    const dates = find(svg, n => n.className === 'qh-tick' && /^\d+-\d\d$/.test(n.text || ''));
    assert.equal(dates.length, g.ticks);
    assert.ok(W - c.qhX(to, from, to, g) >= 8, 'right pad fits the selection ring');
    assert.equal(Number(svg.attrs.height), g.top + g.capH + g.xh, 'no API price pane');
  }
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
    const W = 800, g = c.qhGeom(W), sg = c.qhFull(gc, 'primary', from, to, W, '5h', []);
    const solid = find(sg, n => n.className === 'qh-step'), gaps = find(sg, n => n.className === 'qh-step gap'), joins = find(sg, n => n.className === 'qh-step join');
    assert.equal(solid.length, 4, 'a solid segment for every cycle with a value, none for the one without');
    assert.equal(gaps.length, 2, 'dashed bridge over the idle gap and over the cycle without an estimate');
    assert.equal(joins.length, 1, 'touching cycles keep a solid riser');
    const X = t => c.qhX(t, from, to, g);
    assert.ok(Math.abs(parseFloat(gaps[0].attrs.x1) - X(gc[0].ended_ms)) < 0.06 && Math.abs(parseFloat(gaps[0].attrs.x2) - X(gc[1].started_ms)) < 0.06, 'bridge runs from the last end to the next start');
    assert.equal(gaps[0].attrs.y1, solid[0].attrs.y1);assert.equal(gaps[0].attrs.y2, solid[1].attrs.y1);
    assert.ok(Math.abs(parseFloat(gaps[1].attrs.x1) - X(gc[2].ended_ms)) < 0.06 && Math.abs(parseFloat(gaps[1].attrs.x2) - X(gc[4].started_ms)) < 0.06, 'bridge skips the cycle without an estimate');
    assert.match(Array.from(c.qhTipLines(gc[4], null, null, false, 1)).join('|'), /前面 1 个周期没有容量估算（虚线跳过）/);
    const mg = c.qhMini({cycles: gc.map(x => Object.assign({}, x, {window: 'secondary'})), events: []}, from, to, 320);
    assert.equal(find(mg, n => n.className === 'qh-step gap').length, 1, 'row sparkline: one dashed path for the bridges');
    assert.equal((find(mg, n => n.className === 'qh-step gap')[0].attrs.d.match(/M/g) || []).length, 2);
    assert.equal((find(mg, n => n.className === 'qh-step')[0].attrs.d.match(/M/g) || []).length, 3, 'solid path restarts after each bridge');
  }
  assert.match(html, /\.qh-svg \.qh-step\.gap\{stroke-width:1\.2;stroke-dasharray:3 3;opacity:\.55\}/);
  assert.match(html, /没有容量估算（图上虚线跳过）/, '明细 names cycles without an estimate');
  // open cycle: dashed, no dot; dense (5h) histories keep one dot per 8 px, the popover still has every reset
  const openC = cycles.concat([{window: 'primary', started_ms: from + 15 * day, ended_ms: null, open: true, capacity_credits: 70}]);
  const so = c.qhFull(openC, 'primary', from, to, 800, null, ev);
  assert.equal(find(so, n => n.className === 'qh-step open').length, 1);
  assert.equal(find(so, n => n.className === 'qh-pt').length, 2);
  const dense5 = Array.from({length: 140}, (_, i) => ({window: 'primary', started_ms: from + i * 5 * 3600e3, ended_ms: from + (i + 1) * 5 * 3600e3, capacity_credits: 45 + (i % 7), close_kind: 'natural', confidence: 'ok'}));
  const sd = c.qhFull(dense5, 'primary', from, to, 360, '5h', []);
  const dd = find(sd, n => n.className === 'qh-pt').map(n => parseFloat(n.attrs.cx));
  assert.ok(dd.length > 10 && dd.length < 140, `dense 5h dots thinned (${dd.length})`);
  assert.ok(dd.every((x, i) => !i || x - dd[i - 1] >= 16), 'thinned dots at least 16 px apart');
  assert.ok(find(sd, n => n.className === 'qh-pt').every(n => n.attrs.r === '2.2'), 'dense: smaller dots');
  assert.ok(c.qhGeom(360).L < c.qhGeom(800).L, 'narrow charts use a narrower axis gutter');
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
  for (const W of [344, 800]) {
    const g = c.qhGeom(W), svgO = c.qhFull(outl, 'primary', from, to, W), bot = g.top + g.capH;
    const yt = find(svgO, n => n.className === 'qh-tick' && /^\d+$/.test(n.text || '') && n.attrs['text-anchor'] === 'end').map(n => Number(n.text));
    assert.equal(Math.min(...yt), 0, 'axis keeps 0');
    assert.ok(Math.max(...yt) <= 100, `top follows the inliers, not the 600 outlier (${yt})`);
    const steps = find(svgO, n => /\bqh-step\b/.test(n.className) && !/\b(join|gap)\b/.test(n.className)).map(n => Number(n.attrs.y1));
    assert.equal(steps.length, vals.length, 'every cycle is drawn');
    const inl = steps.filter((_, i) => vals[i] > 10 && vals[i] < 100), med = (bot - inl.sort((a, b) => a - b)[inl.length >> 1]) / g.capH;
    assert.ok(med > 0.3 && med < 0.7, `inlier median sits mid-plot (${med.toFixed(2)})`);
    assert.equal(steps[6], g.top, 'outlier step pinned to the top edge');
    assert.ok(steps[9] > g.top && steps[9] <= bot && steps[9] > steps[0], 'low value drawn at its real height');
    const overs = find(svgO, n => n.className === 'qh-over');
    assert.equal(overs.length, 1, 'one ▲ for the capacity; no value labels, no price pane');
    assert.ok(overs.every(o => o.tag === 'polygon' && !o.children.length));
    assert.ok(Number(overs.find(o => Number(o.attrs.points.split(/[ ,]/)[1]) < g.top).attrs.points.split(/[ ,]/)[1]) < g.top, '▲ sits on the top edge');
    assert.equal(find(svgO, n => n.tag === 'text' && /600/.test(n.text || '')).length, 0);
    assert.ok(all(svgO).every(n => !n.attrs || n.attrs.style === undefined));
  }
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
  // Pro panel: 周 only, no toggle, no 5h anywhere
  const proPanel = c.histPanel({from_ms: from, to_ms: to, events: [], cycles: weekly}, pro, 'b|x');
  assert.equal(find(proPanel, n => n.className === 'seg').length, 0, 'weekly-only: no toggle');
  assert.deepEqual(find(proPanel, n => /\bqh-fit\b/.test(n.className)).map(n => n.attrs['aria-label']), ['周 周期容量']);
  assert.ok(walk(proPanel).join(' ').includes('额度记录 · 30 天 · 周'));
  assert.ok(!/5h/.test(walk(proPanel).join(' ')), 'no 5h anywhere in a pro account panel');
  // Plus panel: 周 | 5h toggle (button group, aria-pressed), default 周, choice kept for the session
  const pdata = {from_ms: from, to_ms: to, events: [], cycles: plusCycles};
  const panel = c.histPanel(pdata, plus, 'a|x');
  const seg = find(panel, n => n.className === 'seg')[0];
  assert.equal(seg.attrs.role, 'group');
  assert.deepEqual(seg.children.map(b => [b.tag, b.text || b.children.join(''), b.attrs['aria-pressed'], b.attrs.type]), [['button', '周', 'true', 'button'], ['button', '5h', 'false', 'button']]);
  assert.deepEqual(seg.children.map(b => b.attrs['data-fk']), ['a|x|qwl', 'a|x|qws']);
  const fitOf = p => find(p, n => /\bqh-fit\b/.test(n.className));
  assert.deepEqual(fitOf(panel).map(n => n.attrs['aria-label']), ['周 周期容量'], 'one chart at a time');
  seg.children[1].on.click();
  assert.deepEqual(seg.children.map(b => b.attrs['aria-pressed']), ['false', 'true']);
  assert.deepEqual(fitOf(panel).map(n => n.attrs['aria-label']), ['5h 周期容量'], '5h on its own chart');
  assert.equal(store.ciallopa_qh_win, 's');
  assert.deepEqual(fitOf(c.histPanel(pdata, plus)).map(n => n.attrs['aria-label']), ['5h 周期容量'], 'the choice carries to the next panel');
  assert.deepEqual(fitOf(c.histPanel({from_ms: from, to_ms: to, events: [], cycles: weekly}, pro)).map(n => n.attrs['aria-label']), ['周 周期容量'], 'pro ignores a 5h choice');
  seg.children[0].on.click();assert.equal(store.ciallopa_qh_win, 'l');
  // plus account with only weekly history so far: toggle still there, 5h says there is nothing yet
  const p2 = c.histPanel({from_ms: from, to_ms: to, events: [], cycles: weekly.map(x => Object.assign({}, x, {window: 'secondary'}))}, plus);
  find(p2, n => n.className === 'seg')[0].children[1].on.click();
  assert.ok(walk(p2).join(' ').includes('暂无已记录的周期'));
  const text = walk(panel).join(' ');
  for (const label of ['额度记录 · 30 天', '明细', '重置（悬停或点按看详情）', 'API 价格']) assert.ok(text.includes(label), label);
  const leg = find(panel, n => n.className === 'qh-leg')[0];
  assert.equal(leg.children.length, 1, 'legend: one item');
  for (const gone of ['历史中位', '估算范围', '手动卡', '可能有其他端用量']) assert.ok(!walk(leg).join('').includes(gone), gone);
  const fits = fitOf(panel);
  assert.equal(fits[0].attrs.role, 'group', 'interactive chart is not role=img');
  // no quota read yet: slots with history decide; only one → no toggle
  assert.equal(find(c.histPanel({from_ms: from, to_ms: to, events: ev, cycles}), n => n.className === 'seg').length, 0);
  // keyboard + touch popover: focusable svg, arrow keys, Escape, 22 px hit radius (44 px), 关闭 button
  assert.match(html, /sv\.setAttribute\("tabindex","0"\)/);
  assert.match(html, /ArrowRight[\s\S]{0,200}ArrowLeft[\s\S]{0,200}Escape/);
  assert.match(html, /let b=-1,bd=23;pts\.forEach\(\(p,i\)=>\{const d=Math\.abs\(p\.x-x\)/, 'nearest reset in x within 22 px');
  assert.match(html, /\.qh-hd \.seg button\{height:44px;min-width:56px\}/, 'toggle is 44 px on touch');
  assert.match(html, /h\("button",\{class:"xs qh-tx",type:"button"\},"关闭"\)/);
  assert.match(html, /@media \(pointer:coarse\)\{\.qh-tip \.qh-tx\{min-height:44px;min-width:44px/);
  assert.match(html, /\.qh-tip:not\(\.pin\)\{pointer-events:none\}/, 'hover tooltip never covers the cursor');
  assert.match(html, /t\.style\.left=/, 'popover placed via CSSOM, not a style attribute');
  assert.equal(fits[0]._draw(640).attrs.width, '640');
  assert.match(walk(c.histPanel(null)).join(''), /加载中/);
  assert.match(walk(c.histPanel({error: 'x'})).join(''), /读取失败：x/);
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
  quotaHistoryRender();
  resetCardDisplay();
  await partialReads();
  await prewarmFollowsOverview();
  await historyAndCards();
  await supersededReads();
  await loginChanges();
  await deviceLoginIsolation();
  await accessGate();
  console.log('PASS: add-panel toggle/Esc/one solid button, reset-card display, auto-use and quota-auto switch state, account snapshots, prewarm preference, read/login races, device start/poll cancellation and identity isolation, Cloudflare Access expiry, quota history charts (px-fitted), single history read, card flags');
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => clearTimeout(watchdog));
