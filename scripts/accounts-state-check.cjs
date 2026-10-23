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
  section('function fail(e)', 'function ok('),
  section('// ---- add panel:', '// ---- account reads:'),
  section('// ---- account reads:', '$("refresh").addEventListener'),
  section('// Device-code login:', 'onLogout='),
  section('\nonLogout=()=>{', '\nlogin('),
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
        focus() {},
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

const watchdog = setTimeout(() => {
  console.error('FAIL: accounts checks did not settle within 5 seconds');
  process.exitCode = 1;
}, 5000);

(async () => {
  await partialReads();
  await supersededReads();
  await loginChanges();
  await deviceLoginIsolation();
  console.log('PASS: account snapshots, read/login races, device start/poll cancellation and identity isolation');
})().catch(error => {console.error(error); process.exitCode = 1;}).finally(() => clearTimeout(watchdog));
