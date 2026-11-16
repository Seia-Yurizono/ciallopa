// Shell / foundation regression checks. Run from any directory: node scripts/shell-check.cjs
// Loads the real ui/js sources (in ui.rs manifest order) into a Node vm with small browser stubs.
// Covers: manifest ↔ ui.rs, CSP blocks, Core formatting in both languages, I18n key parity, Api
// semantics (401, admin_required, Cloudflare Access gate, late responses, timeout), routing and
// legacy links, scope ↔ URL round trip, and source-level CSP rules.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {manifest, read, assemble, inlineBlocks, cspFor, loadScripts, SRC} = require('./lib/page.cjs');

const tests = [];
const test = (name, fn) => tests.push({name, fn});

function storage() {
  const m = new Map();
  return {getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k),
    _m: m};
}

// A fresh sandbox: stores, navigator and a scriptable fetch.
function sandbox(o) {
  o = o || {};
  const calls = [];
  const env = {
    localStorage: storage(),
    sessionStorage: storage(),
    navigator: {language: o.navLang || 'zh-CN'},
    Intl,
    fetch: async (url, init) => {
      calls.push({url, init});
      return env.respond(url, init);
    },
    respond: async () => json(200, {}),
  };
  const ctx = loadScripts(env);
  ctx.calls = calls;
  ctx.env = env;
  ctx.toasts = [];
  ctx.UI.toast = (text, opts) => ctx.toasts.push({text, opts});
  return ctx;
}

function headers(h) {
  const m = new Map(Object.entries(h || {}).map(([k, v]) => [k.toLowerCase(), v]));
  return {get: k => m.get(k.toLowerCase()) || null, keys: () => m.keys()};
}

function json(status, body, extra) {
  return Object.assign({status, ok: status >= 200 && status < 300, type: 'basic', redirected: false,
    headers: headers({'content-type': 'application/json'}), json: async () => body}, extra || {});
}

// ---------- manifest & delivery ----------
test('manifest lists every ui source file exactly once, in a sane order', () => {
  const m = manifest();
  assert.equal(m.shell, 'ui/index.html');
  const onDisk = [];
  const walk = d => {
    for (const e of fs.readdirSync(path.join(SRC, d), {withFileTypes: true})) {
      const rel = d + '/' + e.name;
      if (e.isDirectory()) walk(rel);
      else if (/\.(css|js)$/.test(e.name)) onDisk.push(rel);
    }
  };
  walk('ui');
  assert.deepEqual([...m.css, ...m.js].sort(), onDisk.sort(), 'ui.rs manifest and ui/ files differ');
  assert.equal(new Set([...m.css, ...m.js]).size, m.css.length + m.js.length);
  assert.equal(m.css[0], 'ui/css/tokens.css');
  assert.equal(m.js[0], 'ui/js/core.js');
  assert.equal(m.js[m.js.length - 1], 'ui/js/main.js');
  const views = m.js.filter(f => f.includes('/views/'));
  assert.ok(m.js.indexOf('ui/js/shell.js') < m.js.indexOf(views[0]), 'views load after the shell');
});

test('assembled page has one hashed <style> and one hashed <script>', () => {
  const html = assemble();
  assert.equal(inlineBlocks(html, 'script').length, 1);
  assert.equal(inlineBlocks(html, 'style').length, 1);
  assert.ok(!html.includes('/*@css*/') && !html.includes('/*@js*/'));
  const csp = cspFor(html);
  assert.match(csp, /script-src 'sha256-[A-Za-z0-9+/=]{44}'; style-src 'sha256-[A-Za-z0-9+/=]{44}';/);
  assert.ok(!csp.includes('unsafe'));
  assert.ok(Buffer.byteLength(html) < 1 << 20, 'page must stay below the 1 MiB test read limit');
});

test('ui.rs serves the same client routes as the router', () => {
  const rs = fs.readFileSync(path.join(SRC, 'ui.rs'), 'utf8');
  const paths = [...rs.match(/pub const PATHS: \[&str; \d+\] = \[([^\]]+)\];/)[1].matchAll(/"([^"]+)"/g)].map(m => m[1]);
  const ctx = sandbox();
  assert.deepEqual(paths.slice().sort(), Array.from(ctx.Shell.ROUTES, r => r.path).sort());
  const dev = fs.readFileSync(path.join(__dirname, 'ui-dev.cjs'), 'utf8');
  for (const p of paths) assert.ok(dev.includes(`'${p}'`), 'ui-dev.cjs misses ' + p);
});

test('sources follow the CSP rules (no inline handlers / style attributes / eval / innerHTML)', () => {
  const m = manifest();
  for (const f of [m.shell, ...m.js]) {
    const src = read(f);
    assert.ok(!/\son[a-z]+\s*=\s*["']/i.test(f.endsWith('.html') ? src.replace(/<script>[\s\S]*<\/script>/, '') : ''),
      f + ': inline event handler');
    assert.ok(!/\sstyle\s*=\s*["']/.test(src), f + ': style attribute');
    assert.ok(!/\b(eval|new Function)\s*\(/.test(src), f + ': eval');
    assert.ok(!/\.(innerHTML|outerHTML)\s*=|insertAdjacentHTML/.test(src), f + ': HTML string injection');
    if (f.endsWith('.js')) assert.ok(!src.includes('</script'), f + ': </script> inside the bundle');
    // index.html icon <symbol> lines carry path data and are exempt
    for (const [i, line] of src.split('\n').entries()) {
      if (!line.trimStart().startsWith('<symbol')) assert.ok(line.length <= 140, `${f}:${i + 1} longer than 140 chars`);
    }
  }
});

// ---------- i18n ----------
test('zh and en dictionaries define the same keys', () => {
  const ctx = sandbox();
  const d = ctx.I18n.dicts();
  const zh = Object.keys(d.zh).sort(), en = Object.keys(d.en).sort();
  const onlyZh = zh.filter(k => !d.en[k] && d.en[k] !== ''), onlyEn = en.filter(k => !d.zh[k] && d.zh[k] !== '');
  assert.deepEqual(onlyZh, [], 'missing in en');
  assert.deepEqual(onlyEn, [], 'missing in zh');
});

test('every static data-i18n key and every literal t("…") key exists', () => {
  const ctx = sandbox();
  const keys = new Set(Object.keys(ctx.I18n.dicts().zh));
  const html = read('ui/index.html');
  for (const m of html.matchAll(/data-i18n(?:-[a-z-]+)?="([^"]+)"/g)) assert.ok(keys.has(m[1]), 'index.html: ' + m[1]);
  for (const f of manifest().js) {
    for (const m of read(f).matchAll(/\b(?:t|I18n\.t)\("([a-zA-Z][\w.]*)"/g)) {
      if (m[1].endsWith('.')) continue;
      assert.ok(keys.has(m[1]), f + ': missing key ' + m[1]);
    }
  }
});

test('language: detection, interpolation, functions, switching', () => {
  const ctx = sandbox();
  const {I18n} = ctx;
  assert.equal(I18n.detect(null, 'zh-TW'), 'zh');
  assert.equal(I18n.detect(null, 'en-GB'), 'en');
  assert.equal(I18n.detect('en', 'zh-CN'), 'en');
  assert.equal(I18n.detect('fr', 'de'), 'en');
  assert.equal(I18n.t('time.minAgo', {n: 5}), '5 分钟前');
  const seen = [];
  I18n.onChange(l => seen.push(l));
  I18n.setLang('en');
  assert.deepEqual(seen, ['en']);
  assert.equal(ctx.env.localStorage.getItem('ciallopa_lang'), 'en');
  assert.equal(I18n.locale(), 'en');
  assert.equal(I18n.t('time.hourAgo', {n: 1}), '1 hour ago');
  assert.equal(I18n.t('time.hourAgo', {n: 3}), '3 hours ago');
  assert.equal(I18n.t('nope.key'), 'nope.key');
});

// ---------- formatting ----------
test('Core.fmt: numbers, money, durations (language independent)', () => {
  const {Core} = sandbox();
  const f = Core.fmt;
  assert.equal(f.n(null), '—');
  assert.equal(f.n(950), '950');
  assert.equal(f.n(1234), '1.2k');
  assert.equal(f.n(3.4e6), '3.40M');
  assert.equal(f.n(1.2e9), '1.20B');
  assert.equal(f.ms(320.4), '320ms');
  assert.equal(f.ms(1240), '1.24s');
  assert.equal(f.ms(12345), '12.3s');
  assert.equal(f.ms(252000), '4m 12s');
  assert.equal(f.usd(0), '$0.00');
  assert.equal(f.usd(0.004), '<$0.01');
  assert.equal(f.usd(1234.5), '$1,234.50');
  assert.equal(f.usd(undefined), '—');
  assert.equal(f.exactUsd(0.0123), '$0.012300');
  assert.equal(f.pct(0.0321), '3.2%');
  assert.equal(f.pct(0.5), '50%');
  assert.equal(f.pct(0), '0%');
  assert.equal(f.bytes(1536), '1.5 KB');
  assert.equal(f.io(1700, 233), '1.7k → 233');
  assert.equal(f.exact(1234567), '1,234,567');
  assert.match(f.iso(Date.UTC(2026, 10, 16, 2, 0, 0)), /^2026-11-1[56]T\d\d:00:00[+-]\d\d:\d\d$/);
});

test('Core.fmt: countdown and relative time follow the language', () => {
  const ctx = sandbox();
  const f = ctx.Core.fmt;
  assert.equal(f.dur(0), '已到期');
  assert.equal(f.dur(3 * 86400 + 9 * 3600), '3天 9时');
  assert.equal(f.dur(3600 + 360), '1时 06分');
  assert.equal(f.dur(252), '4分 12秒');
  assert.equal(f.ago(Date.now() - 10e3), '刚刚');
  assert.equal(f.ago(Date.now() - 5 * 60e3), '5 分钟前');
  ctx.I18n.setLang('en');
  assert.equal(f.dur(3600 + 360), '1h 06m');
  assert.equal(f.ago(Date.now() - 2 * 864e5), '2 days ago');
});

// ---------- Api ----------
test('Api.req sends the Bearer key and parses JSON', async () => {
  const ctx = sandbox();
  ctx.Api.setKey('sk-a');
  ctx.env.respond = async () => json(200, {ok: 1});
  const j = await ctx.Api.get('/usage/v1/quota');
  assert.equal(j.ok, 1);
  const init = ctx.calls[0].init;
  assert.equal(init.headers.Authorization, 'Bearer sk-a');
  assert.equal(init.redirect, 'manual');
  assert.equal(init.cache, 'no-store');
  await ctx.Api.post('/admin/v1/x', {a: 1});
  assert.equal(ctx.calls[1].init.headers['Content-Type'], 'application/json');
  assert.equal(ctx.calls[1].init.body, '{"a":1}');
});

test('Api: 401 of the current identity logs out; a late 401 of an old identity does not', async () => {
  const ctx = sandbox();
  const logouts = [];
  ctx.Api.onUnauthorized(m => logouts.push(m));
  ctx.Api.setKey('sk-old');
  let release;
  ctx.env.respond = () => new Promise(r => {
    release = () => r(json(401, null));
  });
  const late = ctx.Api.get('/usage/v1/summary').catch(e => e);
  ctx.Api.bump();
  ctx.Api.setKey('sk-new');
  release();
  const e = await late;
  assert.equal(e.message, '401');
  assert.deepEqual(logouts, []);
  assert.equal(ctx.Api.key(), 'sk-new', 'a stale 401 must not clear the new key');
  ctx.env.respond = async () => json(401, null);
  await ctx.Api.get('/usage/v1/summary').catch(x => x);
  assert.equal(logouts.length, 1);
  assert.equal(ctx.Api.key(), '');
});

test('Api: admin_required marks the identity and explains; a stale one does not', async () => {
  const ctx = sandbox();
  ctx.Api.setKey('k');
  ctx.env.respond = async () => json(403, {error: {type: 'admin_required', message: 'x'}});
  const e = await ctx.Api.get('/admin/v1/runtime').catch(x => x);
  assert.equal(e.status, 403);
  assert.ok(ctx.Api.isAdminError(e));
  assert.match(e.message, /管理权限/);
  assert.equal(ctx.Api.noAdmin(), true);
  ctx.Api.bump();
  assert.equal(ctx.Api.noAdmin(), false, 'a new identity starts without the flag');
  let release;
  ctx.env.respond = () => new Promise(r => {
    release = () => r(json(403, {error: {type: 'admin_required'}}));
  });
  const p = ctx.Api.get('/admin/v1/runtime').catch(x => x);
  ctx.Api.bump();
  release();
  await p;
  assert.equal(ctx.Api.noAdmin(), false);
});

test('Api: Cloudflare Access gate keeps the key and asks for a reload', async () => {
  const cases = [
    json(0, null, {type: 'opaqueredirect'}),
    json(200, null, {headers: headers({'content-type': 'text/html; charset=utf-8'})}),
    json(401, null, {headers: headers({'cf-access-domain': 'x', 'content-type': 'application/json'})}),
    json(200, {}, {redirected: true}),
  ];
  for (const r of cases) {
    const ctx = sandbox();
    let logged = 0;
    ctx.Api.onUnauthorized(() => logged++);
    ctx.Api.setKey('k');
    ctx.env.respond = async () => r;
    const e = await ctx.Api.get('/usage/v1/quota').catch(x => x);
    assert.equal(e.kind, 'access');
    assert.match(e.message, /Cloudflare Access/);
    assert.equal(ctx.Api.key(), 'k');
    assert.equal(logged, 0);
  }
});

test('Api: network error, timeout, abort and HTTP errors', async () => {
  const ctx = sandbox();
  ctx.Api.setKey('k');
  ctx.env.respond = async () => {
    throw new Error('boom');
  };
  let e = await ctx.Api.get('/x').catch(x => x);
  assert.equal(e.kind, 'network');
  assert.match(e.message, /boom/);
  ctx.env.respond = (url, init) => new Promise((_, rej) => init.signal.addEventListener('abort', () => rej(new Error('aborted'))));
  e = await ctx.Api.get('/x', {timeout: 20}).catch(x => x);
  assert.equal(e.kind, 'timeout');
  const ac = new AbortController();
  const p = ctx.Api.get('/x', {signal: ac.signal}).catch(x => x);
  ac.abort();
  e = await p;
  assert.equal(e.kind, 'aborted');
  assert.ok(ctx.Api.isQuiet(e));
  ctx.env.respond = async () => json(409, {error: {message: 'revision conflict'}});
  e = await ctx.Api.get('/admin/v1/config').catch(x => x);
  assert.equal(e.status, 409);
  assert.equal(e.body.error.message, 'revision conflict');
  assert.equal(ctx.Api.why(e), 'HTTP 409 · revision conflict');
  ctx.env.respond = async () => json(500, null);
  e = await ctx.Api.get('/usage/v1/records?x=1').catch(x => x);
  assert.equal(e.message, '/usage/v1/records → HTTP 500');
});

test('Api: strict calls reject a late success of a superseded identity', async () => {
  const ctx = sandbox();
  ctx.Api.setKey('k');
  let release;
  ctx.env.respond = () => new Promise(r => {
    release = () => r(json(200, {v: 1}));
  });
  const p = ctx.Api.get('/x', {strict: true}).catch(x => x);
  ctx.Api.bump();
  release();
  const e = await p;
  assert.equal(e.kind, 'stale');
  assert.ok(ctx.Api.isQuiet(e));
});

test('Api.busyRun: busy state, error toast, ignores a second press, quiet on 401', async () => {
  const ctx = sandbox();
  const cls = new Set(), attrs = {};
  const b = {textContent: 'Go', disabled: false, isConnected: true,
    classList: {add: c => cls.add(c), remove: c => cls.delete(c)},
    setAttribute: (k, v) => {
      attrs[k] = v;
    }, removeAttribute: k => delete attrs[k]};
  let release;
  const first = ctx.Api.busyRun(b, '保存', () => new Promise((_, rej) => {
    release = () => rej(Object.assign(new Error('bad'), {status: 400}));
  }), '保存中…');
  assert.ok(cls.has('busy'));
  assert.equal(b.textContent, '保存中…');
  assert.equal(attrs['aria-disabled'], 'true');
  assert.equal(await ctx.Api.busyRun(b, 'again', async () => {}), false);
  release();
  assert.equal(await first, false);
  assert.ok(!cls.has('busy'));
  assert.equal(b.textContent, 'Go');
  assert.equal(ctx.toasts.length, 1);
  assert.equal(ctx.toasts[0].text, '保存失败：HTTP 400 · bad');
  await ctx.Api.busyRun(b, 'x', async () => {
    throw new Error('401');
  });
  assert.equal(ctx.toasts.length, 1, '401 is handled by the login flow, not a toast');
});

// ---------- routing & scope ----------
test('routes and legacy links', () => {
  const {Shell} = sandbox();
  const r = (p, s, h) => Shell.parseRoute(p, s, h);
  assert.equal(r('/ui', '', '').view, 'overview');
  assert.equal(r('/ui/', '', '').view, 'overview');
  assert.equal(r('/ui/requests', '?fproblem=error', '').view, 'requests');
  assert.equal(r('/ui/settings', '', '#keys').hash, '#keys');
  assert.equal(r('/ui/unknown', '', '').view, 'overview');
  let x = r('/ui', '', '#system');
  assert.equal(x.view, 'system');
  assert.equal(x.redirect, '/ui/system');
  x = r('/ui', '?fproblem=error&fcause=quota&facct=alice&range=7d', '');
  assert.equal(x.view, 'requests');
  assert.equal(x.redirect, '/ui/requests?fproblem=error&fcause=quota&facct=alice&range=7d');
  for (const k of ['fthread', 'record', 'rid', 'foutcome', 'rfrom']) assert.equal(r('/ui', '?' + k + '=1', '').view, 'requests', k);
  assert.equal(r('/ui', '?range=7d&facct=a', '').redirect, undefined, 'scope-only links stay on the overview');
});

test('scope ↔ URL round trip, validation and stored range', () => {
  const {Shell} = sandbox();
  const q = s => new URLSearchParams(s);
  let s = Shell.scopeFromQuery(q('range=7d&facct=alice&fmodel=%20gpt-5%20&fkey=laptop&finstance=vps'), null);
  assert.equal(s.range, '7d');
  assert.equal(s.account, 'alice');
  assert.equal(s.model, 'gpt-5');
  assert.equal(s.from, null);
  const back = new URLSearchParams(Shell.scopeToQuery(s));
  assert.deepEqual(Shell.scopeFromQuery(back, null), s);
  s = Shell.scopeFromQuery(q('from=1000&to=5000'), null);
  assert.equal(s.from, 1000);
  assert.equal(s.to, 5000);
  assert.deepEqual(JSON.parse(JSON.stringify(Shell.scopeWindow(s))), {from: 1000, to: 5000, fixed: true});
  assert.equal(Shell.scopeFromQuery(q('from=5000&to=1000'), null).from, null, 'to must follow from');
  assert.equal(Shell.scopeFromQuery(q('from=0&to=' + 40 * 864e5), null).from, null, 'at most 31 days');
  assert.equal(Shell.scopeFromQuery(q('range=nope'), null).range, '24h');
  assert.equal(Shell.scopeFromQuery(q(''), '30d').range, '30d', 'no params: last chosen range');
  assert.equal(Shell.scopeFromQuery(q('facct=a'), '30d').range, '24h', 'a shared link does not pick up local state');
  const w = Shell.scopeWindow({range: '1h', from: null, to: null}, 10 * 36e5);
  assert.equal(w.to - w.from, 36e5);
  assert.equal(w.fixed, false);
  assert.deepEqual(JSON.parse(JSON.stringify(Shell.scopeToQuery({range: '24h', from: null, to: null, account: '', model: '',
    key: '', instance: ''}))), {});
});

test('account badge and usable-account banner counts come from backend flags only', () => {
  const {Shell} = sandbox();
  const quota = {accounts: [
    {account: 'a', disabled: false, needs_relogin: false},
    {account: 'b', disabled: true, needs_relogin: true},
    {account: 'c', disabled: false, needs_relogin: true},
    {account: 'd', configured: false, needs_relogin: true},
  ]};
  assert.equal(Shell.attentionCount(quota), 2);
  assert.equal(Shell.usableCount(quota), 1);
  assert.equal(Shell.usableCount({accounts: []}), 0);
  assert.equal(Shell.attentionCount(null), null);
});

test('Charts.niceScale gives round axis maxima', () => {
  const {Charts} = sandbox();
  const s = m => JSON.parse(JSON.stringify(Charts.niceScale(m)));
  assert.deepEqual(s(0), {max: 1, step: 0.25});
  assert.deepEqual(s(87), {max: 100, step: 25});
  assert.deepEqual(s(1234), {max: 1500, step: 500});
  assert.deepEqual(s(9), {max: 10, step: 2.5});
});

test('Charts.xTicks: hourly bars over days are labelled at day changes, labels never overlap', () => {
  const {Charts} = sandbox();
  const H = 36e5, t0 = Date.UTC(2026, 10, 10, 5);
  const xs = Array.from({length: 169}, (_, i) => t0 + i * H);
  const day = ms => String(Math.floor((ms - Date.UTC(2026, 10, 10)) / 864e5));
  const ticks = JSON.parse(JSON.stringify(Charts.xTicks(xs, day, 4, 676)));
  assert.equal(ticks[0].i, 19, 'the partial first day is not labelled when a midnight follows');
  assert.ok(ticks.every(tk => (xs[tk.i] - Date.UTC(2026, 10, 10)) % 864e5 === 0), 'dates sit at midnight');
  const hours = JSON.parse(JSON.stringify(Charts.xTicks(xs.slice(0, 25), ms => new Date(ms).toISOString().slice(11, 16), 40, 1000)));
  assert.equal(hours[0].i, 0);
  for (let k = 1; k < hours.length; k++) assert.ok((hours[k].i - hours[k - 1].i) * 40 >= 40, 'distinct labels are thinned');
});

test('every view honours the contract', () => {
  const {Views} = sandbox();
  for (const name of ['overview', 'requests', 'accounts', 'settings', 'system']) {
    const v = Views[name];
    assert.ok(v, name);
    assert.equal(typeof v.title, 'function', name + '.title');
    assert.equal(typeof v.mount, 'function', name + '.mount');
    assert.equal(typeof v.unmount, 'function', name + '.unmount');
    assert.equal(typeof v.title(), 'string');
    if (v.scope) for (const f of v.scope) assert.ok(['time', 'account', 'model', 'key', 'instance'].includes(f), name + ' scope ' + f);
  }
});

(async () => {
  let failed = 0;
  for (const {name, fn} of tests) {
    try {
      await fn();
      console.log('ok   ' + name);
    } catch (e) {
      failed++;
      console.log('FAIL ' + name + '\n     ' + (e.stack || e).toString().split('\n').slice(0, 4).join('\n     '));
    }
  }
  console.log(`\n${tests.length - failed}/${tests.length} passed`);
  process.exit(failed ? 1 : 0);
})();
