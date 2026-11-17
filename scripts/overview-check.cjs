// Overview / system view checks. Run from any directory: node scripts/overview-check.cjs
// Loads the real sources (foundation + views/overview.js + views/system.js) into a Node vm and checks
// the pure model: query construction (one scope for summary, trend and request drill-downs), bucket
// layout, previous-period cache key, deltas, unknown-vs-zero rules, the quota pool, drill-down
// parameters, stored preferences, latest-wins loading, memory warnings and the system badge sum.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {loadScripts, read} = require('./lib/page.cjs');

const tests = [];
const test = (name, fn) => tests.push({name, fn});

const FILES = ['core.js', 'i18n.js', 'i18n/zh.js', 'i18n/en.js', 'api.js', 'components.js', 'charts.js', 'shell.js',
  'views/overview.js', 'views/system.js'].map(f => 'ui/js/' + f);

function storage() {
  const m = new Map();
  return {getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k)};
}

function sandbox(lang) {
  const badgeEls = {system: {className: '', textContent: ''}};
  const env = {
    localStorage: storage(), sessionStorage: storage(), navigator: {language: lang === 'en' ? 'en-US' : 'zh-CN'}, Intl,
    fetch: async () => ({status: 200, ok: true, type: 'basic', headers: {get: () => 'application/json'}, json: async () => ({})}),
    document: {
      querySelectorAll: sel => {
        const m = /data-badge="(\w+)"/.exec(sel);
        return m && badgeEls[m[1]] ? [badgeEls[m[1]]] : [];
      },
      getElementById: () => null,
    },
  };
  const ctx = loadScripts(env, FILES);
  ctx.I18n.setLang(lang || 'zh', {silent: true});
  ctx.badgeEls = badgeEls;
  return ctx;
}

const M = c => c.Views.overview.model;
const tick = () => new Promise(r => setImmediate(r));
const ids = list => JSON.stringify(list.map(a => a.id));
const SYS = c => c.Views.system.model;

// ---------- i18n ----------
test('overview and system keys exist in both languages', () => {
  const keys = new Set();
  for (const f of ['views/overview.js', 'views/system.js']) {
    const src = read('ui/js/' + f);
    for (const m of src.matchAll(/"((?:overview|system)\.[\w.]+)":/g)) keys.add(m[1]);
    for (const m of src.matchAll(/\bt\("((?:overview|system)\.[\w.]+)"/g)) if (!m[1].endsWith('.')) keys.add(m[1]);
  }
  assert.ok(keys.size > 100);
  for (const lang of ['zh', 'en']) {
    const c = sandbox(lang);
    const missing = [...keys].filter(k => !c.I18n.has(k));
    assert.deepEqual(missing, [], lang + ' is missing keys');
  }
  // composed keys used through prefixes
  const c = sandbox('en');
  for (const p of ['overview.fc.', 'overview.m.', 'overview.g.', 'overview.gr.', 'overview.r.', 'overview.u.']) {
    assert.ok([...keys].some(k => k.startsWith(p)), p);
  }
  for (const g of M(c).GROUPS) assert.ok(c.I18n.has('overview.gr.' + g), g);
  for (const k of M(c).FCLS) assert.ok(c.I18n.has('overview.fc.' + k), k);
});

// ---------- queries ----------
test('summary query uses the scope parameters unchanged and picks the bucket', () => {
  const c = sandbox();
  const m = M(c);
  c.Shell.scope.set({account: 'alice', model: 'model-a', key: 'laptop', instance: 'home', range: '24h'}, {silent: true});
  const scope = c.Shell.scope.params(95400000);
  const q = m.summaryParams(scope, 'auto', 'account');
  for (const k of ['from', 'to', 'account', 'model', 'key', 'instance', 'exclude_prewarm']) assert.equal(q[k], scope[k], k);
  assert.equal(q.exclude_prewarm, 1, 'prewarm excluded by default');
  assert.equal(q.bucket, 'hour');
  assert.equal(q.group, 'account');
  assert.equal(q.tz, -new Date(scope.to).getTimezoneOffset());
  const week = {from: 0, to: 7 * 864e5};
  assert.equal(m.summaryParams(week, 'auto', 'model').bucket, 'day');
  assert.equal(m.summaryParams(week, 'hour', 'model').bucket, 'hour', 'a grain never changes the window');
  assert.equal(m.summaryParams(week, 'hour', 'model').to - m.summaryParams(week, 'hour', 'model').from, 7 * 864e5);
  assert.equal(m.summaryParams({from: 0, to: 864e5}, 'day', 'x').bucket, 'day');
  assert.equal(m.isHourly('auto', {from: 0, to: 2 * 864e5}), true);
  assert.equal(m.isHourly('auto', {from: 0, to: 2 * 864e5 + 1}), false);
  const occ = m.occParams({from: 1, to: 2, instance: '', tz: 480, bucket: 'hour', account: 'a'});
  assert.equal(JSON.stringify(occ), JSON.stringify({from: 1, to: 2, instance: '', tz: 480, step: '1h'}), 'occupancy: window + instance only');
});

test('previous period cache key: rolling windows drop the bounds, the group never matters', () => {
  const m = M(sandbox());
  const a = m.prevSig({from: 1000, to: 5000, account: 'x', group: 'account', bucket: 'hour'}, false);
  const b = m.prevSig({from: 2000, to: 6000, account: 'x', group: 'model', bucket: 'hour'}, false);
  assert.equal(a, b, 'same span, rolled forward');
  assert.notEqual(a, m.prevSig({from: 1000, to: 9000, account: 'x', bucket: 'hour'}, false), 'span changes the key');
  assert.notEqual(a, m.prevSig({from: 1000, to: 5000, account: 'y', bucket: 'hour'}, false), 'scope changes the key');
  assert.notEqual(m.prevSig({from: 1000, to: 5000}, true), m.prevSig({from: 2000, to: 6000}, true), 'fixed windows keep the bounds');
});

// ---------- buckets ----------
test('a rolling 24 hours covers 25 partial/whole hourly buckets', () => {
  const m = M(sandbox());
  const pts = m.trendPoints({from_ms: 1800000, to_ms: 88200000, buckets: [
    {start_ms: 3600000, count: 4, failures: 1, cancelled: 1, incomplete: 0, unknown: 0, prewarm_count: 1, usage_samples: 0,
      input_tokens: 0, output_tokens: 0, cost_usd: 0, unknown_price: 2},
  ]}, true, 0);
  assert.equal(pts.length, 25);
  assert.equal(pts[0].from, 1800000);
  assert.equal(pts[0].to, 3599999);
  assert.equal(pts[0].partial, true);
  assert.equal(pts[1].partial, false);
  assert.equal(pts.at(-1).from, 86400000);
  assert.equal(pts.at(-1).to, 88200000);
  assert.equal(pts[1].n, 4);
  assert.equal(pts[1].tokens, null, 'requests without reported usage: tokens unknown, not 0');
  assert.equal(pts[2].tokens, 0, 'an empty bucket is a real 0');
  assert.equal(pts[1].records, 5);
  // previous period aligned from the end
  const prev = m.trendPoints({from_ms: 0, to_ms: 3 * 36e5 - 1, buckets: []}, true, 0);
  const al = m.alignPrev(pts, prev);
  assert.equal(al.length, 25);
  assert.equal(al[0], null);
  assert.equal(al.at(-1), prev.at(-1));
  assert.equal(m.alignPrev(pts, null), null);
});

test('occupancy folds instances per bucket; peak over the range', () => {
  const m = M(sandbox());
  const occ = {rows: [
    {minute_ms: 1, inflight_peak: 3, ws_peak: 1, max_inflight_requests: 64, ws_max_connections: 0, full_minutes: 1, inflight_avg: 1, ws_avg: 0.5},
    {minute_ms: 1, inflight_peak: 9, ws_peak: 0, max_inflight_requests: 32, ws_max_connections: 8, full_minutes: 2, inflight_avg: 2, ws_avg: 0},
    {minute_ms: 2, inflight_peak: 1, ws_peak: 4, max_inflight_requests: 64, ws_max_connections: 8, full_minutes: 0},
  ]};
  const by = m.occByBucket(occ).get(1);
  assert.equal(JSON.stringify(by), JSON.stringify({inf: 9, ws: 1, li: 64, lw: 8, full: 3, ia: 3, wa: 0.5}));
  assert.equal(JSON.stringify(m.occPeak(occ)), JSON.stringify({inf: 9, ws: 4, li: 64, lw: 8, full: 3}));
  assert.equal(m.occPeak(null), null);
  assert.equal(m.occPeak({rows: []}), null);
});

// ---------- figures ----------
test('deltas: relative, points and milliseconds; colour only where direction has a meaning', () => {
  const m = M(sandbox());
  assert.equal(JSON.stringify(m.delta(132, 100, 'rel', null)), JSON.stringify({text: '+32%', tone: ''}));
  assert.equal(JSON.stringify(m.delta(0.14, 0.04, 'pt', 'down')), JSON.stringify({text: '+10pt', tone: 'bad'}));
  assert.equal(JSON.stringify(m.delta(1500, 1640, 'ms', 'down')), JSON.stringify({text: '−140ms', tone: 'good'}));
  assert.equal(JSON.stringify(m.delta(173.4, 173.6, 'rel', 'up')), JSON.stringify({text: '−0.1%', tone: 'bad'}));
  assert.equal(m.delta(5, 5, 'rel', 'up').text, '±0');
  assert.equal(m.delta(5, 5, 'rel', 'up').tone, '');
  assert.equal(m.delta(5, 0, 'rel', null), null, 'no relative change from zero');
  assert.equal(m.delta(null, 3, 'ms', 'down'), null, 'unknown stays unknown');
  // hero deltas need a previous period with requests; unpriced costs are not compared
  const tot = {count: 10, failure_rate: 0.1, stream_ttft_p50_ms: 100, generation_tps: 10, usage_samples: 5, input_tokens: 10,
    output_tokens: 10, cost_usd: 2, unknown_price: 0};
  assert.equal(JSON.stringify(m.heroDeltas(tot, {count: 0})), '{}');
  const d = m.heroDeltas(tot, Object.assign({}, tot, {count: 5, unknown_price: 3}));
  assert.equal(d.requests.text, '+100%');
  assert.equal(d.cost, null);
  assert.equal(m.heroDeltas(tot, Object.assign({}, tot, {usage_samples: 0})).tokens, null, 'previous tokens unknown');
});

test('unknown is not zero: tokens and estimated cost', () => {
  const zh = sandbox('zh'), en = sandbox('en');
  const m = M(zh);
  assert.equal(m.tokenTotal({count: 3, prewarm_count: 0, usage_samples: 0, input_tokens: 0, output_tokens: 0}), null);
  assert.equal(m.tokenTotal({count: 0, prewarm_count: 0, usage_samples: 0}), 0, 'nothing to report is 0');
  assert.equal(m.tokenTotal({count: 3, usage_samples: 2, input_tokens: 5, output_tokens: 7}), 12);
  assert.equal(m.costText(0, 4), '未定价', 'nothing priced is never $0.00');
  assert.equal(M(en).costText(0, 4), 'Unpriced');
  assert.equal(m.costText(1.5, 4), '$1.50', 'the priced part is shown');
  assert.equal(m.costText(0, 0), '$0.00');
  assert.equal(m.costText(null, 0), '—');
});

// ---------- quota pool ----------
test('quota pool follows window_minutes; unknown accounts stay unknown; disabled are left out', () => {
  const c = sandbox('zh');
  const m = M(c);
  const win = (pct, mins, resets) => ({used_pct: pct, resets_at: resets || null, window_minutes: mins});
  const accts = [
    {account: 'plus', quota: {primary: win(60, 300), secondary: win(20, 10080)}},
    {account: 'pro', quota: {primary: win(30, 10080), secondary: {used_pct: null, window_minutes: null}}},
    {account: 'free', quota: {primary: win(10, 43200), secondary: {used_pct: null}}},
    {account: 'new', quota: null},
    {account: 'off', disabled: true, quota: {primary: win(0, 300)}},
    {account: 'gone', configured: false, quota: {primary: win(0, 300)}},
  ];
  const five = m.pool(accts, 's', 0);
  assert.equal(ids(five.members), '["plus","new"]');
  assert.equal(five.members[1].rem, null);
  assert.equal(five.avg, 40, 'average over known 5h windows only');
  const week = m.pool(accts, 'l', 0);
  assert.equal(ids(week.members), '["plus","pro","free","new"]');
  assert.equal(m.pool([accts[1]], 's', 0), null, 'only pro accounts: no 5h pool');
  assert.equal(m.slots(accts[1].quota).s, null, 'pro primary weekly window is never the 5h slot');
  assert.equal(m.slots(accts[1].quota).l.label, '周');
  assert.equal(m.remaining(win(70, 300, 1000), 999999), 30);
  assert.equal(m.remaining(win(70, 300, 1000), 1000001), 100, 'a window past its reset is full again');
  assert.equal(m.remaining({used_pct: null}), null);
  assert.equal(m.pool(null, 's'), null);
});

// ---------- drill-downs ----------
test('drill-downs use the request view parameters', () => {
  const m = M(sandbox());
  const j = x => JSON.stringify(x);
  assert.equal(j(m.drillParams('failed')), j({fproblem: 'error', fcause: ''}));
  assert.equal(j(m.drillParams('failed', 'rate_limit')), j({fproblem: 'error', fcause: 'rate_limit'}));
  assert.equal(m.drillParams('failed', 'local').fcause, 'gateway');
  assert.equal(m.drillParams('failed', 'other').fcause, 'gateway');
  assert.equal(j(m.drillParams('cancelled')), j({fproblem: 'unfinished', fcause: 'client_closed'}));
  assert.equal(j(m.drillParams('unknown')), j({fproblem: 'unfinished', fcause: 'unconfirmed'}));
  assert.equal(j(m.drillParams('incomplete')), j({foutcome: 'incomplete'}));
  assert.equal(j(m.drillParams('refused')), j({fproblem: 'refused'}));
  assert.equal(j(m.drillParams('business')), j({foutcome: 'business'}));
  // grouped rows
  assert.equal(j(m.groupAction('account', 'alice')), j({scope: {account: 'alice'}}));
  assert.equal(j(m.groupAction('instance', 'home')), j({scope: {instance: 'home'}}));
  assert.equal(j(m.groupAction('error', 'usage_limit')), j({params: {fcode: 'usage_limit', fproblem: 'error'}}));
  assert.equal(j(m.groupAction('err_source', 'stream')), j({params: {fsrc: 'stream', fproblem: 'error'}}));
  assert.equal(j(m.groupAction('service_tier', 'flex')), j({params: {ftier: 'flex'}}), 'tier rows are not limited to errors');
  assert.equal(m.groupAction('endpoint', 'responses'), null);
  assert.equal(m.groupAction('account', null), null, 'the (none) row has no action');
  // bucket: the bucket inside a fixed copy of the scope window
  assert.equal(j(m.bucketParams({from: 1, to: 9}, {from: 3, to: 5}, 'requests')), j({from: 1, to: 9, rfrom: 3, rto: 5}));
  assert.equal(m.bucketParams({from: 1, to: 9}, {from: 3, to: 5}, 'fail').fproblem, 'error');
});

test('Shell.href carries the scope into the request view', () => {
  const c = sandbox();
  // the request view (another file) declares the scope it uses; only that declaration matters here
  c.Views.requests = {scope: ['time', 'account', 'model', 'key', 'instance']};
  c.Shell.scope.set({account: 'bob', range: '7d'}, {silent: true});
  const u = new URL(c.Shell.href('requests', M(c).drillParams('failed', 'quota')), 'http://x');
  assert.equal(u.pathname, '/ui/requests');
  assert.equal(u.searchParams.get('facct'), 'bob');
  assert.equal(u.searchParams.get('range'), '7d');
  assert.equal(u.searchParams.get('fproblem'), 'error');
  assert.equal(u.searchParams.get('fcause'), 'quota');
  assert.equal(u.searchParams.has('fcause') && !u.searchParams.has('foutcome'), true);
});

test('group names: tier and source labels, raw values otherwise, (none) for null', () => {
  const zh = M(sandbox('zh')), en = M(sandbox('en'));
  assert.equal(zh.groupName('service_tier', 'standard'), '标准');
  assert.equal(en.groupName('service_tier', 'priority'), 'Fast');
  assert.equal(en.groupName('err_source', 'stream'), 'In-stream');
  assert.equal(zh.groupName('model', '<img src=x>'), '<img src=x>', 'text, never markup');
  assert.equal(zh.groupName('account', null), '（无）');
  assert.equal(en.groupName('error', 'usage_limit'), 'usage_limit');
});

test('preferences: URL first (old names), then this browser, then defaults; compare on by default', () => {
  const m = M(sandbox());
  const store = o => k => (k in o ? o[k] : null);
  const q = s => new URLSearchParams(s);
  assert.equal(JSON.stringify(m.readPrefs(q(''), store({}))),
    JSON.stringify({metric: 'requests', mode: 'auto', group: 'account', compare: true}));
  const p = m.readPrefs(q('tmetric=lat&group=error'), store({ciallopa_f_tmetric: 'cost', ciallopa_f_tmode: 'day', ciallopa_compare: '0'}));
  assert.equal(p.metric, 'lat');
  assert.equal(p.mode, 'day');
  assert.equal(p.group, 'error');
  assert.equal(p.compare, false);
  const bad = m.readPrefs(q('tmetric=nope&tmode=x&group=y'), store({ciallopa_f_group: 'also-bad'}));
  assert.equal(JSON.stringify([bad.metric, bad.mode, bad.group]), JSON.stringify(['requests', 'auto', 'account']));
});

// ---------- races ----------
test('latest-wins loads: a late answer or failure of an older query is dropped', async () => {
  const m = M(sandbox());
  const L = m.latest();
  const applied = [];
  let releaseOld, failOld;
  const old = L.run(() => new Promise(r => (releaseOld = r)), d => applied.push(d));
  await tick();
  const newer = L.run(async () => 'new', d => applied.push(d));
  assert.equal(await newer, true);
  releaseOld('old');
  assert.equal(await old, false);
  assert.deepEqual(applied, ['new']);
  // an older failure is dropped, the current one is reported
  const oldFail = L.run(() => new Promise((_, rej) => (failOld = rej)), () => {});
  await tick();
  const cur = L.run(async () => 'ok', d => applied.push(d));
  failOld(new Error('late'));
  assert.equal(await oldFail, false);
  assert.equal(await cur, true);
  await assert.rejects(L.run(async () => {
    throw new Error('now');
  }, () => {}), /now/);
  // the fetcher can ask whether it is still current (passed to ctx.get as `current`)
  let probe;
  const p = L.run(cur2 => {
    probe = cur2;
    return Promise.resolve(1);
  }, () => {});
  await Promise.resolve();
  L.bump();
  assert.equal(probe(), false);
  assert.equal(await p, false);
});

// ---------- system ----------
test('memory warnings: buffer at 80 % of a set limit, WebSocket at a set cap; no limit, no warning', () => {
  const s = SYS(sandbox());
  const MiB = 1 << 20;
  const mem = (buf, lim, ws, cap) => ({now: {buffered_body_bytes: buf, ws_connections: ws}, buffered_body_limit_bytes: lim,
    ws_max_connections: cap, history: []});
  assert.equal(ids(s.memAlerts(mem(220 * MiB, 256 * MiB, 24, 24))), '["buffer","ws"]');
  assert.equal(ids(s.memAlerts(mem(200 * MiB, 256 * MiB, 23, 24))), '[]');
  assert.equal(ids(s.memAlerts(mem(999 * MiB, 0, 999, 0))), '[]', '0 = no limit');
  assert.equal(s.memAlerts(null).length, 0);
  assert.equal(JSON.stringify(s.usage(5, 0)), JSON.stringify({ratio: null, unlimited: true}));
  assert.equal(s.usage(5, 10).ratio, 0.5);
  assert.equal(s.recorderGap({dropped: 0, write_errors: 0}), false);
  assert.equal(s.recorderGap({dropped: 0, write_errors: 2}), true);
  assert.equal(s.recorderGap(null), false);
  const m = mem(0, 1, 0, 0);
  m.history = [{at_ms: 1}, {at_ms: 2}];
  m.now.at_ms = 3;
  assert.equal(JSON.stringify(s.samples(m).map(x => x.at_ms)), '[1,2,3]', 'history then the current sample');
  assert.equal(s.samples(null).length, 0);
});

test('system badge sums its sources; the memory probe makes no request for a usage key', async () => {
  const c = sandbox();
  const badge = c.badgeEls.system;
  c.Shell.systemAlert('egress-block', 1);
  c.Views.system.noteMemory({now: {buffered_body_bytes: 9, ws_connections: 2}, buffered_body_limit_bytes: 10, ws_max_connections: 2});
  assert.equal(badge.textContent, '3');
  c.Views.system.noteRecorder({dropped: 1});
  c.Views.system.noteRuntime({runtime: {runtime: {draining: true}}});
  assert.equal(badge.textContent, '5');
  c.Shell.systemAlert('egress-block', 0);
  c.Views.system.noteRuntime({runtime: null});
  c.Views.system.noteRecorder({dropped: 0, write_errors: 0});
  assert.equal(badge.textContent, '2');
  c.Views.system.noteMemory(null);
  assert.equal(badge.textContent, '', 'no sources, no badge');
  // usage key: no request, memory source cleared
  c.Shell.systemAlert('memory', 2);
  let calls = 0;
  c.Api.noAdmin = () => true;
  const r = await c.Views.system.probeMemory({get: async () => {
    calls++;
    return {};
  }});
  assert.equal(r, null);
  assert.equal(calls, 0);
  assert.equal(badge.textContent, '');
  // a newer probe wins over a slower older one
  c.Api.noAdmin = () => false;
  const replies = [];
  const ctx = {get: (p, o) => new Promise(res => replies.push({res, o}))};
  const p1 = c.Views.system.probeMemory(ctx), p2 = c.Views.system.probeMemory(ctx);
  assert.equal(replies[0].o.current(), false, 'the older probe is no longer current');
  replies[1].res({now: {buffered_body_bytes: 0, ws_connections: 0}, buffered_body_limit_bytes: 1, ws_max_connections: 0});
  replies[0].res({now: {buffered_body_bytes: 1, ws_connections: 0}, buffered_body_limit_bytes: 1, ws_max_connections: 0});
  await Promise.all([p1, p2]);
  assert.equal(badge.textContent, '', 'the late older snapshot did not raise the badge');
});

// ---------- source rules ----------
test('overview and system sources: no markup from data, no inline styles', () => {
  for (const f of ['views/overview.js', 'views/system.js']) {
    const src = fs.readFileSync(path.join(__dirname, '../crates/server/src/ui/js', f), 'utf8');
    assert.ok(!/innerHTML|insertAdjacentHTML|outerHTML\s*=|\.style\.(?!setProperty)\w+\s*=|eval\(|new Function/.test(src), f);
    src.split('\n').forEach((l, i) => assert.ok(l.length <= 140, f + ':' + (i + 1) + ' longer than 140 chars'));
  }
});

(async () => {
  let failed = 0;
  for (const t of tests) {
    try {
      await t.fn();
      console.log('ok   ' + t.name);
    } catch (e) {
      failed++;
      console.log('FAIL ' + t.name + '\n     ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n     '));
    }
  }
  console.log('\n' + (tests.length - failed) + '/' + tests.length + ' passed');
  process.exit(failed ? 1 : 0);
})();
