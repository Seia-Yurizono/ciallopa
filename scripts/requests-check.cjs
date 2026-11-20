// Requests view regression checks. Run from any directory: node scripts/requests-check.cjs
// Loads the real ui/js sources (ui.rs manifest order) into a Node vm. Pure logic (RequestsModel,
// RequestsText) is called directly; the view's async behaviour (late responses, paging, session
// resolution, held / pending refreshes, detail loads) runs against a small DOM stub with a
// scripted ctx.get, so every request is answered by the test in the order it chooses.
const assert = require('node:assert/strict');
const {loadScripts, vm} = require('./lib/page.cjs');

const tests = [];
const test = (name, fn) => tests.push({name, fn});

// ---------- minimal DOM ----------
const SPLIT = '(min-width: 1280px)';
function makeDom() {
  const doc = {activeElement: null, listeners: {}};
  const camel = s => s.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
  const kebab = s => s.replace(/[A-Z]/g, c => '-' + c.toLowerCase());
  class Text {
    constructor(s) {
      this.nodeType = 3;
      this.data = String(s);
      this.parentNode = null;
    }
    get textContent() { return this.data; }
    set textContent(v) { this.data = String(v); }
    remove() { if (this.parentNode) this.parentNode.removeChild(this); }
  }
  // selector: comma list of descendant chains of compounds tag.cls[attr="v"][attr^="v"][attr]
  function parseCompound(s) {
    const c = {tag: null, cls: [], attrs: []};
    const re = /([a-zA-Z][\w-]*)|\.([\w-]+)|\[([\w-]+)(?:(\^?)="([^"]*)")?\]/g;
    let m;
    while ((m = re.exec(s))) {
      if (m[1]) c.tag = m[1].toUpperCase();
      else if (m[2]) c.cls.push(m[2]);
      else c.attrs.push([m[3], m[5], m[4]]);
    }
    return c;
  }
  const parseSel = sel => sel.split(',').map(s => s.trim().match(/(?:[^\s"\[]|\[[^\]]*\])+/g).map(parseCompound));
  const matchCompound = (el, c) => (!c.tag || el.tagName === c.tag) && c.cls.every(x => el.classList.contains(x)) &&
    c.attrs.every(([k, v, pre]) => el.hasAttribute(k) &&
      (v === undefined || (pre ? el.getAttribute(k).startsWith(v) : el.getAttribute(k) === v)));
  function matchOne(el, chain) {
    if (!matchCompound(el, chain[chain.length - 1])) return false;
    let i = chain.length - 2;
    for (let n = el.parentNode; n && n.nodeType === 1 && i >= 0; n = n.parentNode) if (matchCompound(n, chain[i])) i--;
    return i < 0;
  }
  class El {
    constructor(tag) {
      this.nodeType = 1;
      this.tagName = tag.toUpperCase();
      this.childNodes = [];
      this.parentNode = null;
      this.attrs = new Map();
      this.listeners = {};
      this.props = {};
      const self = this;
      this.style = {setProperty(k, v) { this[k] = v; }, removeProperty(k) { delete this[k]; }};
      this.dataset = new Proxy({}, {
        get: (_, k) => self.getAttribute('data-' + kebab(String(k))) ?? undefined,
        set: (_, k, v) => { self.setAttribute('data-' + kebab(String(k)), v); return true; },
        has: (_, k) => self.hasAttribute('data-' + kebab(String(k))),
        deleteProperty: (_, k) => { self.removeAttribute('data-' + kebab(String(k))); return true; },
      });
      this.classList = {
        list: () => (self.getAttribute('class') || '').split(/\s+/).filter(Boolean),
        contains: c => self.classList.list().includes(c),
        add: (...cs) => self.setAttribute('class', [...new Set(self.classList.list().concat(cs))].join(' ')),
        remove: (...cs) => self.setAttribute('class', self.classList.list().filter(x => !cs.includes(x)).join(' ')),
        toggle: (c, force) => {
          const on = force === undefined ? !self.classList.contains(c) : !!force;
          if (on) self.classList.add(c); else self.classList.remove(c);
          return on;
        },
      };
    }
    get children() { return this.childNodes.filter(n => n.nodeType === 1); }
    get firstChild() { return this.childNodes[0] || null; }
    get firstElementChild() { return this.children[0] || null; }
    get parentElement() { return this.parentNode && this.parentNode.nodeType === 1 ? this.parentNode : null; }
    get nextElementSibling() {
      if (!this.parentNode) return null;
      const sib = this.parentNode.children;
      return sib[sib.indexOf(this) + 1] || null;
    }
    get isConnected() { let n = this; while (n.parentNode) n = n.parentNode; return n === doc.body || n === doc.documentElement; }
    get textContent() { return this.childNodes.map(n => n.textContent).join(''); }
    set textContent(v) { for (const n of this.childNodes) n.parentNode = null; this.childNodes = []; if (v !== '') this.appendChild(new Text(v)); }
    get className() { return this.getAttribute('class') || ''; }
    set className(v) { this.setAttribute('class', v); }
    get id() { return this.getAttribute('id') || ''; }
    set id(v) { this.setAttribute('id', v); }
    get title() { return this.getAttribute('title') || ''; }
    set title(v) { this.setAttribute('title', v); }
    get disabled() { return this.hasAttribute('disabled'); }
    set disabled(v) { if (v) this.setAttribute('disabled', ''); else this.removeAttribute('disabled'); }
    get tabIndex() { return Number(this.getAttribute('tabindex') ?? -1); }
    set tabIndex(v) { this.setAttribute('tabindex', String(v)); }
    get value() { return this.props.value ?? this.getAttribute('value') ?? ''; }
    set value(v) { this.props.value = String(v); }
    get checked() { return !!this.props.checked; }
    set checked(v) { this.props.checked = !!v; }
    get type() { return this.getAttribute('type') || ''; }
    get offsetParent() { return null; }
    setAttribute(k, v) { this.attrs.set(k, String(v)); }
    getAttribute(k) { return this.attrs.has(k) ? this.attrs.get(k) : null; }
    hasAttribute(k) { return this.attrs.has(k); }
    removeAttribute(k) { this.attrs.delete(k); }
    appendChild(n) {
      if (n.parentNode) n.parentNode.removeChild(n);
      n.parentNode = this;
      this.childNodes.push(n);
      return n;
    }
    append(...ns) { for (const n of ns) this.appendChild(typeof n === 'object' ? n : new Text(n)); }
    insertBefore(n, ref) {
      if (!ref) return this.appendChild(n);
      if (n.parentNode) n.parentNode.removeChild(n);
      n.parentNode = this;
      this.childNodes.splice(this.childNodes.indexOf(ref), 0, n);
      return n;
    }
    removeChild(n) {
      const i = this.childNodes.indexOf(n);
      if (i >= 0) this.childNodes.splice(i, 1);
      n.parentNode = null;
      if (doc.activeElement && (doc.activeElement === n || (n.contains && n.contains(doc.activeElement)))) doc.activeElement = null;
      return n;
    }
    remove() { if (this.parentNode) this.parentNode.removeChild(this); }
    replaceWith(n) {
      const p = this.parentNode;
      if (!p) return;
      p.insertBefore(n, this);
      p.removeChild(this);
    }
    contains(n) { for (; n; n = n.parentNode) if (n === this) return true; return false; }
    all() { const out = []; const walk = e => { for (const c of e.children) { out.push(c); walk(c); } }; walk(this); return out; }
    matches(sel) { return parseSel(sel).some(c => matchOne(this, c)); }
    closest(sel) { for (let n = this; n && n.nodeType === 1; n = n.parentNode) if (n.matches(sel)) return n; return null; }
    querySelectorAll(sel) { const cs = parseSel(sel); return this.all().filter(e => cs.some(c => matchOne(e, c))); }
    querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
    addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); }
    removeEventListener(t, f) { this.listeners[t] = (this.listeners[t] || []).filter(x => x !== f); }
    dispatch(type, extra) {
      const ev = Object.assign({type, target: this, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; },
        stopPropagation() { this.stopped = true; }}, extra || {});
      for (let n = this; n && !ev.stopped; n = n.parentNode) for (const f of (n.listeners && n.listeners[type]) || []) f(ev);
      return ev;
    }
    click() { if (!this.disabled) this.dispatch('click'); }
    focus() { doc.activeElement = this; }
    blur() { if (doc.activeElement === this) doc.activeElement = doc.body; }
    getBoundingClientRect() { return {top: 0, bottom: 0, left: 0, right: 0, width: 0, height: 0}; }
    scrollIntoView() {}
  }
  doc.createElement = t => new El(t);
  doc.createElementNS = (_, t) => new El(t);
  doc.createTextNode = s => new Text(s);
  doc.documentElement = new El('html');
  doc.body = new El('body');
  doc.documentElement.appendChild(doc.body);
  doc.activeElement = doc.body;
  doc.scrollingElement = {scrollTop: 0};
  doc.getElementById = id => doc.body.querySelector('[id="' + id + '"]');
  doc.querySelector = s => doc.body.querySelector(s);
  doc.querySelectorAll = s => doc.body.querySelectorAll(s);
  doc.addEventListener = (t, f) => { (doc.listeners[t] = doc.listeners[t] || []).push(f); };
  doc.removeEventListener = (t, f) => { doc.listeners[t] = (doc.listeners[t] || []).filter(x => x !== f); };
  doc.elementFromPoint = () => null;
  doc.contains = n => doc.documentElement.contains(n);
  doc.hidden = false;
  return doc;
}

function storage() {
  const m = new Map();
  return {getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k)};
}

function sandbox(o) {
  o = o || {};
  const document = makeDom();
  const loc = new URL('http://local' + (o.url || '/ui/requests'));
  const env = {
    document, localStorage: storage(), sessionStorage: storage(), navigator: {language: 'zh-CN', clipboard: {writeText: async () => {}}},
    Intl, CSS: {escape: s => String(s)}, innerWidth: 1440, innerHeight: 900, scrollY: 0,
    location: {get href() { return loc.href; }, get pathname() { return loc.pathname; }, get search() { return loc.search; },
      get hash() { return loc.hash; }, origin: 'http://local'},
    history: {state: null, replaceState(_, __, u) { const n = new URL(u, 'http://local'); loc.pathname = n.pathname; loc.search = n.search; loc.hash = n.hash; },
      pushState(_, __, u) { this.replaceState(null, null, u); }},
    matchMedia: q => ({matches: q === SPLIT ? o.split !== false : false, addEventListener() {}, removeEventListener() {}}),
    getComputedStyle: () => ({overflowY: 'visible', position: 'static'}),
    getSelection: () => ({toString: () => ''}),
    scrollBy() {}, scrollTo() {}, addEventListener() {}, removeEventListener() {},
    requestAnimationFrame: f => setTimeout(f, 0),
    fetch: async () => { throw new Error('no fetch in requests-check'); },
  };
  const c = loadScripts(env);
  c.window = c;
  c.I18n.setLang(o.lang || 'zh', {persist: false});
  c.toasts = [];
  c.UI.toast = (text, opts) => c.toasts.push({text, opts});
  c.visits = [];
  c.Shell.go = url => c.visits.push(url);
  c.loc = loc;
  return c;
}

// A view ctx whose get() hands every request to the test.
function fakeCtx(c) {
  const calls = [];
  const subs = {refresh: [], scope: [], lang: [], cleanup: []};
  let alive = true;
  const root = c.document.createElement('main');
  c.document.body.appendChild(root);
  const layers = c.document.createElement('div');
  layers.setAttribute('id', 'layers');
  c.document.body.appendChild(layers);
  const ctx = {
    root, name: 'requests',
    route: {view: 'requests', query: new URLSearchParams(c.loc.search)},
    alive: () => alive,
    get(path, opts) {
      return new Promise((resolve, reject) => {
        const call = {path, url: new URL(path, 'http://local'), opts, done: false,
          ok(data) {
            call.done = true;
            if (!alive || (opts && opts.current && !opts.current())) reject(Object.assign(new Error('stale'), {kind: 'stale'}));
            else resolve(data);
          },
          fail(status, message) {
            call.done = true;
            reject(Object.assign(new Error(message || 'HTTP ' + status), {status}));
          }};
        calls.push(call);
      });
    },
    setParams(patch) {
      const q = new URLSearchParams(c.loc.search);
      for (const k in patch) {
        const v = patch[k];
        if (v === '' || v === null || v === undefined || v === false) q.delete(k); else q.set(k, String(v));
      }
      c.loc.search = q.toString() ? '?' + q : '';
    },
    onRefresh: f => subs.refresh.push(f),
    onScope: f => subs.scope.push(c.Shell.scope.onChange(f)),
    onLang: f => subs.lang.push(f),
    onCleanup: f => subs.cleanup.push(f),
    listen() {},
    busy: p => p,
    fails: [],
    fail(e) { ctx.fails.push(e); },
  };
  ctx.calls = calls;
  ctx.subs = subs;
  ctx.kill = () => { alive = false; };
  // latest unanswered call whose path starts with prefix
  ctx.open = prefix => calls.filter(x => !x.done && x.path.startsWith(prefix)).at(-1);
  ctx.openAll = prefix => calls.filter(x => !x.done && x.path.startsWith(prefix));
  return ctx;
}

const tick = async (n) => { for (let i = 0; i < (n || 4); i++) await new Promise(r => setImmediate(r)); };
const rec = (id, extra) => Object.assign({request_id: id, ts_ms: 1794832351275, model: 'gpt-5', status: 200, outcome: 'completed',
  transport: 'sse', duration_ms: 1000, ttft_ms: 200, input_tokens: 10, output_tokens: 5, account: 'alice', next: [], has_detail: false}, extra);
const ids = (from, n) => Array.from({length: n}, (_, i) => (from - i).toString(16).padStart(16, '0'));

function mountView(c) {
  const ctx = fakeCtx(c);
  c.Shell.scope.set(c.Shell.scopeFromQuery(new URLSearchParams(c.loc.search)), {silent: true});
  c.Views.requests.mount(ctx.root, ctx);
  return ctx;
}
async function answerInitial(ctx, rows, extra) {
  await tick();
  ctx.open('/usage/v1/records?').ok(Object.assign({records: rows, total: rows.length, next_before_id: null}, extra || {}));
  const cnt = ctx.open('/usage/v1/records/counts');
  if (cnt) cnt.ok({all: rows.length, error: 0, upstream: 0, unfinished: 0, refused: 0, causes: {}});
  const sum = ctx.open('/usage/v1/summary');
  if (sum) sum.ok({summary: {buckets: []}});
  await tick();
}
const M = c => vm.runInContext('RequestsModel', c);
const X = c => vm.runInContext('RequestsText', c);
const S = c => c.Views.requests._state();

// ---------- pure logic ----------
test('URL parsing keeps the old names, rejects unknown values and needs a fixed window for rfrom/rto', () => {
  const c = sandbox();
  const m = M(c);
  const q = new URLSearchParams('fproblem=error&fcause=quota&foutcome=nope&fclass=network&ftier=dg&fsrc=upstream,stream,network,local' +
    '&fcode=usage_limit&fthread=%20abc%20&ferr=1&fretry=yes&rid=1A1E540EE3300001&rfrom=100&rto=200');
  let st = m.parse(q, {from: 0, to: 1000, fixed: true});
  assert.deepEqual(JSON.parse(JSON.stringify(st.f)), {fproblem: 'error', fcause: 'quota', foutcome: '', fclass: 'network', ftier: 'dg',
    fsrc: 'upstream,stream,network,local', fcode: 'usage_limit', fthread: 'abc', ferr: '1', fretry: ''});
  assert.deepEqual(JSON.parse(JSON.stringify(st.win)), {from: 100, to: 200});
  assert.equal(st.record, '1a1e540ee3300001', 'rid is an alias of record, lower-cased');
  assert.equal(m.parse(q, {from: 0, to: 1000, fixed: false}).win, null, 'a rolling window drops rfrom/rto');
  assert.equal(m.parse(q, {from: 150, to: 1000, fixed: true}).win, null, 'a period outside the window is dropped');
  st = m.parse(new URLSearchParams('fproblem=upstream&fcause=quota&record=xyz'), null);
  assert.equal(st.f.fcause, '', 'a cause of another group is dropped');
  assert.equal(st.record, '');
  const p = m.toParams({f: Object.assign(m.emptyFilters(), {fproblem: 'refused'}), win: {from: 1, to: 2}, record: 'ab'});
  assert.equal(p.fproblem, 'refused');
  assert.equal(p.rfrom, '1');
  assert.equal(p.rid, '', 'rid is folded into record');
  assert.equal(p.fcause, '');
});

test('records query: prewarm, business, tier, session and cause rules', () => {
  const c = sandbox();
  const m = M(c);
  const scope = {from: 10, to: 20, account: 'a', model: 'm', key: 'k', instance: 'i', exclude_prewarm: 1};
  const st = f => ({f: Object.assign(m.emptyFilters(), f), win: null, record: ''});
  const q = (f, pw, win) => new URLSearchParams(m.qs(m.recordQuery(Object.assign(st(f), {win: win || null}), scope, pw)));
  assert.equal(q({}).get('exclude_prewarm'), '1', 'all outcomes hide prewarm by default');
  assert.equal(q({}, true).has('exclude_prewarm'), false, 'include prewarm widens the list');
  assert.equal(q({foutcome: 'business'}, true).get('exclude_prewarm'), '1', 'business always excludes prewarm');
  assert.equal(q({foutcome: 'business'}).has('outcome'), false);
  assert.equal(q({foutcome: 'prewarm'}).has('exclude_prewarm'), false, 'the prewarm outcome is never self-excluded');
  assert.equal(q({ftier: 'dg'}).get('downgraded'), '1');
  assert.equal(q({ftier: 'dg'}).has('tier'), false);
  assert.equal(q({fcause: 'quota'}).has('cause'), false, 'a cause needs its problem group');
  assert.equal(q({fproblem: 'error', fcause: 'quota'}).get('cause'), 'quota');
  const th = q({fthread: 'abcdefgh'});
  assert.ok(!th.has('from') && !th.has('to'), 'a session ignores the time range');
  assert.equal(th.get('account'), 'a', 'a session keeps the shared scope');
  const thw = q({fthread: 'abcdefgh'}, false, {from: 12, to: 14});
  assert.deepEqual([thw.get('from'), thw.get('to')], ['12', '14'], 'a chart period still applies to a session');
  assert.deepEqual(['errors', 'retried', 'source', 'error', 'failure_class'].map(k => q({ferr: '1', fretry: '1', fsrc: 'client', fcode: 'x',
    fclass: 'network'}).get(k)), ['1', '1', 'client', 'x', 'network']);
  const cq = new URLSearchParams(m.qs(m.countQuery(st({fproblem: 'error', fcause: 'quota', foutcome: 'refused'}), scope, false)));
  assert.ok(!cq.has('problem') && !cq.has('cause') && !cq.has('outcome'), 'counts drop problem / cause and the refused outcome');
  assert.equal(cq.get('exclude_prewarm'), '1');
  const rq = m.refusalQuery(st({fproblem: 'refused', fcause: 'ws', fcode: 'x'}), scope);
  assert.deepEqual(JSON.parse(JSON.stringify(rq)), {from: 10, to: 20, key: 'k', instance: 'i', reason: 'ws'},
    'refusals follow range, key and instance only');
  assert.equal(m.scopedOut(scope), true);
  assert.equal(m.scopedOut({key: 'k'}), false);
});

test('filter coupling: groups, outcome, failure class and session', () => {
  const c = sandbox();
  const m = M(c);
  const f0 = Object.assign(m.emptyFilters(), {fproblem: 'error', fcause: 'quota', foutcome: 'failed', fclass: 'network'});
  let f = m.pickProblem(f0, 'upstream');
  assert.deepEqual([f.fproblem, f.fcause, f.foutcome, f.fclass], ['upstream', '', '', ''], 'a group resets its cause and the old selects');
  f = m.setFilter(Object.assign(m.emptyFilters(), {fproblem: 'error', fcause: 'quota'}), 'fclass', 'rate_limit');
  assert.deepEqual([f.foutcome, f.fproblem, f.fcause], ['failed', '', ''], 'a failure class implies 失败 and leaves the groups');
  f = m.setFilter(f, 'foutcome', 'completed');
  assert.equal(f.fclass, '', 'another outcome clears the class');
  f = m.setFilter(Object.assign(m.emptyFilters(), {fclass: 'quota', foutcome: 'failed'}), 'fclass', '');
  assert.equal(f.foutcome, 'failed', 'clearing the class keeps the outcome');
  f = m.setFilter(Object.assign(m.emptyFilters(), {fproblem: 'refused', fcause: 'ws'}), 'fthread', ' 0199abcd ');
  assert.deepEqual([f.fthread, f.fproblem, f.fcause], ['0199abcd', '', ''], 'a session leaves 本地拒绝');
  f = m.setFilter(Object.assign(m.emptyFilters(), {foutcome: 'refused'}), 'fthread', '0199abcd');
  assert.equal(f.foutcome, '');
  assert.equal(m.setFilter(m.emptyFilters(), 'fretry', true).fretry, '1');
  assert.equal(m.refusedMode({foutcome: 'refused'}), true);
  assert.deepEqual(['', '1a1e540ee3300001', 'abc', '0199abcd', '019a-ffff-1234', 'a b c d e f g h'].map(s => m.parseSearch(s).kind),
    ['empty', 'record', 'invalid', 'thread', 'thread', 'invalid']);
  const prefixed = m.parseSearch('CP-1a1e540ee3300001');
  assert.equal(prefixed.kind, 'record');
  assert.equal(prefixed.id, '1a1e540ee3300001');
  assert.equal(m.parseSearch('cp-1a1e540ee33000').kind, 'thread');
});

test('row classification, next steps and conclusions come from backend fields only', () => {
  const c = sandbox();
  const m = M(c);
  assert.equal(m.outcomeOf({endpoint: 'prewarm'}), 'prewarm');
  assert.equal(m.outcomeOf({error: 'client_closed'}), 'cancelled');
  assert.equal(m.outcomeOf({error: 'observation_incomplete', err_source: 'local', status: 200}), 'unknown');
  assert.equal(m.outcomeOf({status: 502}), 'failed');
  assert.equal(m.outcomeOf({incomplete: 'interrupted', status: 200}), 'completed', 'interrupted is a normal end');
  assert.equal(m.outcomeOf({incomplete: 'max_output_tokens', status: 200}), 'incomplete');
  assert.equal(m.outcomeOf({outcome: 'failed', status: 200}), 'failed', 'the backend outcome wins');
  assert.deepEqual([{outcome: 'failed', status: 429}, {outcome: 'failed', status: 502}, {outcome: 'completed', rescued: true},
    {outcome: 'completed'}, {outcome: 'cancelled'}, {outcome: 'prewarm'}].map(r => m.tone(r)), ['warn', 'err', 'warn', 'ok', 'off', 'off']);
  assert.equal(m.srcOf({err_source: 'network', error: 'proxy_connect'}), 'proxy');
  assert.equal(m.labelledError({error: 'observation_incomplete', outcome: 'failed'}), '', 'observation_incomplete is only named when unknown');
  assert.equal(m.incompleteCode({incomplete: 'max_output_tokens'}), 'incomplete_max_output_tokens');
  assert.equal(m.ttftOf({transport: 'http', ttft_ms: 5}), null, 'no TTFT for non-stream requests');
  const r = {account: 'bob', next: ['refresh_quota', 'unknown_thing', 'detail', 'new_session', 'egress']};
  assert.deepEqual(m.nextSteps(r), ['refresh_quota', 'egress']);
  assert.deepEqual(JSON.parse(JSON.stringify(m.nextTarget('refresh_quota', r))), {view: 'accounts', hash: 'acc=bob'},
    '刷新额度 opens the account row, never a reset card');
  assert.deepEqual(JSON.parse(JSON.stringify(m.nextTarget('egress', r))), {view: 'settings', hash: 'network'});
  assert.deepEqual(JSON.parse(JSON.stringify(m.nextTarget('attempts', r))), {inPage: 'attempts'});
  assert.equal(m.conclusion({problem: 'all', next: []}), null, 'a plain success has no conclusion');
  assert.deepEqual(JSON.parse(JSON.stringify(m.conclusion({rescued: true, problem: 'upstream'}))), {kind: 'rescued', counted: false, upstream: true});
  const e = m.conclusion({problem: 'error', cause: 'request', next: ['new_session'], upstream_fault: false});
  assert.deepEqual([e.counted, e.newSession, e.upstream], [true, true, false]);
  assert.equal(m.conclusion({problem: 'unfinished', cause: 'client_closed'}).counted, false);
  assert.equal(m.shortId('0199aaaa-bbbb-cccc-dddd-eeeeffff0001'), '…ffff0001', 'the session short form is the tail');
});

test('attempt bars, histogram buckets and chart periods', () => {
  const c = sandbox();
  const m = M(c);
  const a = m.attemptBars([{account: 'bob', t_ms: 0, action: 'switch', status: 502}, {dropped: 3},
    {account: 'alice', t_ms: 640, action: 'ok', status: 200}], 9800);
  assert.deepEqual(JSON.parse(JSON.stringify(a.rows.map(r => r.dropped || [r.start, r.end, r.tone].join(':')))), ['0:640:err', 3, '640:9800:ok']);
  const H = 36e5, t0 = 1794744000000;
  const hist = m.histogram([{start_ms: t0, end_ms: t0 + H, count: 5, failures: 2, refused: 1}, {start_ms: t0 + 3 * H, end_ms: t0 + 4 * H, count: 1,
    failures: 0, refused: 0}], 'hour', false);
  assert.equal(hist.rows.length, 4, 'empty hours between buckets are zero bars at their real time');
  assert.deepEqual(JSON.parse(JSON.stringify(hist.series.map(s => s.values.join(',')))), ['2,0,0,0', '3,0,0,1']);
  assert.equal(hist.series[0].color, 'err', 'failures are always the error colour');
  assert.equal(hist.total, 6);
  assert.deepEqual(JSON.parse(JSON.stringify(m.histogram([{start_ms: t0, count: 5, failures: 1, refused: 4}], 'day', true).series.map(s => s.id))), ['refused']);
  assert.deepEqual(JSON.parse(JSON.stringify(m.windowOf(hist.rows, 2, 1, {from: t0 + 1.5 * H, to: t0 + 10 * H}))),
    {from: t0 + 1.5 * H, to: t0 + 3 * H - 1}, 'a picked period is clipped to the scope window; bucket ends are exclusive');
  const full = m.histogram([{start_ms: t0 + 2 * H, end_ms: t0 + 3 * H, count: 4, failures: 1, refused: 0}], 'hour', false,
    {from: t0 + 0.5 * H, to: t0 + 4.5 * H}, 0);
  assert.deepEqual(JSON.parse(JSON.stringify(full.x.map(x => (x - t0) / H))), [0, 1, 2, 3, 4],
    'with the window, empty hours before and after the data are drawn too');
  assert.equal(full.total, 4);
  assert.equal(m.chartBucket(0, 7 * 864e5), 'hour');
  assert.equal(m.chartBucket(0, 30 * 864e5), 'day');
});

test('labels: unknown codes stay verbatim, attempt actions read in both languages', () => {
  const c = sandbox();
  const x = X(c);
  assert.equal(x.errLabel('some_new_code'), '', 'no invented label for an unknown code');
  assert.equal(x.errLabel('proxy_connect'), '出口代理连接失败');
  assert.equal(x.actionText('retry(1500ms)'), '退避 1.5 秒后重试同一账号');
  assert.equal(x.actionText('cooldown(30s)+switch'), '冷却 30s，换账号');
  assert.equal(x.actionText('weird'), 'weird');
  c.I18n.setLang('en', {persist: false});
  assert.equal(x.actionText('cooldown(30s)+switch'), 'Cool down 30s, Switch account');
  assert.equal(x.causeLabel('unfinished', 'reconnect'), 'Needs reconnect');
  const d = c.I18n.dicts();
  const zh = Object.keys(d.zh).filter(k => k.startsWith('requests.')), en = Object.keys(d.en).filter(k => k.startsWith('requests.'));
  assert.deepEqual(zh.filter(k => !(k in d.en)), []);
  assert.deepEqual(en.filter(k => !(k in d.zh)), []);
  for (const k of zh) {
    if (typeof d.zh[k] === 'string') assert.doesNotMatch(d.zh[k], /[。]$/, 'short copy without a trailing 。: ' + k);
  }
});

// ---------- view behaviour ----------
test('paging uses the ID cursor and a late page of an old query is dropped', async () => {
  const c = sandbox();
  const ctx = mountView(c);
  const first = ids(100, 50).map(id => rec(id));
  await answerInitial(ctx, first, {next_before_id: '0000000000000033', total: 120});
  const st = S(c);
  assert.equal(st.list.rows.length, 50);
  // load more
  const more = c.document.body.querySelector('[data-fk="rq-more"]');
  assert.ok(more, 'a load-more button while the cursor is open');
  more.click();
  await tick();
  const page2 = ctx.open('/usage/v1/records?');
  assert.equal(page2.url.searchParams.get('before_id'), '0000000000000033', 'page two passes the full ID cursor');
  assert.equal(page2.url.searchParams.has('offset'), false, 'never an offset');
  // the filter changes before page two answers
  c.document.body.querySelector('[data-problem="error"]').click();
  await tick();
  page2.ok({records: ids(50, 50).map(id => rec(id)), total: 120, next_before_id: null});
  await tick();
  assert.equal(st.list.rows.length, 50, 'the late page is not appended to the new query');
  const fresh = ctx.open('/usage/v1/records?');
  assert.equal(fresh.url.searchParams.get('problem'), 'error');
  fresh.ok({records: [rec('00000000000000ff', {problem: 'error', outcome: 'failed', status: 502})], total: 1, next_before_id: null});
  await tick();
  assert.deepEqual(st.list.rows.map(r => r.request_id), ['00000000000000ff']);
  assert.equal(c.loc.search, '?fproblem=error', 'the group is kept in the URL under its old name');
});

test('a late answer of an older filter never replaces the newer list', async () => {
  const c = sandbox();
  const ctx = mountView(c);
  await answerInitial(ctx, [rec('0000000000000001')]);
  c.document.body.querySelector('[data-problem="error"]').click();
  await tick();
  const older = ctx.open('/usage/v1/records?');
  c.document.body.querySelector('[data-problem="unfinished"]').click();
  await tick();
  const newer = ctx.open('/usage/v1/records?');
  newer.ok({records: [rec('0000000000000003')], total: 1, next_before_id: null});
  await tick();
  older.ok({records: [rec('0000000000000002')], total: 1, next_before_id: null});
  await tick();
  assert.deepEqual(S(c).list.rows.map(r => r.request_id), ['0000000000000003']);
});

test('session fragments resolve to the full ID; an ambiguous fragment is an empty list, not a failure', async () => {
  const c = sandbox();
  const ctx = mountView(c);
  await answerInitial(ctx, [rec('0000000000000001')]);
  const input = c.document.body.querySelector('[data-search]');
  input.value = 'ffff0001';
  input.closest('form').dispatch('submit');
  await tick();
  let call = ctx.open('/usage/v1/records?');
  assert.equal(call.url.searchParams.get('thread'), 'ffff0001');
  assert.ok(!call.url.searchParams.has('from'), 'a session searches the whole retention');
  call.ok({records: [rec('0000000000000009')], total: 1, next_before_id: null, thread: '0199aaaa-bbbb-cccc-dddd-eeeeffff0001'});
  await tick();
  assert.equal(new URLSearchParams(c.loc.search).get('fthread'), '0199aaaa-bbbb-cccc-dddd-eeeeffff0001', 'URL keeps the full ID');
  assert.match(c.document.body.querySelector('.rq-count').textContent, /不限时间/);
  assert.match(c.document.body.querySelector('.rq-active').textContent, /…ffff0001/, 'the chip shows the tail');
  // ambiguous
  input.value = 'abababab';
  input.closest('form').dispatch('submit');
  await tick();
  ctx.open('/usage/v1/records?').fail(400, 'ambiguous thread');
  await tick();
  assert.equal(S(c).list.ambiguous, true);
  assert.match(c.document.body.querySelector('.rq-list-body').textContent, /多个会话/);
  assert.equal(ctx.fails.length + c.toasts.length, 0, 'not reported as an update failure');
});

test('refusals: account / model scope fetches nothing; the refused group is disabled without a count', async () => {
  const c = sandbox({url: '/ui/requests?facct=alice&fproblem=refused'});
  const ctx = mountView(c);
  await tick();
  assert.equal(ctx.openAll('/usage/v1/refusals').length, 0, 'no refusal fetch under an account scope');
  assert.equal(ctx.openAll('/usage/v1/records?').length, 0);
  assert.match(c.document.body.querySelector('.rq-list-body').textContent, /按账号或模型筛选时不显示拒绝记录/);
  ctx.open('/usage/v1/records/counts').ok({all: 3, error: 1, upstream: 0, unfinished: 0, refused: null, causes: {}});
  await tick();
  const btn = c.document.body.querySelector('[data-problem="refused"]');
  assert.equal(btn.disabled, false, 'the selected refused group stays usable');
  c.document.body.querySelector('[data-problem=""]').click();
  await tick();
  ctx.open('/usage/v1/records/counts').ok({all: 3, error: 1, upstream: 0, unfinished: 0, refused: null, causes: {}});
  await tick();
  assert.equal(c.document.body.querySelector('[data-problem="refused"]').disabled, true, 'disabled, not zero');
  // without account scope: per-minute groups with key / instance / reason
  const c2 = sandbox({url: '/ui/requests?fkey=laptop&finstance=home&fproblem=refused&fcause=ws'});
  const ctx2 = mountView(c2);
  await tick();
  const rq = ctx2.open('/usage/v1/refusals').url.searchParams;
  assert.deepEqual(['key', 'instance', 'reason', 'limit', 'offset'].map(k => rq.get(k)), ['laptop', 'home', 'ws', '50', '0']);
});

test('auto refresh: several pages hold the list; fresh rows wait behind the pill unless the reader is at a quiet top', async () => {
  const c = sandbox();
  const ctx = mountView(c);
  const first = ids(200, 50).map(id => rec(id));
  await answerInitial(ctx, first, {next_before_id: ids(150, 1)[0], total: 120});
  const st = S(c);
  const nodeOf = id => st.rowsBox.querySelector('[data-key="' + id + '"]');
  const keep = nodeOf(first[3].request_id);
  // a refresh with an identical row keeps its node (a new JSON object with the same fields)
  ctx.subs.refresh[0]({auto: false});
  await tick();
  ctx.open('/usage/v1/records?').ok({records: first.map(r => Object.assign({}, r)), total: 120, next_before_id: ids(150, 1)[0]});
  await tick();
  assert.equal(nodeOf(first[3].request_id), keep, 'an unchanged row keeps its node');
  // open a detail → the list is not quiet; a new row waits behind the pill
  st.detail.show(first[0]);
  c.Views.requests._state().st.record = first[0].request_id;
  ctx.subs.refresh[0]({auto: true});
  await tick();
  const fresh = [rec('00000000000000c9')].concat(first.slice(0, 49).map(r => r.request_id === first[3].request_id ?
    Object.assign({}, r, {duration_ms: 5555}) : r));
  ctx.open('/usage/v1/records?').ok({records: fresh, total: 121, next_before_id: ids(151, 1)[0]});
  await tick();
  assert.equal(st.list.rows[0].request_id, first[0].request_id, 'the list under the reader does not move');
  assert.equal(st.list.rows[3].duration_ms, 5555, 'rows on screen still get their final state');
  assert.equal(st.pendingCount, 1);
  assert.equal(c.document.body.querySelector('.rq-pill').classList.contains('hide'), false);
  c.document.body.querySelector('.rq-pill').click();
  await tick();
  assert.equal(st.list.rows[0].request_id, '00000000000000c9', 'the pill applies the fresh page');
  // two pages loaded: auto refresh leaves the list alone
  c.document.body.querySelector('[data-fk="rq-more"]').click();
  await tick();
  ctx.open('/usage/v1/records?').ok({records: ids(140, 50).map(id => rec(id)), total: 121, next_before_id: null});
  await tick();
  assert.equal(st.list.rows.length, 100);
  const before = ctx.calls.filter(x => x.path.startsWith('/usage/v1/records?')).length;
  ctx.subs.refresh[0]({auto: true});
  await tick();
  assert.equal(ctx.calls.filter(x => x.path.startsWith('/usage/v1/records?')).length, before, 'no records fetch while held');
  assert.match(c.document.body.querySelector('.rq-foot').textContent, /自动刷新暂停更新列表/);
});

test('detail: late answers of another record are ignored; 404 is expired, other failures retry in place', async () => {
  const c = sandbox();
  const ctx = mountView(c);
  const a = rec('000000000000000a', {has_detail: true, problem: 'error', outcome: 'failed', status: 502, next: ['attempts', 'account']});
  const b = rec('000000000000000b', {has_detail: true, problem: 'error', outcome: 'failed', status: 429, cause: 'rate_limit'});
  await answerInitial(ctx, [a, b]);
  const st = S(c);
  st.rowsBox.querySelector('[data-key="000000000000000a"]').click();
  await tick();
  assert.equal(new URLSearchParams(c.loc.search).get('record'), '000000000000000a', 'the open record is in the URL');
  const da = ctx.open('/usage/v1/records/000000000000000a/detail');
  // move to the next record before the first detail answers
  c.document.body.querySelector('.rq-dtools button[title^="下一条"]').click();
  await tick();
  da.ok({attempts: [{account: 'alice', t_ms: 0, action: 'stop', status: 502}], headers: {}, body: null, body_len: 0});
  await tick();
  assert.equal(st.detail.record().request_id, '000000000000000b');
  assert.equal(c.document.body.querySelectorAll('.rq-att').length, 0, 'record a\'s attempts never fill record b');
  ctx.open('/usage/v1/records/000000000000000b/detail').fail(404, 'gone');
  await tick();
  assert.match(c.document.body.querySelector('.rq-dload').textContent, /过保留期/);
  assert.ok(c.document.body.querySelector('.rq-dload-row .btn').classList.contains('hide'), 'no retry for an expired detail');
  // back to a: a network failure offers a retry, which re-reads only the detail
  c.document.body.querySelector('.rq-dtools button[title^="上一条"]').click();
  await tick();
  ctx.open('/usage/v1/records/000000000000000a/detail').fail(503, 'busy');
  await tick();
  const retry = c.document.body.querySelector('.rq-dload-row .btn');
  assert.equal(retry.classList.contains('hide'), false);
  retry.click();
  await tick();
  const again = ctx.open('/usage/v1/records/000000000000000a/detail');
  assert.ok(again, 'retry reads the detail again');
  again.ok({attempts: [{account: 'bob', t_ms: 0, action: 'switch', status: 502}, {account: 'alice', t_ms: 300, action: 'ok', status: 200}],
    headers: {'x-a': '1'}, body: '{"a":1}', body_len: 7});
  await tick();
  assert.equal(c.document.body.querySelectorAll('.rq-att').length, 2);
  const attemptsBtn = c.document.body.querySelector('.rq-concl [data-next="attempts"]');
  assert.equal(attemptsBtn.classList.contains('hide'), false, '查看尝试 appears once attempts are loaded');
  attemptsBtn.click();
  assert.equal(c.document.activeElement, c.document.body.querySelector('.rq-dh[tabindex]'), 'keyboard reading continues at the attempts');
  // a next step to another page goes through the shell, carrying the account
  c.document.body.querySelector('.rq-concl [data-next="account"]').click();
  assert.equal(c.visits.at(-1), '/ui/accounts#acc=alice');
  // closing invalidates a pending answer
  st.detail.show(b);
  const late = ctx.open('/usage/v1/records/000000000000000b/detail');
  c.document.body.querySelector('.rq-aside-head .btn').click();
  late.ok({attempts: [{account: 'x', t_ms: 0, action: 'ok'}], headers: {}, body: null, body_len: 0});
  await tick();
  assert.equal(st.detail.record(), null);
  assert.equal(new URLSearchParams(c.loc.search).has('record'), false);
});

test('a record link opens by ID; a missing record says so and leaves the URL', async () => {
  const c = sandbox({url: '/ui/requests?record=00000000000000aa'});
  const ctx = mountView(c);
  await tick();
  ctx.open('/usage/v1/records/00000000000000aa').fail(404, 'no such record');
  await tick();
  assert.match(c.document.body.querySelector('.rq-lookup').textContent, /没有找到记录 00000000000000aa/);
  assert.equal(new URLSearchParams(c.loc.search).has('record'), false);
  const c2 = sandbox({url: '/ui/requests?rid=00000000000000AB'});
  const ctx2 = mountView(c2);
  await tick();
  assert.equal(new URLSearchParams(c2.loc.search).get('record'), '00000000000000ab', 'rid becomes record');
  ctx2.open('/usage/v1/records/00000000000000ab').ok({record: rec('00000000000000ab')});
  await tick();
  assert.equal(S(c2).detail.record().request_id, '00000000000000ab');
  assert.match(c2.document.body.querySelector('.rq-pos').textContent, /按记录 ID 打开/);
});

test('只看这个会话 clears request filters and the shared account / model / key / instance scope', async () => {
  const c = sandbox({url: '/ui/requests?facct=alice&fkey=k&fproblem=error'});
  const ctx = mountView(c);
  const r = rec('00000000000000aa', {thread_id: '0199aaaa-bbbb-cccc-dddd-eeeeffff0001'});
  await answerInitial(ctx, [r]);
  S(c).rowsBox.querySelector('[data-key="00000000000000aa"]').click();
  await tick();
  const only = c.document.body.querySelectorAll('.rq-idv .btn').find(b => b.textContent === '只看这个会话');
  assert.ok(only);
  only.click();
  await tick();
  const sc = c.Shell.scope.get();
  assert.deepEqual([sc.account, sc.key], ['', '']);
  const q = new URLSearchParams(c.loc.search);
  assert.equal(q.get('fthread'), '0199aaaa-bbbb-cccc-dddd-eeeeffff0001');
  assert.equal(q.has('fproblem'), false);
  assert.equal(S(c).detail.record(), null, 'the detail closes');
});

test('view sources follow the CSP rules', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const dir = path.join(__dirname, '../crates/server/src/ui/js/views');
  for (const f of ['requests.js', 'requests/text.js', 'requests/model.js', 'requests/detail.js']) {
    const src = fs.readFileSync(path.join(dir, f), 'utf8');
    assert.doesNotMatch(src, /innerHTML|insertAdjacentHTML|new Function|\beval\(|style\s*=\s*["']|\.style\.[a-z]+\s*=|setAttribute\(\s*["']style/, f);
    for (const [i, line] of src.split('\n').entries()) assert.ok(line.length <= 140, f + ':' + (i + 1) + ' longer than 140');
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
