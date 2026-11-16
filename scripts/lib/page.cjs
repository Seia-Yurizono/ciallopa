// Assembles the management page exactly like crates/server/src/ui.rs: the include_str!("ui/…")
// lines of ui.rs are the manifest (index.html, then stylesheets and scripts in order).
// Used by scripts/*-check.cjs and scripts/ui-dev.cjs. No dependencies.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const vm = require('node:vm');

const SRC = path.join(__dirname, '../../crates/server/src');
const UI_RS = path.join(SRC, 'ui.rs');

// {shell, css: [relative paths], js: [relative paths]} in ui.rs order.
function manifest() {
  const rs = fs.readFileSync(UI_RS, 'utf8');
  const files = [...rs.matchAll(/include_str!\("(ui\/[^"]+)"\)/g)].map(m => m[1]);
  const shell = files.find(f => f.endsWith('.html'));
  if (!shell) throw new Error('ui.rs: no ui/*.html in the manifest');
  return {shell, css: files.filter(f => f.endsWith('.css')), js: files.filter(f => f.endsWith('.js'))};
}

const read = rel => fs.readFileSync(path.join(SRC, rel), 'utf8');

// The served document, read from disk now.
function assemble() {
  const m = manifest();
  const css = m.css.map(read).join('');
  const js = m.js.map(read).join('');
  return read(m.shell).replace('/*@css*/', () => css).replace('/*@js*/', () => js);
}

function inlineBlocks(html, tag) {
  const open = `<${tag}>`, close = `</${tag}>`, out = [];
  let rest = html;
  for (;;) {
    const i = rest.indexOf(open);
    if (i < 0) break;
    const body = rest.slice(i + open.length);
    const j = body.indexOf(close);
    if (j < 0) break;
    out.push(body.slice(0, j));
    rest = body.slice(j + close.length);
  }
  return out;
}

const hashes = (html, tag) => inlineBlocks(html, tag)
  .map(b => `'sha256-${crypto.createHash('sha256').update(b, 'utf8').digest('base64')}'`).join(' ');

// Same policy string as ui::csp_for.
function cspFor(html) {
  return `default-src 'none'; script-src ${hashes(html, 'script')}; style-src ${hashes(html, 'style')}; connect-src 'self'; ` +
    `img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`;
}

// Load the given scripts (manifest-relative paths, default: all but main.js) into a vm context
// whose globals are `globals`; returns the context. Top-level `const` namespaces are exposed on the
// context through a trailer, so tests can reach Core / I18n / Api / Shell / UI / Charts / Views.
function loadScripts(globals, files) {
  const m = manifest();
  const list = files || m.js.filter(f => !f.endsWith('/main.js'));
  const ctx = vm.createContext(Object.assign({console, URLSearchParams, URL, AbortController, setTimeout, clearTimeout,
    setInterval, clearInterval, Promise}, globals || {}));
  const names = ['Views', 'Core', 'I18n', 'Api', 'UI', 'Charts', 'Shell'];
  const trailer = '\n;' + names.map(n => `if (typeof ${n} !== "undefined") globalThis.${n} = ${n};`).join('');
  vm.runInContext(list.map(read).join('\n') + trailer, ctx, {filename: 'ui-bundle.js'});
  return ctx;
}

module.exports = {SRC, UI_RS, manifest, read, assemble, inlineBlocks, cspFor, loadScripts, vm};
