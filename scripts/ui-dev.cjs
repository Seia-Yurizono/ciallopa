#!/usr/bin/env node
// UI dev server, no dependencies: node scripts/ui-dev.cjs [port=8398] [backend=http://127.0.0.1:8399]
// Serves the management page assembled from disk on every request (same manifest, CSP and security
// headers as crates/server/src/ui.rs), and proxies /usage/*, /admin/*, /favicon.ico and /healthz to
// a running ciallopa (e.g. `cargo run -p ciallopa --example ui_demo`). Editing ui/** needs no Rust build.
const http = require('node:http');
const {assemble, cspFor} = require('./lib/page.cjs');

const port = Number(process.argv[2] || 8398);
const backend = new URL(process.argv[3] || 'http://127.0.0.1:8399');
const PAGES = new Set(['/ui', '/ui/requests', '/ui/accounts', '/ui/settings', '/ui/system']);
const PROXIED = /^\/(usage|admin)\/|^\/(favicon\.ico|healthz)$/;

function servePage(res) {
  let html;
  try {
    html = assemble();
  } catch (e) {
    res.writeHead(500, {'content-type': 'text/plain; charset=utf-8'});
    res.end('assemble failed: ' + e.message);
    return;
  }
  res.writeHead(200, {
    'content-type': 'text/html; charset=utf-8',
    'content-security-policy': cspFor(html),
    'x-content-type-options': 'nosniff',
    'referrer-policy': 'no-referrer',
    'x-frame-options': 'DENY',
    'cache-control': 'no-store, no-transform',
  });
  res.end(html);
}

function proxy(req, res) {
  const headers = Object.assign({}, req.headers, {host: backend.host});
  const up = http.request({hostname: backend.hostname, port: backend.port || 80, path: req.url, method: req.method, headers}, r => {
    res.writeHead(r.statusCode || 502, r.headers);
    r.pipe(res);
  });
  up.on('error', e => {
    if (!res.headersSent) res.writeHead(502, {'content-type': 'application/json'});
    res.end(JSON.stringify({error: {type: 'dev_proxy', message: 'backend ' + backend.origin + ': ' + e.message}}));
  });
  req.pipe(up);
}

http.createServer((req, res) => {
  const p = new URL(req.url, 'http://x').pathname.replace(/\/+$/, '') || '/';
  if (p === '/') {
    res.writeHead(307, {location: '/ui'});
    res.end();
  } else if (PAGES.has(p) && (req.method === 'GET' || req.method === 'HEAD')) servePage(res);
  else if (PROXIED.test(p)) proxy(req, res);
  else {
    res.writeHead(404, {'content-type': 'text/plain'});
    res.end('not found');
  }
}).listen(port, '127.0.0.1', () => {
  console.log(`ui dev server on http://127.0.0.1:${port}/ui (backend ${backend.origin})`);
});
