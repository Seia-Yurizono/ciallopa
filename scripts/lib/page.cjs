// The server and node checks assemble the same inline shell before executing a page.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const root = path.join(__dirname, '../../crates/server/src');
function readPage(name) {
  return fs.readFileSync(path.join(root, name + '.html'), 'utf8')
    .replace('/*@shared.css*/', () => fs.readFileSync(path.join(root, 'shared.css'), 'utf8'))
    .replace('/*@shared.js*/', () => fs.readFileSync(path.join(root, 'shared.js'), 'utf8'));
}
function section(html, name) {
  const open = `/*@unit ${name}*/`, close = `/*@end ${name}*/`;
  const a = html.indexOf(open), b = html.indexOf(close, a + open.length);
  assert.ok(a >= 0 && b > a, `Missing page unit: ${name}`);
  return stripMarkers(html.slice(a + open.length, b));
}
function stripMarkers(html) { return html.replace(/\/\*@(unit|end) [\w$-]+\*\//g, ''); }
function loadPage(name) {
  const source = readPage(name);
  return {html: stripMarkers(source), section: name => section(source, name)};
}
class Node {
  constructor(tag = 'div') {
    this.tag = tag;
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.attrs = {};
    this.style = {};
    this.dataset = {};
    this.listeners = {};
    this.value = '';
    this.validity = {badInput: false};
    this.isConnected = true;
    this.classes = new Set();
    this.classList = {
      add: (...names) => names.forEach(n => this.classes.add(n)),
      remove: (...names) => names.forEach(n => this.classes.delete(n)),
      contains: n => this.classes.has(n),
      toggle: (n, on = !this.classes.has(n)) => on ? this.classes.add(n) : this.classes.delete(n),
    };
  }
  get kids() { return this.children; }
  set kids(nodes) { this.children = nodes; }
  get text() { return this.textContent; }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return (this._text || '') + this.children.map(x => x.textContent ?? String(x)).join(''); }
  get firstChild() { return this.children[0] || null; }
  get childElementCount() { return this.children.filter(n => n instanceof Node).length; }
  append(...nodes) { this.children.push(...nodes.flat().filter(x => x !== null && x !== undefined)); }
  appendChild(node) { this.append(node); return node; }
  removeChild(node) { this.children.splice(this.children.indexOf(node), 1); return node; }
  prepend(node) { this.children.unshift(node); }
  addEventListener(type, fn) { this.listeners[type] = fn; }
  dispatchEvent(event) { return this.listeners[event.type]?.(event); }
  fire(type, event = {}) { return this.dispatchEvent({type, ...event}); }
  click() { return this.fire('click'); }
  setAttribute(name, value) {
    this.attrs[name] = String(value); this[name] = value;
    if (name.startsWith('data-')) this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = String(value);
  }
  getAttribute(name) { return this.attrs[name] ?? null; }
  hasAttribute(name) { return name in this.attrs; }
  removeAttribute(name) {
    delete this.attrs[name]; delete this[name];
    if (name.startsWith('data-')) delete this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())];
  }
  toggleAttribute(name, on = !this.hasAttribute(name)) { if (on) this.setAttribute(name, ''); else this.removeAttribute(name); }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  focus() {}
  blur() {}
  scrollIntoView() {}
}
function createDom() {
  const nodes = new Map();
  const $ = id => {
    if (!nodes.has(id)) nodes.set(id, new Node());
    return nodes.get(id);
  };
  const h = (tag, attrs, ...children) => {
    const node = new Node(tag);
    if (typeof attrs === 'string') attrs = {class: attrs};
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v);
    }
    node.append(...children);
    return node;
  };
  return {$, h, clear: n => {n.textContent = '';}, nodes};
}
// Only the DOM operations used by the chip and inspector flows; no browser/layout claims.
function interactionDom(html) {
  const controls = new Map(), handlers = {};
  const document = {activeElement: null, contains: () => true,
    addEventListener(type, fn) {(handlers[type] ||= []).push(fn);}};
  const element = (tag = 'div', attrs = {}, ...children) => {
    const classes = new Set((attrs.class || '').split(' '));
    const e = {tagName: tag.toUpperCase(), value: '', checked: false, children, dataset: {}, events: {}, textContent: '',
      classList: {contains: n => classes.has(n), add: n => classes.add(n), remove: n => classes.delete(n),
        toggle(n, on) {if (on) classes.add(n); else classes.delete(n);if (n === 'hide' && on && e.children.includes(document.activeElement)) document.activeElement = null;}},
      setAttribute(n, v) {attrs[n] = String(v); if (n === 'data-clear-filter') this.dataset.clearFilter = v;},
      getAttribute: n => attrs[n], addEventListener(type, fn) {this.events[type] = fn;},
      appendChild(child) {this.children.push(child);}, append(...items) {this.children.push(...items);},
      querySelectorAll() {return this.children.filter(child => child.tagName === 'BUTTON');},
      focus() {document.activeElement = this;},
      get selectedIndex() {return this.options?.findIndex(o => o.value === this.value);},
    };
    let disabled = false;
    Object.defineProperty(e, 'disabled', {get: () => disabled, set(on) {disabled = on;if (on && document.activeElement === e) document.activeElement = null;}});
    for (const [n, v] of Object.entries(attrs)) e.setAttribute(n, v);
    return e;
  };
  document.documentElement = element('html');
  const control = id => {
    if (!controls.has(id)) {
      const select = html.match(new RegExp('<select id="' + id + '"[^>]*>(.*?)</select>', 's'));
      const e = element(select ? 'select' : 'div');
      if (select) e.options = [...select[1].matchAll(/<option value="([^"]*)"[^>]*>([^<]*)/g)].map(m => ({value: m[1], textContent: m[2]}));
      if (id === 'ferr' || id === 'fretry') e.type = 'checkbox';
      controls.set(id, e);
    }
    return controls.get(id);
  };
  return {$: control, document, clear: e => {e.children = [];e.textContent = '';},
    h: (tag, attrs, ...children) => element(tag, typeof attrs === 'string' ? {class: attrs} : attrs || {}, ...children),
    keydown(key, modifiers = {}) {const event = {key, ...modifiers, preventDefault() {this.defaultPrevented = true;}};for (const fn of handlers.keydown || []) fn(event);return event;}};
}

function createContext(globals = {}) { return vm.createContext(globals); }
module.exports = {readPage, loadPage, section, Node, createDom, interactionDom, createContext, vm};
