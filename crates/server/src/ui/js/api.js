"use strict";
// Api: authenticated JSON calls to /usage/v1/* and /admin/v1/*.
// The access key lives in sessionStorage["ciallopa_key"] (this tab only) and goes out as
// `Authorization: Bearer`. Every call is bound to the identity that issued it: the key value and an
// epoch bumped on each login / logout, so a late response from a previous identity can never act
// on (or clear) the current one.
const Api = (() => {
  const KEY = "ciallopa_key";
  const T = {timeout: 45000};
  let epoch = 0;
  let noAdmin = false;
  let onUnauthorized = () => {};

  const key = () => Core.session.get(KEY) || "";
  const setKey = k => Core.session.set(KEY, k);
  const clearKey = () => Core.session.del(KEY);

  // New identity (login, logout, key change): in-flight calls of the old one become stale.
  function bump() {
    epoch++;
    noAdmin = false;
    return epoch;
  }

  // Behind Cloudflare Access an expired or missing login answers an API call with a redirect to
  // the sign-in page (an opaque redirect under redirect:"manual") or an HTML page instead of JSON.
  // That is neither a network error nor a bad key: keep the key and ask for a page reload.
  function isAccessGate(r) {
    const hd = n => (r.headers && r.headers.get ? (r.headers.get(n) || "").toLowerCase() : "");
    const names = r.headers && r.headers.keys ? Array.from(r.headers.keys()) : [];
    return r.type === "opaqueredirect" || r.status === 0 || r.redirected || hd("content-type").includes("text/html") ||
      ((r.status === 401 || r.status === 403) && names.some(k => k.toLowerCase().startsWith("cf-access")));
  }

  function err(message, props) {
    return Object.assign(new Error(message), props || {});
  }

  const adminRequired = j => !!(j && j.error && j.error.type === "admin_required");
  const adminMsg = j => (adminRequired(j) ? I18n.t("api.adminRequired") : null);
  const isAdminError = e => !!(e && e.status === 403 && adminRequired(e.body));
  const isAborted = e => !!(e && e.kind === "aborted");
  // A failure the caller should stay silent about: superseded identity, logout, view unmounted.
  const isQuiet = e => !!(e && (e.kind === "aborted" || e.kind === "stale" || e.message === "401"));

  // req(method, path, body?, headers?, opts?) → {j, r}
  // opts.timeout (ms): abort when no answer arrives in time (error kind "timeout").
  // opts.current(): extra staleness check (e.g. "still the same selection"); a stale 401 does not
  //   log out and a stale 403 admin_required does not flag the identity.
  // opts.signal: AbortSignal (a view's ctx.signal); aborting rejects with kind "aborted".
  // Errors: kind "network" | "timeout" | "access" | "aborted"; HTTP errors carry .status and .body.
  async function req(method, path, body, headers, opts) {
    opts = opts || {};
    const auth = key(), at = epoch;
    const current = () => key() === auth && epoch === at && (!opts.current || opts.current());
    const hd = {Authorization: "Bearer " + auth};
    if (body !== undefined) hd["Content-Type"] = "application/json";
    Object.assign(hd, headers || {});
    const ac = new AbortController();
    let timedOut = false;
    const tm = opts.timeout ? setTimeout(() => {
      timedOut = true;
      ac.abort();
    }, opts.timeout) : null;
    const relay = () => ac.abort();
    if (opts.signal) {
      if (opts.signal.aborted) ac.abort();
      else opts.signal.addEventListener("abort", relay, {once: true});
    }
    let r;
    try {
      r = await fetch(path, {
        method,
        headers: hd,
        body: body === undefined ? undefined : JSON.stringify(body),
        cache: "no-store",
        credentials: "same-origin",
        redirect: "manual",
        referrerPolicy: "no-referrer",
        signal: ac.signal,
      });
    } catch (e) {
      if (timedOut) throw err(I18n.t("api.timeout", {s: Math.round(opts.timeout / 1000)}), {kind: "timeout"});
      if (ac.signal.aborted) throw err("aborted", {kind: "aborted"});
      throw err(I18n.t("api.network", {msg: e.message}), {kind: "network"});
    } finally {
      clearTimeout(tm);
      if (opts.signal) opts.signal.removeEventListener("abort", relay);
    }
    if (isAccessGate(r)) throw err(I18n.t("api.access"), {kind: "access"});
    if (r.status === 401) {
      if (current()) {
        clearKey();
        onUnauthorized(I18n.t("api.unauthorized"));
      }
      throw err("401", {status: 401});
    }
    let j = null;
    try {
      j = await r.json();
    } catch (_) {
      // not JSON: handled by status below
    }
    if (current() && r.status === 403 && adminRequired(j)) noAdmin = true;
    if (!r.ok) {
      const msg = adminMsg(j) || (j && j.error && j.error.message) || path.split("?")[0] + " → HTTP " + r.status;
      throw err(msg, {status: r.status, body: j});
    }
    if (!current() && opts.strict) throw err("stale", {kind: "stale"});
    return {j, r};
  }

  const get = async (path, opts) => (await req("GET", path, undefined, undefined, opts)).j;
  const post = async (path, body, opts) => (await req("POST", path, body || {}, undefined, opts)).j;
  const put = async (path, body, opts) => (await req("PUT", path, body || {}, undefined, opts)).j;
  const del = async (path, opts) => (await req("DELETE", path, undefined, undefined, opts)).j;

  // Query string from an object, skipping "", null and undefined.
  function qs(o) {
    const u = new URLSearchParams();
    for (const k in o) if (o[k] !== "" && o[k] !== null && o[k] !== undefined) u.set(k, o[k]);
    return u.toString();
  }

  // Reason of a failed call for a toast: timeout / network / access as such, HTTP with status.
  function why(e) {
    if (!e) return I18n.t("api.unknown");
    if (e.kind || !e.status) return e.message || String(e);
    return "HTTP " + e.status + " · " + e.message;
  }

  // A button action: busy state (spinner, optional label) while it runs; a failure becomes an error
  // toast "<name> failed: <reason>". Returns false on failure. The button stays focusable while
  // busy (aria-disabled), so keyboard focus is not dropped; a second press while busy is ignored.
  async function busyRun(b, name, f, label) {
    if (b.disabled || b._busy) return false;
    const text = b.textContent;
    b._busy = true;
    b.classList.add("busy");
    b.setAttribute("aria-disabled", "true");
    b.setAttribute("aria-busy", "true");
    if (label) b.textContent = label;
    try {
      await f();
      return true;
    } catch (e) {
      if (!isQuiet(e)) UI.toast(I18n.t("api.failed", {name, why: why(e)}), {kind: "err"});
      return false;
    } finally {
      b._busy = false;
      b.classList.remove("busy");
      b.removeAttribute("aria-disabled");
      b.removeAttribute("aria-busy");
      if (label && b.isConnected) b.textContent = text;
    }
  }

  return {KEY, T, key, setKey, clearKey, bump, epoch: () => epoch, noAdmin: () => noAdmin, isAccessGate, req, get,
    post, put, del, qs, why, busyRun, adminMsg, isAdminError, isAborted, isQuiet,
    onUnauthorized: fn => {
      onUnauthorized = fn;
    }};
})();
