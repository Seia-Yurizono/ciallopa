#!/usr/bin/env python3
"""Real-browser user flows against the synthetic ui_demo (no provider account, no network).

    uv run --no-project --with playwright==1.65.2 python scripts/ui-browser-check.py \
        --demo target/debug/examples/ui_demo [--self-test] [--check NAME]

Each check drives the pages the way a user does and states its expected result independently of
the page code: the chosen account is the only account listed, focus comes back to the row that
opened a dialog, names containing markup stay text, and a phone-width page does not scroll
sideways. `--self-test` also runs every check against a fault injected at a boundary the page does
not control (an HTTP reply, a DOM API, a style) and requires that run to fail, so a check
that can no longer fail is reported. Exit 0 passed, 1 failed, 77 browser tooling unavailable.
"""
import argparse
import asyncio
import contextlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeout
    from playwright.async_api import async_playwright
except ImportError:
    print("playwright is not installed; run through `uv run --with playwright==1.65.2`", file=sys.stderr)
    sys.exit(77)

DEMO_KEY = "sk-demo-0123456789abcdef"  # crates/server/examples/ui_demo.rs
EVIL = '<img src=x onerror="window.__xss=1">'  # the demo's account email, key label and model name
PAGES = ["/ui", "/ui/accounts", "/ui/settings"]
WAIT_MS = 15_000  # watchdog for one user-visible result, not a speed requirement


class CheckFailed(Exception):
    pass


class Run:
    """One check in a fresh browser context; failures name the step and keep a screenshot."""

    def __init__(self, browser, base, evidence, name):
        self.browser, self.base, self.evidence, self.name = browser, base, evidence, name
        self.step_name = "start"
        self.page_errors, self.server_errors = [], []

    async def open(self, viewport=(1280, 900), init_script=None):
        self.ctx = await self.browser.new_context(viewport={"width": viewport[0], "height": viewport[1]})
        if init_script:
            await self.ctx.add_init_script(init_script)
        self.page = await self.ctx.new_page()
        self.page.set_default_timeout(WAIT_MS)
        self.page.on("pageerror", lambda e: self.page_errors.append(f"{self.step_name}: {e}"))
        self.page.on("response", self._response)
        return self.page

    def _response(self, r):
        if r.url.startswith(self.base) and r.status >= 500:
            self.server_errors.append(f"{self.step_name}: {r.status} {r.url}")

    def step(self, name):
        self.step_name = name

    def check(self, ok, what):
        if not ok:
            raise CheckFailed(f"{self.step_name}: {what}")

    async def close(self, failed):
        if failed and getattr(self, "page", None) and not self.page.is_closed():
            shot = self.evidence / f"{self.name}.png"
            try:
                await self.page.screenshot(path=str(shot), full_page=True)
            except PlaywrightError:
                pass
        if getattr(self, "ctx", None):
            if getattr(self, "page", None) and not self.page.is_closed():
                await self.page.unroute_all(behavior="ignoreErrors")  # replies a failed check still held
            await self.ctx.close()

    def assert_clean(self):
        self.check(not self.page_errors, f"uncaught page errors: {self.page_errors}")
        self.check(not self.server_errors, f"server errors: {self.server_errors}")


async def login(run, path="/ui", key=DEMO_KEY):
    page = run.page
    await page.goto(run.base + path)
    await page.get_by_label("访问密钥").fill(key)
    await page.get_by_role("button", name="登录", exact=True).click()


def shown(page, text):
    return page.get_by_text(text).filter(visible=True).first


def requests_section(page):
    return page.locator("section", has=page.get_by_role("heading", name="请求", exact=True))


async def wait_rows(run):
    rows = requests_section(run.page).locator("table:visible tbody tr[data-ak]")
    await rows.first.wait_for()
    return rows


async def ready(run, path):
    """The page has rendered its synthetic data (the request list, or the demo's EVIL names)."""
    if path == "/ui":
        await wait_rows(run)
    else:
        await shown(run.page, EVIL).wait_for()


async def column(run, header):
    table = requests_section(run.page).locator("table:visible")
    heads = [h.strip() for h in await table.locator("thead th").all_text_contents()]
    run.check(header in heads, f"no {header!r} column in {heads}")
    return heads.index(header)


# ---- checks -------------------------------------------------------------------------------------


async def check_login(run, fault=None):
    """A wrong key is refused with a message; the demo key opens the request list."""
    page = await run.open()
    if fault == "api-401":
        await page.route("**/usage/v1/**", lambda r: r.fulfill(status=401, json={"error": {"message": "x"}}))
    run.step("wrong key")
    await login(run, key="sk-wrong-0000000000000000")
    alert = page.get_by_role("alert").filter(has_text="密钥")
    await alert.first.wait_for()
    run.check(await page.get_by_label("访问密钥").is_visible(), "login form hidden after a wrong key")
    run.step("demo key")
    await page.get_by_label("访问密钥").fill(DEMO_KEY)
    await page.get_by_role("button", name="登录", exact=True).click()
    try:
        rows = await wait_rows(run)
    except PlaywrightTimeout:
        raise CheckFailed("demo key: request list never showed a row")
    run.check(await rows.count() > 0, "no request rows")
    run.check(not await page.get_by_label("访问密钥").is_visible(), "login form still visible")
    run.assert_clean()


async def check_account_filter_with_late_reply(run, fault=None):
    """Choosing an account lists only that account, even when an earlier reply arrives later."""
    page = await run.open(init_script=PARSED_MARKER)
    await login(run)
    await wait_rows(run)
    col = await column(run, "账号")
    held = []

    async def records(route):
        query = dict(parse_qsl(urlsplit(route.request.url).query))
        if query.get("account") != "bob":
            held.append(route)  # the reply for the previous (all accounts) list
            return
        if fault == "filter-ignored":  # the server forgets the filter
            u = urlsplit(route.request.url)
            query.pop("account")
            await route.continue_(url=urlunsplit(u._replace(query=urlencode(query))))
            return
        await route.continue_()

    await page.route("**/usage/v1/records?*", records)
    try:
        run.step("refresh with every account")
        async with page.expect_request(lambda r: "/usage/v1/records?" in r.url):
            await page.get_by_role("button", name="刷新", exact=True).click()
        run.step("choose bob while that reply is held")
        await requests_section(page).get_by_label("按账号筛选").select_option("bob")
        only_bob = """([col]) => { const sec = [...document.querySelectorAll('section')]
            .find(s => s.querySelector('h2')?.textContent.trim() === '请求');
          const t = sec && [...sec.querySelectorAll('table')].find(t => t.offsetParent);
          const rows = t ? [...t.tBodies[0].rows].filter(r => r.dataset.ak) : [];
          return rows.length > 0 && rows.every(r => r.cells[col].textContent.trim() === 'bob'); }"""
        try:
            await page.wait_for_function(only_bob, arg=[col])
        except PlaywrightTimeout:
            accounts = await requests_section(page).locator(f"table:visible tbody tr[data-ak] td:nth-child({col + 1})").all_text_contents()
            raise CheckFailed(f"{run.step_name}: rows never showed only bob: {sorted(set(accounts))}")
        run.step("release the earlier reply")
        run.check(held, "no earlier records request was held")
        while held:
            route = held.pop(0)
            url = route.request.url
            await route.continue_()
            # Barrier: the page has parsed that body, then one more task has run.
            await page.wait_for_function("(u) => (window.__parsed || []).includes(u)", arg=url)
        await page.evaluate("() => new Promise(r => setTimeout(r, 0))")
        run.check(await page.evaluate(only_bob, [col]), "the late reply replaced the bob-only list")
        chips = page.locator('[role=group][aria-label^="已应用"]:visible').filter(has_text="bob")
        run.check(await chips.count() > 0, "no applied-filter chip names bob")
    finally:  # a failed run must not leave the page's replies held
        for route in held:
            with contextlib.suppress(PlaywrightError):
                await route.continue_()
    run.assert_clean()


PARSED_MARKER = """(() => { const json = Response.prototype.json;
  Response.prototype.json = async function () { const v = await json.call(this);
    (window.__parsed ||= []).push(this.url); return v; }; })();"""


async def check_detail_focus(run, fault=None):
    """A request detail opens with focus inside it; Esc closes it and focus returns to the row."""
    page = await run.open(init_script=FOCUS_LOST if fault == "focus-lost" else None)
    await login(run)
    rows = await wait_rows(run)
    opener = rows.first.get_by_role("button").first
    label = await opener.get_attribute("aria-label")
    run.step("open the first request")
    await opener.click()
    dialog = page.get_by_role("dialog")
    await dialog.wait_for()
    inside = await page.evaluate("() => !!document.activeElement?.closest('[role=dialog]')")
    run.check(inside, "focus is not inside the open dialog")
    run.step("close with Escape")
    await page.keyboard.press("Escape")
    await dialog.wait_for(state="hidden")
    await page.evaluate("() => new Promise(r => setTimeout(r, 0))")
    focused = await page.evaluate("() => document.activeElement?.getAttribute('aria-label')")
    run.check(focused == label, f"focus went to {focused!r}, not the opener {label!r}")
    run.assert_clean()


FOCUS_LOST = """document.addEventListener('keydown', e => { if (e.key === 'Escape')
  setTimeout(() => document.activeElement && document.activeElement.blur(), 0); }, true);"""


async def check_pages_render_names_as_text(run, fault=None):
    """All three pages load without uncaught errors and show a markup-looking name as text."""
    page = await run.open(init_script={"names-as-html": NAMES_AS_HTML, "uncaught": UNCAUGHT}.get(fault))
    await login(run)
    await wait_rows(run)
    for path in PAGES:
        run.step(path)
        await page.goto(run.base + path)
        if path == "/ui":  # the request page shows key labels when grouped by key
            await wait_rows(run)
            await page.get_by_label("汇总分组").select_option("key")
        # Rendered either way: as visible text, or as an element built from the name's markup.
        await page.wait_for_function(NAME_RENDERED, arg=EVIL)
        # The CSP may stop the handler from running; an element made from data is the regression.
        made = await page.evaluate("() => document.querySelectorAll('[onerror]').length")
        run.check(made == 0, f"{made} element(s) were built from a name's markup")
        run.check(await page.evaluate("() => window.__xss") is None, "a rendered name executed markup")
        run.check(await shown(page, EVIL).count() > 0, f"the name {EVIL!r} is not shown as text")
    run.assert_clean()


NAME_RENDERED = """(name) => !!document.querySelector('[onerror]') ||
  [...document.querySelectorAll('td, span, div, p, li, code, b')].some(e => e.offsetParent && e.textContent.includes(name))"""
# An escaping regression anywhere: text given to createTextNode or textContent is parsed as HTML.
NAMES_AS_HTML = """(() => { const parse = v => { const t = document.createElement('template');
    t.innerHTML = String(v); return t.content; };
  Document.prototype.createTextNode = function (v) { return parse(v); };
  const d = Object.getOwnPropertyDescriptor(Node.prototype, 'textContent');
  Object.defineProperty(Node.prototype, 'textContent', { configurable: true, get: d.get,
    set(v) { if (this instanceof Element) { this.replaceChildren(parse(v)); } else d.set.call(this, v); } }); })();"""
UNCAUGHT = "window.addEventListener('load', () => setTimeout(() => { throw new Error('injected'); }, 0));"


async def check_phone_width(run, fault=None):
    """At 390 px no page scrolls sideways; wide tables scroll inside their own container."""
    page = await run.open(viewport=(390, 844), init_script=TOO_WIDE if fault == "too-wide" else None)
    await login(run)
    await wait_rows(run)
    for path in PAGES:
        run.step(path)
        await page.goto(run.base + path)
        await ready(run, path)
        page_w, view_w, outside = await page.evaluate(OUTSIDE_VIEWPORT)
        run.check(page_w <= view_w, f"page is {page_w} px wide in a {view_w} px viewport")
        run.check(not outside, f"cut off at the right edge (not in a horizontal scroller): {outside}")
    run.assert_clean()


# Content clipped by an overflow:hidden ancestor does not widen the page but is still lost.
OUTSIDE_VIEWPORT = """() => { const w = document.documentElement.clientWidth, out = [];
  const scroller = e => { for (let p = e.parentElement; p && p !== document.body; p = p.parentElement) {
    const x = getComputedStyle(p).overflowX; if (x === 'auto' || x === 'scroll') return true; } return false; };
  for (const e of document.body.querySelectorAll('*')) { const r = e.getBoundingClientRect();
    if (!r.width || !r.height || r.right <= w + 1 || getComputedStyle(e).visibility === 'hidden' || scroller(e)) continue;
    out.push(e.tagName.toLowerCase() + (e.id ? '#' + e.id : '') + ' right=' + Math.round(r.right)); }
  return [document.documentElement.scrollWidth, w, out.slice(0, 5)]; }"""
# Through CSSOM: the pages' CSP blocks an injected <style> element.
TOO_WIDE = "document.addEventListener('DOMContentLoaded', () => document.querySelector('main')?.style.setProperty('min-width', '640px'));"

CHECKS = {
    "login": (check_login, ["api-401"]),
    "account-filter-late-reply": (check_account_filter_with_late_reply, ["filter-ignored"]),
    "detail-focus": (check_detail_focus, ["focus-lost"]),
    "names-as-text": (check_pages_render_names_as_text, ["names-as-html", "uncaught"]),
    "phone-width": (check_phone_width, ["too-wide"]),
}


# ---- runner -------------------------------------------------------------------------------------


def start_demo(binary, evidence):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    log = open(evidence / "demo.log", "w")
    proc = subprocess.Popen([str(binary), f"127.0.0.1:{port}", str(evidence / "demo-data")],
                            stdout=subprocess.PIPE, stderr=log, text=True)
    ready = threading.Event()

    def pump():
        for line in proc.stdout:
            log.write(line)
            log.flush()
            if line.startswith("ui demo on "):
                ready.set()

    threading.Thread(target=pump, daemon=True).start()
    deadline = time.monotonic() + 60
    while not ready.wait(0.1):
        if proc.poll() is not None or time.monotonic() > deadline:
            stop(proc)
            raise SystemExit(f"ui_demo did not start; see {evidence / 'demo.log'}")
    base = f"http://127.0.0.1:{port}"
    while True:
        try:
            with urllib.request.urlopen(base + "/ui", timeout=2) as r:
                if r.status == 200:
                    return proc, base
        except OSError:
            pass
        if proc.poll() is not None or time.monotonic() > deadline:
            stop(proc)
            raise SystemExit(f"ui_demo did not serve /ui; see {evidence / 'demo.log'}")
        time.sleep(0.1)


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


async def run_one(browser, base, evidence, name, fault):
    fn = CHECKS[name][0]
    label = name + (f"+{fault}" if fault else "")
    run = Run(browser, base, evidence, label.replace("+", "--"))
    started = time.monotonic()
    failed = None
    try:
        await fn(run, fault)
    except CheckFailed as e:
        failed = str(e)
    except PlaywrightTimeout as e:
        failed = f"{run.step_name}: timed out: {str(e).splitlines()[0]}"
    finally:
        await run.close(failed is not None)
    return {"check": name, "fault": fault, "failed": failed, "seconds": round(time.monotonic() - started, 2)}


async def main_async(args, evidence, base):
    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch()
        except PlaywrightError as e:
            print(f"cannot launch Chromium: {str(e).splitlines()[0]}\n"
                  "install it with: uv run --no-project --with playwright==1.65.2 playwright install chromium-headless-shell",
                  file=sys.stderr)
            return 77, []
        results = []
        for name in args.check or list(CHECKS):
            clean = await run_one(browser, base, evidence, name, None)
            results.append(clean)
            if args.self_test:
                for fault in CHECKS[name][1]:
                    if clean["failed"]:  # a failure would prove nothing about this fault
                        results.append({"check": name, "fault": fault, "failed": None, "seconds": 0, "skipped": True})
                    else:
                        results.append(await run_one(browser, base, evidence, name, fault))
        await browser.close()
    ok = True
    for r in results:
        if r.get("skipped"):
            good, verdict = False, "inconclusive"
        elif r["fault"] is None:
            good, verdict = r["failed"] is None, "pass" if r["failed"] is None else "FAIL"
        else:  # an injected fault must be detected
            good, verdict = r["failed"] is not None, "detected" if r["failed"] else "NOT DETECTED"
        ok &= good
        what = f"{r['check']}" + (f" with fault {r['fault']}" if r["fault"] else "")
        print(f"{verdict:>12}  {what}  ({r['seconds']} s)" + (f"\n{'':>14}{r['failed']}" if r["failed"] else ""))
    return (0 if ok else 1), results


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--demo", type=Path, default=Path("target/debug/examples/ui_demo"),
                    help="built ui_demo executable (cargo build --locked -p ciallopa --example ui_demo)")
    ap.add_argument("--check", action="append", choices=list(CHECKS), help="run only this check (repeatable)")
    ap.add_argument("--self-test", action="store_true", help="also require each injected fault to fail its check")
    ap.add_argument("--evidence", type=Path, help="new directory for the demo log, screenshots and results.json")
    args = ap.parse_args()
    if not args.demo.is_file() or not os.access(args.demo, os.X_OK):
        ap.error(f"{args.demo} is not an executable; build it with cargo build --locked -p ciallopa --example ui_demo")
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        evidence = args.evidence
    else:
        evidence = Path(tempfile.mkdtemp(prefix="ciallopa-ui-browser-"))
    proc, base = start_demo(args.demo.resolve(), evidence)
    try:
        code, results = asyncio.run(main_async(args, evidence, base))
    finally:
        stop(proc)
    (evidence / "results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n")
    print(f"evidence: {evidence}")
    sys.exit(code)


if __name__ == "__main__":
    main()
