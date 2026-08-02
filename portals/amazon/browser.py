"""Portal-agnostic Selenium/Chrome driver management.

Use case:
    Every portal scraper (portals/amazon.py, portals/flipkart.py, ...) needs
    the exact same browser plumbing - launch Chrome, load session cookies,
    scroll/delay like a human, detect a CAPTCHA wall, clean up the process
    tree on close. None of that is specific to any one site, so it lives
    here once instead of being copied into every portal module.

    Deliberately NOT undetected_chromedriver - see _make_driver's docstring
    for why plain Selenium (Service + ChromeDriverManager) with a handful of
    manual stealth flags replaced it.
"""

import logging
import os
import random
import tempfile
import time

log = logging.getLogger("browser")

# This scraper's own temp-file root - per-slot Chrome profile dirs (see
# worker_pool._PROFILE_ROOT) live under here. Used by worker_pool's orphan
# reaper to recognize this scraper's own Chrome/chromedriver processes by
# command line, so it never touches the user's own real Chrome browser
# (which doesn't run out of the temp dir).
_TEMP_DIR = tempfile.gettempdir()

_xvfb_proc = None
_xvfb_lock = None


def start_xvfb() -> None:
    """Start a virtual framebuffer once per process and point DISPLAY at it.

    Real headless mode (--headless=new) crashes on first navigation with
    the deployed VPS's nix-provided Chromium (confirmed by direct
    reproduction: "disconnected: unable to send message to renderer" on
    every single driver.get(), 100% of the time). Running Chrome
    non-headless against an Xvfb virtual display avoids that code path
    entirely while still requiring no real display/GPU. Idempotent and
    thread-safe - every worker thread calls this before launching a driver
    on a non-Windows host."""
    global _xvfb_proc, _xvfb_lock
    import shutil
    import subprocess
    import threading

    if os.environ.get("DISPLAY"):
        return
    if _xvfb_lock is None:
        _xvfb_lock = threading.Lock()
    with _xvfb_lock:
        if os.environ.get("DISPLAY"):
            return
        xvfb_path = shutil.which("Xvfb")
        if not xvfb_path:
            log.warning("Xvfb not found on PATH - falling back to no virtual display")
            return
        _xvfb_proc = subprocess.Popen(
            [xvfb_path, ":99", "-screen", "0", "1440x900x24", "-nolisten", "tcp"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.environ["DISPLAY"] = ":99"
        time.sleep(1)
        log.info("Xvfb virtual display started on :99")


def _detect_chrome_binary() -> str | None:
    """Find whatever Chrome/Chromium binary is actually installed. On the
    VPS (Nixpacks) this is nix's "chromium" package - there's no fixed path
    since nix store paths are hash-based, so PATH lookup via shutil.which is
    the only portable way to find it. On a dev machine with real Google
    Chrome, this returns None and Selenium falls back to its own default
    binary discovery."""
    import shutil

    for name in ("chromium", "chromium-browser", "chromium.exe", "google-chrome", "google-chrome-stable"):
        path = shutil.which(name)
        if path:
            return path
    return None


_driver_executable_path: str | None = None
_driver_path_lock = None


def _resolve_driver_path() -> str:
    """Resolve (and cache) the chromedriver executable path once per
    process. webdriver_manager's ChromeDriverManager().install() is already
    safe to call repeatedly - it just returns the cached path once
    downloaded - but there's no reason to re-touch it on every scrape."""
    import threading

    global _driver_executable_path, _driver_path_lock

    if _driver_executable_path is None:
        if _driver_path_lock is None:
            _driver_path_lock = threading.Lock()
        with _driver_path_lock:
            if _driver_executable_path is None:
                from webdriver_manager.chrome import ChromeDriverManager

                _driver_executable_path = ChromeDriverManager().install()
    return _driver_executable_path


def prepare_shared_driver() -> None:
    """Call once, synchronously, before starting any concurrent workers, so
    the (possibly slow, first-time-only) chromedriver download happens at
    startup rather than blocking the very first real scrape. Raises if this
    fails - better to fail loud at startup than to silently fail every job
    later."""
    path = _resolve_driver_path()
    log.info("Chromedriver ready at %s", path)


def make_driver(headless: bool = True, user_data_dir: str | None = None):
    """Launch a plain Selenium Chrome driver."""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service

    opts = webdriver.ChromeOptions()
    opts.add_argument("--window-size=1440,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-IN")
    opts.add_argument("--accept-lang=en-IN,en;q=0.9")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    binary_path = _detect_chrome_binary()
    if binary_path:
        opts.binary_location = binary_path

    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")

    # Real headless mode works fine on a Windows dev machine's real Chrome
    # install. On the deployed VPS, nix's Chromium crashes on first
    # navigation under --headless=new - run non-headless against a virtual
    # Xvfb display there instead, same as before.
    use_real_headless = os.name == "nt"
    if headless and use_real_headless:
        opts.add_argument("--headless=new")
    elif not use_real_headless:
        start_xvfb()

    service = Service(_resolve_driver_path())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    # Plain Selenium doesn't spoof navigator.webdriver on its own the way
    # undetected_chromedriver does - this one-line override is the actual
    # anti-detection primitive that matters in practice (excludeSwitches/
    # useAutomationExtension above handle the rest of the common tells).
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def close_driver(driver) -> None:
    """driver.quit() alone isn't always reliable at actually killing the
    underlying Chrome process tree - explicitly kill the chromedriver
    service process (and its child Chrome browser process) via psutil as a
    safety net so a long-running worker pool can't slowly exhaust memory
    from leaked processes. Plain Selenium's Service spawns chromedriver,
    which in turn spawns Chrome as its own child - killing service_pid's
    whole subtree covers both.

    Waits (briefly) for each killed process to actually exit before
    returning - psutil's kill() only *sends* the signal and returns
    immediately, so the very next launch reusing the same profile dir (see
    worker_pool._get_slot_driver) could otherwise start before Chrome had
    actually released its profile lockfile, failing with "session not
    created: Chrome instance exited" on effectively every subsequent job in
    a cascade - not a launch problem at all, just this function returning
    before the processes it just killed were actually gone."""
    service_pid = getattr(getattr(driver, "service", None), "process", None)
    service_pid = getattr(service_pid, "pid", None)
    try:
        driver.quit()
    except Exception as exc:  # noqa: BLE001
        log.debug("driver.quit() raised (continuing to process-kill fallback): %s", exc)

    if not service_pid:
        return
    import psutil

    try:
        proc = psutil.Process(service_pid)
        procs = proc.children(recursive=True) + [proc]
        for p in procs:
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(procs, timeout=5)
    except Exception as exc:  # noqa: BLE001 - best-effort cleanup, never fail the scrape over this
        log.debug("Process-kill fallback failed for pid %s: %s", service_pid, exc)


def human_delay(min_s: float = 2.0, max_s: float = 4.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


def human_scroll(driver) -> None:
    """Scroll down the page in irregular steps instead of jumping straight to
    reading page_source. Reviews/rating widgets are commonly lazy-loaded
    below the fold, and a page that never scrolls is also a stronger bot
    signal than the pattern this mimics."""
    total_height = driver.execute_script("return document.body.scrollHeight")
    pos = 0
    while pos < total_height:
        step = random.randint(300, 600)
        pos = min(pos + step, total_height)
        driver.execute_script(f"window.scrollTo(0, {pos});")
        time.sleep(random.uniform(0.08, 0.2))


def is_captcha(driver) -> bool:
    """Generic CAPTCHA/bot-check text patterns common across most sites.
    Portal modules can layer on site-specific checks (e.g. a particular
    page title) on top of this if needed."""
    src = driver.page_source.lower()
    return (
        "type the characters" in src
        or "enter the characters" in src
        or "captcha" in src
        or "verify you are human" in src
        or "unusual traffic" in src
    )


def load_cookies(driver, cookies=None, cookies_path: str | None = None) -> bool:
    """Load exported session cookies (e.g. from the "Cookie-Editor" or "Get
    cookies.txt LOCALLY" browser extension's JSON export, or the dashboard's
    Cookies panel) into the driver so it inherits a real logged-in session.

    Pass `cookies` (a pre-loaded list, e.g. from the DB via jobs.get_cookies())
    or `cookies_path` (a JSON file) - cookies takes priority if both given.

    Must be called with the driver already on the target site's domain
    (cookies can only be added for the currently-loaded domain). Never
    handles a password - only pre-existing session cookies the user
    exported themselves from their own already-logged-in browser."""
    import json

    if cookies is None:
        if not cookies_path or not os.path.exists(cookies_path):
            return False
        try:
            with open(cookies_path, encoding="utf-8") as f:
                cookies = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Could not read cookies file %s: %s", cookies_path, exc)
            return False

    if not cookies:
        return False

    loaded = 0
    for c in cookies:
        cookie = {"name": c["name"], "value": c["value"], "path": c.get("path", "/")}
        if c.get("domain"):
            cookie["domain"] = c["domain"]
        if "expirationDate" in c:
            cookie["expiry"] = int(c["expirationDate"])
        try:
            driver.add_cookie(cookie)
            loaded += 1
        except Exception as exc:  # noqa: BLE001 - one bad cookie shouldn't block the rest
            log.debug("Skipped cookie %s: %s", c.get("name"), exc)

    log.info("Loaded %s/%s cookies.", loaded, len(cookies))
    return loaded > 0
