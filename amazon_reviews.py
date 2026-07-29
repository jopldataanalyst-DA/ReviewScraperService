"""Amazon review scraper.

Use case:
    Given an Amazon product listing URL (or bare ASIN), scrapes the public
    product detail page and returns reviews + rating breakdown. Deliberately
    fails soft: any CAPTCHA wall or parse error on a given product just
    returns whatever was already parsed - the caller (run_scheduler.py)
    treats a short/empty result as "try again next run", not a fatal error,
    so one blocked SKU never takes down the whole daily batch.

    This is part of the standalone Review Scraper service - deliberately
    NOT part of the main PricingModule app (which only reads what this
    service writes to Postgres; see that repo's App/Api/Reviews.py).
"""

import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from bs4 import BeautifulSoup

log = logging.getLogger("amazon_reviews_scraper")

# Force setuptools' distutils shim to register once, deterministically, at
# module load time (single-threaded, before any concurrent worker calls
# _make_driver) rather than relying on it happening implicitly via a .pth
# hook at interpreter startup. Confirmed by direct reproduction: under
# concurrent load, `import undetected_chromedriver` (which needs
# `distutils`, removed from the stdlib in Python 3.12) can fail once
# transiently - and because a failed import leaves the module half-
# registered in sys.modules, every later import in that same process then
# fails identically forever, even though the root cause was momentary.
try:
    import setuptools  # noqa: F401
except ImportError:
    pass


@dataclass
class ScrapedReview:
    review_id: str
    author: str = ""
    rating: Optional[float] = None
    title: str = ""
    review_text: str = ""
    review_date: Optional[datetime] = None
    verified_purchase: Optional[bool] = None
    helpful_count: Optional[int] = None
    images: list[str] = field(default_factory=list)
    size: str = ""
    color: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

BASE_URL = "https://www.amazon.in"
MAX_REVIEW_PAGES = 10  # Amazon caps the review listing at 10 pages anyway


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


def extract_asin(url_or_asin: str) -> str:
    """Pull the ASIN out of any Amazon product URL, or pass through a bare ASIN."""
    if not url_or_asin:
        return ""
    match = re.search(r"/(?:dp|gp/product|product-reviews)/([A-Z0-9]{10})", url_or_asin)
    if match:
        return match.group(1)
    if re.fullmatch(r"[A-Z0-9]{10}", url_or_asin.strip()):
        return url_or_asin.strip()
    return ""


def _detect_chrome_binary() -> tuple[str | None, str]:
    """Find whatever Chrome/Chromium binary is actually installed and report
    which chrome_type webdriver_manager should use to fetch a matching
    driver. On the VPS (Nixpacks) this is nix's "chromium" package - there's
    no fixed path since nix store paths are hash-based, so PATH lookup via
    shutil.which is the only portable way to find it. On a dev machine with
    real Google Chrome, this returns (None, "google-chrome") and Selenium
    falls back to its own default binary discovery."""
    import shutil
    from webdriver_manager.core.os_manager import ChromeType

    for name in ("chromium", "chromium-browser", "chromium.exe"):
        path = shutil.which(name)
        if path:
            return path, ChromeType.CHROMIUM
    return None, ChromeType.GOOGLE


# --- One-time, single-threaded driver setup -------------------------------
#
# Redesigned after extensive direct reproduction showed that resolving and
# copying/patching the chromedriver binary on EVERY scrape attempt was the
# common root of every intermittent failure mode seen under concurrent load
# (WinError 32 file-copy collisions, and near-certainly the sporadic
# "No module named 'distutils'" import failures too - both cluster around
# this exact hot path, and neither reproduces in isolation or at low
# concurrency, only under sustained multi-worker load). Rather than layering
# more retry/self-heal logic onto a fundamentally racy per-call operation,
# this resolves the source binary, imports undetected_chromedriver, and lets
# uc patch ONE stable copy of the driver exactly once - synchronously,
# single-threaded, at process startup, before any worker thread exists.
# Every subsequent driver launch just points at that same already-patched,
# read-only-in-practice file. No repeated copies, no repeated first-imports,
# no concurrency exposure in this path at all after startup.

_shared_driver_path: str | None = None
_shared_binary_path: str | None = None
_shared_chrome_type = None
_uc_module = None  # cached undetected_chromedriver module object - see prepare_shared_driver


def prepare_shared_driver() -> None:
    """Call once, synchronously, before starting any concurrent workers.
    Resolves the Chrome binary, imports undetected_chromedriver, and patches
    one stable chromedriver copy. Raises if this fails - better to fail loud
    at startup than to silently fail every job later.

    Caches the imported module object in _uc_module so every later call in
    _launch_chrome uses that direct reference instead of re-executing an
    `import undetected_chromedriver` statement per call - even though a
    repeat import of an already-cached module is normally an instant
    sys.modules lookup, direct reproduction showed sporadic
    "No module named 'distutils'" failures persisting under sustained
    concurrent load even after eliminating every other per-call import/copy
    in this path, so this removes the last remaining per-call import
    statement in the hot concurrent path entirely."""
    import shutil
    import tempfile

    global _shared_driver_path, _shared_binary_path, _shared_chrome_type, _uc_module

    import undetected_chromedriver as uc  # import once, here, single-threaded

    _uc_module = uc

    binary_path, chrome_type = _detect_chrome_binary()
    _shared_binary_path = binary_path
    _shared_chrome_type = chrome_type

    src = shutil.which("chromedriver")
    if not src:
        from webdriver_manager.chrome import ChromeDriverManager

        src = ChromeDriverManager(chrome_type=chrome_type).install()

    exe_suffix = ".exe" if src.lower().endswith(".exe") else ""
    stable_path = os.path.join(tempfile.gettempdir(), f"uc_chromedriver_shared{exe_suffix}")
    shutil.copy2(src, stable_path)
    os.chmod(stable_path, 0o755)

    # Force the patch (strip cdc_ signatures etc.) to happen now, once, by
    # actually launching and closing a real headless Chrome instance against
    # this file - the same code path _make_driver uses, just done eagerly
    # and synchronously so every later concurrent call skips straight to a
    # pre-patched binary instead of racing to patch it themselves.
    test_driver = _launch_chrome(stable_path, binary_path, headless=True, user_data_dir=None)
    _close_driver(test_driver)

    _shared_driver_path = stable_path
    log.info("Shared chromedriver prepared and patched at %s", stable_path)


def _launch_chrome(driver_path: str, binary_path: str | None, headless: bool, user_data_dir: str | None):
    uc = _uc_module  # cached module reference - no import statement in this hot concurrent path

    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1440,900")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-IN")
    opts.add_argument("--accept-lang=en-IN,en;q=0.9")

    # Container/headless stability flags - the VPS's nix-provided Chromium has
    # no real GPU/drivers, and letting Chrome attempt GPU compositing for
    # actual page rendering (not just process startup, which succeeds fine
    # even without these) is a common cause of the renderer disconnecting
    # entirely mid-navigation (confirmed by direct reproduction on the
    # deployed Linux server: "disconnected: unable to send message to
    # renderer" on every single driver.get() call, 100% of the time, even
    # at low concurrency - Chrome itself launches fine, only real page loads
    # crash it). These trade off unnecessary subsystems Chrome doesn't need
    # for a scraping workload for stability in a constrained container.
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-translate")
    opts.add_argument("--metrics-recording-only")
    opts.add_argument("--mute-audio")
    opts.add_argument("--no-first-run")
    opts.add_argument("--safebrowsing-disable-auto-update")
    opts.add_argument("--disable-setuid-sandbox")

    # undetected_chromedriver's own patching (stripped cdc_ variables, spoofed
    # driver signature) is what actually evades Amazon's automation checks -
    # plain Selenium's excludeSwitches/useAutomationExtension flags alone are
    # not enough anymore. uc handles the navigator.webdriver override itself.
    #
    # use_subprocess=False: confirmed by direct reproduction on the deployed
    # Linux container that use_subprocess=True (uc's default) reliably
    # produces "disconnected: unable to send message to renderer" on every
    # single navigation - 100% failure, deterministic, regardless of
    # concurrency (tested down to 1-2 workers) or available memory (tested
    # up to 6GB limit). This is a well-documented undetected_chromedriver
    # quirk specifically in Docker/CI-style containers: use_subprocess's
    # process-group/session handling for the Chrome process doesn't play
    # well with some container runtimes' process namespacing, breaking the
    # DevTools websocket connection to the renderer. False lets uc manage
    # the Chrome process the same way plain Selenium does.
    driver = uc.Chrome(
        options=opts,
        headless=headless,
        driver_executable_path=driver_path,
        browser_executable_path=binary_path or None,
        user_data_dir=user_data_dir,
        use_subprocess=False,
    )
    driver.set_page_load_timeout(30)
    return driver


_prepare_lock = None  # lazily created threading.Lock() - see _make_driver's fallback


def _per_worker_driver_copy(unique_key: str) -> str:
    """Copy the already-patched shared driver to a per-worker file.

    undetected_chromedriver's Chrome() constructs a fresh Patcher and calls
    .auto() on EVERY instantiation, not just once (confirmed by reading
    undetected_chromedriver/__init__.py directly) - so even though
    prepare_shared_driver() patches one binary up front, every concurrent
    uc.Chrome() call still touches that file's patcher logic again. Multiple
    workers doing that against the SAME file at the same time is exactly the
    kind of concurrent-file-access collision that caused WinError 32 before
    (and is the strongest remaining candidate for the sporadic
    "No module named 'distutils'" failures too - both are symptoms of
    concurrent access to one shared file, just surfacing through different
    internal code paths). Copying from the now-stable, unchanging, already-
    patched file (not the original webdriver_manager cache, which is what
    caused the original collision) is fast and gives each worker an
    independent file with no shared-access exposure at all."""
    import shutil
    import tempfile

    exe_suffix = ".exe" if _shared_driver_path.lower().endswith(".exe") else ""
    dest = os.path.join(tempfile.gettempdir(), f"uc_chromedriver_{unique_key}{exe_suffix}")
    shutil.copy2(_shared_driver_path, dest)
    os.chmod(dest, 0o755)
    return dest


def _make_driver(headless: bool = True, user_data_dir: str | None = None):
    import threading

    global _prepare_lock
    if _shared_driver_path is None:
        # Safety net (e.g. ad-hoc scripts/tests that never called
        # prepare_shared_driver() explicitly) - do the one-time setup now
        # instead of failing, guarded by a lock so concurrent callers don't
        # race each other into doing it twice. In the real worker pool this
        # should never trigger, since dashboard.py calls
        # prepare_shared_driver() at startup before any worker thread exists.
        if _prepare_lock is None:
            _prepare_lock = threading.Lock()
        with _prepare_lock:
            if _shared_driver_path is None:
                prepare_shared_driver()

    unique_key = os.path.basename(user_data_dir) if user_data_dir else str(threading.get_ident())
    per_worker_path = _per_worker_driver_copy(unique_key)
    return _launch_chrome(per_worker_path, _shared_binary_path, headless, user_data_dir)


def _close_driver(driver) -> None:
    """driver.quit() alone isn't reliable at actually killing the underlying
    Chrome process tree - confirmed by direct reproduction: dozens of
    orphaned chrome.exe/chromedriver.exe processes accumulated across a test
    session even though every scrape called driver.quit() in a finally
    block. Explicitly kill the browser process (and children) via psutil as
    a safety net so a long-running worker pool can't slowly exhaust memory
    on the VPS from leaked Chrome processes."""
    browser_pid = getattr(driver, "browser_pid", None)
    try:
        driver.quit()
    except Exception as exc:  # noqa: BLE001
        log.debug("driver.quit() raised (continuing to process-kill fallback): %s", exc)

    if not browser_pid:
        return
    try:
        import psutil

        proc = psutil.Process(browser_pid)
        children = proc.children(recursive=True)
        for child in children:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass
    except Exception as exc:  # noqa: BLE001 - best-effort cleanup, never fail the scrape over this
        log.debug("Process-kill fallback failed for pid %s: %s", browser_pid, exc)


def _human_delay(min_s: float = 2.0, max_s: float = 4.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _human_scroll(driver) -> None:
    """Scroll down the page in irregular steps instead of jumping straight to
    reading page_source. Reviews/rating-histogram widgets are lazy-loaded
    below the fold, and a page that never scrolls is also a stronger bot
    signal to Amazon than the pattern this mimics."""
    total_height = driver.execute_script("return document.body.scrollHeight")
    pos = 0
    while pos < total_height:
        step = random.randint(300, 600)
        pos = min(pos + step, total_height)
        driver.execute_script(f"window.scrollTo(0, {pos});")
        time.sleep(random.uniform(0.15, 0.4))


def _is_captcha(driver) -> bool:
    src = driver.page_source.lower()
    return (
        "type the characters" in src
        or "enter the characters" in src
        or "captcha" in src
        or driver.title.lower().strip() == "robot check"
    )


def _parse_variant(raw: str) -> tuple[str, str]:
    """Amazon's "format-strip" variant text has no separator between fields
    - e.g. "Size: 2XL" or "Size: XLColour: Navy Blue" (literally
    "XLColour", no space) - so Size has to stop right before "Colour"
    appears, not at the end of string."""
    if not raw:
        return "", ""
    size_m = re.search(r"Size:\s*(.+?)(?=Colour:|$)", raw)
    color_m = re.search(r"Colour:\s*(.+)$", raw)
    size = size_m.group(1).strip() if size_m else ""
    color = color_m.group(1).strip() if color_m else ""
    return size, color


def _parse_review_date(raw: str) -> Optional[datetime]:
    # Typical Amazon.in text: "Reviewed in India on 12 March 2024"
    match = re.search(r"on\s+(\d{1,2}\s+\w+\s+\d{4})", raw or "")
    if not match:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(match.group(1), fmt)
        except ValueError:
            continue
    return None


def parse_rating_summary(html: str) -> dict:
    """Parse the star-rating histogram widget on the product page - the
    "4.5 out of 5, 101 global ratings, 5 star 59% / 4 star 16% / ..." box
    shown next to the review list. Returns {} if the product has no ratings
    (histogram widget absent entirely, e.g. a brand-new listing)."""
    soup = BeautifulSoup(html, "lxml")
    summary: dict = {
        "average_rating": None,
        "total_ratings": None,
        "star_breakdown_pct": {},
    }

    avg_el = soup.select_one('[data-hook="average-star-rating"] span.a-icon-alt, [data-hook="rating-out-of-text"]')
    if avg_el:
        m = re.search(r"([\d.]+)\s*out of", avg_el.get_text())
        if m:
            summary["average_rating"] = float(m.group(1))

    total_el = soup.select_one('[data-hook="total-review-count"]')
    if total_el:
        m = re.search(r"([\d,]+)", total_el.get_text())
        if m:
            summary["total_ratings"] = int(m.group(1).replace(",", ""))

    breakdown = {}
    for row in soup.select('#histogramTable a[aria-label*="percent of reviews have"]'):
        label = row.get("aria-label", "")
        m = re.search(r"(\d+)\s*percent of reviews have (\d)\s*star", label)
        if m:
            breakdown[f"{m.group(2)}_star_pct"] = int(m.group(1))
    summary["star_breakdown_pct"] = breakdown

    if summary["average_rating"] is None and summary["total_ratings"] is None and not breakdown:
        return {}
    return summary


def scrape_amazon_rating_summary(product_url_or_asin: str, headless: bool = True) -> dict:
    """Convenience wrapper: load the product page once and return just the
    rating-histogram summary (no review cards). Useful when you only need
    the 5/4/3/2/1-star percentage breakdown, not the individual reviews."""
    asin = extract_asin(product_url_or_asin)
    if not asin:
        return {}

    driver = _make_driver(headless=headless)
    try:
        driver.get(BASE_URL)
        _human_delay(1.5, 3.0)
        driver.get(f"{BASE_URL}/dp/{asin}")
        _human_delay(2.0, 4.0)
        if _is_captcha(driver):
            log.warning("CAPTCHA hit for ASIN %s - will retry next run.", asin)
            return {}
        return parse_rating_summary(driver.page_source)
    finally:
        driver.quit()


def _parse_reviews_page(html: str) -> list[ScrapedReview]:
    soup = BeautifulSoup(html, "lxml")
    reviews: list[ScrapedReview] = []

    # The product page's own widget uses <div data-hook="review">, but the
    # full "Customer reviews" listing page uses <li data-hook="review"> -
    # match on the attribute alone so both work (confirmed by direct
    # reproduction of both page types).
    for card in soup.select('[data-hook="review"]'):
        try:
            review_id = card.get("id", "")
            if not review_id:
                continue

            author_el = card.select_one("span.a-profile-name")
            author = _clean(author_el.get_text()) if author_el else ""

            rating = None
            rating_el = card.select_one('i[data-hook="review-star-rating"] span, i[data-hook="cmps-review-star-rating"] span')
            if rating_el:
                m = re.search(r"([\d.]+)\s*out of", rating_el.get_text())
                if m:
                    rating = float(m.group(1))

            title_el = card.select_one('[data-hook="reviewTitle"]')
            title = _clean(title_el.get_text()) if title_el else ""

            date_el = card.select_one('span[data-hook="review-date"]')
            date_raw = date_el.get_text() if date_el else ""
            review_date = _parse_review_date(date_raw)

            body_el = (
                card.select_one('div[data-hook="reviewRichContentContainer"]')
                or card.select_one('span[data-hook="review-body"] span')
                or card.select_one('span[data-hook="review-body"]')
            )
            review_text = _clean(body_el.get_text()) if body_el else ""

            verified_el = card.select_one('span[data-hook="avp-badge"]')
            verified_purchase = bool(verified_el and "verified purchase" in verified_el.get_text().lower())

            helpful_el = card.select_one('span[data-hook="helpful-vote-statement"]')
            helpful_count = None
            if helpful_el:
                m = re.search(r"([\d,]+)", helpful_el.get_text())
                if m:
                    helpful_count = int(m.group(1).replace(",", ""))

            variant_el = card.select_one('a[data-hook="format-strip"]')
            variant = _clean(variant_el.get_text()) if variant_el else ""
            size, color = _parse_variant(variant)

            images = [
                img.get("src", "")
                for img in card.select('img[data-hook="review-image-tile"]')
                if img.get("src")
            ]

            vine_badge = card.select_one('span.a-color-success.a-text-bold')
            is_vine = bool(vine_badge and "vine" in vine_badge.get_text().lower())

            reviews.append(
                ScrapedReview(
                    review_id=review_id,
                    author=author,
                    rating=rating,
                    title=title,
                    review_text=review_text,
                    review_date=review_date,
                    verified_purchase=verified_purchase,
                    helpful_count=helpful_count,
                    images=images,
                    size=size,
                    color=color,
                    extra={
                        "variant_purchased": variant,
                        "vine_voice": is_vine,
                        "raw_date_text": _clean(date_raw),
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001 - one bad card shouldn't drop the rest
            log.warning("Failed to parse a review card: %s", exc)

    return reviews


def check_cookies_valid(cookies: list, headless: bool = True) -> bool:
    """Real live check of whether a set of session cookies is still valid -
    loads them, reloads the homepage, and looks for Amazon's own logged-out
    indicator ("Hello, sign in" in the nav greeting) rather than depending
    on any specific product page. Used by the dashboard's "Test Cookies"
    button. Always closes its own driver."""
    driver = _make_driver(headless=headless)
    try:
        driver.get(BASE_URL)
        _human_delay(1.0, 2.0)
        if not _load_cookies(driver, cookies=cookies):
            return False
        driver.get(BASE_URL)
        _human_delay(1.5, 3.0)
        return "hello, sign in" not in driver.page_source.lower()
    finally:
        _close_driver(driver)


MAX_LOAD_MORE_CLICKS = 30  # ~10 reviews/click -> up to ~300 reviews per product


def _open_all_reviews_page(driver, asin: str) -> bool:
    """From an already-loaded /dp/{asin} page (with an established browsing
    session), navigate to the full "Customer reviews" listing - this page
    exposes every review via a "Show N more reviews" button, unlike the
    product page's own widget which only ever surfaces ~8-10 reviews.

    Confirmed by direct reproduction that the "see all reviews" link on the
    product page is /portal/customer-reviews/{asin}/...?reviewerType=all_reviews
    - clicking it isn't reliable in a headless viewport (the link can be
    off-screen and Selenium's .click() silently no-ops instead of
    navigating), so this constructs and navigates to that URL directly
    rather than finding/clicking the element.

    Returns True if the page loaded without landing on a sign-in wall,
    False otherwise (falls back to whatever scrape_amazon_product already
    parsed from the product page itself)."""
    driver.get(f"{BASE_URL}/portal/customer-reviews/{asin}/?ie=UTF8&reviewerType=all_reviews")
    _human_delay(1.5, 3.0)

    if "sign-in" in driver.current_url.lower() or "ap/signin" in driver.current_url.lower():
        log.info("All-reviews page redirected to sign-in for ASIN %s - using product-page reviews only.", asin)
        return False
    return True


def _load_more_reviews(driver) -> list:
    """Repeatedly click the "Show N more reviews" button on the all-reviews
    page, collecting every review card seen so far after each click, until
    the button disappears, stops adding new reviews, or a CAPTCHA/hard cap
    is hit. Returns the deduped list of ScrapedReview across all pages."""
    from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException
    from selenium.webdriver.common.by import By

    seen_ids: set[str] = set()
    all_reviews: list = []

    def _collect() -> None:
        for r in _parse_reviews_page(driver.page_source):
            if r.review_id not in seen_ids:
                seen_ids.add(r.review_id)
                all_reviews.append(r)

    _collect()

    button_selectors = [
        '[data-hook="show-more-button"]',
        '[data-hook="cr-pagination-more-reviews-trigger"]',
        "//span[contains(text(), 'more reviews')]/ancestor::*[self::button or self::a][1]",
        "//span[contains(text(), 'more review')]/ancestor::*[self::button or self::a][1]",
    ]

    for _ in range(MAX_LOAD_MORE_CLICKS):
        if _is_captcha(driver):
            log.warning("CAPTCHA hit while paginating reviews - stopping with %s collected so far.", len(all_reviews))
            break

        button = None
        for selector in button_selectors:
            try:
                if selector.startswith("//"):
                    button = driver.find_element(By.XPATH, selector)
                else:
                    button = driver.find_element(By.CSS_SELECTOR, selector)
                break
            except NoSuchElementException:
                continue
        if button is None:
            break

        before = len(all_reviews)
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            _human_delay(0.3, 0.8)
            button.click()
        except ElementClickInterceptedException:
            driver.execute_script("arguments[0].click();", button)
        _human_delay(1.5, 3.0)
        _collect()

        if len(all_reviews) == before:
            break  # button present but nothing new loaded - avoid an infinite click loop

    return all_reviews


DEFAULT_COOKIES_PATH = os.environ.get("AMAZON_COOKIES_PATH", "amazon_cookies.json")


def _load_cookies(driver, cookies=None, cookies_path: str | None = None) -> bool:
    """Load exported Amazon session cookies (e.g. from the "Cookie-Editor"
    or "Get cookies.txt LOCALLY" browser extension's JSON export, or the
    dashboard's Cookies panel) into the driver so it inherits a real
    logged-in session - required to reach the full "Customer reviews"
    listing page, which redirects a logged-out session to a sign-in wall
    (confirmed by direct reproduction).

    Pass `cookies` (a pre-loaded list, e.g. from the DB via jobs.get_cookies())
    or `cookies_path` (a JSON file) - cookies takes priority if both given.

    Must be called with the driver already on an amazon.in page (cookies
    can only be added for the currently-loaded domain). Never handles a
    password - only pre-existing session cookies the user exported
    themselves from their own already-logged-in browser."""
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


def scrape_amazon_product(
    product_url_or_asin: str, headless: bool = True, user_data_dir: str | None = None,
    full_reviews: bool = True, cookies: list | None = None, cookies_path: str | None = None,
) -> dict:
    """Load an Amazon product page once and return both the review cards and
    the rating-histogram summary (5/4/3/2/1-star percentages, average
    rating, total ratings) from that single page load.

    The rating histogram always comes from the product page's own widget.
    For reviews: if full_reviews=True (default), after loading /dp/{asin}
    this clicks through to the "Customer reviews" listing (the "See all
    reviews" link) and repeatedly clicks "Show N more reviews" there,
    collecting every review card seen - not just the ~8-10 the product page
    itself surfaces. Hitting that page cold/logged-out used to redirect to a
    sign-in wall; going through the product page first with an established
    browsing session (and undetected_chromedriver) avoids that in practice.
    Falls back to the product page's own ~8-10 reviews if the "see all
    reviews" link isn't found or a CAPTCHA interrupts pagination.

    Returns {"asin": ..., "reviews": [ScrapedReview, ...], "rating_summary": {...},
    "blocked": bool}. rating_summary/reviews can legitimately both be empty
    for a real product with zero reviews yet (confirmed this happens for
    real listings, not a scraper bug) - "blocked" is what actually
    distinguishes a CAPTCHA/load failure (retriable, no real data) from a
    successful load of a genuinely empty product page (done, zero results).
    """
    asin = extract_asin(product_url_or_asin)
    if not asin:
        log.warning("Could not extract ASIN from %s", product_url_or_asin)
        return {"asin": "", "reviews": [], "rating_summary": {}, "blocked": True}

    driver = _make_driver(headless=headless, user_data_dir=user_data_dir)
    try:
        try:
            driver.get(BASE_URL)
            _human_delay(1.5, 3.0)
            if _load_cookies(driver, cookies=cookies, cookies_path=cookies_path or DEFAULT_COOKIES_PATH):
                driver.get(BASE_URL)  # reload with the session cookies now attached
                _human_delay(1.0, 2.0)
        except Exception as exc:  # noqa: BLE001
            log.warning("Homepage load failed for ASIN %s: %s", asin, exc)
            return {"asin": asin, "reviews": [], "rating_summary": {}, "blocked": True}

        # A page that never times out on Amazon's end is unusual; retry a
        # couple of times before giving up on this product for this run.
        for attempt in range(1, 4):
            try:
                driver.get(f"{BASE_URL}/dp/{asin}")
                _human_delay(2.0, 4.0)
                _human_scroll(driver)
                _human_delay(0.5, 1.5)
                break
            except Exception as exc:  # noqa: BLE001
                log.warning("Product page load failed for ASIN %s (attempt %s): %s", asin, attempt, exc)
                if attempt == 3:
                    return {"asin": asin, "reviews": [], "rating_summary": {}, "blocked": True}
                time.sleep(5)

        if _is_captcha(driver):
            log.warning("CAPTCHA hit for ASIN %s - will retry next run.", asin)
            return {"asin": asin, "reviews": [], "rating_summary": {}, "blocked": True}

        # Rating histogram lives on the product page itself either way - grab
        # it before possibly navigating away to the all-reviews listing.
        rating_summary = parse_rating_summary(driver.page_source)

        reviews = _parse_reviews_page(driver.page_source)
        if full_reviews and _open_all_reviews_page(driver, asin):
            if _is_captcha(driver):
                log.warning("CAPTCHA hit opening all-reviews page for ASIN %s - using product-page reviews only.", asin)
            else:
                reviews = _load_more_reviews(driver)

        return {
            "asin": asin,
            "reviews": reviews,
            "rating_summary": rating_summary,
            "blocked": False,
        }
    finally:
        _close_driver(driver)


def scrape_amazon_reviews(product_url_or_asin: str, headless: bool = True, max_pages: int = MAX_REVIEW_PAGES) -> list[ScrapedReview]:
    """Back-compat wrapper: reviews only (used by the Reviews API/scheduler,
    which doesn't need the rating histogram)."""
    return scrape_amazon_product(product_url_or_asin, headless=headless)["reviews"]
