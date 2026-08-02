"""Amazon.in review scraper - portal-specific parsing/navigation logic.

Use case:
    Given an Amazon product listing URL (or bare ASIN), scrapes the public
    product detail page and returns reviews + rating breakdown. Deliberately
    fails soft: any CAPTCHA wall or parse error on a given product just
    returns whatever was already parsed - the caller (worker_pool.py) treats
    a short/empty result as "try again next run", not a fatal error, so one
    blocked SKU never takes down the whole batch.

    Generic browser/driver plumbing (launch Chrome, cookies, human-like
    delay/scroll, CAPTCHA detection) lives in browser.py - this file only
    has what's actually specific to amazon.in's URLs and DOM.
"""

import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from bs4 import BeautifulSoup

from browser import close_driver, human_delay, human_scroll, is_captcha, load_cookies, make_driver

log = logging.getLogger("amazon.scraper")

BASE_URL = "https://www.amazon.in"
MAX_REVIEW_PAGES = 10  # Amazon caps the review listing at 10 pages anyway
MAX_LOAD_MORE_CLICKS = 30  # ~10 reviews/click -> up to ~300 reviews per product
DEFAULT_COOKIES_PATH = os.environ.get("AMAZON_COOKIES_PATH", "amazon_cookies.json")


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


def _empty_result(product_id: str = "", blocked: bool = True) -> dict:
    return {"asin": product_id, "reviews": [], "rating_summary": {}, "blocked": blocked}


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


def _hover_rating_badge(driver) -> None:
    """The average-rating text and the star-breakdown histogram aren't
    always present in the initial page_source - on some listings Amazon
    only injects that markup into the DOM once the rating badge/stars near
    the title is actually hovered (the same popover a real shopper triggers
    to see "N global ratings" + the per-star breakdown). Confirmed by direct
    reproduction: a product showing "2.5 out of 5 (2 ratings)" in a real
    browser parsed as a completely empty rating_summary ({}) from this
    scraper's page_source until this hover was added. Best-effort - a
    product with zero ratings has no badge to hover, so finding nothing
    here is normal and not an error."""
    from selenium.common.exceptions import NoSuchElementException
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By

    selectors = [
        '#acrPopover',
        '[data-hook="rating-out-of-text"]',
        '[data-hook="average-star-rating"]',
        '#averageCustomerReviews',
    ]
    for selector in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
        except NoSuchElementException:
            continue
        try:
            ActionChains(driver).move_to_element(el).perform()
            human_delay(0.3, 0.6)
        except Exception as exc:  # noqa: BLE001 - best-effort, never fail the scrape over a hover
            log.debug("Hovering rating badge failed: %s", exc)
        return


def _dismiss_continue_shopping(driver) -> bool:
    """Amazon sometimes shows a soft "Click the button below to continue
    shopping" interstitial instead of an image CAPTCHA - a real click-through
    gate, not a puzzle. Clicking it is just completing normal page
    navigation, the same as clicking any other button on the site; it's not
    solving a challenge. Returns True if the interstitial was present and
    dismissed."""
    from selenium.common.exceptions import NoSuchElementException
    from selenium.webdriver.common.by import By

    if "continue shopping" not in driver.page_source.lower():
        return False
    try:
        button = driver.find_element(
            By.XPATH,
            "//button[contains("
            "translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
            "'continue shopping')]",
        )
        button.click()
        log.info("Dismissed 'Continue shopping' interstitial.")
        human_delay(0.5, 1.0)
        return True
    except NoSuchElementException:
        log.debug("'Continue shopping' text present but no matching button found.")
        return False


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

    # Amazon renders the rating badge two different ways depending on how
    # many ratings a product has: the usual "X out of 5, N global ratings"
    # histogram widget (data-hook selectors below), or - confirmed by direct
    # reproduction on a real product with just 1 rating and 0 written
    # reviews - the older/simpler acrPopover badge near the title
    # (#acrPopover / #acrCustomerReviewText), which the data-hook selectors
    # don't match at all.
    avg_el = soup.select_one(
        '[data-hook="average-star-rating"] span.a-icon-alt, '
        '[data-hook="rating-out-of-text"], '
        '#acrPopover span.a-icon-alt, '
        '#averageCustomerReviews span.a-icon-alt'
    )
    if avg_el:
        m = re.search(r"([\d.]+)\s*out of", avg_el.get_text())
        if m:
            summary["average_rating"] = float(m.group(1))

    total_el = soup.select_one(
        '[data-hook="total-review-count"], '
        '#acrCustomerReviewText, '
        '#averageCustomerReviews #acrCustomerReviewText'
    )
    if total_el:
        m = re.search(r"([\d,]+)", total_el.get_text())
        if m:
            summary["total_ratings"] = int(m.group(1).replace(",", ""))

    breakdown = {}
    for row in soup.select('#histogramTable a[aria-label*="percent of reviews have"], #cm-cr-dp-review-histogram a[aria-label*="percent of reviews have"]'):
        label = row.get("aria-label", "")
        m = re.search(r"(\d+)\s*percent of reviews have (\d)\s*star", label)
        if m:
            breakdown[f"{m.group(2)}_star_pct"] = int(m.group(1))
    summary["star_breakdown_pct"] = breakdown

    if summary["average_rating"] is None and summary["total_ratings"] is None and not breakdown:
        return {}
    return summary


def _parse_reviews_page(html: str) -> list[ScrapedReview]:
    soup = BeautifulSoup(html, "lxml")
    reviews: list[ScrapedReview] = []

    # The product page's own widget uses <div data-hook="review">, but the
    # full "Customer reviews" listing page uses <li data-hook="review"> -
    # match on the attribute alone so both work.
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


def _open_all_reviews_page(driver, asin: str) -> bool:
    """From an already-loaded /dp/{asin} page (with an established browsing
    session), navigate to the full "Customer reviews" listing - this page
    exposes every review via a "Show N more reviews" button, unlike the
    product page's own widget which only ever surfaces ~8-10 reviews.

    Navigates by constructing the URL directly rather than clicking the "see
    all reviews" link - clicking it isn't reliable in a headless viewport
    (the link can be off-screen and Selenium's .click() silently no-ops
    instead of navigating).

    Returns True if the page loaded without landing on a sign-in wall,
    False otherwise (falls back to whatever scrape_product_page already
    parsed from the product page itself)."""
    driver.get(f"{BASE_URL}/portal/customer-reviews/{asin}/?ie=UTF8&reviewerType=all_reviews")
    human_delay(0.8, 1.6)
    _dismiss_continue_shopping(driver)

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
        if is_captcha(driver):
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
            human_delay(0.2, 0.5)
            button.click()
        except ElementClickInterceptedException:
            driver.execute_script("arguments[0].click();", button)
        human_delay(0.8, 1.6)
        _collect()

        if len(all_reviews) == before:
            break  # button present but nothing new loaded - avoid an infinite click loop

    return all_reviews


def warm_up_driver(driver, cookies: list | None = None, cookies_path: str | None = None) -> None:
    """Visit the homepage once and attach session cookies - call this once
    right after creating a driver (or after recreating one), NOT before
    every single product (see worker_pool._get_slot_driver, which reuses
    one driver across many jobs)."""
    driver.get(BASE_URL)
    human_delay(0.8, 1.6)
    _dismiss_continue_shopping(driver)
    if load_cookies(driver, cookies=cookies, cookies_path=cookies_path or DEFAULT_COOKIES_PATH):
        driver.get(BASE_URL)  # reload with the session cookies now attached
        human_delay(0.5, 1.0)
        _dismiss_continue_shopping(driver)


def check_cookies_valid(cookies: list, headless: bool = True) -> bool:
    """Real live check of whether a set of session cookies is still valid -
    loads them, reloads the homepage, and looks for Amazon's own logged-out
    indicator ("Hello, sign in" in the nav greeting) rather than depending
    on any specific product page. Used by the dashboard's "Test Cookies"
    button. Always closes its own driver."""
    driver = make_driver(headless=headless)
    try:
        driver.get(BASE_URL)
        human_delay(0.5, 1.0)
        if not load_cookies(driver, cookies=cookies):
            return False
        driver.get(BASE_URL)
        human_delay(0.8, 1.6)
        return "hello, sign in" not in driver.page_source.lower()
    finally:
        close_driver(driver)


def scrape_product_page(driver, product_url_or_asin: str, full_reviews: bool = True) -> dict:
    """Load an Amazon product page on an ALREADY-CREATED, already-warmed-up
    driver (see warm_up_driver) and return both the review cards and the
    rating-histogram summary from that single page load.

    For reviews: if full_reviews=True (default), after loading /dp/{asin}
    this clicks through to the "Customer reviews" listing and repeatedly
    clicks "Show N more reviews" there, collecting every review card seen -
    not just the ~8-10 the product page itself surfaces. Falls back to the
    product page's own ~8-10 reviews if the "see all reviews" link isn't
    found or a CAPTCHA interrupts pagination.

    Does NOT create or close the driver - that's the caller's
    responsibility (see worker_pool, which keeps one driver alive across
    many calls to this function instead of one per call)."""
    asin = extract_asin(product_url_or_asin)
    if not asin:
        log.warning("Could not extract ASIN from %s", product_url_or_asin)
        return _empty_result()

    log.info("[%s] scraping started", asin)

    # A page that never times out on Amazon's end is unusual; retry a
    # couple of times before giving up on this product for this run.
    for attempt in range(1, 4):
        try:
            driver.get(f"{BASE_URL}/dp/{asin}")
            human_delay(1.0, 2.0)
            _dismiss_continue_shopping(driver)
            human_scroll(driver)
            human_delay(0.3, 0.8)
            break
        except Exception as exc:  # noqa: BLE001
            log.warning("Product page load failed for ASIN %s (attempt %s): %s", asin, attempt, exc)
            if attempt == 3:
                return _empty_result(asin)
            time.sleep(5)

    if is_captcha(driver):
        log.warning("CAPTCHA hit for ASIN %s - will retry next run.", asin)
        return _empty_result(asin)

    # Rating histogram lives on the product page itself either way - grab
    # it before possibly navigating away to the all-reviews listing.
    _hover_rating_badge(driver)
    rating_summary = parse_rating_summary(driver.page_source)

    reviews = _parse_reviews_page(driver.page_source)

    # Product page's own widget already shows zero ratings and zero
    # reviews - a genuinely empty listing (STATUS_NA case). Opening the
    # all-reviews listing and clicking "show more" would just confirm
    # the same zero again at the cost of a full extra page load, so
    # skip straight to returning instead of paying that round-trip on
    # every empty product.
    is_empty_product = not reviews and not rating_summary

    if full_reviews and not is_empty_product and _open_all_reviews_page(driver, asin):
        if is_captcha(driver):
            log.warning("CAPTCHA hit opening all-reviews page for ASIN %s - using product-page reviews only.", asin)
        else:
            reviews = _load_more_reviews(driver)

    avg = rating_summary.get("average_rating")
    total = rating_summary.get("total_ratings")
    log.info(
        "[%s] scraping finished - %s review(s) collected, rating %s/5 (%s ratings)",
        asin, len(reviews), avg if avg is not None else "n/a", total if total is not None else "n/a",
    )
    return {
        "asin": asin,
        "reviews": reviews,
        "rating_summary": rating_summary,
        "blocked": False,
    }


def scrape_amazon_product(
    product_url_or_asin: str, headless: bool = True, user_data_dir: str | None = None,
    full_reviews: bool = True, cookies: list | None = None, cookies_path: str | None = None,
) -> dict:
    """Back-compat one-shot wrapper: creates a driver, warms it up, scrapes
    exactly one product, and closes the driver. Used by ad-hoc scripts/tests
    (test_scrape.py) - the live worker pool uses warm_up_driver +
    scrape_product_page directly against one persistent driver per worker
    slot instead of calling this per job (see worker_pool.py)."""
    driver = make_driver(headless=headless, user_data_dir=user_data_dir)
    try:
        try:
            warm_up_driver(driver, cookies=cookies, cookies_path=cookies_path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Homepage load failed for %s: %s", product_url_or_asin, exc)
            return _empty_result(extract_asin(product_url_or_asin))
        return scrape_product_page(driver, product_url_or_asin, full_reviews=full_reviews)
    finally:
        close_driver(driver)
