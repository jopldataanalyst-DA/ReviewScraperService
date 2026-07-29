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
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from bs4 import BeautifulSoup

log = logging.getLogger("amazon_reviews_scraper")


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


def _make_driver(headless: bool = True):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    binary_path, chrome_type = _detect_chrome_binary()

    opts = webdriver.ChromeOptions()
    if binary_path:
        opts.binary_location = binary_path
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-IN")
    opts.add_argument("--accept-lang=en-IN,en;q=0.9")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager(chrome_type=chrome_type).install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def _human_delay(min_s: float = 2.0, max_s: float = 4.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


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

    for card in soup.select('div[data-hook="review"]'):
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


def scrape_amazon_product(product_url_or_asin: str, headless: bool = True) -> dict:
    """Load an Amazon product page once and return both the review cards and
    the rating-histogram summary (5/4/3/2/1-star percentages, average
    rating, total ratings) from that single page load.

    Amazon.in's dedicated /product-reviews/{asin} page redirects to a
    sign-in wall when hit directly without an established browsing session
    (confirmed by direct reproduction - title comes back "Amazon Sign-In",
    even with a valid cookie-free Chrome profile and no CAPTCHA involved).
    The product detail page (/dp/{asin}) itself embeds both the "Top
    reviews from India" widget and the rating histogram with no such wall,
    so that's the source for everything here. Reviews returned are the
    ~4-10 Amazon surfaces on that widget (most helpful/recent), not a full
    historical export - good enough for tracking new reviews day to day.

    Returns {"asin": ..., "reviews": [ScrapedReview, ...], "rating_summary": {...}}.
    rating_summary is {} if the product has no ratings at all (e.g. a new
    listing with zero reviews - confirmed this happens for real listings,
    not a scraper bug).
    """
    asin = extract_asin(product_url_or_asin)
    if not asin:
        log.warning("Could not extract ASIN from %s", product_url_or_asin)
        return {"asin": "", "reviews": [], "rating_summary": {}}

    driver = _make_driver(headless=headless)
    try:
        try:
            driver.get(BASE_URL)
            _human_delay(1.5, 3.0)
            driver.get(f"{BASE_URL}/dp/{asin}")
        except Exception as exc:  # noqa: BLE001
            log.warning("Page load failed for ASIN %s: %s", asin, exc)
            return {"asin": asin, "reviews": [], "rating_summary": {}}

        _human_delay(2.0, 4.0)

        if _is_captcha(driver):
            log.warning("CAPTCHA hit for ASIN %s - will retry next run.", asin)
            return {"asin": asin, "reviews": [], "rating_summary": {}}

        html = driver.page_source
        return {
            "asin": asin,
            "reviews": _parse_reviews_page(html),
            "rating_summary": parse_rating_summary(html),
        }
    finally:
        driver.quit()


def scrape_amazon_reviews(product_url_or_asin: str, headless: bool = True, max_pages: int = MAX_REVIEW_PAGES) -> list[ScrapedReview]:
    """Back-compat wrapper: reviews only (used by the Reviews API/scheduler,
    which doesn't need the rating histogram)."""
    return scrape_amazon_product(product_url_or_asin, headless=headless)["reviews"]
