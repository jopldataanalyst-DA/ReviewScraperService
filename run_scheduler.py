"""Review Scraper service - standalone entrypoint.

Use case:
    Fully separate from the PricingModule app (different repo, different
    Dokploy service, own requirements.txt/nixpacks.toml). Reads Amazon
    product links from the main app's Postgres database (multi_product_id_mapping,
    a public-schema table owned by PricingModule's Multi Product ID module -
    read-only reference data, not duplicated) and item_master for Category,
    scrapes each product's reviews + rating breakdown, and writes the
    results into reviews.amazon_reviews (also in that same Postgres
    instance, its own dedicated schema) - the ONLY thing this service and
    PricingModule share is that one Postgres database, reached over the
    network (no shared filesystem, no shared code, no shared container).

    PricingModule's App/Api/Reviews.py only ever SELECTs from
    reviews.amazon_reviews - it has zero scraping/browser-automation code
    or dependencies (selenium/webdriver-manager/beautifulsoup4/lxml/
    chromium all live only in this repo now).

    Runs once immediately on start, then sleeps 24h and repeats.

Environment variables needed (same values as PricingModule's DB env vars):
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, SKIP_SSH_TUNNEL=true
"""

import logging
import time

from database import fetch_all, get_cursor
from amazon_reviews import scrape_amazon_product

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("review_scheduler")

RUN_INTERVAL_SECONDS = 24 * 60 * 60
TABLE = "reviews.amazon_reviews"


def fetch_amazon_products_to_scrape() -> list[dict]:
    return fetch_all(
        """
        SELECT DISTINCT m.seller_sku, m.product_id, m.product_link, m.master_sku, m.style_id, m.company,
               im."Category" AS category
        FROM multi_product_id_mapping m
        LEFT JOIN item_master im ON im."Master SKU" = m.master_sku
        WHERE m.portal = 'Amazon' AND m.product_link IS NOT NULL AND m.product_link <> ''
        """,
    )


def insert_new_reviews(company, master_sku, style_id, category, product_id, reviews) -> int:
    if not reviews:
        return 0
    import psycopg2.extras

    inserted = 0
    with get_cursor(commit=True) as cursor:
        for r in reviews:
            cursor.execute(
                f"""
                INSERT INTO {TABLE}
                    (company, master_sku, style_id, category, product_id, review_id,
                     author, rating, title, review_text, review_date,
                     verified_purchase, helpful_count, images, size, color, extra)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id, review_id) WHERE review_id IS NOT NULL DO NOTHING
                """,
                (
                    company, master_sku, style_id, category, product_id, r.review_id,
                    r.author, r.rating, r.title, r.review_text, r.review_date,
                    r.verified_purchase, r.helpful_count,
                    psycopg2.extras.Json(r.images), r.size, r.color, psycopg2.extras.Json(r.extra),
                ),
            )
            if cursor.rowcount:
                inserted += 1
    return inserted


def insert_rating_snapshot(company, master_sku, style_id, category, product_id, rating_summary: dict) -> None:
    if not rating_summary:
        return
    breakdown = rating_summary.get("star_breakdown_pct", {})
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"""
            INSERT INTO {TABLE}
                (company, master_sku, style_id, category, product_id,
                 average_rating, total_ratings, star_5_pct, star_4_pct, star_3_pct, star_2_pct, star_1_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                company, master_sku, style_id, category, product_id,
                rating_summary.get("average_rating"), rating_summary.get("total_ratings"),
                breakdown.get("5_star_pct"), breakdown.get("4_star_pct"), breakdown.get("3_star_pct"),
                breakdown.get("2_star_pct"), breakdown.get("1_star_pct"),
            ),
        )


def run_once() -> dict:
    targets = fetch_amazon_products_to_scrape()
    total_new = 0
    scraped_products = 0
    failed_products = 0

    for t in targets:
        try:
            result = scrape_amazon_product(t["product_link"], headless=True)
            product_id = t.get("product_id") or t["seller_sku"]

            inserted = insert_new_reviews(
                t.get("company") or "", t.get("master_sku"), t.get("style_id"),
                t.get("category"), product_id, result["reviews"],
            )
            insert_rating_snapshot(
                t.get("company") or "", t.get("master_sku"), t.get("style_id"),
                t.get("category"), product_id, result["rating_summary"],
            )
            total_new += inserted
            scraped_products += 1
        except Exception:  # noqa: BLE001 - one failing SKU must not stop the batch
            log.exception("Amazon review scrape failed for seller_sku=%s", t.get("seller_sku"))
            failed_products += 1

    return {"products_scraped": scraped_products, "products_failed": failed_products, "new_reviews": total_new}


def main():
    log.info("Review scraper starting - Amazon, runs every 24h.")
    while True:
        try:
            result = run_once()
            log.info("Amazon review scrape complete: %s", result)
        except Exception:
            log.exception("Amazon review scrape failed with an unhandled error")

        log.info("Sleeping %s hours until next run.", RUN_INTERVAL_SECONDS / 3600)
        time.sleep(RUN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
