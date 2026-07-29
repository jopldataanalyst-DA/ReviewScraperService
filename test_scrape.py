"""One-off manual test: scrape a single Amazon product URL and insert the
results into reviews.amazon_reviews, without waiting for the 24h scheduler.

Run from inside the deployed container (Dokploy > Open Terminal), where
DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME are already set as env vars:

    python test_scrape.py "https://www.amazon.in/dp/B0FJY2TKFB/?psc=1"

Uses placeholder company/master_sku/style_id/category values since this
product isn't necessarily in multi_product_id_mapping yet - just for
confirming the scrape -> DB write path works end to end.
"""

import sys

from run_scheduler import insert_new_reviews, insert_rating_snapshot
from amazon_reviews import scrape_amazon_product, extract_asin


def main():
    if len(sys.argv) != 2:
        print("Usage: python test_scrape.py <amazon_product_url>")
        sys.exit(1)

    url = sys.argv[1]
    asin = extract_asin(url)
    print(f"Scraping ASIN {asin}...")

    result = scrape_amazon_product(url, headless=True)
    print(f"Reviews found: {len(result['reviews'])}")
    print(f"Rating summary: {result['rating_summary']}")

    product_id = result["asin"] or asin
    inserted = insert_new_reviews(
        "TestCompany", "TEST-MASTER-SKU", "TEST-STYLE", "TestCategory",
        product_id, result["reviews"],
    )
    print(f"Inserted {inserted} new review row(s).")

    insert_rating_snapshot(
        "TestCompany", "TEST-MASTER-SKU", "TEST-STYLE", "TestCategory",
        product_id, result["rating_summary"],
    )
    print("Inserted rating snapshot row.")


if __name__ == "__main__":
    main()
