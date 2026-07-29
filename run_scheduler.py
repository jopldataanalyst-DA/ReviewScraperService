"""Shared DB-write helpers for inserting scraped Amazon reviews/ratings.

Use case:
    Used by worker_pool.py (the live concurrent scraper, run via
    dashboard.py) and test_scrape.py (one-off manual testing). Writes into
    reviews.amazon_reviews - a dedicated schema in the same Postgres
    instance PricingModule reads from (see PricingModule's
    App/Api/Reviews.py, which only ever SELECTs from this table).
"""

import psycopg2.extras

from database import get_cursor

TABLE = "reviews.amazon_reviews"


def insert_new_reviews(company, master_sku, style_id, category, product_id, reviews, rating_summary: dict = None) -> int:
    """Insert one row per review, with the product's rating-histogram summary
    denormalized onto every row (not just a separate summary-only row) - so
    every row in reviews.amazon_reviews carries complete data, no join
    needed to see a product's average_rating/total_ratings/star breakdown
    alongside its review text."""
    if not reviews:
        return 0

    rating_summary = rating_summary or {}
    breakdown = rating_summary.get("star_breakdown_pct", {})
    rating_cols = (
        rating_summary.get("average_rating"), rating_summary.get("total_ratings"),
        breakdown.get("5_star_pct"), breakdown.get("4_star_pct"), breakdown.get("3_star_pct"),
        breakdown.get("2_star_pct"), breakdown.get("1_star_pct"),
    )

    inserted = 0
    with get_cursor(commit=True) as cursor:
        for r in reviews:
            cursor.execute(
                f"""
                INSERT INTO {TABLE}
                    (company, master_sku, style_id, category, product_id, review_id,
                     author, rating, title, review_text, review_date,
                     verified_purchase, helpful_count, images, size, color, extra,
                     average_rating, total_ratings, star_5_pct, star_4_pct, star_3_pct, star_2_pct, star_1_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id, review_id) WHERE review_id IS NOT NULL
                DO UPDATE SET
                    average_rating = EXCLUDED.average_rating, total_ratings = EXCLUDED.total_ratings,
                    star_5_pct = EXCLUDED.star_5_pct, star_4_pct = EXCLUDED.star_4_pct,
                    star_3_pct = EXCLUDED.star_3_pct, star_2_pct = EXCLUDED.star_2_pct,
                    star_1_pct = EXCLUDED.star_1_pct
                """,
                (
                    company, master_sku, style_id, category, product_id, r.review_id,
                    r.author, r.rating, r.title, r.review_text, r.review_date,
                    r.verified_purchase, r.helpful_count,
                    psycopg2.extras.Json(r.images), r.size, r.color, psycopg2.extras.Json(r.extra),
                    *rating_cols,
                ),
            )
            if cursor.rowcount:
                inserted += 1
    return inserted


def insert_rating_snapshot(company, master_sku, style_id, category, product_id, rating_summary: dict) -> None:
    """Only used when a product has ratings but zero individual reviews to
    attach them to (rare, but possible for a brand-new listing) - inserts a
    single review-less row so the rating summary isn't lost."""
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
