"""Job-queue layer on top of reviews.scrape_jobs / reviews.scraper_control.

Use case:
    Turns the scraper from "loop over every product once and forget" into a
    real per-product job model so a dashboard can show live status (pending/
    running/done/failed/captcha), retry just the failures, or trigger a
    single product on demand - without waiting for the 24h batch.
"""

from typing import Optional

import psycopg2.extras

from database import fetch_all, get_cursor

JOBS_TABLE = "reviews.scrape_jobs"
CONTROL_TABLE = "reviews.scraper_control"

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CAPTCHA = "captcha"


def sync_jobs_from_mapping() -> int:
    """Upsert one job row per Amazon product found in multi_product_id_mapping.
    Existing jobs keep their status/history - only new products get a fresh
    'pending' row, and stale metadata (company/category/link) is refreshed.
    Returns the number of products synced."""
    rows = fetch_all(
        """
        SELECT DISTINCT m.seller_sku, m.product_id, m.product_link, m.master_sku, m.style_id, m.company,
               im."Category" AS category
        FROM multi_product_id_mapping m
        LEFT JOIN item_master im ON im."Master SKU" = m.master_sku
        WHERE m.portal = 'Amazon' AND m.product_link IS NOT NULL AND m.product_link <> ''
        """,
    )
    # multi_product_id_mapping can have multiple rows collapsing onto the same
    # product_id (e.g. duplicate seller_sku mappings) - dedupe here since a
    # single multi-row upsert can't hit the same ON CONFLICT target twice.
    by_product_id = {}
    for r in rows:
        product_id = r.get("product_id") or r["seller_sku"]
        by_product_id[product_id] = (
            product_id, r.get("seller_sku"), r.get("master_sku"),
            r.get("style_id"), r.get("company"), r.get("category"), r["product_link"],
        )
    values = list(by_product_id.values())
    if not values:
        return 0

    with get_cursor(commit=True) as cursor:
        psycopg2.extras.execute_values(
            cursor,
            f"""
            INSERT INTO {JOBS_TABLE}
                (product_id, seller_sku, master_sku, style_id, company, category, product_link)
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                seller_sku = EXCLUDED.seller_sku,
                master_sku = EXCLUDED.master_sku,
                style_id = EXCLUDED.style_id,
                company = EXCLUDED.company,
                category = EXCLUDED.category,
                product_link = EXCLUDED.product_link,
                updated_at = now()
            """,
            values,
        )
    return len(values)


def claim_pending_jobs(limit: int) -> list[dict]:
    """Atomically mark up to `limit` pending jobs as running and return them,
    so multiple worker threads never grab the same job."""
    with get_cursor(dictionary=True, commit=True) as cursor:
        cursor.execute(
            f"""
            UPDATE {JOBS_TABLE}
            SET status = %s, started_at = now(), updated_at = now(), attempt_count = attempt_count + 1
            WHERE id IN (
                SELECT id FROM {JOBS_TABLE}
                WHERE status = %s
                ORDER BY updated_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, product_id, seller_sku, master_sku, style_id, company, category, product_link
            """,
            (STATUS_RUNNING, STATUS_PENDING, limit),
        )
        return cursor.fetchall()


def mark_job_result(job_id: int, status: str, reviews_found: int = 0, error: Optional[str] = None) -> None:
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"""
            UPDATE {JOBS_TABLE}
            SET status = %s, reviews_found = %s, last_error = %s, finished_at = now(), updated_at = now()
            WHERE id = %s
            """,
            (status, reviews_found, error, job_id),
        )


def requeue_product(product_id: str) -> bool:
    """Trigger a specific product on demand by resetting its job to pending."""
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"UPDATE {JOBS_TABLE} SET status = %s, updated_at = now() WHERE product_id = %s",
            (STATUS_PENDING, product_id),
        )
        return cursor.rowcount > 0


def requeue_failures() -> int:
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"UPDATE {JOBS_TABLE} SET status = %s, updated_at = now() WHERE status IN (%s, %s)",
            (STATUS_PENDING, STATUS_FAILED, STATUS_CAPTCHA),
        )
        return cursor.rowcount


def requeue_all() -> int:
    with get_cursor(commit=True) as cursor:
        cursor.execute(f"UPDATE {JOBS_TABLE} SET status = %s, updated_at = now()", (STATUS_PENDING,))
        return cursor.rowcount


SORTABLE_COLUMNS = {
    "product_id", "company", "master_sku", "status", "reviews_found",
    "attempt_count", "updated_at", "created_at",
}


def list_jobs_page(
    status: Optional[str] = None, search: Optional[str] = None,
    sort_by: str = "updated_at", sort_dir: str = "desc",
    page: int = 1, page_size: int = 100,
) -> dict:
    """Server-side paginated, sorted, filtered job listing. page is 1-indexed.
    `search` matches product_id/seller_sku/master_sku/company (case-insensitive,
    substring). sort_by must be a known column - falls back to updated_at
    otherwise, since it's interpolated into the query (never take it from an
    unvalidated source)."""
    page = max(page, 1)
    page_size = min(max(page_size, 1), 500)
    offset = (page - 1) * page_size

    sort_by = sort_by if sort_by in SORTABLE_COLUMNS else "updated_at"
    sort_dir = "ASC" if str(sort_dir).lower() == "asc" else "DESC"

    conditions = []
    params: list = []
    if status:
        conditions.append("status = %s")
        params.append(status)
    if search:
        conditions.append(
            "(product_id ILIKE %s OR seller_sku ILIKE %s OR master_sku ILIKE %s OR company ILIKE %s)"
        )
        like = f"%{search}%"
        params.extend([like, like, like, like])
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    total = fetch_all(f"SELECT count(*) AS n FROM {JOBS_TABLE} {where}", tuple(params))[0]["n"]
    rows = fetch_all(
        f"SELECT * FROM {JOBS_TABLE} {where} ORDER BY {sort_by} {sort_dir} NULLS LAST LIMIT %s OFFSET %s",
        tuple(params) + (page_size, offset),
    )
    total_pages = max((total + page_size - 1) // page_size, 1)
    return {
        "rows": rows, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages,
        "sort_by": sort_by, "sort_dir": sort_dir.lower(),
    }


def get_stats() -> dict:
    rows = fetch_all(f"SELECT status, count(*) AS n, coalesce(sum(reviews_found), 0) AS reviews FROM {JOBS_TABLE} GROUP BY status")
    stats = {"pending": 0, "running": 0, "done": 0, "failed": 0, "captcha": 0, "total_reviews_found": 0, "total_jobs": 0}
    for r in rows:
        stats[r["status"]] = r["n"]
        stats["total_reviews_found"] += r["reviews"]
        stats["total_jobs"] += r["n"]
    return stats


def is_paused() -> bool:
    rows = fetch_all(f"SELECT paused FROM {CONTROL_TABLE} WHERE id = 1")
    return bool(rows and rows[0]["paused"])


def set_paused(paused: bool) -> None:
    with get_cursor(commit=True) as cursor:
        cursor.execute(f"UPDATE {CONTROL_TABLE} SET paused = %s, updated_at = now() WHERE id = 1", (paused,))


def get_control() -> dict:
    rows = fetch_all(
        f"""SELECT paused, review_date_from, review_date_to, max_reviews_per_product, max_concurrent_workers
            FROM {CONTROL_TABLE} WHERE id = 1"""
    )
    return rows[0] if rows else {
        "paused": False, "review_date_from": None, "review_date_to": None,
        "max_reviews_per_product": None, "max_concurrent_workers": 2,
    }


def set_control(date_from=None, date_to=None, max_reviews=None, max_workers=None) -> dict:
    """Set the date range/count filters and concurrency applied to scraping.
    Pass None for date_from/date_to/max_reviews to clear that filter (no
    bound). max_workers is left unchanged if not given (0/absent)."""
    with get_cursor(commit=True) as cursor:
        if max_workers:
            cursor.execute(
                f"""
                UPDATE {CONTROL_TABLE}
                SET review_date_from = %s, review_date_to = %s, max_reviews_per_product = %s,
                    max_concurrent_workers = %s, updated_at = now()
                WHERE id = 1
                """,
                (date_from, date_to, max_reviews, max_workers),
            )
        else:
            cursor.execute(
                f"""
                UPDATE {CONTROL_TABLE}
                SET review_date_from = %s, review_date_to = %s, max_reviews_per_product = %s, updated_at = now()
                WHERE id = 1
                """,
                (date_from, date_to, max_reviews),
            )
    return get_control()


def get_cookies_status() -> dict:
    """Metadata about the stored Amazon session cookies - never returns the
    actual cookie values (those stay server-side, used only internally by
    the scraper's own driver session)."""
    rows = fetch_all(
        f"""SELECT cookies_json, cookies_updated_at, cookies_last_check_ok, cookies_last_check_at
            FROM {CONTROL_TABLE} WHERE id = 1"""
    )
    if not rows or not rows[0]["cookies_json"]:
        return {
            "present": False, "count": 0, "updated_at": None,
            "last_check_ok": None, "last_check_at": None,
        }
    row = rows[0]
    return {
        "present": True,
        "count": len(row["cookies_json"]),
        "updated_at": row["cookies_updated_at"],
        "last_check_ok": row["cookies_last_check_ok"],
        "last_check_at": row["cookies_last_check_at"],
    }


def get_cookies() -> Optional[list]:
    """The actual cookie list, for internal use by the scraper only - never
    exposed via an API response."""
    rows = fetch_all(f"SELECT cookies_json FROM {CONTROL_TABLE} WHERE id = 1")
    return rows[0]["cookies_json"] if rows and rows[0]["cookies_json"] else None


def set_cookies(cookies: list) -> None:
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"""UPDATE {CONTROL_TABLE}
                SET cookies_json = %s, cookies_updated_at = now(),
                    cookies_last_check_ok = NULL, cookies_last_check_at = NULL
                WHERE id = 1""",
            (psycopg2.extras.Json(cookies),),
        )


def set_cookies_check_result(ok: bool) -> None:
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"""UPDATE {CONTROL_TABLE}
                SET cookies_last_check_ok = %s, cookies_last_check_at = now()
                WHERE id = 1""",
            (ok,),
        )


def reset_stuck_running(older_than_minutes: int = 30) -> int:
    """Safety net: if the process crashed mid-scrape, a job can be left stuck
    in 'running' forever. Anything running longer than this gets requeued."""
    with get_cursor(commit=True) as cursor:
        cursor.execute(
            f"""
            UPDATE {JOBS_TABLE}
            SET status = %s, updated_at = now()
            WHERE status = %s AND started_at < now() - (%s || ' minutes')::interval
            """,
            (STATUS_PENDING, STATUS_RUNNING, older_than_minutes),
        )
        return cursor.rowcount
