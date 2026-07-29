"""Concurrent scrape worker pool.

Use case:
    Replaces the old "one Chrome instance, one product at a time" loop with
    several worker threads, each driving its own isolated Chrome instance,
    pulling jobs from reviews.scrape_jobs. Runs forever in a background
    thread inside the dashboard process: syncs new products from
    multi_product_id_mapping, claims a batch of pending jobs per cycle,
    scrapes them concurrently, writes reviews + rating snapshot, and updates
    job status - so the dashboard always reflects live progress.
"""

import logging
import os
import random
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import jobs
from amazon_reviews import scrape_amazon_product
from run_scheduler import insert_new_reviews, insert_rating_snapshot

log = logging.getLogger("worker_pool")

DEFAULT_MAX_WORKERS = int(os.environ.get("MAX_CONCURRENT_WORKERS", "3"))
CYCLE_SLEEP_SECONDS = 10
FULL_SYNC_INTERVAL_SECONDS = 24 * 60 * 60
MAX_SCRAPE_ATTEMPTS = 5
RETRY_WAIT_SECONDS = 5

# Chrome/chromedriver process names to reap - covers a launch that fails
# before scrape_amazon_product's own driver.quit()/psutil cleanup ever gets
# a handle to the process (confirmed by direct reproduction: a
# SessionNotCreatedException during uc.Chrome() construction leaves an
# orphaned browser process with no Python reference to it at all).
REAPABLE_PROCESS_NAMES = {"chrome", "chrome.exe", "chromedriver", "chromedriver.exe", "uc_chromedriver.exe"}
REAP_AGE_SECONDS = 600  # generous margin above the worst-case single job duration (5 retries * ~1min each)
REAP_INTERVAL_SECONDS = 300


def _reap_orphaned_chrome_processes() -> int:
    """Kill any Chrome/chromedriver process older than REAP_AGE_SECONDS.
    A normal scrape (including all retries) finishes in well under this
    window, so anything still alive past it is orphaned - from a failed
    launch, a crashed worker, or any other path that skipped the normal
    driver.quit() cleanup. Best-effort: never raises."""
    import psutil

    now = time.time()
    reaped = 0
    for proc in psutil.process_iter(["name", "create_time"]):
        try:
            if proc.info["name"] not in REAPABLE_PROCESS_NAMES:
                continue
            if now - proc.info["create_time"] < REAP_AGE_SECONDS:
                continue
            proc.kill()
            reaped += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if reaped:
        log.info("Reaped %s orphaned Chrome/chromedriver process(es).", reaped)
    return reaped

# One persistent Chrome profile dir per worker "slot" (not per job), reused
# across cycles - each slot builds up real cookies/browsing history over
# time instead of every scrape looking like a brand new, history-less
# browser, which is itself a bot signal to Amazon.
_PROFILE_ROOT = os.path.join(tempfile.gettempdir(), "amazon_scraper_profiles")


def _profile_dir_for_slot(slot: int) -> str:
    path = os.path.join(_PROFILE_ROOT, f"slot_{slot}")
    os.makedirs(path, exist_ok=True)
    return path


def _clear_stale_profile_lock(slot: int) -> None:
    """Remove Chrome's singleton-instance lock file(s) from a profile dir
    before relaunching on it. Confirmed by direct reproduction: retrying on
    the same slot's profile immediately after a Chrome process exits can hit
    "session not created: cannot connect to chrome" because Chrome/the OS
    hasn't fully released the lock yet - deleting it is safe since we only
    ever run one Chrome instance per slot at a time (single-threaded within
    a job's retry loop)."""
    path = _profile_dir_for_slot(slot)
    for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket", "lockfile"):
        lock_path = os.path.join(path, lock_name)
        try:
            if os.path.exists(lock_path) or os.path.islink(lock_path):
                os.remove(lock_path)
        except OSError:
            pass


def _apply_review_filters(reviews: list, control: dict) -> list:
    """Sort newest-first, then apply the dashboard's date-range and
    max-reviews-per-product controls before anything gets stored.

    Amazon's product page only ever surfaces its own ~8-10 "top/recent"
    review cards (see amazon_reviews.scrape_amazon_product) - these controls
    filter/cap within that set, they don't unlock scraping deeper history."""
    date_from = control.get("review_date_from")
    date_to = control.get("review_date_to")
    max_reviews = control.get("max_reviews_per_product")

    filtered = sorted(reviews, key=lambda r: r.review_date or datetime.min, reverse=True)

    if date_from:
        filtered = [r for r in filtered if r.review_date is None or r.review_date.date() >= date_from]
    if date_to:
        filtered = [r for r in filtered if r.review_date is None or r.review_date.date() <= date_to]
    if max_reviews:
        filtered = filtered[:max_reviews]
    return filtered


def _scrape_with_retries(product_link: str, slot: int) -> tuple[dict, int, str]:
    """Try scraping a product up to MAX_SCRAPE_ATTEMPTS times, only pausing
    RETRY_WAIT_SECONDS between attempts when one actually failed (blocked, or
    the scrape raised - e.g. a transient Chrome launch failure) - a
    successful attempt returns immediately, no delay tacked on after it.
    Returns (last_result, attempts_used, last_error_detail). last_error_detail
    is the real cause of the last failure (exception message or "CAPTCHA
    wall or page load failure"), not a generic label - this is what actually
    went wrong, which product_link parses to a fine ASIN either way."""
    result = {"asin": "", "reviews": [], "rating_summary": {}, "blocked": True}
    last_error = "unknown"
    for attempt in range(1, MAX_SCRAPE_ATTEMPTS + 1):
        # Clear any stale Chrome singleton lock from a previous attempt on
        # this same slot's profile dir before relaunching - confirmed by
        # direct reproduction that back-to-back relaunches on the same
        # user-data-dir can otherwise fail with "session not created:
        # cannot connect to chrome" even though the product link is fine.
        _clear_stale_profile_lock(slot)
        try:
            result = scrape_amazon_product(
                product_link, headless=True, user_data_dir=_profile_dir_for_slot(slot),
            )
            last_error = "CAPTCHA wall or page load failure" if result["blocked"] else ""
        except Exception as exc:  # noqa: BLE001 - e.g. transient Chrome/driver launch failure, still worth retrying
            log.warning("Scrape attempt %s raised for %s: %s", attempt, product_link, exc)
            result = {"asin": "", "reviews": [], "rating_summary": {}, "blocked": True}
            last_error = str(exc).splitlines()[0][:300]

        if result["asin"] and not result["blocked"]:
            return result, attempt, ""
        if attempt < MAX_SCRAPE_ATTEMPTS:
            time.sleep(RETRY_WAIT_SECONDS)
    return result, MAX_SCRAPE_ATTEMPTS, last_error


def _run_one_job(job: dict, slot: int, stagger_seconds: float) -> None:
    from amazon_reviews import extract_asin

    product_id = job["product_id"]
    time.sleep(stagger_seconds)  # spread concurrent workers apart instead of all hitting Amazon at once
    try:
        # extract_asin is pure string parsing, no network - check it once up
        # front so a genuinely malformed link fails fast and clearly, instead
        # of being conflated with a scrape/launch failure on a perfectly
        # valid link (confirmed by direct reproduction: every product_link in
        # this table parses to a valid ASIN locally, even ones that failed
        # all 5 scrape attempts - the real cause was a transient Chrome
        # launch issue, not the link).
        if not extract_asin(job["product_link"]):
            jobs.mark_job_result(job["id"], jobs.STATUS_FAILED, error="Product link has no parseable ASIN")
            return

        result, attempts, last_error = _scrape_with_retries(job["product_link"], slot)

        if result["blocked"]:
            # A load failure that never resolved across all retry attempts -
            # a genuinely empty-but-successfully-loaded product page is NOT
            # this branch (reviews/rating_summary can legitimately both be
            # empty for a real zero-review listing - see amazon_reviews
            # docstring). undetected_chromedriver dropped the CAPTCHA rate to
            # near-zero, so this is folded into the regular 'failed' status
            # (still retriable via Retry Failed) rather than tracked
            # separately.
            jobs.mark_job_result(
                job["id"], jobs.STATUS_FAILED,
                error=f"{last_error} (after {attempts} attempt(s))",
            )
            return

        control = jobs.get_control()
        result["reviews"] = _apply_review_filters(result["reviews"], control)

        inserted = insert_new_reviews(
            job.get("company") or "", job.get("master_sku"), job.get("style_id"),
            job.get("category"), product_id, result["reviews"], result["rating_summary"],
        )
        if not result["reviews"]:
            # Ratings but no reviews to attach them to (rare) - the summary
            # would otherwise be lost since insert_new_reviews had nothing to write.
            insert_rating_snapshot(
                job.get("company") or "", job.get("master_sku"), job.get("style_id"),
                job.get("category"), product_id, result["rating_summary"],
            )
        jobs.mark_job_result(job["id"], jobs.STATUS_DONE, reviews_found=inserted)
        log.info("product_id=%s done (%s new reviews)", product_id, inserted)
    except Exception as exc:  # noqa: BLE001 - one failing job must not kill the worker
        log.exception("product_id=%s failed", product_id)
        jobs.mark_job_result(job["id"], jobs.STATUS_FAILED, error=str(exc)[:500])


def _run_cycle(max_workers: int) -> int:
    batch = jobs.claim_pending_jobs(max_workers)
    if not batch:
        return 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_run_one_job, job, slot, stagger_seconds=slot * random.uniform(1.5, 3.5))
            for slot, job in enumerate(batch)
        ]
        for f in as_completed(futures):
            f.result()  # re-raise unexpected errors into the log via the pool
    return len(batch)


def run_forever() -> None:
    log.info("Worker pool starting.")
    last_full_sync = 0.0
    last_reap = 0.0
    last_logged_workers = None

    while True:
        try:
            control = jobs.get_control()

            if time.time() - last_reap > REAP_INTERVAL_SECONDS:
                _reap_orphaned_chrome_processes()
                last_reap = time.time()

            if control.get("paused"):
                time.sleep(CYCLE_SLEEP_SECONDS)
                continue

            if time.time() - last_full_sync > FULL_SYNC_INTERVAL_SECONDS or last_full_sync == 0.0:
                synced = jobs.sync_jobs_from_mapping()
                jobs.reset_stuck_running()
                log.info("Synced %s Amazon product(s) from multi_product_id_mapping.", synced)
                last_full_sync = time.time()

            max_workers = control.get("max_concurrent_workers") or DEFAULT_MAX_WORKERS
            if max_workers != last_logged_workers:
                log.info("Concurrency set to %s worker(s).", max_workers)
                last_logged_workers = max_workers

            processed = _run_cycle(max_workers)
            if processed == 0:
                time.sleep(CYCLE_SLEEP_SECONDS)
        except Exception:
            log.exception("Worker pool cycle failed with an unhandled error")
            time.sleep(CYCLE_SLEEP_SECONDS)


def start_background_thread() -> threading.Thread:
    thread = threading.Thread(target=run_forever, name="worker-pool", daemon=True)
    thread.start()
    return thread
