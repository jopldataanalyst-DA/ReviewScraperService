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
from browser import close_driver as _close_driver, make_driver as _make_driver
from scraper import scrape_product_page, warm_up_driver
from run_scheduler import insert_new_reviews, insert_rating_snapshot

log = logging.getLogger("worker_pool")

DEFAULT_MAX_WORKERS = int(os.environ.get("MAX_CONCURRENT_WORKERS", "3"))
CYCLE_SLEEP_SECONDS = 10
FULL_SYNC_INTERVAL_SECONDS = 24 * 60 * 60

# Real throughput cap, independent of concurrency: enforces a minimum gap
# between any two job starts across all worker threads, so raising
# max_concurrent_workers can't accidentally produce a burst of simultaneous
# Amazon requests again (the exact pattern that triggered the earlier
# session-wide CAPTCHA gate). Tune via MIN_JOB_START_GAP_SECONDS env var.
MIN_JOB_START_GAP_SECONDS = float(os.environ.get("MIN_JOB_START_GAP_SECONDS", "3"))
_last_job_start_lock = threading.Lock()
_last_job_start_time = 0.0

# Jobs that exhausted their in-job attempts stay 'failed' but automatically
# re-enter the pending queue after this cooldown, rather than needing a
# manual "Retry Failed" click - CAPTCHA is probabilistic per request, so a
# product that failed now will very likely succeed on a later, separately-
# timed attempt. Capped by MAX_AUTO_REQUEUE_ATTEMPTS so a genuinely broken
# link doesn't retry forever.
STALE_FAILED_REQUEUE_MINUTES = 20
MAX_AUTO_REQUEUE_ATTEMPTS = 8

REAP_AGE_SECONDS = 600  # generous margin above the worst-case single job duration (5 retries * ~1min each)
REAP_INTERVAL_SECONDS = 300


def _reap_orphaned_chrome_processes() -> int:
    """Kill any of this scraper's own Chrome/chromedriver processes older than
    REAP_AGE_SECONDS. A normal scrape (including all retries) finishes in well
    under this window, so anything still alive past it is orphaned - from a
    failed launch, a crashed worker, or any other path that skipped the
    normal driver.quit() cleanup. Confirmed by direct reproduction: a stuck
    uc_chromedriver_slot_0.exe blocked every subsequent copy2 onto that same
    slot's file with PermissionError: [WinError 32] until reaped.

    Deliberately does NOT match on bare process name ("chrome"/"chromedriver")
    - this runs on a real desktop machine (not an isolated container) where
    the user's own actual Chrome browser is also named chrome.exe, and a
    name-only match would kill their browser windows too once they'd been
    open longer than REAP_AGE_SECONDS. Matched on command line instead:
    only processes whose exe/cmdline reference this scraper's own temp paths
    (the per-slot profile dirs, or the uc_chromedriver_* driver copies) are
    ever touched. Best-effort: never raises."""
    import psutil

    from browser import _TEMP_DIR

    now = time.time()
    reaped = 0
    for proc in psutil.process_iter(["name", "create_time", "cmdline"]):
        try:
            name = (proc.info["name"] or "").lower()
            if "chrome" not in name:
                continue
            if now - proc.info["create_time"] < REAP_AGE_SECONDS:
                continue
            cmdline = " ".join(proc.info["cmdline"] or []).lower()
            if _TEMP_DIR.lower() not in cmdline:
                continue
            proc.kill()
            reaped += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if reaped:
        log.info("Reaped %s orphaned Chrome/chromedriver process(es).", reaped)

    reaped += _reap_stale_driver_copies()
    return reaped


def _reap_stale_driver_copies() -> int:
    """Delete leftover uc_chromedriver_*.exe per-attempt copies (see
    amazon_reviews._per_worker_driver_copy) older than REAP_AGE_SECONDS.
    Each launch now gets its own uuid-suffixed file instead of reusing one
    shared-per-slot name (that reuse was the actual cause of the WinError 32
    file-lock collisions), and _close_driver deletes its own copy once that
    job's driver has fully quit - this just sweeps the rare ones left behind
    by a launch that failed before ever producing a driver object to clean
    up after itself. Best-effort: never raises."""
    import glob

    from browser import _TEMP_DIR

    now = time.time()
    removed = 0
    for path in glob.glob(os.path.join(_TEMP_DIR, "uc_chromedriver_*")):
        # uc_chromedriver_shared.exe is the one master patched copy every
        # launch copies FROM (see prepare_shared_driver) - never delete it,
        # only the per-attempt uuid-suffixed copies made from it.
        if os.path.basename(path).lower() == "uc_chromedriver_shared.exe":
            continue
        try:
            if now - os.path.getmtime(path) < REAP_AGE_SECONDS:
                continue
            os.remove(path)
            removed += 1
        except OSError:
            continue
    if removed:
        log.info("Removed %s stale chromedriver copy file(s).", removed)
    return removed


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


# One persistent Chrome DRIVER per worker slot, reused across many jobs -
# not created and destroyed per job. Confirmed by direct reproduction (and
# by a known-working reference scraper that launches Chrome exactly once for
# an entire run) that relaunching a fresh Chrome+chromedriver process for
# every single product is the root cause of almost every driver-launch race
# seen here (WinError 32 file-copy collisions, "session not created: cannot
# connect to chrome"): none of those failure modes exist if there's no
# repeated relaunch to race in the first place. Reusing one warmed-up
# session per slot is also simply faster - no ~5-10s browser startup per
# product. Recreated (see _get_slot_driver) only when a scrape actually
# raises, or after RECYCLE_AFTER_JOBS uses as a periodic safety refresh
# against unbounded memory growth in one long-lived session.
_slot_drivers: dict[int, object] = {}
_slot_driver_headless: dict[int, bool] = {}
_slot_driver_uses: dict[int, int] = {}
_slot_driver_lock = threading.Lock()
RECYCLE_AFTER_JOBS = 40


def _close_slot_driver(slot: int) -> None:
    with _slot_driver_lock:
        driver = _slot_drivers.pop(slot, None)
        _slot_driver_headless.pop(slot, None)
        _slot_driver_uses.pop(slot, None)
    if driver is not None:
        try:
            _close_driver(driver)
        except Exception:  # noqa: BLE001 - best-effort, never fail the scrape over cleanup
            log.debug("Closing slot %s driver raised", slot, exc_info=True)


def _get_slot_driver(slot: int, headless: bool):
    """Return this slot's persistent driver, creating (or recreating) it if
    missing, if headless mode changed since it was created, or if it's been
    used past RECYCLE_AFTER_JOBS times. Only one driver is ever live per slot
    at once - safe without a lock across the actual scrape since _run_cycle
    never runs two jobs on the same slot concurrently."""
    driver = _slot_drivers.get(slot)
    stale_mode = driver is not None and _slot_driver_headless.get(slot) != headless
    stale_age = driver is not None and _slot_driver_uses.get(slot, 0) >= RECYCLE_AFTER_JOBS
    if driver is not None and (stale_mode or stale_age):
        _close_slot_driver(slot)
        driver = None

    if driver is None:
        _clear_stale_profile_lock(slot)
        driver = _make_driver(headless=headless, user_data_dir=_profile_dir_for_slot(slot))
        warm_up_driver(driver, cookies=jobs.get_cookies())
        with _slot_driver_lock:
            _slot_drivers[slot] = driver
            _slot_driver_headless[slot] = headless
            _slot_driver_uses[slot] = 0

    return driver


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


def _scrape_once(product_link: str, slot: int, headless: bool) -> tuple[dict, str]:
    """Try scraping a product exactly once - no in-job retry loop. A failed
    attempt (blocked, or the scrape raised - e.g. a driver crash) sends the
    job straight back to 'pending' (see _run_one_job) instead of retrying
    it in place while the job sits marked 'running': that in-place retry
    used to hold the worker slot hostage on one product for up to
    ~1.5 minutes of backoff sleeps, blocking every other pending product
    behind it. A single attempt per claim keeps a slot always either
    actively scraping or immediately free for the next job - retries still
    happen, just via the normal pending queue instead of a private loop.
    Returns (result, error_detail): error_detail is the real cause (exception
    message or "CAPTCHA wall or page load failure"), empty on success."""
    try:
        driver = _get_slot_driver(slot, headless)
        result = scrape_product_page(driver, product_link)
        _slot_driver_uses[slot] = _slot_driver_uses.get(slot, 0) + 1
        if result["asin"] and not result["blocked"]:
            return result, ""
        return result, "CAPTCHA wall or page load failure"
    except Exception as exc:  # noqa: BLE001 - e.g. the persistent driver died/crashed
        log.exception("Scrape attempt raised for %s", product_link)
        # The exception means this slot's driver is likely dead (crashed
        # renderer, disconnected session, etc.) - force a fresh one on the
        # next job rather than repeatedly trying to reuse a broken session.
        _close_slot_driver(slot)
        result = {"asin": "", "reviews": [], "rating_summary": {}, "blocked": True}
        return result, str(exc).splitlines()[0][:300]


def _throttle_job_start() -> None:
    """Block until at least MIN_JOB_START_GAP_SECONDS has passed since the
    last job (any worker) started. This is the actual volume cap - it holds
    even if max_concurrent_workers is raised, so concurrency and throughput
    are controlled independently instead of concurrency alone setting the
    request rate."""
    global _last_job_start_time
    with _last_job_start_lock:
        wait = MIN_JOB_START_GAP_SECONDS - (time.time() - _last_job_start_time)
        if wait > 0:
            time.sleep(wait)
        _last_job_start_time = time.time()


def _run_one_job(job: dict, slot: int, stagger_seconds: float) -> None:
    from scraper import extract_asin

    product_id = job["product_id"]
    time.sleep(stagger_seconds)  # spread concurrent workers apart instead of all hitting Amazon at once
    _throttle_job_start()
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

        # Read fresh each job (not cached at pool startup) so toggling
        # headless/headed mode from the dashboard takes effect on the very
        # next job to start, without needing a restart.
        headless = jobs.get_control().get("headless_mode", True)
        result, last_error = _scrape_once(job["product_link"], slot, headless)

        if result["blocked"]:
            # A single attempt failed (blocked, or the scrape raised) - a
            # genuinely empty-but-successfully-loaded product page is NOT
            # this branch (reviews/rating_summary can legitimately both be
            # empty for a real zero-review listing - see amazon_reviews
            # docstring). Send it straight back to 'pending' so the worker
            # is immediately free for the next job in the queue, instead of
            # burning this slot on in-place retry backoff. attempt_count
            # (bumped by claim_pending_jobs on every claim, including this
            # one) is what escalates a persistently-broken product to a
            # terminal STATUS_FAILED instead of requeuing it forever.
            if job.get("attempt_count", 0) >= MAX_AUTO_REQUEUE_ATTEMPTS:
                jobs.mark_job_result(
                    job["id"], jobs.STATUS_FAILED,
                    error=f"{last_error} (after {job['attempt_count']} attempt(s))",
                )
            else:
                jobs.requeue_job(job["id"], error=last_error)
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

        # Page loaded fine (not blocked/CAPTCHA'd) but the product genuinely
        # has zero ratings AND zero reviews - confirmed by direct
        # reproduction that some real listings simply have no rating widget
        # in the DOM at all (brand-new/unrated listings). Tracked as its own
        # status instead of folding into STATUS_DONE, so "has real data" and
        # "confirmed empty" don't blend into one bucket.
        if not result["reviews"] and not result["rating_summary"]:
            jobs.mark_job_result(job["id"], jobs.STATUS_NA, reviews_found=inserted)
            log.info("product_id=%s marked N/A (no rating or reviews found)", product_id)
        else:
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

    # Any job still marked 'running' from before THIS process started is
    # necessarily stale - this process just booted, so nothing it claimed
    # could possibly still be legitimately in flight. Previously this only
    # ran once every 24h (as part of the full sync below), so restarting the
    # dashboard (e.g. to deploy a fix) left stale 'running' rows visible on
    # the dashboard for up to a day, confusingly showing more "running" jobs
    # than max_concurrent_workers actually allows. Reset immediately, every
    # startup, regardless of age.
    n = jobs.reset_stuck_running(older_than_minutes=0)
    if n:
        log.info("Reset %s stale 'running' job(s) left over from a previous run.", n)

    last_full_sync = 0.0
    last_reap = 0.0
    last_requeue_sweep = 0.0
    last_logged_workers = None

    while True:
        try:
            control = jobs.get_control()

            if time.time() - last_reap > REAP_INTERVAL_SECONDS:
                _reap_orphaned_chrome_processes()
                # Also catch a job genuinely hung mid-scrape (not just a
                # restart, which the startup call above already handles) -
                # on the same cadence as the process reaper rather than
                # waiting for the once-a-day full sync.
                n = jobs.reset_stuck_running(older_than_minutes=30)
                if n:
                    log.info("Reset %s job(s) stuck 'running' for over 30 minutes.", n)
                last_reap = time.time()

            if time.time() - last_requeue_sweep > STALE_FAILED_REQUEUE_MINUTES * 60:
                n = jobs.requeue_stale_failures(STALE_FAILED_REQUEUE_MINUTES, MAX_AUTO_REQUEUE_ATTEMPTS)
                if n:
                    log.info("Auto-requeued %s stale failed job(s) for a later attempt.", n)
                last_requeue_sweep = time.time()

            if control.get("paused"):
                time.sleep(CYCLE_SLEEP_SECONDS)
                continue

            if time.time() - last_full_sync > FULL_SYNC_INTERVAL_SECONDS or last_full_sync == 0.0:
                synced = jobs.sync_jobs_from_mapping()
                log.info("Synced %s Amazon product(s) from multi_product_id_mapping.", synced)
                last_full_sync = time.time()

            max_workers = control.get("max_concurrent_workers") or DEFAULT_MAX_WORKERS
            if max_workers != last_logged_workers:
                log.info("Concurrency set to %s worker(s).", max_workers)
                last_logged_workers = max_workers

            processed = _run_cycle(max_workers)
            if processed == 0:
                time.sleep(CYCLE_SLEEP_SECONDS)
            else:
                # A batch that fails fast (CAPTCHA is detected right after
                # the first page load, well before the slower page-through-
                # reviews path a real success takes) let the pool claim and
                # launch the next batch with zero gap - confirmed by direct
                # reproduction that back-to-back CAPTCHA-only batches with no
                # cooldown between them kept a session's rate gate from ever
                # clearing. A short mandatory pause between every batch,
                # success or not, keeps request cadence human-like.
                time.sleep(random.uniform(3.0, 6.0))
        except Exception:
            log.exception("Worker pool cycle failed with an unhandled error")
            time.sleep(CYCLE_SLEEP_SECONDS)


def start_background_thread() -> threading.Thread:
    thread = threading.Thread(target=run_forever, name="worker-pool", daemon=True)
    thread.start()
    return thread
