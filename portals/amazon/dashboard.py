"""Live dashboard + control API for the Amazon review scraper.

Use case:
    Single process that (a) serves a small web dashboard + JSON API for
    watching scrape progress/stats and (b) runs the concurrent worker pool
    in a background thread. Replaces run_scheduler.py as the service
    entrypoint - deploy this instead, same env vars.
"""

import logging
import os
import signal

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

import jobs
import log_buffer
from worker_pool import start_background_thread

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dashboard")
log_buffer.install()

# selenium/urllib3/webdriver_manager are chatty at INFO and drown out the
# actually useful per-product progress logs in the terminal panel. Raising
# these to ERROR keeps real failures visible while cutting the noise.
for _noisy_logger in ("selenium", "urllib3", "webdriver_manager"):
    logging.getLogger(_noisy_logger).setLevel(logging.ERROR)


def _reap_zombie_children(signum, frame) -> None:
    """Chrome/chromedriver processes we kill via psutil never get wait()'d
    on, so they stay <defunct> forever - their parent is this long-lived
    process, not tini (tini only reaps re-parented orphans, never zombies
    whose original parent is still alive). Confirmed by direct reproduction:
    ps aux inside the deployed container showed 100+ accumulating
    uc_chromedriver/chromium zombies even after adding tini, eventually
    exhausting the process table so Chrome could launch but not fork a
    renderer ("disconnected: unable to send message to renderer"). A SIGCHLD
    handler reaps every child the instant it exits, regardless of who killed
    it."""
    try:
        while True:
            pid, _status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
    except ChildProcessError:
        pass


if hasattr(signal, "SIGCHLD"):
    signal.signal(signal.SIGCHLD, _reap_zombie_children)

if os.name == "nt":
    # Windows equivalent of the Linux nofile ulimit fix: the C runtime caps
    # open file/handle-backed streams at 512 by default (_setmaxstdio),
    # covering every pipe/socket a Selenium+Chrome worker opens. Confirmed
    # by direct reproduction: running the local dashboard unattended
    # overnight accumulated enough open handles across repeated Chrome
    # launches to hit "[Errno 24] Too many open files" on every subsequent
    # job, permanently, for the rest of that process's life. 8192 is the
    # practical ceiling msvcrt.setmaxstdio() accepts.
    import ctypes

    try:
        # Some Windows Python builds don't expose msvcrt.setmaxstdio even
        # though the underlying CRT function exists - call it directly.
        # legacy msvcrt.dll caps this at 2048 (the newer ucrtbase.dll
        # supports up to 8192, but 2048 is already a 4x improvement over the
        # 512 default and is safely supported everywhere).
        result = ctypes.CDLL("msvcrt")._setmaxstdio(2048)
        if result == -1:
            raise OSError("_setmaxstdio returned -1")
        log.info("Raised Windows max stdio handles to %d", result)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not raise Windows stdio handle limit: %s", exc)

app = FastAPI(title="Amazon Review Scraper")

# Images/ is shared at the repo root (portals/amazon/dashboard.py -> repo
# root is two levels up), not duplicated per portal folder.
_IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "Images")
if os.path.isdir(_IMAGES_DIR):
    app.mount("/static", StaticFiles(directory=_IMAGES_DIR), name="static")


@app.on_event("startup")
def _startup() -> None:
    from browser import prepare_shared_driver

    # Synchronous, single-threaded, before any worker thread exists - see
    # the design note above prepare_shared_driver() in amazon_reviews.py for
    # why this replaced per-attempt driver resolution/patching.
    jobs.ensure_schema()

    log.info("Preparing chromedriver (one-time setup)...")
    prepare_shared_driver()
    log.info("Chromedriver ready.")

    start_background_thread()


@app.get("/api/logs")
def api_logs(after_id: int = 0, limit: int = 500) -> list[dict]:
    return log_buffer.get_since(after_id=after_id, limit=limit)


@app.get("/api/stats")
def api_stats() -> dict:
    stats = jobs.get_stats()
    stats["paused"] = jobs.is_paused()
    stats["cookies"] = jobs.get_cookies_status()
    return stats


@app.get("/api/jobs")
def api_jobs(
    status: str | None = None, search: str | None = None,
    sort_by: str = "updated_at", sort_dir: str = "desc",
    page: int = 1, page_size: int = 100,
) -> dict:
    return jobs.list_jobs_page(
        status=status, search=search, sort_by=sort_by, sort_dir=sort_dir, page=page, page_size=page_size,
    )


@app.get("/api/control/settings")
def api_get_settings() -> dict:
    return jobs.get_control()


@app.get("/api/cookies/status")
def api_cookies_status() -> dict:
    return jobs.get_cookies_status()


@app.post("/api/cookies")
def api_set_cookies(cookies: list[dict]) -> dict:
    if not cookies:
        raise HTTPException(status_code=400, detail="Cookie list is empty")
    for c in cookies:
        if "name" not in c or "value" not in c:
            raise HTTPException(status_code=400, detail="Each cookie needs at least a 'name' and 'value' field")
    jobs.set_cookies(cookies)
    return jobs.get_cookies_status()


@app.post("/api/cookies/test")
def api_test_cookies() -> dict:
    from scraper import check_cookies_valid

    cookies = jobs.get_cookies()
    if not cookies:
        raise HTTPException(status_code=400, detail="No cookies saved yet")
    ok = check_cookies_valid(cookies)
    jobs.set_cookies_check_result(ok)
    return jobs.get_cookies_status()


@app.post("/api/control/settings")
def api_set_settings(
    date_from: str | None = None, date_to: str | None = None,
    max_reviews: int | None = None, max_workers: int | None = None,
) -> dict:
    return jobs.set_control(date_from=date_from or None, date_to=date_to or None, max_reviews=max_reviews, max_workers=max_workers)


@app.post("/api/control/headless")
def api_set_headless(headless: bool) -> dict:
    jobs.set_headless_mode(headless)
    return {"headless_mode": headless}


@app.post("/api/control/pause")
def api_pause() -> dict:
    jobs.set_paused(True)
    return {"paused": True}


@app.post("/api/control/resume")
def api_resume() -> dict:
    jobs.set_paused(False)
    return {"paused": False}


@app.post("/api/control/retry-failed")
def api_retry_failed() -> dict:
    n = jobs.requeue_failures()
    return {"requeued": n}


@app.post("/api/control/retry-all")
def api_retry_all() -> dict:
    n = jobs.requeue_all()
    return {"requeued": n}


@app.post("/api/control/sync")
def api_sync() -> dict:
    n = jobs.sync_jobs_from_mapping()
    return {"synced": n}


@app.post("/api/jobs/{product_id}/trigger")
def api_trigger(product_id: str) -> dict:
    ok = jobs.requeue_product(product_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"No job found for product_id={product_id}")
    return {"product_id": product_id, "status": "pending"}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return _DASHBOARD_HTML


_DASHBOARD_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Amazon Review Scraper</title>
<link rel="icon" type="image/png" href="/static/icons8-star-96.png">
<style>
  body { font-family: system-ui, sans-serif; background: #0f1115; color: #e5e7eb; margin: 0; padding: 24px; }
  h1 { font-size: 20px; margin-bottom: 4px; display: flex; align-items: center; gap: 10px; }
  h1 img { width: 26px; height: 26px; }
  .sub { color: #9ca3af; font-size: 13px; margin-bottom: 20px; }
  .stats { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
  .card { background: #171a21; border: 1px solid #2a2e37; border-radius: 8px; padding: 12px 16px; min-width: 110px; }
  .card .n { font-size: 22px; font-weight: 600; }
  .card .l { font-size: 12px; color: #9ca3af; }
  .section { margin-bottom: 22px; }
  .section-head { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
  .section-head h2 { font-size: 14px; margin: 0; }
  .section-head .count { font-size: 12px; color: #9ca3af; }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  .dot.pending { background: #6b7280; }
  .dot.running { background: #3b82f6; }
  .dot.done { background: #22c55e; }
  .dot.na { background: #a855f7; }
  .dot.failed { background: #ef4444; }
  .dot.live { animation: dot-pulse 1.3s ease-in-out infinite; }
  @keyframes dot-pulse {
    0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(59,130,246,.55); }
    50% { opacity: .55; box-shadow: 0 0 0 4px rgba(59,130,246,0); }
  }
  .card.live-card { border-color: #3b82f6; box-shadow: 0 0 0 1px rgba(59,130,246,.35); }
  .spinner { width: 12px; height: 12px; border-radius: 50%; border: 2px solid #2a2e37; border-top-color: #3b82f6; display: inline-block; animation: spin .7s linear infinite; margin-right: 8px; vertical-align: -2px; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .tabs { display: flex; gap: 6px; margin-bottom: 16px; border-bottom: 1px solid #2a2e37; }
  .tab { display: flex; align-items: center; gap: 8px; padding: 9px 14px; cursor: pointer; border: 1px solid transparent; border-bottom: none; border-radius: 8px 8px 0 0; font-size: 13px; color: #9ca3af; }
  .tab:hover { color: #e5e7eb; }
  .tab.active { color: #e5e7eb; background: #171a21; border-color: #2a2e37; }
  .tab .count { font-size: 12px; }
  .controls { display: flex; gap: 8px; align-items: center; margin-bottom: 20px; flex-wrap: wrap; }
  button, input { font-size: 13px; padding: 7px 12px; border-radius: 6px; border: 1px solid #2a2e37; background: #1f232c; color: #e5e7eb; }
  button { cursor: pointer; }
  button:hover { background: #2a2e37; }
  input { min-width: 260px; }
  .table-wrap { max-height: 260px; overflow-y: auto; border: 1px solid #2a2e37; border-radius: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #1f232c; }
  th { color: #9ca3af; font-weight: 500; position: sticky; top: 0; background: #171a21; cursor: pointer; user-select: none; white-space: nowrap; }
  th:hover { color: #e5e7eb; }
  th .arrow { color: #3b82f6; margin-left: 3px; }
  .pagenum-input { width: 55px; min-width: auto; text-align: center; padding: 5px 6px; }
  .badge { padding: 2px 8px; border-radius: 999px; font-size: 11px; }
  .pending { background: #374151; }
  .running { background: #1d4ed8; }
  .done { background: #15803d; }
  .na { background: #7e22ce; }
  .failed { background: #b91c1c; }
  #paused-banner { display: none; background: #b45309; padding: 8px 12px; border-radius: 6px; margin-bottom: 16px; font-size: 13px; }
  .panel { background: #171a21; border: 1px solid #2a2e37; border-radius: 8px; padding: 14px 16px; margin-bottom: 20px; }
  .panel-title { font-size: 12px; color: #9ca3af; margin-bottom: 10px; text-transform: uppercase; letter-spacing: .04em; }
  .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  label { font-size: 12px; color: #9ca3af; }
  .filters { margin-bottom: 12px; }
  select { font-size: 13px; padding: 6px 8px; border-radius: 6px; border: 1px solid #2a2e37; background: #1f232c; color: #e5e7eb; }
  .pagebar { display: flex; align-items: center; gap: 10px; margin-top: 12px; font-size: 13px; }
  .pagebar button { padding: 5px 10px; }
  .pagebar button:disabled { opacity: .4; cursor: default; }
  .pageof { background: #1f232c; border: 1px solid #2a2e37; border-radius: 6px; padding: 5px 10px; display: flex; gap: 8px; align-items: center; }
  .pageof b { color: #e5e7eb; }
  .muted { color: #9ca3af; }
  a.link { color: #3b82f6; text-decoration: none; }
  a.link:hover { text-decoration: underline; }
  .terminal { background: #05070a; border: 1px solid #2a2e37; border-radius: 8px; padding: 12px; height: 420px; overflow-y: auto; font-family: ui-monospace, "Cascadia Code", "SF Mono", Consolas, monospace; font-size: 12.5px; white-space: pre-wrap; word-break: break-word; }
  .terminal .line { padding: 1px 0; }
  .terminal .lvl-INFO { color: #9ca3af; }
  .terminal .lvl-WARNING { color: #f59e0b; }
  .terminal .lvl-ERROR, .terminal .lvl-CRITICAL { color: #ef4444; }
  .terminal .ts { color: #4b5563; }
  .terminal .src { color: #3b82f6; }
  textarea { width: 100%; min-height: 160px; font-family: ui-monospace, "Cascadia Code", "SF Mono", Consolas, monospace; font-size: 12px; padding: 10px; box-sizing: border-box; resize: vertical; }
  .status-badge { padding: 3px 10px; border-radius: 999px; font-size: 12px; font-weight: 500; }
  .status-badge.ok { background: #15803d; }
  .status-badge.bad { background: #b91c1c; }
  .status-badge.unknown { background: #374151; }
</style>
</head>
<body>
<h1><img src="/static/icons8-star-96.png" alt="logo">Amazon Review Scraper</h1>
<div class="sub">Live scrape progress and controls</div>

<div id="paused-banner">Scraper is PAUSED — no new jobs will start until resumed.</div>

<div class="stats" id="stats"></div>

<div class="controls">
  <button onclick="post('/api/control/pause')">Pause</button>
  <button onclick="post('/api/control/resume')">Resume</button>
  <button onclick="post('/api/control/retry-failed')">Retry Failed</button>
  <button onclick="post('/api/control/sync')">Sync Products</button>
  <button id="headless-toggle" onclick="toggleHeadless()">Show Browser</button>
  <input id="trigger-input" placeholder="product_id to trigger" />
  <button onclick="triggerProduct()">Trigger Product</button>
</div>

<div class="panel">
  <div class="panel-title">Review filters — applied when storing scraped reviews (newest-first, capped)</div>
  <div class="row">
    <label>From date <input type="date" id="date-from" style="min-width:auto"></label>
    <label>To date <input type="date" id="date-to" style="min-width:auto"></label>
    <label>Max reviews per product <input type="number" id="max-reviews" min="1" placeholder="no limit" style="min-width:auto;width:110px"></label>
    <label>Concurrent workers <input type="number" id="max-workers" min="1" max="20" style="min-width:auto;width:80px"></label>
    <button onclick="saveSettings()">Save Settings</button>
    <span id="settings-saved" class="muted" style="display:none">Saved.</span>
  </div>
</div>

<div class="row" style="margin-bottom:16px">
  <input id="search-input" placeholder="Search product ID, SKU, or company..." style="min-width:280px" oninput="onSearchInput()">
</div>

<div class="tabs" id="tabs"></div>
<div id="sections"></div>

<div class="section" id="section-terminal" style="display:none">
  <div class="row" style="margin-bottom:8px">
    <button onclick="clearTerminal()">Clear</button>
    <label style="display:flex;align-items:center;gap:6px"><input type="checkbox" id="autoscroll-toggle" checked style="min-width:auto;width:auto"> Auto-scroll</label>
  </div>
  <div id="terminal" class="terminal"></div>
</div>

<div class="section" id="section-cookies" style="display:none">
  <div class="panel">
    <div class="panel-title">Session cookie status</div>
    <div id="cookies-status" class="row"></div>
  </div>
  <div class="panel">
    <div class="panel-title">Update cookies — paste either the raw "name=value; name2=value2" cookie string (copied from DevTools/Network tab) or a JSON array export (Cookie-Editor / "Get cookies.txt LOCALLY")</div>
    <textarea id="cookies-input" placeholder='session-id=...; at-acbin=...; session-token=...   OR   [{"name": "session-id", "value": "...", "domain": ".amazon.in", "path": "/"}, ...]'></textarea>
    <div class="row" style="margin-top:10px">
      <button onclick="saveCookies()">Save Cookies</button>
      <button onclick="testCookies()">Test Cookies</button>
      <span id="cookies-msg" class="muted"></span>
    </div>
  </div>
</div>

<script>
async function post(url) {
  await fetch(url, { method: 'POST' });
  refresh();
}
async function triggerProduct() {
  const id = document.getElementById('trigger-input').value.trim();
  if (!id) return;
  await fetch(`/api/jobs/${encodeURIComponent(id)}/trigger`, { method: 'POST' });
  refresh();
}
async function saveSettings() {
  const dateFrom = document.getElementById('date-from').value;
  const dateTo = document.getElementById('date-to').value;
  const maxReviews = document.getElementById('max-reviews').value;
  const maxWorkers = document.getElementById('max-workers').value;
  const params = new URLSearchParams();
  if (dateFrom) params.set('date_from', dateFrom);
  if (dateTo) params.set('date_to', dateTo);
  if (maxReviews) params.set('max_reviews', maxReviews);
  if (maxWorkers) params.set('max_workers', maxWorkers);
  await fetch(`/api/control/settings?${params.toString()}`, { method: 'POST' });
  const saved = document.getElementById('settings-saved');
  saved.style.display = 'inline';
  setTimeout(() => saved.style.display = 'none', 2000);
}
let headlessMode = true;
function updateHeadlessButton() {
  document.getElementById('headless-toggle').textContent = headlessMode ? 'Show Browser' : 'Hide Browser';
}
async function toggleHeadless() {
  headlessMode = !headlessMode;
  updateHeadlessButton();
  // Takes effect on the next job that starts (worker_pool re-reads this
  // per job) - already-running scrapes keep whatever mode they launched in.
  await fetch(`/api/control/headless?headless=${headlessMode}`, { method: 'POST' });
}
async function loadSettings() {
  const s = await (await fetch('/api/control/settings')).json();
  if (s.review_date_from) document.getElementById('date-from').value = s.review_date_from;
  if (s.review_date_to) document.getElementById('date-to').value = s.review_date_to;
  if (s.max_reviews_per_product) document.getElementById('max-reviews').value = s.max_reviews_per_product;
  document.getElementById('max-workers').value = s.max_concurrent_workers ?? 2;
  headlessMode = s.headless_mode ?? true;
  updateHeadlessButton();
}
let searchTerm = '';
let searchDebounce = null;

const STATUSES = ['pending', 'running', 'done', 'na', 'failed'];
const STATUS_LABELS = { na: 'N/A' };
const COLUMNS = [
  { key: 'product_id', label: 'Product ID' },
  { key: 'company', label: 'Company' },
  { key: 'master_sku', label: 'Master SKU' },
  { key: null, label: 'Rating' },
  { key: null, label: 'Total Ratings' },
  { key: 'reviews_found', label: 'Reviews Found' },
  { key: 'attempt_count', label: 'Attempts' },
  { key: 'updated_at', label: 'Updated' },
  { key: null, label: 'Error' },
];

// Per-status section state: page, sort, and page-size are independent per section.
const sectionState = {};
STATUSES.forEach(s => sectionState[s] = { page: 1, pageSize: 20, sortBy: 'updated_at', sortDir: 'desc' });

let activeTab = 'pending';

document.getElementById('sections').innerHTML = STATUSES.map(s => `
  <div class="section" id="section-${s}" style="display:none">
    <div class="table-wrap">
      <table>
        <thead><tr id="head-${s}"></tr></thead>
        <tbody id="body-${s}"></tbody>
      </table>
    </div>
    <div class="pagebar">
      <button id="prev-${s}" onclick="prevPage('${s}')">&lsaquo;</button>
      <div class="pageof">
        Page
        <input type="number" min="1" id="jump-${s}" class="pagenum-input" value="1" onkeydown="if(event.key==='Enter') jumpToPage('${s}')">
        of <b id="total-${s}">1</b>
      </div>
      <button id="next-${s}" onclick="nextPage('${s}')">&rsaquo;</button>
      <button onclick="jumpToPage('${s}')">Go</button>
      <select id="size-${s}" onchange="onPageSizeChange('${s}')">
        <option value="10">10 rows</option>
        <option value="20" selected>20 rows</option>
        <option value="50">50 rows</option>
        <option value="100">100 rows</option>
      </select>
      <span class="muted" id="rc-${s}"></span>
    </div>
  </div>
`).join('');

function selectTab(status) {
  activeTab = status;
  STATUSES.forEach(s => document.getElementById(`section-${s}`).style.display = s === status ? 'block' : 'none');
  document.getElementById('section-terminal').style.display = status === 'terminal' ? 'block' : 'none';
  document.getElementById('section-cookies').style.display = status === 'cookies' ? 'block' : 'none';
  renderTabs();
  if (status === 'terminal') {
    refreshTerminal();
  } else if (status === 'cookies') {
    refreshCookiesStatus();
  } else {
    refreshSection(status);
  }
}
let lastRunningCount = 0;
function renderTabs() {
  const statusTabs = STATUSES.map(s => `
    <div class="tab ${s === activeTab ? 'active' : ''}" onclick="selectTab('${s}')">
      <span class="dot ${s} ${s === 'running' && lastRunningCount > 0 ? 'live' : ''}"></span>${STATUS_LABELS[s] ?? (s.charAt(0).toUpperCase() + s.slice(1))}
      <span class="count muted" id="tabcount-${s}"></span>
    </div>
  `).join('');
  const extraTabs = `
    <div class="tab ${activeTab === 'terminal' ? 'active' : ''}" onclick="selectTab('terminal')">Terminal</div>
    <div class="tab ${activeTab === 'cookies' ? 'active' : ''}" onclick="selectTab('cookies')">Cookies</div>
  `;
  document.getElementById('tabs').innerHTML = statusTabs + extraTabs;
}

function sortByColumn(status, key) {
  if (!key) return;
  const st = sectionState[status];
  if (st.sortBy === key) { st.sortDir = st.sortDir === 'asc' ? 'desc' : 'asc'; } else { st.sortBy = key; st.sortDir = 'desc'; }
  st.page = 1;
  refreshSection(status);
}
function prevPage(status) {
  const st = sectionState[status];
  if (st.page > 1) { st.page -= 1; refreshSection(status); }
}
function nextPage(status) {
  sectionState[status].page += 1;
  refreshSection(status);
}
function jumpToPage(status) {
  const n = parseInt(document.getElementById(`jump-${status}`).value, 10);
  if (n >= 1) { sectionState[status].page = n; refreshSection(status); }
}
function onPageSizeChange(status) {
  sectionState[status].pageSize = parseInt(document.getElementById(`size-${status}`).value, 10);
  sectionState[status].page = 1;
  refreshSection(status);
}
function onSearchInput() {
  clearTimeout(searchDebounce);
  searchDebounce = setTimeout(() => {
    searchTerm = document.getElementById('search-input').value.trim();
    STATUSES.forEach(s => sectionState[s].page = 1);
    refreshAll();
  }, 300);
}

async function refreshSection(status) {
  const st = sectionState[status];

  document.getElementById(`head-${status}`).innerHTML = COLUMNS.map(c => {
    const arrow = c.key && st.sortBy === c.key ? `<span class="arrow">${st.sortDir === 'asc' ? '▲' : '▼'}</span>` : '';
    return `<th onclick="sortByColumn('${status}', ${c.key ? `'${c.key}'` : 'null'})">${c.label}${arrow}</th>`;
  }).join('');

  const params = new URLSearchParams({
    status, page: st.page, page_size: st.pageSize, sort_by: st.sortBy, sort_dir: st.sortDir,
  });
  if (searchTerm) params.set('search', searchTerm);

  let result = await (await fetch(`/api/jobs?${params.toString()}`)).json();
  if (st.page > result.total_pages) {
    st.page = result.total_pages;
    params.set('page', st.page);
    result = await (await fetch(`/api/jobs?${params.toString()}`)).json();
  }
  st.page = result.page;

  document.getElementById(`jump-${status}`).value = result.page;
  document.getElementById(`total-${status}`).textContent = result.total_pages;
  document.getElementById(`rc-${status}`).textContent = `${result.total.toLocaleString()} records`;
  document.getElementById(`prev-${status}`).disabled = result.page <= 1;
  document.getElementById(`next-${status}`).disabled = result.page >= result.total_pages;

  const isLive = status === 'running';
  document.getElementById(`body-${status}`).innerHTML = result.rows.map(j => `
    <tr>
      <td>${isLive ? '<span class="spinner"></span>' : ''}${j.product_link
        ? `<a class="link" href="${j.product_link}" target="_blank" rel="noopener noreferrer">${j.product_id ?? ''}</a>`
        : (j.product_id ?? '')}</td>
      <td>${j.company ?? ''}</td>
      <td>${j.master_sku ?? ''}</td>
      <td>${j.average_rating != null ? '★ ' + Number(j.average_rating).toFixed(1) : ''}</td>
      <td>${j.total_ratings ?? ''}</td>
      <td>${j.reviews_found ?? 0}</td>
      <td>${j.attempt_count ?? 0}</td>
      <td>${j.updated_at ? new Date(j.updated_at).toLocaleString() : ''}</td>
      <td title="${j.last_error ?? ''}">${(j.last_error ?? '').slice(0, 60)}</td>
    </tr>
  `).join('');
}

function cookieStatusCard(c) {
  let label, cls;
  if (!c || !c.present) { label = 'Not set'; cls = 'unknown'; }
  else if (c.last_check_ok === null || c.last_check_ok === undefined) { label = 'Untested'; cls = 'unknown'; }
  else if (c.last_check_ok) { label = 'Working'; cls = 'ok'; }
  else { label = 'Invalid'; cls = 'bad'; }
  return `<div class="card" style="cursor:pointer" onclick="selectTab('cookies')">
    <div class="n" style="padding-top:2px"><span class="status-badge ${cls}">${label}</span></div>
    <div class="l">session cookies</div>
  </div>`;
}
async function refreshAll() {
  const stats = await (await fetch('/api/stats')).json();
  document.getElementById('paused-banner').style.display = stats.paused ? 'block' : 'none';
  lastRunningCount = stats.running ?? 0;
  document.getElementById('stats').innerHTML = STATUSES.map(k =>
    `<div class="card ${k === 'running' && lastRunningCount > 0 ? 'live-card' : ''}">
      <div class="n">${k === 'running' && lastRunningCount > 0 ? '<span class="spinner"></span>' : ''}${stats[k] ?? 0}</div>
      <div class="l">${STATUS_LABELS[k] ?? k}</div>
    </div>`
  ).join('')
    + `<div class="card"><div class="n">${stats.total_reviews_found ?? 0}</div><div class="l">total reviews found</div></div>`
    + cookieStatusCard(stats.cookies);

  STATUSES.forEach(s => {
    const el = document.getElementById(`tabcount-${s}`);
    if (el) el.textContent = `(${(stats[s] ?? 0).toLocaleString()})`;
  });
  const runningDot = document.querySelector('.tab .dot.running');
  if (runningDot) runningDot.classList.toggle('live', lastRunningCount > 0);

  // Only the visible tab's table needs live row data - the others just show
  // their count until clicked, so we're not running 5x the queries every cycle.
  if (activeTab === 'terminal') {
    await refreshTerminal();
  } else {
    await refreshSection(activeTab);
  }
}

let lastLogId = 0;

function clearTerminal() {
  document.getElementById('terminal').innerHTML = '';
}

async function refreshTerminal() {
  const lines = await (await fetch(`/api/logs?after_id=${lastLogId}&limit=500`)).json();
  if (!lines.length) return;

  const term = document.getElementById('terminal');
  const atBottom = term.scrollHeight - term.scrollTop - term.clientHeight < 40;

  for (const l of lines) {
    lastLogId = Math.max(lastLogId, l.id);
    const div = document.createElement('div');
    div.className = 'line';
    const time = new Date(l.ts).toLocaleTimeString();
    div.innerHTML = `<span class="ts">${time}</span> <span class="src">${l.logger}</span> <span class="lvl-${l.level}">${l.message}</span>`;
    term.appendChild(div);
  }
  while (term.children.length > 2000) term.removeChild(term.firstChild);

  if (document.getElementById('autoscroll-toggle').checked && atBottom) {
    term.scrollTop = term.scrollHeight;
  }
}

function renderCookiesStatus(s) {
  const badge = s.last_check_ok === null || s.last_check_ok === undefined
    ? `<span class="status-badge unknown">Untested</span>`
    : s.last_check_ok
      ? `<span class="status-badge ok">Working</span>`
      : `<span class="status-badge bad">Expired / Invalid</span>`;
  document.getElementById('cookies-status').innerHTML = `
    <div class="card"><div class="n">${s.present ? s.count : 0}</div><div class="l">cookies stored</div></div>
    <div class="card"><div class="n" style="font-size:14px;padding-top:4px">${s.updated_at ? new Date(s.updated_at).toLocaleString() : 'never'}</div><div class="l">last updated</div></div>
    <div class="card"><div class="n" style="padding-top:6px">${badge}</div><div class="l">status ${s.last_check_at ? '(' + new Date(s.last_check_at).toLocaleString() + ')' : ''}</div></div>
  `;
}
async function refreshCookiesStatus() {
  const s = await (await fetch('/api/cookies/status')).json();
  renderCookiesStatus(s);
}
function parseRawCookieHeader(text) {
  // Accepts the raw "name=value; name2=value2" string you copy straight out
  // of a browser's DevTools (Network tab request header, or the address-bar
  // cookie string) - not just the JSON array export format.
  const cookies = [];
  for (const part of text.split(';')) {
    const trimmed = part.trim();
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    cookies.push({
      name: trimmed.slice(0, eq).trim(),
      value: trimmed.slice(eq + 1).trim(),
      domain: '.amazon.in',
      path: '/',
    });
  }
  return cookies;
}
async function saveCookies() {
  const msg = document.getElementById('cookies-msg');
  const raw = document.getElementById('cookies-input').value.trim();
  let cookies;
  try {
    cookies = JSON.parse(raw);
  } catch (e) {
    cookies = parseRawCookieHeader(raw);
    if (!cookies.length) {
      msg.textContent = "Couldn't parse as JSON or as a raw 'name=value; ...' cookie string.";
      return;
    }
  }
  const resp = await fetch('/api/cookies', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cookies),
  });
  if (!resp.ok) {
    const err = await resp.json();
    msg.textContent = 'Error: ' + (err.detail || resp.statusText);
    return;
  }
  const s = await resp.json();
  renderCookiesStatus(s);
  msg.textContent = 'Saved.';
  setTimeout(() => msg.textContent = '', 3000);
}
async function testCookies() {
  const msg = document.getElementById('cookies-msg');
  const startedAt = Date.now();
  const tick = setInterval(() => {
    const secs = Math.floor((Date.now() - startedAt) / 1000);
    msg.textContent = `Testing... ${secs}s elapsed (opens a real browser - can take much longer than usual while other scrapes are running concurrently)`;
  }, 1000);
  msg.textContent = 'Testing...';

  try {
    const resp = await fetch('/api/cookies/test', { method: 'POST' });
    clearInterval(tick);
    if (!resp.ok) {
      const err = await resp.json();
      msg.textContent = 'Error: ' + (err.detail || resp.statusText);
      return;
    }
    const s = await resp.json();
    renderCookiesStatus(s);
    msg.textContent = s.last_check_ok ? 'Cookies are working.' : 'Cookies are expired or invalid - export fresh ones.';
  } catch (e) {
    clearInterval(tick);
    msg.textContent = 'Request failed: ' + e.message;
  }
}

renderTabs();
document.getElementById(`section-${activeTab}`).style.display = 'block';
loadSettings();
refreshAll();
setInterval(refreshAll, 1500);
</script>
</body>
</html>
"""
