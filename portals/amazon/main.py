"""Single entrypoint for running the Amazon portal locally.

Use case:
    Loads the repo-root .env (if present - a real deployment sets these as
    actual environment variables instead, so this is a no-op there) and
    starts the dashboard, which brings up the worker pool as a background
    thread on FastAPI's startup event (see dashboard.py's
    @app.on_event("startup")). .env is two levels up from this file
    (portals/amazon/main.py -> repo root) since all portal folders share
    one set of DB credentials.

RUN (from this directory):
    python main.py
"""

import os
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")


def main() -> None:
    port = int(os.environ.get("PORT", "8010"))
    # reload=True is NOT safe here: this app runs a long-lived background
    # worker thread doing real Chrome automation (see worker_pool.py), and
    # its own log file writes into this same folder that --reload watches -
    # confirmed by direct reproduction that this caused reload storms mid-
    # scrape, killing the persistent Chrome driver and cascading into
    # repeated "Chrome failed to start: crashed" errors on every subsequent
    # job. reload is for stateless web apps with a frontend; this isn't one.
    uvicorn.run("dashboard:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    main()
