"""In-memory ring buffer of recent log lines, exposed via the dashboard as a
live "terminal" view - so you can see what the background worker pool is
doing (scrape attempts, retries, CAPTCHA hits, sync events) without needing
a separate SSH/Dokploy terminal session.
"""

import logging
from collections import deque
from datetime import datetime, timezone
from itertools import count
from threading import Lock

_MAX_LINES = 2000
_buffer: deque = deque(maxlen=_MAX_LINES)
_lock = Lock()
_next_id = count(1)

# Noisy loggers not worth showing in the live terminal view.
_EXCLUDED_LOGGERS = {"uvicorn.access", "WDM"}


class RingBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if record.name in _EXCLUDED_LOGGERS:
            return
        entry = {
            "id": next(_next_id),
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self.format(record),
        }
        with _lock:
            _buffer.append(entry)


def install() -> None:
    handler = RingBufferHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)


def get_since(after_id: int = 0, limit: int = 500) -> list[dict]:
    with _lock:
        snapshot = list(_buffer)
    lines = [e for e in snapshot if e["id"] > after_id]
    return lines[-limit:]
