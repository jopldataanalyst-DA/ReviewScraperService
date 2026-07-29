"""Minimal, self-contained Postgres connector for the Review Scraper service.

Deliberately does NOT depend on anything from the PricingModule/
PricingManagementSystem codebase - this service is a fully separate
project. Reads the same DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME env
vars as the main app so it can point at the same Supabase/Postgres
instance, but connects directly with plain psycopg2 (no connection pool,
no SSH-tunnel auto-detection - this runs inside Dokploy with
SKIP_SSH_TUNNEL=true and a direct DB_HOST, same as the main app in
production).
"""

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


def _config() -> dict:
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.environ.get("DB_PORT", "5432")),
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "dbname": os.environ.get("DB_NAME", "postgres"),
    }


@contextmanager
def get_cursor(dictionary: bool = False, commit: bool = False):
    conn = psycopg2.connect(**_config())
    cursor_factory = psycopg2.extras.RealDictCursor if dictionary else None
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_all(query: str, params: tuple = (), dictionary: bool = True) -> list:
    with get_cursor(dictionary=dictionary) as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()
