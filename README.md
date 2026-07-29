# Review Scraper Service

Standalone service that scrapes Amazon product reviews/ratings and writes them to Postgres. Deliberately separate from the `PricingModule` repo/app — the only thing shared between the two is the same Postgres/Supabase database (reached over the network), no shared code or filesystem.

## What it does

- Reads Amazon product links from `multi_product_id_mapping` (public schema, owned by PricingModule's Multi Product ID module — read-only reference data).
- Scrapes each product's reviews + star-rating breakdown via headless Selenium.
- Writes results into `reviews.amazon_reviews` (its own dedicated Postgres schema).
- Runs once on start, then every 24h, forever.

PricingModule's `App/Api/Reviews.py` only ever `SELECT`s from `reviews.amazon_reviews` to power its Reviews page — it never scrapes anything itself.

## Files

- `amazon_reviews.py` — the scraper (Selenium + BeautifulSoup).
- `database.py` — minimal psycopg2 connector.
- `run_scheduler.py` — entrypoint, the 24h loop.
- `requirements.txt` / `nixpacks.toml` — deploy config (Dokploy, Nixpacks build: python312 + chromium, no Node).

## Environment variables needed

Same DB credentials as the main `PricingModule` app:
```
DB_HOST
DB_PORT
DB_USER
DB_PASSWORD
DB_NAME
SKIP_SSH_TUNNEL=true
```

## Deploying (Dokploy)

New "Application" service, this repo as the source, Nixpacks build (auto-detected from `nixpacks.toml`). No custom Start Command override needed — `nixpacks.toml`'s `[start].cmd` already runs `run_scheduler.py` directly. No Domains/volumes needed — it serves no web traffic and has no persistent local state (Postgres is the only state).

## Known limitation

`multi_product_id_mapping` currently has ~7,000 Amazon product links. At the ~10-15s/product pace needed to avoid Amazon's CAPTCHA wall, one full pass takes 20+ hours — longer than the 24h cadence this runs on. Needs a redesign (concurrency across multiple browser instances, a longer cadence, or splitting the catalog into daily chunks) before running unattended at full scale.
