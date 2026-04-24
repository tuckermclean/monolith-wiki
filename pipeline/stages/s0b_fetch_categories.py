"""
pipeline.stages.s0b_fetch_categories — Stage 0b: Fetch Wikipedia categories.

For ZIM files that strip the ``mw-normal-catlinks`` block (e.g. ``nopic``
variants), this stage fetches real Wikipedia categories via the public API
and populates the ``article_categories`` table that Stage 2 depends on.

Algorithm
---------
1. Collect all Stage-0-indexed article paths that currently have zero
   categories in ``article_categories``.
2. Batch them 50 at a time (API limit per request).
3. Fire batches concurrently via a thread pool (I/O-bound work).
4. Normalise and insert fetched categories into ``article_categories``.

Idempotency
-----------
Articles that already have categories in the DB are skipped; re-running is
safe after a partial failure.

Wikipedia API notes
-------------------
* Endpoint: https://en.wikipedia.org/w/api.php
* ``clshow=!hidden``  — skips maintenance / tracking categories.
* ``cllimit=50``      — max categories returned per title.
* Titles with pipes (``|``) are split into batches of 50.
* A descriptive User-Agent is required by Wikimedia policy.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.config import Config
from pipeline.db import record_stage_complete

logger = logging.getLogger(__name__)

_API_URL = "https://en.wikipedia.org/w/api.php"
_USER_AGENT = (
    "MonolithWikiPipeline/1.0 "
    "(https://github.com/monolith-wiki; pipeline@example.com)"
)
_BATCH_SIZE = 50       # Wikipedia API max titles per request
_MAX_WORKERS = 10      # concurrent HTTP threads
_TIMEOUT = 20          # seconds per request
_RETRY_TOTAL = 3       # retries on transient failures
# cllimit: use 500 (max for unauthenticated requests).
# IMPORTANT: this limit is per-batch-total, not per-title.  Using the max
# and following clcontinue pagination ensures no article is silently skipped.
_CAT_LIMIT = "500"


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = _USER_AGENT
    retry = Retry(
        total=_RETRY_TOTAL,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _fetch_batch(
    batch: list[tuple[int, str]],   # [(article_id, path), ...]
    session: requests.Session,
) -> list[tuple[int, str]]:
    """
    Fetch categories for a batch of articles.
    Returns [(article_id, category_lowercase), ...].

    Follows clcontinue pagination so that articles later in the batch are
    not silently skipped when the total category count exceeds cllimit.
    """
    # Wikipedia paths use underscores; spaces are also accepted via the API.
    titles = "|".join(path.replace(" ", "_") for _, path in batch)
    path_to_id = {path: aid for aid, path in batch}

    rows: list[tuple[int, str]] = []
    params: dict = {
        "action": "query",
        "prop": "categories",
        "clshow": "!hidden",
        "cllimit": _CAT_LIMIT,
        "format": "json",
        "titles": titles,
    }

    while True:
        try:
            resp = session.get(_API_URL, params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("API batch failed (%s); skipping %d titles", exc, len(batch))
            break

        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            # Map API-returned title back to our path (underscores ↔ spaces).
            api_title: str = page.get("title", "")
            art_id = (
                path_to_id.get(api_title)
                or path_to_id.get(api_title.replace(" ", "_"))
                or path_to_id.get(api_title.replace("_", " "))
            )
            if art_id is None:
                continue
            for cat_entry in page.get("categories", []):
                # Strip "Category:" prefix that the API adds.
                cat_title: str = cat_entry.get("title", "")
                if cat_title.startswith("Category:"):
                    cat_title = cat_title[len("Category:"):]
                cat_title = cat_title.strip().lower()
                if cat_title:
                    rows.append((art_id, cat_title))

        # Follow pagination if more category results are available.
        cont = data.get("continue")
        if cont:
            params = {**params, **cont}
        else:
            break

    return rows


def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 0b: fetch Wikipedia categories for uncategorised articles."""

    # Collect articles with no existing categories.
    uncategorised: list[tuple[int, str]] = list(conn.execute(
        """
        SELECT a.id, a.path
        FROM   articles a
        WHERE  NOT EXISTS (
            SELECT 1 FROM article_categories ac WHERE ac.article_id = a.id
        )
        ORDER  BY a.id
        """
    ).fetchall())

    if not uncategorised:
        logger.info("Stage 0b: all articles already have categories — nothing to do.")
        record_stage_complete(conn, "s0b", article_count=0, notes="all cached")
        return

    logger.info(
        "Stage 0b: fetching Wikipedia categories for %d uncategorised articles "
        "(%d batches of up to %d, %d threads) …",
        len(uncategorised),
        -(-len(uncategorised) // _BATCH_SIZE),
        _BATCH_SIZE,
        _MAX_WORKERS,
    )

    # Split into batches.
    batches: list[list[tuple[int, str]]] = [
        uncategorised[i : i + _BATCH_SIZE]
        for i in range(0, len(uncategorised), _BATCH_SIZE)
    ]

    total_cats = 0
    t0 = time.monotonic()

    # One session per thread; sessions are thread-safe but not fork-safe.
    session = _make_session()

    def _worker(b):
        return _fetch_batch(b, session)

    all_rows: list[tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(_worker, b): b for b in batches}
        done = 0
        for fut in as_completed(futures):
            rows = fut.result()
            all_rows.extend(rows)
            done += 1
            if done % 10 == 0 or done == len(batches):
                elapsed = time.monotonic() - t0
                logger.info(
                    "Stage 0b: %d/%d batches done, %d categories so far (%.1fs)",
                    done, len(batches), len(all_rows), elapsed,
                )

    # Bulk-insert all at once (idempotent via INSERT OR IGNORE).
    if all_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO article_categories (article_id, category) VALUES (?, ?)",
            all_rows,
        )
        conn.commit()
        total_cats = len(all_rows)

    elapsed = time.monotonic() - t0
    articles_with_cats = conn.execute(
        "SELECT COUNT(DISTINCT article_id) FROM article_categories"
    ).fetchone()[0]

    logger.info(
        "Stage 0b: complete.  %d categories inserted for %d articles in %.1fs.",
        total_cats, articles_with_cats, elapsed,
    )
    record_stage_complete(
        conn,
        "s0b",
        article_count=articles_with_cats,
        notes=f"categories_fetched={total_cats}",
    )


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging as _logging

    from pipeline.config import load_config
    from pipeline.db import open_db

    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run Stage 0b: fetch Wikipedia categories")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
