"""
pipeline.stages.s0_index â€” Stage 0: ZIM pre-pass indexer.

Single streaming pass over the ZIM file.  For every main-namespace,
non-redirect article, this stage:

  1. Parses HTML with BeautifulSoup / lxml.
  2. Detects redirects, disambiguation pages, and stubs.
  3. Extracts plain-text character count and raw HTML size.
  4. Extracts outbound internal links (href values) from the article body.
  5. Extracts category names from the category links box.
  6. Inserts rows into: ``articles``, ``links``, ``article_categories``.

After the streaming pass:

  7. Resolves ``links.target_path`` â†’ ``articles.id`` and populates
     ``link_edges``.
  8. Updates ``articles.inbound_count`` and ``articles.outbound_count``.

This stage is idempotent; it will clear and re-build the tables if re-run.

Runtime note
------------
For the full English Wikipedia ZIM (~100 GB, ~6 M entries) this stage
takes several hours on a single core.  Progress is reported via tqdm.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import unquote

from tqdm import tqdm

from pipeline.config import Config
from pipeline.db import record_stage_complete
from pipeline.hash import verify_zim
from pipeline.zim_reader import ZimReader

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal link filtering (used by the worker)
# ---------------------------------------------------------------------------

_SKIP_PREFIXES: tuple[str, ...] = (
    "http://", "https://", "//", "mailto:", "#",
    "Category:", "Wikipedia:", "Help:", "File:", "Portal:",
    "Template:", "Special:", "Talk:", "User:", "Draft:",
)


def _filter_hrefs(hrefs: list[str]) -> list[str]:
    """Normalise and deduplicate internal article hrefs, discarding others."""
    seen: set[str] = set()
    results: list[str] = []
    for href in hrefs:
        href = href.strip()
        if not href or any(href.startswith(p) for p in _SKIP_PREFIXES):
            continue
        if "#" in href:
            href = href.split("#")[0]
        if not href:
            continue
        try:
            href = unquote(href, errors="strict")
        except Exception:
            continue
        href = href.lstrip("./")
        if href and href not in seen:
            seen.add(href)
            results.append(href)
    return results


# ---------------------------------------------------------------------------
# Worker function (runs in pool processes, no BS4 overhead)
# ---------------------------------------------------------------------------

def _parse_worker(batch: list[tuple[str, str, int, bytes]]) -> list[dict]:
    """
    Parse a batch of (path, title, html_size, html_bytes) tuples with lxml.
    Runs in a worker process; safe to call concurrently.
    Returns a list of dicts with extracted fields.
    """
    from lxml import html as _lhtml  # deferred so the func is pickle-safe

    results: list[dict] = []
    for path, title, html_size, html_bytes in batch:
        try:
            tree = _lhtml.fromstring(html_bytes)
        except Exception:
            results.append({
                "path": path, "title": title, "html_size": html_size,
                "text_chars": 0, "is_disambig": 0, "is_stub": 0,
                "categories": [], "links": [],
            })
            continue

        # XPath with normalize-space handles multi-class attributes correctly.
        is_disambig = int(bool(tree.xpath(
            './/*[contains(concat(" ",normalize-space(@class)," ")," dmbox-disambig ")'
            ' or contains(concat(" ",normalize-space(@class)," ")," dmbox ")]'
        )))
        is_stub = int(bool(tree.xpath(
            './/*[contains(concat(" ",normalize-space(@class)," ")," stub-article ")'
            ' or contains(concat(" ",normalize-space(@class)," ")," stub ")]'
        )))

        body_els = tree.xpath('//*[@id="mw-content-text"]')
        body = body_els[0] if body_els else tree
        text_chars = len(body.text_content())

        cat_texts = tree.xpath('//*[@id="mw-normal-catlinks"]//a/text()')
        categories = [c.strip().lower() for c in cat_texts if c.strip()]

        link_hrefs: list[str] = body.xpath('.//a/@href') if body_els else []
        links = _filter_hrefs(link_hrefs)

        results.append({
            "path": path, "title": title, "html_size": html_size,
            "text_chars": text_chars, "is_disambig": is_disambig, "is_stub": is_stub,
            "categories": categories, "links": links,
        })
    return results


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Articles per batch submitted to the worker pool.
_PARSE_BATCH: int = 500

# Maximum pending futures before we pause to drain (keeps RSS bounded).
_MAX_PENDING: int = 16


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _clear_tables(conn: sqlite3.Connection) -> None:
    """Clear the tables populated by this stage."""
    conn.executescript("""
        DELETE FROM article_categories;
        DELETE FROM link_edges;
        DELETE FROM links;
        DELETE FROM articles;
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Main stage entry point
# ---------------------------------------------------------------------------

def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 0: populate the SQLite database from the ZIM file."""
    logger.info("Stage 0: verifying ZIM integrity ...")
    verify_zim(cfg.input.zim_path, cfg.input.zim_sha256)

    logger.info("Stage 0: clearing existing index tables ...")
    _clear_tables(conn)

    # Disable fsync + FK checks during bulk load â€” massive write speed-up.
    conn.execute("PRAGMA synchronous  = OFF")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA cache_size   = -262144")  # 256 MB page cache
    conn.execute("PRAGMA temp_store   = MEMORY")

    reader = ZimReader(cfg.input.zim_path)
    n_workers: int = cfg.build.workers or min(os.cpu_count() or 2, 8)
    logger.info(
        "Stage 0: ZIM has %d entries; parse workers=%d",
        reader.entry_count, n_workers,
    )

    # ------------------------------------------------------------------
    # Helper: drain one completed future â†’ insert its results to SQLite.
    # Links + categories are inserted immediately (no in-memory buffers).
    # ------------------------------------------------------------------
    article_count = 0

    def _drain(fut) -> None:
        nonlocal article_count
        results: list[dict] = fut.result()

        art_rows = [
            (r["path"], r["title"], r["is_disambig"], r["is_stub"],
             r["html_size"], r["text_chars"])
            for r in results
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO articles"
            "  (path, title, is_disambig, is_stub, html_size, text_chars)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            art_rows,
        )
        conn.commit()

        # Look up the DB-assigned IDs for these articles.
        paths = [r["path"] for r in results]
        ph = ",".join("?" * len(paths))
        id_map: dict[str, int] = {
            row["path"]: row["id"]
            for row in conn.execute(
                f"SELECT id, path FROM articles WHERE path IN ({ph})", paths
            )
        }

        link_rows = [
            (id_map[r["path"]], target)
            for r in results
            for target in r["links"]
            if r["path"] in id_map
        ]
        cat_rows = [
            (id_map[r["path"]], cat)
            for r in results
            for cat in r["categories"]
            if r["path"] in id_map
        ]
        if link_rows:
            conn.executemany(
                "INSERT INTO links (source_id, target_path) VALUES (?, ?)",
                link_rows,
            )
        if cat_rows:
            conn.executemany(
                "INSERT INTO article_categories (article_id, category) VALUES (?, ?)",
                cat_rows,
            )
        conn.commit()
        article_count += len(art_rows)

    # ------------------------------------------------------------------
    # Streaming pass: read ZIM sequentially (single thread), parse HTML
    # in parallel worker processes, drain results incrementally.
    # ------------------------------------------------------------------
    batch: list[tuple] = []
    pending: list = []

    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        with tqdm(total=reader.entry_count, unit="entry", desc="s0 index") as pbar:
            prev_i = 0

            def _progress(i: int, _total: int) -> None:
                nonlocal prev_i
                pbar.update(i - prev_i)
                prev_i = i

            for article in reader.iter_main_articles(progress_callback=_progress):
                batch.append((
                    article.path, article.title,
                    article.html_size, article.html,
                ))
                if len(batch) >= _PARSE_BATCH:
                    pending.append(pool.submit(_parse_worker, batch))
                    batch = []
                    # Drain any already-completed futures to bound memory.
                    still: list = []
                    for f in pending:
                        if f.done():
                            _drain(f)
                        else:
                            still.append(f)
                    pending = still
                    # If too many still in flight, drain the oldest.
                    while len(pending) > _MAX_PENDING:
                        _drain(pending.pop(0))

        # Flush final partial batch and all in-flight futures.
        if batch:
            pending.append(pool.submit(_parse_worker, batch))
        for f in pending:
            _drain(f)

    logger.info("Stage 0: %d articles indexed; resolving link edges ...", article_count)

    # ------------------------------------------------------------------
    # Resolve link_edges: links.target_path -> articles.id (SQL JOIN).
    # ------------------------------------------------------------------
    conn.execute("""
        INSERT OR IGNORE INTO link_edges (source_id, target_id)
        SELECT l.source_id, a.id
        FROM   links l
        JOIN   articles a ON a.path = l.target_path
    """)
    conn.commit()

    # ------------------------------------------------------------------
    # Update inbound / outbound counts.
    # ------------------------------------------------------------------
    logger.info("Stage 0: updating link counts ...")
    conn.execute("""
        UPDATE articles
        SET outbound_count = (
            SELECT COUNT(*) FROM link_edges le WHERE le.source_id = articles.id
        )
    """)
    conn.execute("""
        UPDATE articles
        SET inbound_count = (
            SELECT COUNT(*) FROM link_edges le WHERE le.target_id = articles.id
        )
    """)
    conn.commit()

    # Restore safe pragma defaults.
    conn.execute("PRAGMA synchronous  = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")

    final_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    logger.info("Stage 0: complete.  %d articles in index.", final_count)
    record_stage_complete(conn, "s0", article_count=final_count, notes="ZIM pre-pass")


# ---------------------------------------------------------------------------
# Module-level __main__ support (for Makefile stage invocation)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging as _logging

    from pipeline.config import load_config
    from pipeline.db import open_db

    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run Stage 0: ZIM pre-pass indexer")
    parser.add_argument("--config", default=None, help="Path to YAML config file")
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
