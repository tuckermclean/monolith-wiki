"""
pipeline.stages.s0_index — Stage 0: ZIM pre-pass indexer.

Single streaming pass over the ZIM file.  For every main-namespace,
non-redirect article, this stage:

  1. Parses HTML with BeautifulSoup / lxml.
  2. Detects redirects, disambiguation pages, and stubs.
  3. Extracts plain-text character count and raw HTML size.
  4. Extracts outbound internal links (href values) from the article body.
  5. Extracts category names from the category links box.
  6. Inserts rows into: ``articles``, ``links``, ``article_categories``.

After the streaming pass:

  7. Resolves ``links.target_path`` → ``articles.id`` and populates
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
import re
import sqlite3
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup
from tqdm import tqdm

from pipeline.config import Config
from pipeline.db import record_stage_complete
from pipeline.hash import verify_zim
from pipeline.zim_reader import ZimReader

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTML parsing constants
# ---------------------------------------------------------------------------

# CSS classes that mark disambiguation pages (rendered by {{Dmbox}}).
_DISAMBIG_CLASSES = {"dmbox-disambig", "dmbox"}

# CSS classes that indicate stub articles.
_STUB_CLASSES = {"stub", "stub-article"}

# IDs / classes of elements that contain the article body links we want.
_BODY_ID = "mw-content-text"

# ID of the categories box.
_CATLINKS_ID = "mw-normal-catlinks"

# Only keep href values that look like local Wikipedia article paths.
# We strip fragment identifiers and external URLs.
_INTERNAL_HREF_RE = re.compile(r"^[A-Za-z0-9_/%()\-+,.:@!*~]+$")


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def _parse_html(html: bytes) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def _is_disambiguation(soup: BeautifulSoup) -> bool:
    return bool(soup.find(class_=_DISAMBIG_CLASSES))


def _is_stub(soup: BeautifulSoup) -> bool:
    return bool(soup.find(class_=_STUB_CLASSES))


def _text_chars(soup: BeautifulSoup) -> int:
    """Return the plain-text character count of the article body."""
    body = soup.find(id=_BODY_ID)
    if body is None:
        body = soup.find("body") or soup
    return len(body.get_text())


def _extract_categories(soup: BeautifulSoup) -> list[str]:
    """
    Return a list of lowercase category name strings from the catlinks box.

    e.g. ["living people", "people from london", "1990 births"]
    """
    catlinks = soup.find(id=_CATLINKS_ID)
    if catlinks is None:
        return []
    return [
        a.get_text().strip().lower()
        for a in catlinks.find_all("a")
        if a.get_text().strip()
    ]


def _extract_outbound_links(soup: BeautifulSoup) -> list[str]:
    """
    Return normalised internal link paths from the article body.

    - Only href values that look like local paths are kept.
    - Fragment identifiers (#...) are stripped.
    - URL percent-encoding is decoded.
    - The result is deduplicated while preserving first-seen order.
    """
    body = soup.find(id=_BODY_ID)
    if body is None:
        return []

    seen: set[str] = set()
    results: list[str] = []

    for a in body.find_all("a", href=True):
        href: str = a["href"].strip()

        # Skip external links, mailto, etc.
        if href.startswith(("http://", "https://", "//", "mailto:", "#")):
            continue

        # Strip fragment
        if "#" in href:
            href = href.split("#")[0]

        if not href:
            continue

        # Decode percent-encoding
        try:
            href = unquote(href, errors="strict")
        except UnicodeDecodeError:
            continue

        # Strip leading "./" or "/"
        href = href.lstrip("./")

        # Skip obviously non-article paths
        if not href:
            continue

        # Skip category / Wikipedia namespace links
        if any(href.startswith(p) for p in (
            "Category:", "Wikipedia:", "Help:", "File:", "Portal:",
            "Template:", "Special:", "Talk:", "User:", "Draft:",
        )):
            continue

        if href not in seen:
            seen.add(href)
            results.append(href)

    return results


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _clear_tables(conn: sqlite3.Connection) -> None:
    """Drop and recreate the tables populated by this stage."""
    conn.executescript("""
        DELETE FROM article_categories;
        DELETE FROM link_edges;
        DELETE FROM links;
        DELETE FROM articles;
    """)
    conn.commit()


# How many articles to batch-insert before committing.
_INSERT_BATCH = 2_000


# ---------------------------------------------------------------------------
# Main stage entry point
# ---------------------------------------------------------------------------

def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """
    Execute Stage 0: populate the SQLite database from the ZIM file.
    """
    logger.info("Stage 0: verifying ZIM integrity …")
    verify_zim(cfg.input.zim_path, cfg.input.zim_sha256)

    logger.info("Stage 0: clearing existing index tables …")
    _clear_tables(conn)

    reader = ZimReader(cfg.input.zim_path)
    logger.info(
        "Stage 0: ZIM has %d total entries (new_ns=%s)",
        reader.entry_count,
        reader.has_new_namespace_scheme,
    )

    # ------------------------------------------------------------------
    # Pass 1: stream ZIM entries → insert articles, links, categories
    # ------------------------------------------------------------------
    article_buf: list[tuple] = []
    link_buf: list[tuple] = []
    cat_buf: list[tuple] = []

    # We need the auto-assigned article.id after INSERT.
    # Strategy: INSERT articles in batches, then retrieve ids by path.
    # To avoid a second lookup per article, we insert articles first,
    # then links/categories (keyed on the article's rowid).

    # Because SQLite assigns rowids sequentially and we INSERT in order,
    # we track the next expected rowid via lastrowid.  For safety we
    # always look up id via path when building the link/cat buffers.

    path_to_id: dict[str, int] = {}

    article_count = 0

    def _flush_articles() -> None:
        nonlocal article_buf
        if not article_buf:
            return
        conn.executemany(
            """
            INSERT OR IGNORE INTO articles
              (path, title, is_redirect, is_disambig, is_stub,
               html_size, text_chars)
            VALUES (?, ?, 0, ?, ?, ?, ?)
            """,
            article_buf,
        )
        conn.commit()
        article_buf = []

    def _flush_links() -> None:
        nonlocal link_buf
        if not link_buf:
            return
        conn.executemany(
            "INSERT INTO links (source_id, target_path) VALUES (?, ?)",
            link_buf,
        )
        conn.commit()
        link_buf = []

    def _flush_cats() -> None:
        nonlocal cat_buf
        if not cat_buf:
            return
        conn.executemany(
            "INSERT INTO article_categories (article_id, category) VALUES (?, ?)",
            cat_buf,
        )
        conn.commit()
        cat_buf = []

    logger.info("Stage 0: streaming ZIM articles …")

    with tqdm(total=reader.entry_count, unit="entry", desc="s0 index") as pbar:
        for article in reader.iter_main_articles(
            progress_callback=lambda i, _t: pbar.update(1) if i % 100 == 0 else None
        ):
            soup = _parse_html(article.html)
            is_disambig = int(_is_disambiguation(soup))
            is_stub = int(_is_stub(soup))
            text_chars = _text_chars(soup)
            categories = _extract_categories(soup)
            outbound = _extract_outbound_links(soup)

            article_buf.append((
                article.path,
                article.title,
                is_disambig,
                is_stub,
                article.html_size,
                text_chars,
            ))
            article_count += 1

            if len(article_buf) >= _INSERT_BATCH:
                _flush_articles()
                # Build path_to_id for the batch just inserted.
                _sync_path_ids(conn, path_to_id, [row[0] for row in article_buf])
                # NOTE: article_buf was just cleared; iterate the DB for the
                # paths we care about.  We loaded them before the clear so
                # we use a small second lookup.

            # We'll build link/cat after the full article pass so we have all IDs.
            # Temporarily stash raw data per-path; resolve ids in Phase 2.
            # (Storing HTML would be too memory-heavy; re-parse is avoided by
            #  storing minimal extracted data alongside the path.)

            # Store links/cats pending id resolution – use a temp structure.
            # Appended to the main buffers in Phase 2 below.
            _raw_links_buffer.setdefault(article.path, []).extend(outbound)
            _raw_cats_buffer.setdefault(article.path, []).extend(categories)

    # Flush remaining articles.
    _flush_articles()

    # ------------------------------------------------------------------
    # Build the complete path → id map after all articles are inserted.
    # ------------------------------------------------------------------
    logger.info("Stage 0: building path→id map …")
    path_to_id.clear()
    for row in conn.execute("SELECT id, path FROM articles"):
        path_to_id[row["path"]] = row["id"]

    logger.info("Stage 0: %d articles indexed; inserting links + categories …", len(path_to_id))

    # ------------------------------------------------------------------
    # Pass 2: insert links and categories now that all IDs are known.
    # ------------------------------------------------------------------
    for path, links in tqdm(_raw_links_buffer.items(), desc="s0 links", unit="art"):
        art_id = path_to_id.get(path)
        if art_id is None:
            continue
        for target in links:
            link_buf.append((art_id, target))
        if len(link_buf) >= _INSERT_BATCH * 10:
            _flush_links()

    _flush_links()
    _raw_links_buffer.clear()

    for path, cats in tqdm(_raw_cats_buffer.items(), desc="s0 cats", unit="art"):
        art_id = path_to_id.get(path)
        if art_id is None:
            continue
        for cat in cats:
            cat_buf.append((art_id, cat))
        if len(cat_buf) >= _INSERT_BATCH * 5:
            _flush_cats()

    _flush_cats()
    _raw_cats_buffer.clear()

    # ------------------------------------------------------------------
    # Pass 3: resolve link_edges (links where target exists in articles).
    # ------------------------------------------------------------------
    logger.info("Stage 0: resolving link edges …")
    conn.execute("""
        INSERT OR IGNORE INTO link_edges (source_id, target_id)
        SELECT l.source_id, a.id
        FROM links l
        JOIN articles a ON a.path = l.target_path
    """)
    conn.commit()

    # ------------------------------------------------------------------
    # Pass 4: update inbound_count and outbound_count on articles.
    # ------------------------------------------------------------------
    logger.info("Stage 0: updating link counts …")
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

    final_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    logger.info("Stage 0: complete.  %d articles in index.", final_count)

    record_stage_complete(conn, "s0", article_count=final_count, notes="ZIM pre-pass")


# ---------------------------------------------------------------------------
# Module-level temporary buffers (cleared during run())
# These avoid re-parsing HTML; they are in-memory for the duration of stage 0.
# ---------------------------------------------------------------------------
_raw_links_buffer: dict[str, list[str]] = {}
_raw_cats_buffer: dict[str, list[str]] = {}


def _sync_path_ids(
    conn: sqlite3.Connection,
    path_to_id: dict[str, int],
    paths: list[str],
) -> None:
    """Update path_to_id for the given list of paths from the DB."""
    if not paths:
        return
    placeholders = ",".join("?" * len(paths))
    for row in conn.execute(
        f"SELECT id, path FROM articles WHERE path IN ({placeholders})", paths
    ):
        path_to_id[row["path"]] = row["id"]


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
