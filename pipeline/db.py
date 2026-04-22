"""
pipeline.db — SQLite schema initialisation and helper utilities.

All stages share a single SQLite database.  Concurrent writers are not
supported; the pipeline runs stages serially.  WAL mode is enabled for
better read performance during long writes.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Generator

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------
-- Core article table.  Populated by Stage 0; updated by all later stages.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS articles (
    id                  INTEGER PRIMARY KEY,
    path                TEXT    NOT NULL UNIQUE,   -- ZIM entry path (URL slug)
    title               TEXT    NOT NULL,
    is_redirect         INTEGER NOT NULL DEFAULT 0,
    is_disambig         INTEGER NOT NULL DEFAULT 0,
    is_stub             INTEGER NOT NULL DEFAULT 0,
    html_size           INTEGER NOT NULL DEFAULT 0, -- raw HTML bytes from ZIM
    text_chars          INTEGER NOT NULL DEFAULT 0, -- plain-text char count
    inbound_count       INTEGER NOT NULL DEFAULT 0, -- filled after link resolution
    outbound_count      INTEGER NOT NULL DEFAULT 0, -- filled after link resolution

    -- Stage 1
    stage1_pass         INTEGER,                   -- 1=pass, 0=fail, NULL=not run
    stage1_fail_reason  TEXT,

    -- Stage 2
    domain              TEXT,

    -- Stage 3
    kss                 REAL,

    -- Stage 4 / 5
    selected            INTEGER NOT NULL DEFAULT 0, -- 1 = in final corpus
    repair_anchor       INTEGER NOT NULL DEFAULT 0, -- 1 = pulled in by repair

    -- Stage 6
    output_path         TEXT    -- relative path inside corpus_dir
);

CREATE INDEX IF NOT EXISTS idx_articles_path     ON articles(path);
CREATE INDEX IF NOT EXISTS idx_articles_domain   ON articles(domain);
CREATE INDEX IF NOT EXISTS idx_articles_selected ON articles(selected);
CREATE INDEX IF NOT EXISTS idx_articles_kss      ON articles(kss);

-- -----------------------------------------------------------------------
-- Raw outbound links (source article path → raw href target string).
-- Populated by Stage 0 before path resolution.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS links (
    source_id    INTEGER NOT NULL REFERENCES articles(id),
    target_path  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_id);
CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_path);

-- -----------------------------------------------------------------------
-- Resolved link edges (both endpoints are known article IDs).
-- Populated at the end of Stage 0 by joining links → articles.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_edges (
    source_id INTEGER NOT NULL REFERENCES articles(id),
    target_id INTEGER NOT NULL REFERENCES articles(id),
    PRIMARY KEY (source_id, target_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_source ON link_edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON link_edges(target_id);

-- -----------------------------------------------------------------------
-- Flat article→category membership.  Categories are lowercase strings.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS article_categories (
    article_id INTEGER NOT NULL REFERENCES articles(id),
    category   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cats_article  ON article_categories(article_id);
CREATE INDEX IF NOT EXISTS idx_cats_category ON article_categories(category);

-- -----------------------------------------------------------------------
-- Build-state / sentinel log.  Each completed stage inserts a row.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS build_state (
    stage          TEXT PRIMARY KEY,
    completed_at   TEXT NOT NULL,   -- ISO-8601 timestamp
    article_count  INTEGER,
    notes          TEXT
);
"""


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def open_db(db_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """
    Open (and initialise if new) the pipeline SQLite database.

    Returns a connection with row_factory set to ``sqlite3.Row`` so columns
    can be accessed by name.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    uri = f"file:{db_path.as_posix()}"
    if read_only:
        uri += "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    else:
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        _init_schema(conn)

    conn.row_factory = sqlite3.Row
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)
    conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iter_rows(
    conn: sqlite3.Connection,
    query: str,
    params: tuple = (),
    batch_size: int = 10_000,
) -> Generator[sqlite3.Row, None, None]:
    """Yield rows from *query* in batches to avoid loading all into memory."""
    cursor = conn.execute(query, params)
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield from batch


def record_stage_complete(
    conn: sqlite3.Connection,
    stage: str,
    article_count: int | None = None,
    notes: str | None = None,
) -> None:
    """Insert or replace a build_state row to mark *stage* as done."""
    from datetime import datetime, timezone

    conn.execute(
        """
        INSERT OR REPLACE INTO build_state (stage, completed_at, article_count, notes)
        VALUES (?, ?, ?, ?)
        """,
        (stage, datetime.now(timezone.utc).isoformat(), article_count, notes),
    )
    conn.commit()


def stage_is_complete(conn: sqlite3.Connection, stage: str) -> bool:
    """Return True if *stage* has a build_state row."""
    row = conn.execute(
        "SELECT 1 FROM build_state WHERE stage = ?", (stage,)
    ).fetchone()
    return row is not None
