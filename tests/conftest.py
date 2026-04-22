"""
tests/conftest.py — shared fixtures and helpers for the test suite.

No ZIM file is required; all tests work against in-memory SQLite databases
populated with synthetic article data.
"""

from __future__ import annotations

import sqlite3
import pytest

from pipeline.config import Config
from pipeline.db import open_db, _init_schema


# ---------------------------------------------------------------------------
# In-memory DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def db():
    """Provide a fresh in-memory SQLite connection with the pipeline schema."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _init_schema(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Default config fixture (no ZIM hash check, temp paths)
# ---------------------------------------------------------------------------

@pytest.fixture()
def cfg(tmp_path):
    """Provide a Config instance pointing to a temp build dir."""
    c = Config()
    c.build.build_dir = str(tmp_path / "build")
    c.input.zim_sha256 = None  # skip hash verification in tests
    return c


# ---------------------------------------------------------------------------
# Helpers for populating test data
# ---------------------------------------------------------------------------

def insert_article(
    conn: sqlite3.Connection,
    *,
    path: str,
    title: str = "",
    is_redirect: int = 0,
    is_disambig: int = 0,
    is_stub: int = 0,
    html_size: int = 10_000,
    text_chars: int = 10_000,
    inbound_count: int = 10,
    outbound_count: int = 5,
    stage1_pass: int | None = None,
    domain: str | None = None,
    kss: float | None = None,
    selected: int = 0,
) -> int:
    """Insert an article row and return its rowid."""
    conn.execute(
        """
        INSERT INTO articles
          (path, title, is_redirect, is_disambig, is_stub,
           html_size, text_chars, inbound_count, outbound_count,
           stage1_pass, domain, kss, selected)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            path, title or path, is_redirect, is_disambig, is_stub,
            html_size, text_chars, inbound_count, outbound_count,
            stage1_pass, domain, kss, selected,
        ),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_edge(conn: sqlite3.Connection, source_id: int, target_id: int) -> None:
    """Insert a link_edge row."""
    conn.execute(
        "INSERT OR IGNORE INTO link_edges (source_id, target_id) VALUES (?, ?)",
        (source_id, target_id),
    )
    conn.commit()


def insert_category(conn: sqlite3.Connection, article_id: int, category: str) -> None:
    conn.execute(
        "INSERT INTO article_categories (article_id, category) VALUES (?, ?)",
        (article_id, category),
    )
    conn.commit()
