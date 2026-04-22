"""
pipeline.graph — in-memory graph utilities for Stages 3 and 5.

Rather than holding the entire link graph in RAM, these helpers issue
targeted SQL queries against the already-built ``link_edges`` table.
A thin adjacency-list cache is materialised on demand and shared within
a stage run.
"""

from __future__ import annotations

import random
import sqlite3
from collections import defaultdict
from typing import Sequence


# ---------------------------------------------------------------------------
# Adjacency-list cache
# ---------------------------------------------------------------------------

class AdjacencyCache:
    """
    Lazily-loaded, read-only adjacency list for selected articles.

    The cache is built from the ``link_edges`` table and restricted to
    articles marked ``selected = 1``.  It is used by Stage 5 (repair) and
    Stage 8 (random-walk validation).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._out: dict[int, list[int]] | None = None  # source → targets
        self._in: dict[int, list[int]] | None = None   # target → sources

    def _ensure_loaded(self) -> None:
        if self._out is not None:
            return

        out: dict[int, list[int]] = defaultdict(list)
        inc: dict[int, list[int]] = defaultdict(list)

        rows = self._conn.execute(
            """
            SELECT le.source_id, le.target_id
            FROM link_edges le
            JOIN articles s ON s.id = le.source_id AND s.selected = 1
            JOIN articles t ON t.id = le.target_id AND t.selected = 1
            """
        )
        for row in rows:
            out[row[0]].append(row[1])
            inc[row[1]].append(row[0])

        self._out = dict(out)
        self._in = dict(inc)

    def outbound(self, article_id: int) -> list[int]:
        self._ensure_loaded()
        return self._out.get(article_id, [])  # type: ignore[return-value]

    def inbound(self, article_id: int) -> list[int]:
        self._ensure_loaded()
        return self._in.get(article_id, [])  # type: ignore[return-value]

    def invalidate(self) -> None:
        """Force a rebuild on next access (call after repair mutations)."""
        self._out = None
        self._in = None


# ---------------------------------------------------------------------------
# Random-walk simulation (Stage 8)
# ---------------------------------------------------------------------------

def random_walk_dead_end_rate(
    cache: AdjacencyCache,
    selected_ids: Sequence[int],
    walk_count: int = 5_000,
    walk_steps: int = 200,
    rng_seed: int = 42,
) -> float:
    """
    Return the fraction of random walks that hit a dead end.

    A dead end is defined as arriving at a node with no outbound links
    to other selected articles.

    Parameters
    ----------
    cache : AdjacencyCache
        Pre-loaded adjacency cache restricted to selected articles.
    selected_ids : sequence of int
        Pool of article IDs to start walks from.
    walk_count : int
        Number of independent walks to simulate.
    walk_steps : int
        Maximum steps per walk.
    rng_seed : int
        Fixed seed for reproducibility.

    Returns
    -------
    float
        Fraction of walks (0.0–1.0) that terminated at a dead end.
    """
    rng = random.Random(rng_seed)
    ids_list = list(selected_ids)
    if not ids_list:
        return 1.0

    dead_ends = 0
    for _ in range(walk_count):
        current = rng.choice(ids_list)
        for _step in range(walk_steps):
            neighbours = cache.outbound(current)
            if not neighbours:
                dead_ends += 1
                break
            current = rng.choice(neighbours)

    return dead_ends / walk_count


# ---------------------------------------------------------------------------
# Cross-domain fraction helper (Stage 3)
# ---------------------------------------------------------------------------

def cross_domain_fractions(
    conn: sqlite3.Connection,
) -> dict[int, float]:
    """
    Return a mapping of article_id → cross-domain fraction for all articles.

    Cross-domain fraction = (outbound links to articles in a DIFFERENT domain)
                            / (total outbound links to articles with a domain)

    Articles with zero outbound links receive a fraction of 0.0.
    """
    rows = conn.execute(
        """
        SELECT
            le.source_id,
            SUM(CASE WHEN s.domain != t.domain THEN 1 ELSE 0 END) AS cross_count,
            COUNT(*) AS total_count
        FROM link_edges le
        JOIN articles s ON s.id = le.source_id
        JOIN articles t ON t.id = le.target_id
        WHERE s.domain IS NOT NULL AND t.domain IS NOT NULL
        GROUP BY le.source_id
        """
    )
    result: dict[int, float] = {}
    for row in rows:
        total = row["total_count"]
        result[row["source_id"]] = row["cross_count"] / total if total > 0 else 0.0
    return result
