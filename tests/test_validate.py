"""tests/test_validate.py — unit tests for Stage 8 (Validation) helpers."""

from __future__ import annotations

import json
import random
import pytest

from tests.conftest import insert_article, insert_edge
from pipeline.graph import AdjacencyCache, random_walk_dead_end_rate
from pipeline.hash import compute_build_hash, write_build_hash


# ---------------------------------------------------------------------------
# AdjacencyCache
# ---------------------------------------------------------------------------

def test_adjacency_outbound(db):
    a = insert_article(db, path="a", selected=1)
    b = insert_article(db, path="b", selected=1)
    insert_edge(db, a, b)

    cache = AdjacencyCache(db)
    assert b in cache.outbound(a)
    assert a not in cache.outbound(b)


def test_adjacency_inbound(db):
    a = insert_article(db, path="in_a", selected=1)
    b = insert_article(db, path="in_b", selected=1)
    insert_edge(db, a, b)

    cache = AdjacencyCache(db)
    assert a in cache.inbound(b)


def test_adjacency_excludes_unselected(db):
    a = insert_article(db, path="sel_a", selected=1)
    b = insert_article(db, path="unsel_b", selected=0)
    insert_edge(db, a, b)

    cache = AdjacencyCache(db)
    assert b not in cache.outbound(a)


def test_adjacency_invalidate(db):
    a = insert_article(db, path="inv_a", selected=1)
    b = insert_article(db, path="inv_b", selected=1)
    insert_edge(db, a, b)

    cache = AdjacencyCache(db)
    _ = cache.outbound(a)  # populate

    # Add another article without invalidating.
    c = insert_article(db, path="inv_c", selected=1)
    insert_edge(db, a, c)
    # Still shows old cached result.
    assert c not in cache.outbound(a)

    # After invalidation the new edge is visible.
    cache.invalidate()
    assert c in cache.outbound(a)


# ---------------------------------------------------------------------------
# Random walk
# ---------------------------------------------------------------------------

def test_walk_no_dead_ends_ring(db):
    """A ring graph should have zero dead ends."""
    ids = [insert_article(db, path=f"ring_{i}", selected=1) for i in range(10)]
    for i, aid in enumerate(ids):
        insert_edge(db, aid, ids[(i + 1) % len(ids)])

    cache = AdjacencyCache(db)
    rate = random_walk_dead_end_rate(cache, ids, walk_count=200, walk_steps=50)
    assert rate == 0.0


def test_walk_all_dead_ends_isolated(db):
    """Isolated articles (no outbound) should yield dead-end rate = 1.0."""
    ids = [insert_article(db, path=f"iso_{i}", selected=1) for i in range(5)]
    cache = AdjacencyCache(db)
    rate = random_walk_dead_end_rate(cache, ids, walk_count=200, walk_steps=50)
    assert rate == pytest.approx(1.0)


def test_walk_deterministic(db):
    """Same seed → same result."""
    ids = [insert_article(db, path=f"det_{i}", selected=1) for i in range(5)]
    for i, aid in enumerate(ids):
        insert_edge(db, aid, ids[(i + 1) % len(ids)])

    cache = AdjacencyCache(db)
    r1 = random_walk_dead_end_rate(cache, ids, walk_count=100, walk_steps=20, rng_seed=99)
    r2 = random_walk_dead_end_rate(cache, ids, walk_count=100, walk_steps=20, rng_seed=99)
    assert r1 == r2


def test_walk_empty_ids(db):
    """Empty selected set should return dead-end rate of 1.0."""
    cache = AdjacencyCache(db)
    rate = random_walk_dead_end_rate(cache, [], walk_count=100, walk_steps=20)
    assert rate == 1.0


# ---------------------------------------------------------------------------
# BUILD_HASH
# ---------------------------------------------------------------------------

def test_build_hash_deterministic(tmp_path):
    entries = [
        {"path": "France", "title": "France", "domain": "Geography",
         "kss": 0.95, "output_path": "Geography/France.html.zst", "compressed_bytes": 5000},
        {"path": "Brazil", "title": "Brazil", "domain": "Geography",
         "kss": 0.90, "output_path": "Geography/Brazil.html.zst", "compressed_bytes": 4800},
    ]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(entries))
    h1 = compute_build_hash(manifest)
    h2 = compute_build_hash(manifest)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_build_hash_order_independent(tmp_path):
    """Manifest order should not affect the hash (sorting is internal)."""
    entries_ab = [
        {"path": "A", "kss": 0.9, "domain": "X", "title": "A",
         "output_path": "X/A.html.zst", "compressed_bytes": 100},
        {"path": "B", "kss": 0.8, "domain": "X", "title": "B",
         "output_path": "X/B.html.zst", "compressed_bytes": 100},
    ]
    entries_ba = list(reversed(entries_ab))
    m1 = tmp_path / "m1.json"
    m2 = tmp_path / "m2.json"
    m1.write_text(json.dumps(entries_ab))
    m2.write_text(json.dumps(entries_ba))
    assert compute_build_hash(m1) == compute_build_hash(m2)


def test_build_hash_write(tmp_path):
    entries = [{"path": "X", "kss": 0.5, "domain": "D", "title": "X",
                "output_path": "D/X.html.zst", "compressed_bytes": 500}]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(entries))
    out = tmp_path / "BUILD_HASH"
    digest = write_build_hash(manifest, out)
    assert out.read_text().strip() == digest
    assert len(digest) == 64


def test_build_hash_changes_on_content_change(tmp_path):
    entries = [{"path": "France", "kss": 0.9, "domain": "G", "title": "France",
                "output_path": "G/France.html.zst", "compressed_bytes": 5000}]
    m = tmp_path / "manifest.json"
    m.write_text(json.dumps(entries))
    h1 = compute_build_hash(m)

    entries[0]["kss"] = 0.5
    m.write_text(json.dumps(entries))
    h2 = compute_build_hash(m)
    assert h1 != h2
