"""tests/test_kss.py — unit tests for Stage 3 (KSS computation)."""

from __future__ import annotations

import math
import pytest
from tests.conftest import insert_article, insert_edge, insert_category
from pipeline.stages.s3_kss import (
    run,
    _log_norm_signal,
    _length_signal,
    _longevity_signal,
    _recency_signal,
)


# ---------------------------------------------------------------------------
# Pure function tests
# ---------------------------------------------------------------------------

def test_log_norm_zero():
    assert _log_norm_signal(0, 100) == 0.0


def test_log_norm_max():
    x = 100
    assert _log_norm_signal(x, x) == pytest.approx(1.0)


def test_log_norm_monotone():
    values = [_log_norm_signal(k, 1000) for k in [0, 1, 10, 100, 500, 1000]]
    assert values == sorted(values)


def test_length_below_floor():
    assert _length_signal(1_000, floor=5_000, soft_cap=100_000) == 0.0


def test_length_above_cap():
    assert _length_signal(200_000, floor=5_000, soft_cap=100_000) == 1.0


def test_length_midpoint():
    mid = (5_000 + 100_000) // 2
    score = _length_signal(mid, floor=5_000, soft_cap=100_000)
    assert 0.4 < score < 0.6


def test_longevity_empty():
    assert _longevity_signal([]) == 0.0


def test_longevity_all_match():
    score = _longevity_signal(["ancient rome", "medieval wars"])
    assert score > 0.0


def test_recency_empty():
    assert _recency_signal([]) == 0.0


def test_recency_all_match():
    score = _recency_signal(["living people", "2020s", "21st century"])
    assert score > 0.0


# ---------------------------------------------------------------------------
# Integration tests with DB
# ---------------------------------------------------------------------------

def _setup_two_domain_articles(db):
    """Create two articles in different domains with a link between them."""
    a1 = insert_article(db, path="geo1", stage1_pass=1, domain="Geography",
                        text_chars=15_000, inbound_count=50)
    a2 = insert_article(db, path="bio1", stage1_pass=1, domain="Biography",
                        text_chars=8_000, inbound_count=5)
    insert_edge(db, a1, a2)
    insert_edge(db, a2, a1)
    # Update outbound counts.
    db.execute("UPDATE articles SET outbound_count = 1 WHERE id IN (?, ?)", (a1, a2))
    db.commit()
    return a1, a2


def test_kss_range(db, cfg):
    """All KSS values must be in [0, 1]."""
    _setup_two_domain_articles(db)
    run(cfg, db)
    rows = db.execute("SELECT kss FROM articles WHERE kss IS NOT NULL").fetchall()
    assert rows, "No KSS values computed"
    for row in rows:
        assert 0.0 <= row["kss"] <= 1.0, f"KSS out of range: {row['kss']}"


def test_kss_high_inbound_scores_higher(db, cfg):
    """Article with more inbound links should score higher (all else equal)."""
    a_hi = insert_article(db, path="hi", stage1_pass=1, domain="Geography",
                          text_chars=10_000, inbound_count=500)
    a_lo = insert_article(db, path="lo", stage1_pass=1, domain="Geography",
                          text_chars=10_000, inbound_count=1)
    run(cfg, db)
    kss_hi = db.execute("SELECT kss FROM articles WHERE path='hi'").fetchone()["kss"]
    kss_lo = db.execute("SELECT kss FROM articles WHERE path='lo'").fetchone()["kss"]
    assert kss_hi > kss_lo


def test_kss_longevity_improves_score(db, cfg):
    """An article with longevity categories should outscore an otherwise identical one."""
    a_lon = insert_article(db, path="lon", stage1_pass=1, domain="History",
                           text_chars=10_000, inbound_count=10)
    a_mod = insert_article(db, path="mod", stage1_pass=1, domain="History",
                           text_chars=10_000, inbound_count=10)
    insert_category(db, a_lon, "ancient rome")
    insert_category(db, a_lon, "medieval warfare")
    run(cfg, db)
    kss_lon = db.execute("SELECT kss FROM articles WHERE path='lon'").fetchone()["kss"]
    kss_mod = db.execute("SELECT kss FROM articles WHERE path='mod'").fetchone()["kss"]
    assert kss_lon > kss_mod


def test_kss_recency_lowers_score(db, cfg):
    """An article with recency categories should score lower."""
    a_anc = insert_article(db, path="anc", stage1_pass=1, domain="History",
                           text_chars=10_000, inbound_count=10)
    a_rec = insert_article(db, path="rec", stage1_pass=1, domain="History",
                           text_chars=10_000, inbound_count=10)
    insert_category(db, a_rec, "2010s")
    insert_category(db, a_rec, "2020s")
    run(cfg, db)
    kss_anc = db.execute("SELECT kss FROM articles WHERE path='anc'").fetchone()["kss"]
    kss_rec = db.execute("SELECT kss FROM articles WHERE path='rec'").fetchone()["kss"]
    assert kss_anc > kss_rec


def test_kss_deterministic(db, cfg):
    """Running Stage 3 twice on the same data must produce identical scores."""
    _setup_two_domain_articles(db)
    run(cfg, db)
    first = {
        row["path"]: row["kss"]
        for row in db.execute("SELECT path, kss FROM articles WHERE kss IS NOT NULL")
    }
    run(cfg, db)
    second = {
        row["path"]: row["kss"]
        for row in db.execute("SELECT path, kss FROM articles WHERE kss IS NOT NULL")
    }
    assert first == second


def test_kss_skips_failed_articles(db, cfg):
    insert_article(db, path="fail", stage1_pass=0, domain="Geography",
                   text_chars=10_000, inbound_count=10)
    run(cfg, db)
    row = db.execute("SELECT kss FROM articles WHERE path='fail'").fetchone()
    assert row["kss"] is None
