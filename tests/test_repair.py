"""tests/test_repair.py — unit tests for Stage 5 (Adjacency Repair)."""

from __future__ import annotations

import pytest
from tests.conftest import insert_article, insert_edge
from pipeline.stages.s5_repair import run


def _selected_paths(conn):
    return {r["path"] for r in conn.execute("SELECT path FROM articles WHERE selected=1")}


def _is_selected(conn, path):
    return conn.execute(
        "SELECT selected FROM articles WHERE path=?", (path,)
    ).fetchone()["selected"] == 1


def _is_anchor(conn, path):
    return conn.execute(
        "SELECT repair_anchor FROM articles WHERE path=?", (path,)
    ).fetchone()["repair_anchor"] == 1


# ---------------------------------------------------------------------------
# Pull-in behaviour
# ---------------------------------------------------------------------------

def test_pulls_in_missing_high_kss_link(db, cfg):
    """
    If a selected article links to an unselected article with KSS >= min_pull_kss,
    that article should be pulled into the corpus.
    """
    cfg.repair.min_pull_kss = 0.40
    cfg.quotas.Geography = 10  # plenty of headroom
    cfg.quotas.Biography = 10

    sel = insert_article(db, path="geo_sel", stage1_pass=1, domain="Geography",
                         kss=0.8, selected=1)
    missing = insert_article(db, path="bio_miss", stage1_pass=1, domain="Biography",
                             kss=0.7, selected=0)
    insert_edge(db, sel, missing)

    run(cfg, db)

    assert _is_selected(db, "bio_miss")
    assert _is_anchor(db, "bio_miss")


def test_does_not_pull_low_kss_link(db, cfg):
    cfg.repair.min_pull_kss = 0.60
    cfg.quotas.Biography = 10

    sel = insert_article(db, path="sel", stage1_pass=1, domain="Geography",
                         kss=0.9, selected=1)
    low = insert_article(db, path="low", stage1_pass=1, domain="Biography",
                         kss=0.30, selected=0)
    insert_edge(db, sel, low)

    run(cfg, db)

    assert not _is_selected(db, "low")


def test_does_not_pull_failed_stage1(db, cfg):
    cfg.repair.min_pull_kss = 0.30
    cfg.quotas.Biography = 10

    sel = insert_article(db, path="sel2", stage1_pass=1, domain="Geography",
                         kss=0.9, selected=1)
    bad = insert_article(db, path="s1fail", stage1_pass=0, domain="Biography",
                         kss=0.8, selected=0)
    insert_edge(db, sel, bad)

    run(cfg, db)
    assert not _is_selected(db, "s1fail")


# ---------------------------------------------------------------------------
# Eviction on quota overflow
# ---------------------------------------------------------------------------

def test_evicts_lowest_kss_on_overflow(db, cfg):
    """
    When a domain is at capacity and a repair-pull would overflow,
    the lowest-KSS non-anchor article is evicted.
    """
    cfg.repair.min_pull_kss = 0.40
    cfg.repair.max_overage_fraction = 0.0  # zero overage tolerance
    cfg.quotas.Biography = 2

    # Two selected Biography articles with KSS 0.5 and 0.3.
    bio_hi = insert_article(db, path="bio_hi", stage1_pass=1, domain="Biography",
                             kss=0.5, selected=1)
    bio_lo = insert_article(db, path="bio_lo", stage1_pass=1, domain="Biography",
                             kss=0.3, selected=1)
    # A selected Geography article that links to an unselected Biography article.
    geo = insert_article(db, path="geo_ptr", stage1_pass=1, domain="Geography",
                         kss=0.9, selected=1)
    bio_new = insert_article(db, path="bio_new", stage1_pass=1, domain="Biography",
                              kss=0.7, selected=0)
    insert_edge(db, geo, bio_new)

    run(cfg, db)

    # bio_new should be pulled in (KSS 0.7 > threshold).
    assert _is_selected(db, "bio_new")
    # bio_lo (KSS 0.3) should be evicted to stay within quota.
    assert not _is_selected(db, "bio_lo")
    # bio_hi (higher KSS 0.5) should survive.
    assert _is_selected(db, "bio_hi")


# ---------------------------------------------------------------------------
# Convergence
# ---------------------------------------------------------------------------

def test_converges_no_changes(db, cfg):
    """A fully-connected corpus should converge immediately with no changes."""
    cfg.quotas.Geography = 10
    a = insert_article(db, path="a", stage1_pass=1, domain="Geography",
                       kss=0.9, selected=1)
    b = insert_article(db, path="b", stage1_pass=1, domain="Geography",
                       kss=0.8, selected=1)
    insert_edge(db, a, b)
    insert_edge(db, b, a)

    run(cfg, db)

    # Nothing should have changed.
    assert _is_selected(db, "a")
    assert _is_selected(db, "b")


def test_max_iterations_respected(db, cfg):
    """Even with max_iterations=1 the stage should not raise."""
    cfg.repair.max_iterations = 1
    cfg.quotas.Geography = 5

    sel = insert_article(db, path="s_art", stage1_pass=1, domain="Geography",
                         kss=0.9, selected=1)
    for i in range(5):
        tgt = insert_article(db, path=f"miss_{i}", stage1_pass=1, domain="Geography",
                              kss=0.8, selected=0)
        insert_edge(db, sel, tgt)

    # Should complete without error.
    run(cfg, db)


# ---------------------------------------------------------------------------
# Anchor protection
# ---------------------------------------------------------------------------

def test_anchor_never_evicted(db, cfg):
    """A repair_anchor article must never be evicted even when domain overflows."""
    cfg.repair.min_pull_kss = 0.40
    cfg.repair.max_overage_fraction = 0.0
    cfg.quotas.Biography = 1  # strict quota

    # One selected article already (will be at quota).
    existing = insert_article(db, path="bio_exist", stage1_pass=1, domain="Biography",
                               kss=0.5, selected=1)
    # Unselected article with higher KSS — will be pulled as anchor.
    anchor_target = insert_article(db, path="bio_anchor", stage1_pass=1, domain="Biography",
                                    kss=0.9, selected=0)
    geo = insert_article(db, path="geo", stage1_pass=1, domain="Geography",
                         kss=0.9, selected=1)
    insert_edge(db, geo, anchor_target)

    run(cfg, db)

    # The anchor must be selected.
    assert _is_selected(db, "bio_anchor")
    assert _is_anchor(db, "bio_anchor")
    # The existing article (non-anchor, lower KSS) should be evicted.
    assert not _is_selected(db, "bio_exist")
