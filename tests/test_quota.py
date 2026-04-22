"""tests/test_quota.py — unit tests for Stage 4 (Domain Quota Selection)."""

from __future__ import annotations

import pytest
from tests.conftest import insert_article
from pipeline.stages.s4_quota import run


def _selected_count(conn, domain=None):
    if domain:
        return conn.execute(
            "SELECT COUNT(*) FROM articles WHERE selected=1 AND domain=?", (domain,)
        ).fetchone()[0]
    return conn.execute("SELECT COUNT(*) FROM articles WHERE selected=1").fetchone()[0]


# ---------------------------------------------------------------------------
# Basic selection
# ---------------------------------------------------------------------------

def test_selects_up_to_quota(db, cfg):
    cfg.quotas.Geography = 3
    for i in range(10):
        insert_article(db, path=f"geo_{i}", stage1_pass=1, domain="Geography",
                       kss=i / 10.0)
    run(cfg, db)
    assert _selected_count(db, "Geography") == 3


def test_selects_all_if_fewer_than_quota(db, cfg):
    cfg.quotas.Geography = 100
    for i in range(5):
        insert_article(db, path=f"geo_{i}", stage1_pass=1, domain="Geography",
                       kss=i / 10.0)
    run(cfg, db)
    assert _selected_count(db, "Geography") == 5


def test_top_kss_are_selected(db, cfg):
    cfg.quotas.Geography = 2
    insert_article(db, path="g_lo", stage1_pass=1, domain="Geography", kss=0.1)
    insert_article(db, path="g_mi", stage1_pass=1, domain="Geography", kss=0.5)
    insert_article(db, path="g_hi", stage1_pass=1, domain="Geography", kss=0.9)
    run(cfg, db)
    assert db.execute(
        "SELECT selected FROM articles WHERE path='g_hi'"
    ).fetchone()["selected"] == 1
    assert db.execute(
        "SELECT selected FROM articles WHERE path='g_mi'"
    ).fetchone()["selected"] == 1
    assert db.execute(
        "SELECT selected FROM articles WHERE path='g_lo'"
    ).fetchone()["selected"] == 0


def test_skips_failed_articles(db, cfg):
    cfg.quotas.Geography = 5
    insert_article(db, path="fail", stage1_pass=0, domain="Geography", kss=0.99)
    insert_article(db, path="pass", stage1_pass=1, domain="Geography", kss=0.50)
    run(cfg, db)
    assert db.execute(
        "SELECT selected FROM articles WHERE path='fail'"
    ).fetchone()["selected"] == 0
    assert db.execute(
        "SELECT selected FROM articles WHERE path='pass'"
    ).fetchone()["selected"] == 1


# ---------------------------------------------------------------------------
# Multiple domains
# ---------------------------------------------------------------------------

def test_multi_domain_quotas(db, cfg):
    cfg.quotas.Geography = 3
    cfg.quotas.Biography = 2
    cfg.quotas.History = 0
    for i in range(5):
        insert_article(db, path=f"geo{i}", stage1_pass=1, domain="Geography", kss=i/10)
    for i in range(5):
        insert_article(db, path=f"bio{i}", stage1_pass=1, domain="Biography", kss=i/10)
    for i in range(5):
        insert_article(db, path=f"his{i}", stage1_pass=1, domain="History", kss=i/10)
    run(cfg, db)
    assert _selected_count(db, "Geography") == 3
    assert _selected_count(db, "Biography") == 2
    assert _selected_count(db, "History") == 0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_idempotent(db, cfg):
    cfg.quotas.Geography = 3
    for i in range(5):
        insert_article(db, path=f"g{i}", stage1_pass=1, domain="Geography", kss=i/10)
    run(cfg, db)
    first = _selected_count(db)
    run(cfg, db)
    second = _selected_count(db)
    assert first == second


# ---------------------------------------------------------------------------
# Requires KSS
# ---------------------------------------------------------------------------

def test_articles_without_kss_not_selected(db, cfg):
    cfg.quotas.Geography = 5
    insert_article(db, path="no_kss", stage1_pass=1, domain="Geography", kss=None)
    run(cfg, db)
    assert db.execute(
        "SELECT selected FROM articles WHERE path='no_kss'"
    ).fetchone()["selected"] == 0
