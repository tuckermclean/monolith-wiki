"""tests/test_prefilter.py — unit tests for Stage 1 (Hard Pre-Filter)."""

from __future__ import annotations

import pytest
from tests.conftest import insert_article
from pipeline.stages.s1_prefilter import run


def _pass_count(conn):
    return conn.execute("SELECT COUNT(*) FROM articles WHERE stage1_pass = 1").fetchone()[0]


def _fail_reason(conn, path):
    row = conn.execute(
        "SELECT stage1_fail_reason FROM articles WHERE path = ?", (path,)
    ).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Basic pass / fail
# ---------------------------------------------------------------------------

def test_all_pass(db, cfg):
    insert_article(db, path="alpha", text_chars=10_000, inbound_count=10)
    insert_article(db, path="beta",  text_chars=20_000, inbound_count=20)
    run(cfg, db)
    assert _pass_count(db) == 2


def test_redirect_excluded(db, cfg):
    insert_article(db, path="redir", is_redirect=1, text_chars=10_000, inbound_count=10)
    run(cfg, db)
    assert _pass_count(db) == 0
    assert _fail_reason(db, "redir") == "redirect"


def test_disambig_excluded(db, cfg):
    insert_article(db, path="dis", is_disambig=1, text_chars=10_000, inbound_count=10)
    run(cfg, db)
    assert _pass_count(db) == 0
    assert _fail_reason(db, "dis") == "disambiguation"


def test_stub_excluded(db, cfg):
    insert_article(db, path="stb", is_stub=1, text_chars=10_000, inbound_count=10)
    run(cfg, db)
    assert _pass_count(db) == 0
    assert _fail_reason(db, "stb") == "stub"


def test_short_article_excluded(db, cfg):
    cfg.prefilter.min_text_chars = 5_000
    insert_article(db, path="short", text_chars=100, inbound_count=10)
    run(cfg, db)
    assert _pass_count(db) == 0
    assert _fail_reason(db, "short") == f"text_chars<{cfg.prefilter.min_text_chars}"


def test_low_inbound_excluded(db, cfg):
    cfg.prefilter.min_inbound_links = 5
    insert_article(db, path="orphan", text_chars=10_000, inbound_count=2)
    run(cfg, db)
    assert _pass_count(db) == 0
    assert _fail_reason(db, "orphan") == f"inbound_links<{cfg.prefilter.min_inbound_links}"


# ---------------------------------------------------------------------------
# Rule priority (first matching rule wins for the reason string)
# ---------------------------------------------------------------------------

def test_redirect_beats_disambig(db, cfg):
    """A redirect that is also a disambig should report 'redirect'."""
    insert_article(db, path="rd", is_redirect=1, is_disambig=1,
                   text_chars=10_000, inbound_count=10)
    run(cfg, db)
    assert _fail_reason(db, "rd") == "redirect"


def test_disambig_beats_stub(db, cfg):
    insert_article(db, path="ds", is_disambig=1, is_stub=1,
                   text_chars=10_000, inbound_count=10)
    run(cfg, db)
    assert _fail_reason(db, "ds") == "disambiguation"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_idempotent(db, cfg):
    insert_article(db, path="x", text_chars=10_000, inbound_count=10)
    insert_article(db, path="y", text_chars=100, inbound_count=10)
    run(cfg, db)
    first_pass = _pass_count(db)
    run(cfg, db)
    second_pass = _pass_count(db)
    assert first_pass == second_pass == 1


# ---------------------------------------------------------------------------
# Mixed batch
# ---------------------------------------------------------------------------

def test_mixed_batch(db, cfg):
    cfg.prefilter.min_text_chars = 5_000
    cfg.prefilter.min_inbound_links = 5
    insert_article(db, path="a", text_chars=10_000, inbound_count=10)   # pass
    insert_article(db, path="b", is_disambig=1,                          # fail
                   text_chars=10_000, inbound_count=10)
    insert_article(db, path="c", text_chars=100, inbound_count=10)        # fail (short)
    insert_article(db, path="d", text_chars=10_000, inbound_count=2)      # fail (orphan)
    insert_article(db, path="e", text_chars=10_000, inbound_count=10)    # pass
    run(cfg, db)
    assert _pass_count(db) == 2
