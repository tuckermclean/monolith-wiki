"""tests/test_domain.py — unit tests for Stage 2 (Domain Classification)."""

from __future__ import annotations

import pytest
from tests.conftest import insert_article, insert_category
from pipeline.stages.s2_domain import run, _score_categories, _pick_domain, DOMAINS, _load_anchors


# ---------------------------------------------------------------------------
# Unit tests for pure scoring functions (no DB needed)
# ---------------------------------------------------------------------------

@pytest.fixture()
def anchors():
    return _load_anchors()


def test_geography_wins(anchors):
    cats = ["countries of europe", "cities by country", "rivers of france"]
    scores = _score_categories(cats, anchors)
    assert _pick_domain(scores) == "Geography"


def test_biography_wins(anchors):
    cats = ["living people", "people from london", "1990 births"]
    scores = _score_categories(cats, anchors)
    assert _pick_domain(scores) == "Biography"


def test_science_wins(anchors):
    cats = ["quantum mechanics", "physics theorems", "subatomic particles"]
    scores = _score_categories(cats, anchors)
    assert _pick_domain(scores) == "Science_Math"


def test_history_wins(anchors):
    cats = ["ancient rome", "medieval battles", "roman empires"]
    scores = _score_categories(cats, anchors)
    assert _pick_domain(scores) == "History"


def test_arts_wins(anchors):
    cats = ["films directed by", "english-language novels", "albums by"]
    scores = _score_categories(cats, anchors)
    assert _pick_domain(scores) == "Arts_Culture"


def test_zero_scores_deterministic(anchors):
    """When all scores are 0 the tiebreak must be alphabetical → 'Arts_Culture'."""
    cats = []
    scores = _score_categories(cats, anchors)
    domain = _pick_domain(scores)
    assert domain == DOMAINS[0]  # alphabetically first


def test_tiebreak_is_alphabetical(anchors):
    """Explicit tie: inject equal scores; alphabetically earliest domain wins."""
    # Force all domains to score 1.
    scores = {d: 1 for d in DOMAINS}
    domain = _pick_domain(scores)
    assert domain == sorted(DOMAINS)[0]


# ---------------------------------------------------------------------------
# Integration tests with DB
# ---------------------------------------------------------------------------

def test_assigns_domain_to_all_passing(db, cfg):
    a1 = insert_article(db, path="a1", stage1_pass=1)
    a2 = insert_article(db, path="a2", stage1_pass=1)
    insert_category(db, a1, "countries of europe")
    insert_category(db, a2, "living people")
    run(cfg, db)
    row1 = db.execute("SELECT domain FROM articles WHERE path='a1'").fetchone()
    row2 = db.execute("SELECT domain FROM articles WHERE path='a2'").fetchone()
    assert row1["domain"] is not None
    assert row2["domain"] is not None


def test_no_misc_domain(db, cfg):
    """Every article must get a domain from the allowed set."""
    for i in range(20):
        insert_article(db, path=f"art_{i}", stage1_pass=1)
    run(cfg, db)
    rows = db.execute(
        "SELECT DISTINCT domain FROM articles WHERE stage1_pass = 1"
    ).fetchall()
    assigned = {r["domain"] for r in rows}
    assert assigned.issubset(set(DOMAINS)), f"Unknown domain(s): {assigned - set(DOMAINS)}"


def test_skips_failed_articles(db, cfg):
    insert_article(db, path="fail_art", stage1_pass=0)
    run(cfg, db)
    row = db.execute("SELECT domain FROM articles WHERE path='fail_art'").fetchone()
    # Domain should remain NULL for failed articles.
    assert row["domain"] is None


def test_deterministic_on_rerun(db, cfg):
    a = insert_article(db, path="det", stage1_pass=1)
    insert_category(db, a, "quantum physics")
    run(cfg, db)
    first = db.execute("SELECT domain FROM articles WHERE path='det'").fetchone()["domain"]
    run(cfg, db)
    second = db.execute("SELECT domain FROM articles WHERE path='det'").fetchone()["domain"]
    assert first == second
