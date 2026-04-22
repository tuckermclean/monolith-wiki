"""
pipeline.stages.s3_kss — Stage 3: Knowledge Stability Score (KSS).

Computes a deterministic scalar KSS ∈ [0, 1] for every Stage-1-passing
article that has been domain-classified.

Formula
-------
KSS = w_inbound  * log_norm(inbound_count)
    + w_cross    * cross_domain_fraction
    + w_length   * length_score
    + w_coverage * category_coverage_score
    + w_longevity * longevity_score
    - w_recency  * recency_penalty

All component signals are normalised to [0, 1] before weighting.
The final KSS is clamped to [0, 1].

Signal definitions
------------------
log_norm(inbound_count)
    log(1 + k) / log(1 + max_k)  where max_k is the max inbound count
    across all passing articles.

cross_domain_fraction
    (outbound links to articles in a different domain) / (total domain-
    resolved outbound links).  Computed in pipeline.graph.

length_score
    linear ramp from length_floor to length_soft_cap, clamped to [0, 1].
    Articles beyond soft_cap score 1.0.

category_coverage_score
    (number of domain-anchor-matching categories) / (max such count across
    all passing articles).  Rewards breadth of topic coverage within domain.

longevity_score
    fraction of an article's categories that match "longevity anchor"
    substrings (e.g. "ancient", "medieval", "classical", "19th century").

recency_penalty
    fraction of categories matching "recency anchor" substrings
    (e.g. "living people", "2010s", "2020s", "21st century events").
"""

from __future__ import annotations

import logging
import math
import sqlite3
from pathlib import Path

import yaml

from pipeline.config import Config, KSSWeights
from pipeline.db import record_stage_complete, iter_rows
from pipeline.graph import cross_domain_fractions

logger = logging.getLogger(__name__)

_DOMAIN_RULES_PATH = Path(__file__).parent.parent.parent / "config" / "domain_rules.yaml"

# ---------------------------------------------------------------------------
# Longevity & recency anchor sets (hard-coded; these are stable signals)
# ---------------------------------------------------------------------------

_LONGEVITY_ANCHORS: list[str] = [
    "ancient", "medieval", "classical", "renaissance", "early modern",
    "late antiquity", "bronze age", "iron age", "neolithic", "paleolithic",
    "prehistoric", "antiquity", "byzantine", "ottoman", "roman", "greek",
    "viking", "feudal", "colonial era", "17th century", "18th century",
    "19th century", "early 20th century",
]

_RECENCY_ANCHORS: list[str] = [
    "living people", "2010s", "2011", "2012", "2013", "2014", "2015",
    "2016", "2017", "2018", "2019", "2020s", "2020", "2021", "2022",
    "2023", "2024", "2025", "2026", "21st century", "21st-century",
    "current",
]


# ---------------------------------------------------------------------------
# Category-coverage helper: count domain-anchor matches per article
# ---------------------------------------------------------------------------

def _build_domain_anchors(rules_path: Path = _DOMAIN_RULES_PATH) -> dict[str, list[str]]:
    with rules_path.open() as fh:
        raw = yaml.safe_load(fh)
    from pipeline.stages.s2_domain import DOMAINS
    return {d: [a.lower() for a in raw.get(d, {}).get("anchors", [])] for d in DOMAINS}


def _count_domain_anchor_matches(categories: list[str], all_anchors: dict[str, list[str]]) -> int:
    """Count total domain-anchor matches across all domains for an article."""
    total = 0
    for cat in categories:
        cat_lower = cat.lower()
        for anchor_list in all_anchors.values():
            for anchor in anchor_list:
                if anchor in cat_lower:
                    total += 1
                    break
    return total


# ---------------------------------------------------------------------------
# Component signal computations
# ---------------------------------------------------------------------------

def _log_norm_signal(inbound: int, max_inbound: int) -> float:
    if max_inbound <= 0:
        return 0.0
    return math.log1p(inbound) / math.log1p(max_inbound)


def _length_signal(text_chars: int, floor: int, soft_cap: int) -> float:
    if text_chars <= floor:
        return 0.0
    if text_chars >= soft_cap:
        return 1.0
    return (text_chars - floor) / (soft_cap - floor)


def _longevity_signal(categories: list[str]) -> float:
    if not categories:
        return 0.0
    matches = sum(
        1 for cat in categories
        if any(anchor in cat.lower() for anchor in _LONGEVITY_ANCHORS)
    )
    return min(1.0, matches / max(1, len(categories)))


def _recency_signal(categories: list[str]) -> float:
    if not categories:
        return 0.0
    matches = sum(
        1 for cat in categories
        if any(anchor in cat.lower() for anchor in _RECENCY_ANCHORS)
    )
    return min(1.0, matches / max(1, len(categories)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Compute and store KSS for all Stage-2-classified articles."""
    w: KSSWeights = cfg.kss.weights
    length_floor = cfg.kss.length_floor
    length_soft_cap = cfg.kss.length_soft_cap

    logger.info("Stage 3: computing KSS …")
    logger.info("  Weights: %s", vars(w))

    # Reset previous KSS values.
    conn.execute("UPDATE articles SET kss = NULL WHERE domain IS NOT NULL")
    conn.commit()

    # ------------------------------------------------------------------
    # Pre-compute global normalisation values
    # ------------------------------------------------------------------
    max_inbound: int = conn.execute(
        "SELECT MAX(inbound_count) FROM articles WHERE stage1_pass = 1"
    ).fetchone()[0] or 1

    # Cross-domain fractions (source_id → fraction).
    logger.info("Stage 3: computing cross-domain fractions …")
    cross_frac: dict[int, float] = cross_domain_fractions(conn)

    # Domain-anchor match counts per article (for category_coverage signal).
    logger.info("Stage 3: loading categories for coverage scoring …")
    all_anchors = _build_domain_anchors()

    art_cats: dict[int, list[str]] = {}
    for row in iter_rows(
        conn,
        """
        SELECT ac.article_id, ac.category
        FROM article_categories ac
        JOIN articles a ON a.id = ac.article_id AND a.stage1_pass = 1
        """,
    ):
        art_cats.setdefault(row["article_id"], []).append(row["category"])

    # First pass: compute raw coverage counts so we can normalise.
    logger.info("Stage 3: computing category coverage …")
    coverage_counts: dict[int, int] = {}
    for art_id, cats in art_cats.items():
        coverage_counts[art_id] = _count_domain_anchor_matches(cats, all_anchors)
    max_coverage = max(coverage_counts.values(), default=1) or 1

    # ------------------------------------------------------------------
    # Main scoring loop
    # ------------------------------------------------------------------
    updates: list[tuple[float, int]] = []  # (kss, article_id)

    def _flush(buf: list[tuple[float, int]]) -> None:
        conn.executemany("UPDATE articles SET kss = ? WHERE id = ?", buf)
        conn.commit()
        buf.clear()

    for row in iter_rows(
        conn,
        "SELECT id, inbound_count, text_chars FROM articles WHERE stage1_pass = 1 AND domain IS NOT NULL ORDER BY id",
    ):
        art_id: int = row["id"]
        cats = art_cats.get(art_id, [])

        s_inbound = _log_norm_signal(row["inbound_count"], max_inbound)
        s_cross = cross_frac.get(art_id, 0.0)
        s_length = _length_signal(row["text_chars"], length_floor, length_soft_cap)
        s_coverage = coverage_counts.get(art_id, 0) / max_coverage
        s_longevity = _longevity_signal(cats)
        s_recency = _recency_signal(cats)

        kss = (
            w.log_inbound * s_inbound
            + w.cross_domain * s_cross
            + w.length_score * s_length
            + w.category_coverage * s_coverage
            + w.longevity * s_longevity
            - w.recency_penalty * s_recency
        )
        # Clamp to [0, 1].
        kss = max(0.0, min(1.0, kss))

        updates.append((kss, art_id))
        if len(updates) >= 10_000:
            _flush(updates)

    _flush(updates)

    scored_count = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE kss IS NOT NULL"
    ).fetchone()[0]
    avg_kss = conn.execute(
        "SELECT AVG(kss) FROM articles WHERE kss IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "Stage 3: complete.  %d articles scored.  Mean KSS=%.4f.",
        scored_count, avg_kss or 0,
    )

    record_stage_complete(
        conn,
        "s3",
        article_count=scored_count,
        notes=f"mean_kss={avg_kss:.4f}" if avg_kss else "mean_kss=0",
    )


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging as _logging

    from pipeline.config import load_config
    from pipeline.db import open_db

    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run Stage 3: KSS computation")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
