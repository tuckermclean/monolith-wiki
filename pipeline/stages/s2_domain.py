"""
pipeline.stages.s2_domain — Stage 2: Domain Classification.

Assigns exactly one primary domain to every Stage-1-passing article.

Algorithm
---------
1. Load domain anchor patterns from ``config/domain_rules.yaml``.
2. For each passing article, retrieve all its category strings.
3. Count how many categories match each domain's anchor list
   (substring match, case-insensitive).
4. Assign the domain with the highest match count.
5. Tiebreak: alphabetical domain name (deterministic).
6. Articles with zero total matches are assigned to the alphabetically
   first domain whose anchor list produces a non-zero partial match on
   the article *title* — if still no match, they receive the domain
   with the globally highest prior article count (balances distribution).

Every passing article receives exactly one domain.  No "Misc" catch-all.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import yaml

from pipeline.config import Config
from pipeline.db import record_stage_complete, iter_rows

logger = logging.getLogger(__name__)

_DOMAIN_RULES_PATH = Path(__file__).parent.parent.parent / "config" / "domain_rules.yaml"

# Ordered list of all valid domains (order used as fallback tiebreak priority).
DOMAINS: list[str] = [
    "Arts_Culture",
    "Biography",
    "Geography",
    "History",
    "Meta_Reference",
    "Science_Math",
    "Society_Politics_Economics",
    "Technology_Computing",
]


# ---------------------------------------------------------------------------
# Anchor loading
# ---------------------------------------------------------------------------

def _load_anchors(rules_path: Path = _DOMAIN_RULES_PATH) -> dict[str, list[str]]:
    """Load domain anchor patterns from YAML; return {domain: [anchors]}."""
    with rules_path.open() as fh:
        raw = yaml.safe_load(fh)

    result: dict[str, list[str]] = {}
    for domain in DOMAINS:
        anchors = raw.get(domain, {}).get("anchors", [])
        # Normalise to lowercase for case-insensitive matching.
        result[domain] = [a.lower() for a in anchors]
    return result


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_categories(
    categories: list[str],
    anchors: dict[str, list[str]],
) -> dict[str, int]:
    """Return a {domain: match_count} dict for a list of categories."""
    scores: dict[str, int] = {d: 0 for d in DOMAINS}
    for cat in categories:
        cat_lower = cat.lower()
        for domain, anchor_list in anchors.items():
            for anchor in anchor_list:
                if anchor in cat_lower:
                    scores[domain] += 1
                    break  # count each category once per domain
    return scores


def _score_title(title: str, anchors: dict[str, list[str]]) -> dict[str, int]:
    """Fallback: score against the article title."""
    return _score_categories([title.lower()], anchors)


def _pick_domain(scores: dict[str, int]) -> str:
    """
    Return the domain with the highest score.
    Tiebreak: alphabetical domain name.
    """
    return max(DOMAINS, key=lambda d: (scores[d], -DOMAINS.index(d)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 2: assign domains to all Stage-1-passing articles."""
    logger.info("Stage 2: loading domain rules from %s …", _DOMAIN_RULES_PATH)
    anchors = _load_anchors()

    # Reset previous run.
    conn.execute("UPDATE articles SET domain = NULL WHERE stage1_pass = 1")
    conn.commit()

    # Build a mapping of article_id → [categories] from the DB.
    logger.info("Stage 2: loading article categories …")
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

    # Track per-domain counts for the zero-match fallback.
    domain_counts: dict[str, int] = {d: 0 for d in DOMAINS}

    # Collect updates in a buffer; flush in batches.
    updates: list[tuple[str, int]] = []  # (domain, article_id)

    def _flush(buf: list[tuple[str, int]]) -> None:
        conn.executemany(
            "UPDATE articles SET domain = ? WHERE id = ?", buf
        )
        conn.commit()
        buf.clear()

    logger.info("Stage 2: classifying articles …")
    passing_rows = list(
        iter_rows(
            conn,
            "SELECT id, title FROM articles WHERE stage1_pass = 1 ORDER BY id",
        )
    )

    zero_match_count = 0

    for row in passing_rows:
        art_id: int = row["id"]
        title: str = row["title"]
        categories = art_cats.get(art_id, [])

        scores = _score_categories(categories, anchors)
        best_score = max(scores.values())

        if best_score == 0:
            # Fallback 1: score against title.
            scores = _score_title(title, anchors)
            best_score = max(scores.values())

        if best_score == 0:
            # Fallback 2: assign the domain with the lowest current count
            # to keep distribution balanced.
            domain = min(DOMAINS, key=lambda d: (domain_counts[d], d))
            zero_match_count += 1
        else:
            domain = _pick_domain(scores)

        domain_counts[domain] += 1
        updates.append((domain, art_id))

        if len(updates) >= 5_000:
            _flush(updates)

    _flush(updates)

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    total_pass = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE stage1_pass = 1"
    ).fetchone()[0]

    logger.info("Stage 2: domain distribution:")
    for row in conn.execute(
        """
        SELECT domain, COUNT(*) AS n FROM articles
        WHERE domain IS NOT NULL
        GROUP BY domain ORDER BY n DESC
        """
    ):
        logger.info("  %-35s %d", row["domain"], row["n"])

    logger.info(
        "Stage 2: complete.  %d articles classified.  %d assigned via zero-match fallback.",
        total_pass,
        zero_match_count,
    )

    record_stage_complete(
        conn,
        "s2",
        article_count=total_pass,
        notes=f"zero_match_fallback={zero_match_count}",
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
    parser = argparse.ArgumentParser(description="Run Stage 2: Domain classification")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
