"""
pipeline.stages.s5_repair — Stage 5: Adjacency Repair (Graph Coherence).

Iteratively repairs the link graph so that the selected corpus has no
isolated articles or dead-end navigation clusters.

Algorithm (per iteration)
--------------------------
1. For each selected article, fetch its top-N outbound links ordered by
   the target's KSS (highest first).
2. For each of those links that points to a non-selected article:
   a. If the target's KSS >= min_pull_kss and it passed Stage 1 and has
      a domain assigned: pull it into the selected set.  Mark it as a
      repair anchor (so it is never evicted later).
   b. If the domain's selected count now exceeds quota * (1 + overage):
      evict the lowest-KSS non-anchor article in that domain.
3. Repeat until no changes occur in a full iteration, or max_iterations
   is reached.

This stage is safe to re-run; it resets repair_anchor flags before
starting and works only on the current selected set.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict

from pipeline.config import Config
from pipeline.db import record_stage_complete
from pipeline.stages.s2_domain import DOMAINS

logger = logging.getLogger(__name__)


def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 5: adjacency repair."""
    r = cfg.repair
    quotas = cfg.quotas.as_dict()

    logger.info("Stage 5: adjacency repair …")
    logger.info(
        "  top_n=%d  min_pull_kss=%.2f  max_overage=%.0f%%  max_iter=%d",
        r.top_n_links, r.min_pull_kss, r.max_overage_fraction * 100, r.max_iterations,
    )

    # Reset repair-anchor flags from any previous run.
    conn.execute("UPDATE articles SET repair_anchor = 0")
    conn.commit()

    total_pulled = 0
    total_evicted = 0

    for iteration in range(1, r.max_iterations + 1):
        pulled_this_iter = 0
        evicted_this_iter = 0

        # ------------------------------------------------------------------
        # Find all (source, target) pairs where target is NOT selected.
        # Limit to the top-N outbound links per source by target KSS.
        # ------------------------------------------------------------------
        missing_rows = conn.execute(
            """
            SELECT
                le.source_id,
                le.target_id,
                t.kss         AS target_kss,
                t.domain      AS target_domain,
                t.stage1_pass AS target_s1
            FROM (
                SELECT
                    le2.source_id,
                    le2.target_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY le2.source_id
                        ORDER BY COALESCE(t2.kss, 0) DESC
                    ) AS rn
                FROM link_edges le2
                JOIN articles s2 ON s2.id = le2.source_id AND s2.selected = 1
                JOIN articles t2 ON t2.id = le2.target_id AND t2.selected = 0
            ) le
            JOIN articles t ON t.id = le.target_id
            WHERE le.rn <= ?
            """,
            (r.top_n_links,),
        ).fetchall()

        if not missing_rows:
            logger.info("  iter %d: no missing links — converged.", iteration)
            break

        # Group missing targets by their domain.
        # We collect (target_id, target_kss, target_domain) for all pull candidates.
        pull_candidates: list[tuple[int, float, str]] = []

        for row in missing_rows:
            # Only pull if KSS threshold met, passed Stage 1, and has a domain.
            if (
                row["target_s1"] == 1
                and row["target_kss"] is not None
                and row["target_kss"] >= r.min_pull_kss
                and row["target_domain"] is not None
            ):
                pull_candidates.append(
                    (row["target_id"], row["target_kss"], row["target_domain"])
                )

        # Deduplicate candidates.
        seen_ids: set[int] = set()
        unique_candidates: list[tuple[int, float, str]] = []
        for cand in pull_candidates:
            if cand[0] not in seen_ids:
                seen_ids.add(cand[0])
                unique_candidates.append(cand)

        if not unique_candidates:
            logger.info(
                "  iter %d: %d missing links but none meet KSS threshold — done.",
                iteration, len(missing_rows),
            )
            break

        # ------------------------------------------------------------------
        # Pull candidates into the selected set.
        # ------------------------------------------------------------------
        for target_id, _kss, domain in unique_candidates:
            conn.execute(
                "UPDATE articles SET selected = 1, repair_anchor = 1 WHERE id = ?",
                (target_id,),
            )
            pulled_this_iter += 1

        conn.commit()

        # ------------------------------------------------------------------
        # Enforce domain quotas: evict lowest-KSS non-anchor articles if
        # any domain exceeds its ceiling.
        # ------------------------------------------------------------------
        # Compute max allowed count per domain.
        max_counts: dict[str, int] = {
            d: int(quotas[d] * (1.0 + r.max_overage_fraction))
            for d in DOMAINS
        }

        for domain in DOMAINS:
            current_count: int = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE selected = 1 AND domain = ?",
                (domain,),
            ).fetchone()[0]

            ceiling = max_counts[domain]
            excess = current_count - ceiling

            if excess <= 0:
                continue

            # Evict the ``excess`` non-anchor articles with the lowest KSS.
            evict_rows = conn.execute(
                """
                SELECT id FROM articles
                WHERE selected = 1
                  AND repair_anchor = 0
                  AND domain = ?
                ORDER BY kss ASC
                LIMIT ?
                """,
                (domain, excess),
            ).fetchall()

            for row in evict_rows:
                conn.execute(
                    "UPDATE articles SET selected = 0 WHERE id = ?",
                    (row["id"],),
                )
                evicted_this_iter += 1

            conn.commit()

        total_pulled += pulled_this_iter
        total_evicted += evicted_this_iter

        logger.info(
            "  iter %d: pulled=%d evicted=%d (cumulative pulled=%d evicted=%d)",
            iteration, pulled_this_iter, evicted_this_iter,
            total_pulled, total_evicted,
        )

        # If nothing changed this iteration, we've converged.
        if pulled_this_iter == 0:
            logger.info("  iter %d: converged (no pulls).", iteration)
            break

    # ------------------------------------------------------------------
    # Final count
    # ------------------------------------------------------------------
    final_selected = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE selected = 1"
    ).fetchone()[0]
    logger.info(
        "Stage 5: complete.  %d articles in corpus (pulled=%d evicted=%d).",
        final_selected, total_pulled, total_evicted,
    )

    record_stage_complete(
        conn,
        "s5",
        article_count=final_selected,
        notes=f"pulled={total_pulled} evicted={total_evicted}",
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
    parser = argparse.ArgumentParser(description="Run Stage 5: Adjacency repair")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
