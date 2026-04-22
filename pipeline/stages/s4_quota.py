"""
pipeline.stages.s4_quota — Stage 4: Domain Quota Selection.

For each domain, selects the top-K articles by descending KSS (where K
equals the configured quota) and marks them ``selected = 1``.

This stage resets all ``selected`` flags before running, so it is safe
to re-run after adjusting quotas or weights.

Articles without a KSS (i.e. they failed Stage 1 or were unclassified)
are never eligible for selection.
"""

from __future__ import annotations

import logging
import sqlite3

from pipeline.config import Config
from pipeline.db import record_stage_complete
from pipeline.stages.s2_domain import DOMAINS

logger = logging.getLogger(__name__)


def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 4: domain quota selection."""
    quotas = cfg.quotas.as_dict()

    logger.info("Stage 4: applying domain quotas …")
    logger.info("  Total target articles: %d", sum(quotas.values()))

    # Reset all selection flags from any previous run.
    conn.execute("UPDATE articles SET selected = 0, repair_anchor = 0")
    conn.commit()

    total_selected = 0

    for domain in DOMAINS:
        quota = quotas.get(domain, 0)
        if quota <= 0:
            continue

        # Fetch the top-K article IDs by KSS for this domain.
        rows = conn.execute(
            """
            SELECT id FROM articles
            WHERE stage1_pass = 1
              AND domain = ?
              AND kss IS NOT NULL
            ORDER BY kss DESC
            LIMIT ?
            """,
            (domain, quota),
        ).fetchall()

        ids = [row["id"] for row in rows]
        if not ids:
            logger.warning("Stage 4: no articles found for domain '%s'.", domain)
            continue

        # Mark as selected using a parameterised bulk update.
        conn.executemany(
            "UPDATE articles SET selected = 1 WHERE id = ?",
            [(aid,) for aid in ids],
        )
        conn.commit()

        actual = len(ids)
        total_selected += actual
        logger.info(
            "  %-35s quota=%5d  selected=%5d",
            domain, quota, actual,
        )

    logger.info("Stage 4: complete.  %d total articles selected.", total_selected)

    record_stage_complete(
        conn,
        "s4",
        article_count=total_selected,
        notes=f"total_selected={total_selected}",
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
    parser = argparse.ArgumentParser(description="Run Stage 4: Domain quota selection")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
