"""
pipeline.stages.s1_prefilter — Stage 1: Hard Pre-Filter (Quality Gate).

Applies hard exclusion rules to the full article index built in Stage 0.
This is a pure SQL stage; no HTML re-parsing is required.

Exclusion criteria (any one is sufficient):
  - is_redirect  = 1            (should already be absent; defensive check)
  - is_disambig  = 1
  - text_chars   < min_text_chars
  - is_stub      = 1
  - inbound_count < min_inbound_links

Results are written to ``articles.stage1_pass`` (1 = pass, 0 = fail)
and ``articles.stage1_fail_reason`` (human-readable explanation).
"""

from __future__ import annotations

import logging
import sqlite3

from pipeline.config import Config
from pipeline.db import record_stage_complete

logger = logging.getLogger(__name__)


def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 1: mark articles as pass/fail in the database."""
    pf = cfg.prefilter

    logger.info("Stage 1: applying hard pre-filter …")
    logger.info(
        "  Thresholds — min_text_chars=%d, min_inbound_links=%d",
        pf.min_text_chars,
        pf.min_inbound_links,
    )

    # Reset any previous run.
    conn.execute("UPDATE articles SET stage1_pass = NULL, stage1_fail_reason = NULL")
    conn.commit()

    # ------------------------------------------------------------------
    # Apply each exclusion rule individually so the fail reason is clear.
    # Order matters: earlier rules take precedence for the reason string.
    # ------------------------------------------------------------------

    rules: list[tuple[str, str]] = [
        (
            "is_redirect = 1",
            "redirect",
        ),
        (
            "is_disambig = 1",
            "disambiguation",
        ),
        (
            "is_stub = 1",
            "stub",
        ),
        (
            f"text_chars < {pf.min_text_chars}",
            f"text_chars<{pf.min_text_chars}",
        ),
        (
            f"inbound_count < {pf.min_inbound_links}",
            f"inbound_links<{pf.min_inbound_links}",
        ),
    ]

    for condition, reason in rules:
        conn.execute(
            f"""
            UPDATE articles
            SET stage1_pass = 0, stage1_fail_reason = ?
            WHERE stage1_pass IS NULL AND ({condition})
            """,
            (reason,),
        )

    # Everything not already failed passes.
    conn.execute(
        "UPDATE articles SET stage1_pass = 1 WHERE stage1_pass IS NULL"
    )
    conn.commit()

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    pass_count = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE stage1_pass = 1"
    ).fetchone()[0]
    fail_count = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE stage1_pass = 0"
    ).fetchone()[0]

    logger.info(
        "Stage 1: %d pass, %d fail (total %d)",
        pass_count, fail_count, pass_count + fail_count,
    )

    # Per-reason breakdown.
    for row in conn.execute(
        """
        SELECT stage1_fail_reason, COUNT(*) AS n
        FROM articles
        WHERE stage1_pass = 0
        GROUP BY stage1_fail_reason
        ORDER BY n DESC
        """
    ):
        logger.info("  fail[%s]: %d", row["stage1_fail_reason"], row["n"])

    record_stage_complete(
        conn,
        "s1",
        article_count=pass_count,
        notes=f"pass={pass_count} fail={fail_count}",
    )


# ---------------------------------------------------------------------------
# Module-level __main__ support
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging as _logging

    from pipeline.config import load_config
    from pipeline.db import open_db

    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Run Stage 1: Hard pre-filter")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
