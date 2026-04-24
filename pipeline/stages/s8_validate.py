"""
pipeline.stages.s8_validate — Stage 8: Validation (Non-Negotiable).

Runs all automated correctness checks on the final corpus.  The build
fails (raises ``SystemExit(1)``) if any check fails.

Checks
------
1. Every selected article has ≥1 inbound link from another selected article.
2. Every selected article has ≥1 outbound link to another selected article.
3. Each domain's article count is within ± ``quota_tolerance`` of its quota.
4. Total compressed size ≤ ``max_total_compressed_bytes``.
5. Random-walk simulation: dead-end rate < ``max_dead_end_rate``.
6. BUILD_HASH recomputed from the manifest matches the stored value.

Results are written to ``build/validation_report.json``.
All individual check outcomes are logged whether they pass or fail.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys

from pipeline.config import Config
from pipeline.db import record_stage_complete, iter_rows
from pipeline.graph import AdjacencyCache, random_walk_dead_end_rate
from pipeline.hash import compute_build_hash

logger = logging.getLogger(__name__)


def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 8: validate the built corpus."""
    results: dict[str, dict] = {}
    failures: list[str] = []

    # ------------------------------------------------------------------
    # Check 1: All selected articles have ≥1 inbound link from selected.
    # ------------------------------------------------------------------
    orphan_no_inbound = conn.execute(
        """
        SELECT COUNT(*) FROM articles a
        WHERE a.selected = 1
          AND NOT EXISTS (
              SELECT 1 FROM link_edges le
              JOIN articles s ON s.id = le.source_id AND s.selected = 1
              WHERE le.target_id = a.id
          )
        """
    ).fetchone()[0]

    results["inbound_coverage"] = {
        "pass": orphan_no_inbound <= cfg.validation.max_inbound_orphans,
        "orphan_count": orphan_no_inbound,
        "max_allowed": cfg.validation.max_inbound_orphans,
        "description": "Articles with no inbound links from selected set ≤ max_inbound_orphans.",
    }
    if orphan_no_inbound > cfg.validation.max_inbound_orphans:
        failures.append(f"inbound_coverage: {orphan_no_inbound} articles have no inbound links")
    logger.info(
        "Check 1 — inbound coverage: %s (%d articles with no inbound links, max=%d)",
        "PASS" if orphan_no_inbound <= cfg.validation.max_inbound_orphans else "FAIL",
        orphan_no_inbound, cfg.validation.max_inbound_orphans,
    )

    # ------------------------------------------------------------------
    # Check 2: All selected articles have ≥1 outbound link to selected.
    # ------------------------------------------------------------------
    orphan_no_outbound = conn.execute(
        """
        SELECT COUNT(*) FROM articles a
        WHERE a.selected = 1
          AND NOT EXISTS (
              SELECT 1 FROM link_edges le
              JOIN articles t ON t.id = le.target_id AND t.selected = 1
              WHERE le.source_id = a.id
          )
        """
    ).fetchone()[0]

    results["outbound_coverage"] = {
        "pass": orphan_no_outbound <= cfg.validation.max_outbound_orphans,
        "dead_end_count": orphan_no_outbound,
        "max_allowed": cfg.validation.max_outbound_orphans,
        "description": "Articles with no outbound links to selected set ≤ max_outbound_orphans.",
    }
    if orphan_no_outbound > cfg.validation.max_outbound_orphans:
        failures.append(f"outbound_coverage: {orphan_no_outbound} articles have no outbound links")
    logger.info(
        "Check 2 — outbound coverage: %s (%d articles with no outbound links, max=%d)",
        "PASS" if orphan_no_outbound <= cfg.validation.max_outbound_orphans else "FAIL",
        orphan_no_outbound, cfg.validation.max_outbound_orphans,
    )

    # ------------------------------------------------------------------
    # Check 3: Domain quotas within tolerance.
    # ------------------------------------------------------------------
    quotas = cfg.quotas.as_dict()
    tol = cfg.validation.quota_tolerance
    quota_failures: list[str] = []

    domain_counts: dict[str, int] = {}
    for row in conn.execute(
        "SELECT domain, COUNT(*) AS n FROM articles WHERE selected = 1 GROUP BY domain"
    ):
        domain_counts[row["domain"]] = row["n"]

    for domain, quota in quotas.items():
        actual = domain_counts.get(domain, 0)
        lo = quota * (1.0 - tol)
        hi = quota * (1.0 + tol)
        ok = lo <= actual <= hi
        if not ok:
            quota_failures.append(f"{domain}: actual={actual} expected=[{lo:.0f},{hi:.0f}]")

    results["quota_compliance"] = {
        "pass": len(quota_failures) == 0,
        "failures": quota_failures,
        "domain_counts": domain_counts,
        "description": f"All domain counts within ±{tol*100:.0f}% of quota.",
    }
    if quota_failures:
        failures.append(f"quota_compliance: {quota_failures}")
    logger.info(
        "Check 3 — quota compliance: %s",
        "PASS" if not quota_failures else f"FAIL {quota_failures}",
    )

    # ------------------------------------------------------------------
    # Check 4: Total compressed size ≤ budget.
    # ------------------------------------------------------------------
    size_check_pass = True
    total_compressed_bytes = 0
    if cfg.report_path.exists():
        with cfg.report_path.open() as fh:
            report_data = json.load(fh)
        total_compressed_bytes = report_data.get("total_compressed_bytes", 0)
        size_check_pass = total_compressed_bytes <= cfg.build.max_total_compressed_bytes
    else:
        size_check_pass = False

    results["size_budget"] = {
        "pass": size_check_pass,
        "total_compressed_bytes": total_compressed_bytes,
        "max_bytes": cfg.build.max_total_compressed_bytes,
        "description": "Total compressed size ≤ max_total_compressed_bytes.",
    }
    if not size_check_pass:
        failures.append(
            f"size_budget: {total_compressed_bytes / 1e6:.1f} MB > "
            f"{cfg.build.max_total_compressed_bytes / 1e6:.0f} MB"
        )
    logger.info(
        "Check 4 — size budget: %s (%.1f MB / %.0f MB)",
        "PASS" if size_check_pass else "FAIL",
        total_compressed_bytes / 1e6,
        cfg.build.max_total_compressed_bytes / 1e6,
    )

    # ------------------------------------------------------------------
    # Check 5: Random-walk dead-end rate.
    # ------------------------------------------------------------------
    selected_ids = [
        row["id"]
        for row in iter_rows(conn, "SELECT id FROM articles WHERE selected = 1")
    ]

    cache = AdjacencyCache(conn)
    dead_end_rate = random_walk_dead_end_rate(
        cache,
        selected_ids,
        walk_count=cfg.validation.random_walk_count,
        walk_steps=cfg.validation.random_walk_steps,
    )
    walk_pass = dead_end_rate <= cfg.validation.max_dead_end_rate

    results["random_walk"] = {
        "pass": walk_pass,
        "dead_end_rate": round(dead_end_rate, 5),
        "max_allowed": cfg.validation.max_dead_end_rate,
        "walk_count": cfg.validation.random_walk_count,
        "walk_steps": cfg.validation.random_walk_steps,
        "description": "Random-walk dead-end rate below threshold.",
    }
    if not walk_pass:
        failures.append(
            f"random_walk: dead_end_rate={dead_end_rate:.4f} > "
            f"max={cfg.validation.max_dead_end_rate}"
        )
    logger.info(
        "Check 5 — random walk: %s (dead_end_rate=%.4f, max=%.4f)",
        "PASS" if walk_pass else "FAIL",
        dead_end_rate, cfg.validation.max_dead_end_rate,
    )

    # ------------------------------------------------------------------
    # Check 6: BUILD_HASH integrity.
    # ------------------------------------------------------------------
    hash_pass = False
    hash_mismatch = ""
    if cfg.manifest_path.exists() and cfg.build_hash_path.exists():
        recomputed = compute_build_hash(cfg.manifest_path)
        stored = cfg.build_hash_path.read_text().strip()
        hash_pass = recomputed == stored
        if not hash_pass:
            hash_mismatch = f"stored={stored[:16]}… recomputed={recomputed[:16]}…"
    else:
        hash_mismatch = "manifest or BUILD_HASH file missing"

    results["build_hash"] = {
        "pass": hash_pass,
        "mismatch": hash_mismatch,
        "description": "BUILD_HASH matches recomputed hash from manifest.",
    }
    if not hash_pass:
        failures.append(f"build_hash: {hash_mismatch}")
    logger.info(
        "Check 6 — BUILD_HASH: %s%s",
        "PASS" if hash_pass else "FAIL",
        f" ({hash_mismatch})" if hash_mismatch else "",
    )

    # ------------------------------------------------------------------
    # Write validation report.
    # ------------------------------------------------------------------
    validation_report = {
        "overall": "PASS" if not failures else "FAIL",
        "failure_count": len(failures),
        "failures": failures,
        "checks": results,
    }
    cfg.validation_report_path.write_text(
        json.dumps(validation_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Stage 8: validation report written to %s", cfg.validation_report_path)

    record_stage_complete(
        conn,
        "s8",
        notes=f"overall={'PASS' if not failures else 'FAIL'} failures={len(failures)}",
    )

    # ------------------------------------------------------------------
    # Hard fail if any check failed.
    # ------------------------------------------------------------------
    if failures:
        logger.error("Stage 8: BUILD FAILED.  %d check(s) failed:", len(failures))
        for f in failures:
            logger.error("  - %s", f)
        sys.exit(1)

    total_articles = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE selected = 1"
    ).fetchone()[0]
    logger.info(
        "Stage 8: ALL CHECKS PASSED.  Corpus: %d articles, %.1f MB compressed.",
        total_articles, total_compressed_bytes / 1e6,
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
    parser = argparse.ArgumentParser(description="Run Stage 8: Validation")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
