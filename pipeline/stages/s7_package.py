"""
pipeline.stages.s7_package — Stage 7: Compression & Packaging.

Takes the normalised HTML files written by Stage 6 and produces the final
compressed corpus with a deterministic filesystem layout.

Output layout
-------------
build/corpus/
  assets/
    style.css                  (shared CSS, one file)
  {domain}/
    {slug}.html.zst            (per-article, zstd-compressed)
build/manifest.json            (machine-readable article manifest)
build/report.json              (build report: counts, sizes, distribution)
build/BUILD_HASH               (SHA-256 of sorted manifest)

Determinism
-----------
Articles are processed in sorted order: (domain, -kss, path).
The manifest is a JSON array sorted by article path.
The BUILD_HASH is a SHA-256 of the canonical serialisation of the manifest.

File naming
-----------
Compressed files keep the same relative path as normalised files, with
".zst" appended.  The manifest records both paths.
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from pathlib import Path

import zstandard as zstd
from tqdm import tqdm

from pipeline.config import Config
from pipeline.db import record_stage_complete, iter_rows
from pipeline.hash import write_build_hash

logger = logging.getLogger(__name__)

_ASSETS_SRC = Path(__file__).parent.parent.parent / "assets" / "style.css"


def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 7: compress articles and write manifest + report."""
    corpus_dir = cfg.corpus_path
    corpus_dir.mkdir(parents=True, exist_ok=True)
    cfg.assets_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Copy shared CSS asset.
    # ------------------------------------------------------------------
    if _ASSETS_SRC.exists():
        shutil.copy(_ASSETS_SRC, cfg.assets_path / cfg.build.css_filename)
        logger.info("Stage 7: copied CSS asset to %s", cfg.assets_path)
    else:
        logger.warning("Stage 7: CSS source not found at %s", _ASSETS_SRC)

    # ------------------------------------------------------------------
    # Fetch selected articles ordered by (domain, kss DESC, path).
    # ------------------------------------------------------------------
    rows = list(iter_rows(
        conn,
        """
        SELECT id, path, title, domain, kss, output_path
        FROM articles
        WHERE selected = 1 AND output_path IS NOT NULL
        ORDER BY domain, kss DESC, path
        """,
    ))

    logger.info("Stage 7: packaging %d articles …", len(rows))

    cctx = zstd.ZstdCompressor(level=cfg.build.zstd_level)
    manifest_entries: list[dict] = []
    total_compressed_bytes = 0
    failed_count = 0

    for row in tqdm(rows, desc="s7 package", unit="art"):
        norm_path = cfg.normalized_path / row["output_path"]
        if not norm_path.exists():
            logger.warning("Stage 7: normalised file missing: %s", norm_path)
            failed_count += 1
            continue

        html_bytes = norm_path.read_bytes()
        compressed = cctx.compress(html_bytes)
        compressed_size = len(compressed)
        total_compressed_bytes += compressed_size

        # Write compressed file.
        zst_path = corpus_dir / (row["output_path"] + ".zst")
        zst_path.parent.mkdir(parents=True, exist_ok=True)
        zst_path.write_bytes(compressed)

        manifest_entries.append({
            "path": row["path"],
            "title": row["title"],
            "domain": row["domain"],
            "kss": round(row["kss"], 6) if row["kss"] is not None else None,
            "output_path": row["output_path"] + ".zst",
            "compressed_bytes": compressed_size,
        })

    # ------------------------------------------------------------------
    # Write manifest (sorted by path for determinism).
    # ------------------------------------------------------------------
    manifest_entries.sort(key=lambda e: e["path"])
    cfg.manifest_path.write_text(
        json.dumps(manifest_entries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Stage 7: manifest written to %s", cfg.manifest_path)

    # ------------------------------------------------------------------
    # Write report.
    # ------------------------------------------------------------------
    domain_counts: dict[str, int] = {}
    domain_bytes: dict[str, int] = {}
    for entry in manifest_entries:
        d = entry["domain"]
        domain_counts[d] = domain_counts.get(d, 0) + 1
        domain_bytes[d] = domain_bytes.get(d, 0) + entry["compressed_bytes"]

    report = {
        "total_articles": len(manifest_entries),
        "total_compressed_bytes": total_compressed_bytes,
        "total_compressed_mb": round(total_compressed_bytes / 1e6, 2),
        "failed_to_package": failed_count,
        "domain_distribution": {
            d: {"count": domain_counts.get(d, 0), "compressed_bytes": domain_bytes.get(d, 0)}
            for d in sorted(domain_counts)
        },
    }
    cfg.report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(
        "Stage 7: report written.  Total: %d articles, %.1f MB compressed.",
        len(manifest_entries), total_compressed_bytes / 1e6,
    )

    # ------------------------------------------------------------------
    # Verify total size budget.
    # ------------------------------------------------------------------
    if total_compressed_bytes > cfg.build.max_total_compressed_bytes:
        raise RuntimeError(
            f"Stage 7: HARD SIZE LIMIT EXCEEDED.  "
            f"Total: {total_compressed_bytes / 1e6:.1f} MB > "
            f"{cfg.build.max_total_compressed_bytes / 1e6:.0f} MB limit.  "
            "Build FAILED."
        )

    # ------------------------------------------------------------------
    # Compute and write deterministic BUILD_HASH.
    # ------------------------------------------------------------------
    build_hash = write_build_hash(cfg.manifest_path, cfg.build_hash_path)
    logger.info("Stage 7: BUILD_HASH = %s", build_hash)

    record_stage_complete(
        conn,
        "s7",
        article_count=len(manifest_entries),
        notes=f"compressed_mb={total_compressed_bytes / 1e6:.1f}",
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
    parser = argparse.ArgumentParser(description="Run Stage 7: Compression & packaging")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
