"""
pipeline.cli — Click-based command-line interface.

Entry point: ``monolith-wiki`` (installed via pyproject.toml).

Commands
--------
  run-all          Run the full pipeline from Stage 0 to Stage 8.
  run --stage sN   Run a single stage (or a comma-separated list).
  status           Print the current build state (completed stages).
  validate-only    Re-run Stage 8 validation without regenerating output.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from pipeline.config import load_config
from pipeline.db import open_db, stage_is_complete


# Ordered list of all stage module paths and their human-readable names.
_STAGES: list[tuple[str, str]] = [
    ("s0",  "Stage 0  — ZIM pre-pass indexer"),
    ("s0b", "Stage 0b — Fetch Wikipedia categories"),
    ("s1",  "Stage 1  — Hard pre-filter"),
    ("s2", "Stage 2 — Domain classification"),
    ("s3", "Stage 3 — KSS computation"),
    ("s4", "Stage 4 — Domain quota selection"),
    ("s5", "Stage 5 — Adjacency repair"),
    ("s6", "Stage 6 — Content normalisation"),
    ("s7", "Stage 7 — Compression & packaging"),
    ("s8", "Stage 8 — Validation"),
]

_STAGE_IDS = [s for s, _ in _STAGES]


def _import_stage(stage_id: str):
    """Dynamically import a stage module and return its ``run()`` function."""
    name_map = {
        "s0":  "pipeline.stages.s0_index",
        "s0b": "pipeline.stages.s0b_fetch_categories",
        "s1":  "pipeline.stages.s1_prefilter",
        "s2": "pipeline.stages.s2_domain",
        "s3": "pipeline.stages.s3_kss",
        "s4": "pipeline.stages.s4_quota",
        "s5": "pipeline.stages.s5_repair",
        "s6": "pipeline.stages.s6_normalize",
        "s7": "pipeline.stages.s7_package",
        "s8": "pipeline.stages.s8_validate",
    }
    import importlib
    mod = importlib.import_module(name_map[stage_id])
    return mod.run


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group()
@click.option(
    "--config",
    "config_path",
    default=None,
    envvar="MONOLITH_CONFIG",
    show_default=True,
    help="Path to YAML config file (default: config/default.yaml or $MONOLITH_CONFIG).",
)
@click.pass_context
def main(ctx: click.Context, config_path: str | None) -> None:
    """Monolith Wiki — Wikipedia Attenuation Pipeline."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


# ---------------------------------------------------------------------------
# run-all
# ---------------------------------------------------------------------------

@main.command("run-all")
@click.option(
    "--from-stage",
    default="s0",
    show_default=True,
    type=click.Choice(_STAGE_IDS),
    help="Start (or re-start) from this stage.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-run stages even if their sentinel exists in build_state.",
)
@click.pass_context
def run_all(ctx: click.Context, from_stage: str, force: bool) -> None:
    """Run the complete pipeline (resume-safe by default)."""
    cfg = load_config(ctx.obj["config_path"])
    Path(cfg.build.build_dir).mkdir(parents=True, exist_ok=True)
    conn = open_db(cfg.db_path)

    start_idx = _STAGE_IDS.index(from_stage)
    for stage_id, label in _STAGES[start_idx:]:
        if not force and stage_is_complete(conn, stage_id):
            click.echo(f"  [skip]  {label} (already complete)")
            continue
        click.echo(f"  [run]   {label}")
        fn = _import_stage(stage_id)
        fn(cfg, conn)
        click.echo(f"  [done]  {label}")

    conn.close()
    click.echo("\nPipeline complete.")


# ---------------------------------------------------------------------------
# run (single stage)
# ---------------------------------------------------------------------------

@main.command("run")
@click.option(
    "--stage",
    required=True,
    help="Stage ID(s) to run, e.g. s3 or s3,s4.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-run even if sentinel exists.",
)
@click.pass_context
def run_stage(ctx: click.Context, stage: str, force: bool) -> None:
    """Run one or more specific pipeline stages."""
    requested = [s.strip() for s in stage.split(",")]
    invalid = [s for s in requested if s not in _STAGE_IDS]
    if invalid:
        click.echo(f"Unknown stage(s): {', '.join(invalid)}.  Valid: {', '.join(_STAGE_IDS)}", err=True)
        sys.exit(1)

    cfg = load_config(ctx.obj["config_path"])
    Path(cfg.build.build_dir).mkdir(parents=True, exist_ok=True)
    conn = open_db(cfg.db_path)

    for stage_id in requested:
        label = next(lbl for sid, lbl in _STAGES if sid == stage_id)
        if not force and stage_is_complete(conn, stage_id):
            click.echo(f"  [skip]  {label} (already complete; use --force to re-run)")
            continue
        click.echo(f"  [run]   {label}")
        fn = _import_stage(stage_id)
        fn(cfg, conn)
        click.echo(f"  [done]  {label}")

    conn.close()


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@main.command("status")
@click.pass_context
def status(ctx: click.Context) -> None:
    """Print the current build state for each stage."""
    cfg = load_config(ctx.obj["config_path"])
    if not cfg.db_path.exists():
        click.echo("No database found.  Run 'monolith-wiki run-all' to start.")
        return

    conn = open_db(cfg.db_path, read_only=True)
    rows = {
        row["stage"]: row
        for row in conn.execute("SELECT * FROM build_state ORDER BY stage")
    }
    conn.close()

    click.echo(f"{'Stage':<6}  {'Status':<10}  {'Completed at':<26}  {'Articles':>10}  Notes")
    click.echo("-" * 80)
    for stage_id, label in _STAGES:
        if stage_id in rows:
            r = rows[stage_id]
            art = r["article_count"] if r["article_count"] is not None else "-"
            notes = r["notes"] or ""
            click.echo(f"{stage_id:<6}  {'COMPLETE':<10}  {r['completed_at']:<26}  {str(art):>10}  {notes}")
        else:
            click.echo(f"{stage_id:<6}  {'PENDING':<10}  {'':26}  {'':>10}")


# ---------------------------------------------------------------------------
# validate-only
# ---------------------------------------------------------------------------

@main.command("validate-only")
@click.pass_context
def validate_only(ctx: click.Context) -> None:
    """Re-run Stage 8 validation without regenerating any output."""
    cfg = load_config(ctx.obj["config_path"])
    conn = open_db(cfg.db_path)
    fn = _import_stage("s8")
    fn(cfg, conn)
    conn.close()


if __name__ == "__main__":
    main()
