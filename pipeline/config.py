"""
pipeline.config — configuration loading and schema.

All pipeline configuration is expressed as plain dataclasses so it is
serialisable, loggable, and has no runtime dependencies beyond PyYAML
and the standard library.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


# ---------------------------------------------------------------------------
# Sub-config dataclasses
# ---------------------------------------------------------------------------

@dataclass
class InputConfig:
    zim_path: str = "data/input/wikipedia_en.zim"
    zim_sha256: Optional[str] = None


@dataclass
class BuildConfig:
    build_dir: str = "data/build"
    db_name: str = "pipeline.db"
    normalized_dir: str = "normalized"
    corpus_dir: str = "corpus"
    css_filename: str = "style.css"
    max_total_compressed_bytes: int = 419_430_400  # 400 MB
    zstd_level: int = 9
    workers: int = 0  # 0 = auto-detect (os.cpu_count())


@dataclass
class PrefilterConfig:
    min_text_chars: int = 5_000
    min_inbound_links: int = 5


@dataclass
class KSSWeights:
    log_inbound: float = 0.30
    cross_domain: float = 0.20
    length_score: float = 0.15
    category_coverage: float = 0.15
    longevity: float = 0.15
    recency_penalty: float = 0.05


@dataclass
class KSSConfig:
    weights: KSSWeights = field(default_factory=KSSWeights)
    length_soft_cap: int = 100_000
    length_floor: int = 5_000


@dataclass
class QuotasConfig:
    Geography: int = 6_000
    Biography: int = 6_000
    History: int = 4_000
    Science_Math: int = 4_000
    Arts_Culture: int = 4_000
    Technology_Computing: int = 3_000
    Society_Politics_Economics: int = 2_000
    Meta_Reference: int = 1_000

    def as_dict(self) -> dict[str, int]:
        return {
            "Geography": self.Geography,
            "Biography": self.Biography,
            "History": self.History,
            "Science_Math": self.Science_Math,
            "Arts_Culture": self.Arts_Culture,
            "Technology_Computing": self.Technology_Computing,
            "Society_Politics_Economics": self.Society_Politics_Economics,
            "Meta_Reference": self.Meta_Reference,
        }

    @property
    def total(self) -> int:
        return sum(self.as_dict().values())


@dataclass
class RepairConfig:
    top_n_links: int = 8
    min_pull_kss: float = 0.40
    max_overage_fraction: float = 0.05
    max_iterations: int = 50


@dataclass
class ValidationConfig:
    random_walk_count: int = 5_000
    random_walk_steps: int = 200
    max_dead_end_rate: float = 0.01
    quota_tolerance: float = 0.05


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------

@dataclass
class Config:
    input: InputConfig = field(default_factory=InputConfig)
    build: BuildConfig = field(default_factory=BuildConfig)
    prefilter: PrefilterConfig = field(default_factory=PrefilterConfig)
    kss: KSSConfig = field(default_factory=KSSConfig)
    quotas: QuotasConfig = field(default_factory=QuotasConfig)
    repair: RepairConfig = field(default_factory=RepairConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)

    # -----------------------------------------------------------------------
    # Derived paths (not stored in YAML; computed at access time)
    # -----------------------------------------------------------------------

    @property
    def db_path(self) -> Path:
        return Path(self.build.build_dir) / self.build.db_name

    @property
    def normalized_path(self) -> Path:
        return Path(self.build.build_dir) / self.build.normalized_dir

    @property
    def corpus_path(self) -> Path:
        return Path(self.build.build_dir) / self.build.corpus_dir

    @property
    def assets_path(self) -> Path:
        return self.corpus_path / "assets"

    @property
    def manifest_path(self) -> Path:
        return Path(self.build.build_dir) / "manifest.json"

    @property
    def report_path(self) -> Path:
        return Path(self.build.build_dir) / "report.json"

    @property
    def build_hash_path(self) -> Path:
        return Path(self.build.build_dir) / "BUILD_HASH"

    @property
    def validation_report_path(self) -> Path:
        return Path(self.build.build_dir) / "validation_report.json"


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override dict into base dict (override wins)."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _from_dict(raw: dict) -> Config:
    """Construct a Config from a parsed YAML dict."""
    cfg = Config()

    inp = raw.get("input", {})
    cfg.input = InputConfig(
        zim_path=inp.get("zim_path", cfg.input.zim_path),
        zim_sha256=inp.get("zim_sha256", cfg.input.zim_sha256),
    )

    bld = raw.get("build", {})
    cfg.build = BuildConfig(
        build_dir=bld.get("build_dir", cfg.build.build_dir),
        db_name=bld.get("db_name", cfg.build.db_name),
        normalized_dir=bld.get("normalized_dir", cfg.build.normalized_dir),
        corpus_dir=bld.get("corpus_dir", cfg.build.corpus_dir),
        css_filename=bld.get("css_filename", cfg.build.css_filename),
        max_total_compressed_bytes=bld.get(
            "max_total_compressed_bytes", cfg.build.max_total_compressed_bytes
        ),
        zstd_level=bld.get("zstd_level", cfg.build.zstd_level),
    )

    pf = raw.get("prefilter", {})
    cfg.prefilter = PrefilterConfig(
        min_text_chars=pf.get("min_text_chars", cfg.prefilter.min_text_chars),
        min_inbound_links=pf.get("min_inbound_links", cfg.prefilter.min_inbound_links),
    )

    kss = raw.get("kss", {})
    w = kss.get("weights", {})
    cfg.kss = KSSConfig(
        weights=KSSWeights(
            log_inbound=w.get("log_inbound", cfg.kss.weights.log_inbound),
            cross_domain=w.get("cross_domain", cfg.kss.weights.cross_domain),
            length_score=w.get("length_score", cfg.kss.weights.length_score),
            category_coverage=w.get("category_coverage", cfg.kss.weights.category_coverage),
            longevity=w.get("longevity", cfg.kss.weights.longevity),
            recency_penalty=w.get("recency_penalty", cfg.kss.weights.recency_penalty),
        ),
        length_soft_cap=kss.get("length_soft_cap", cfg.kss.length_soft_cap),
        length_floor=kss.get("length_floor", cfg.kss.length_floor),
    )

    q = raw.get("quotas", {})
    cfg.quotas = QuotasConfig(
        Geography=q.get("Geography", cfg.quotas.Geography),
        Biography=q.get("Biography", cfg.quotas.Biography),
        History=q.get("History", cfg.quotas.History),
        Science_Math=q.get("Science_Math", cfg.quotas.Science_Math),
        Arts_Culture=q.get("Arts_Culture", cfg.quotas.Arts_Culture),
        Technology_Computing=q.get("Technology_Computing", cfg.quotas.Technology_Computing),
        Society_Politics_Economics=q.get(
            "Society_Politics_Economics", cfg.quotas.Society_Politics_Economics
        ),
        Meta_Reference=q.get("Meta_Reference", cfg.quotas.Meta_Reference),
    )

    rp = raw.get("repair", {})
    cfg.repair = RepairConfig(
        top_n_links=rp.get("top_n_links", cfg.repair.top_n_links),
        min_pull_kss=rp.get("min_pull_kss", cfg.repair.min_pull_kss),
        max_overage_fraction=rp.get("max_overage_fraction", cfg.repair.max_overage_fraction),
        max_iterations=rp.get("max_iterations", cfg.repair.max_iterations),
    )

    vl = raw.get("validation", {})
    cfg.validation = ValidationConfig(
        random_walk_count=vl.get("random_walk_count", cfg.validation.random_walk_count),
        random_walk_steps=vl.get("random_walk_steps", cfg.validation.random_walk_steps),
        max_dead_end_rate=vl.get("max_dead_end_rate", cfg.validation.max_dead_end_rate),
        quota_tolerance=vl.get("quota_tolerance", cfg.validation.quota_tolerance),
    )

    return cfg


def load_config(path: str | os.PathLike | None = None) -> Config:
    """
    Load configuration from a YAML file.

    Resolution order:
      1. Explicit ``path`` argument
      2. ``MONOLITH_CONFIG`` environment variable
      3. ``config/default.yaml`` relative to cwd
    """
    if path is None:
        path = os.environ.get("MONOLITH_CONFIG", "config/default.yaml")

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")

    with config_path.open() as fh:
        raw = yaml.safe_load(fh) or {}

    return _from_dict(raw)
