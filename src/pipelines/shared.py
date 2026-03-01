"""
Shared pipeline utilities for cache management and theme discovery.

Used by both the Airflow DAG and local runner to ensure consistent behavior.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from config.data_foundation import Config
from data.overture_ingest import OvertureTheme

if TYPE_CHECKING:
    pass


def _cache_dir(cfg: Config, category: str) -> Path:
    """Get or create cache directory: data/raw/{category}/.cache."""
    cache_dir = cfg.local.raw / category / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _is_cache_valid(cache_path: Path, ttl_hours: int) -> bool:
    """Check if cache file exists and is not expired."""
    if not cache_path.exists():
        return False
    cache_age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
    return cache_age_hours < ttl_hours


def get_overture_cache_path(
    cfg: Config,
    theme: OvertureTheme,
    bbox: tuple[float, float, float, float],
) -> Path:
    """
    Build cache path for Overture theme.
    Format: data/raw/overture/.cache/overture_{theme}_{release}_{minx}_{maxx}_{miny}_{maxy}.parquet
    """
    release = cfg.overture_release_date or "unknown"
    minx, maxx, miny, maxy = bbox
    cache_filename = (
        f"overture_{theme.value}_{release}_{minx}_{maxx}_{miny}_{maxy}.parquet"
    )
    return _cache_dir(cfg, "overture") / cache_filename


def get_osm_cache_path(
    cfg: Config,
    bbox: tuple[float, float, float, float],
) -> Path:
    """
    Build cache path for OSM.
    Format: data/raw/osm/.cache/osm_{south}_{west}_{north}_{east}.parquet
    """
    south, west, north, east = bbox
    cache_filename = f"osm_{south}_{west}_{north}_{east}.parquet"
    return _cache_dir(cfg, "osm") / cache_filename


def find_all_theme_samples(cfg: Config) -> dict[OvertureTheme, Path]:
    """
    Discover all available Overture theme sample files in the temp directory.
    Returns dict mapping theme to path (only themes that exist).
    """
    temp_dir = cfg.local.raw / "overture" / "temp"
    if not temp_dir.exists():
        return {}

    theme_samples: dict[OvertureTheme, Path] = {}
    for theme in OvertureTheme:
        sample_path = temp_dir / f"overture_{theme.value}_sample.parquet"
        if sample_path.exists():
            theme_samples[theme] = sample_path

    return theme_samples


def get_bbox_from_config(cfg: Config) -> tuple[float, float, float, float]:
    """Get bbox as tuple (minx, maxx, miny, maxy) from config."""
    return (cfg.bbox_minx, cfg.bbox_maxx, cfg.bbox_miny, cfg.bbox_maxy)


def get_osm_bbox_from_config(cfg: Config) -> tuple[float, float, float, float]:
    """Get bbox for OSM as tuple (south, west, north, east) from config."""
    return (cfg.bbox_miny, cfg.bbox_minx, cfg.bbox_maxy, cfg.bbox_maxx)
