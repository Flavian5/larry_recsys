from __future__ import annotations

from datetime import datetime
from os import getenv
from pathlib import Path
from typing import TYPE_CHECKING

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from config.data_foundation import Config
from pipelines.airflow.validation import get_validated_config
from data.conflation import conflate_parquet, silver_to_gold
from data.osm_ingest import extract_osm_pois
from data.overture_ingest import (
    BBox,
    build_overture_parquet_url,
    sample_overture_places_by_bbox,
)

if TYPE_CHECKING:
    pass  # Config used for type hints


def _default_data_dir() -> Path:
    return Path(".") / "data"


def _raw_overture_temp(cfg: Config) -> Path:
    """Temp dir for sampled Overture data: data/raw/overture/temp."""
    return cfg.local.raw / "overture" / "temp"


def _raw_osm_temp(cfg: Config) -> Path:
    """Temp dir for extracted OSM data: data/raw/osm/temp."""
    return cfg.local.raw / "osm" / "temp"


def _overture_source(cfg: Config, data_root: Path) -> str:
    """Resolve Overture Places source from config or env."""
    if cfg.datasets.overture_places:
        return cfg.datasets.overture_places
    release = (
        cfg.overture_release_date or getenv("RPG_OVERTURE_RELEASE_DATE", "")
    ).strip()
    if release:
        return build_overture_parquet_url(
            release, base_url=cfg.datasets.overture_places_base
        )
    return str(data_root / "raw" / "overture" / "places.parquet")


def task_overture_sample(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    data_root = _default_data_dir()
    (data_root / "raw" / "overture").mkdir(parents=True, exist_ok=True)

    source = _overture_source(cfg, data_root)
    bbox = BBox(minx=-122.5, maxx=-122.3, miny=37.7, maxy=37.9)
    temp_dir = _raw_overture_temp(cfg)
    temp_dir.mkdir(parents=True, exist_ok=True)
    output = temp_dir / "overture_sample.parquet"
    sample_overture_places_by_bbox(
        source, bbox, output, limit=cfg.overture_sample_limit
    )


def task_osm_extract(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    raw_osm_dir = cfg.local.raw / "osm"
    raw_osm_dir.mkdir(parents=True, exist_ok=True)

    source = (
        cfg.datasets.osm_extract
        if cfg.datasets.osm_extract
        else str((cfg.local.raw / "osm" / "mini_region.parquet").resolve())
    )
    if not any(source.startswith(p) for p in ("http://", "https://", "s3://", "gs://")):
        source_path = Path(source)
        if not source_path.exists():
            default_relative = "data/raw/osm/mini_region.parquet"
            raise FileNotFoundError(
                f"OSM extract not found: {source_path}\n"
                "Provide an OSM Parquet file by either:\n"
                "  1. Set RPG_OSM_EXTRACT_URI (or --osm-source) to a path or URI,\n"
                f"  2. Place a file at {default_relative}"
            )
    temp_dir = _raw_osm_temp(cfg)
    temp_dir.mkdir(parents=True, exist_ok=True)
    output = temp_dir / "osm_pois.parquet"
    extract_osm_pois(source, output)


def task_build_silver(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    overture_path = _raw_overture_temp(cfg) / "overture_sample.parquet"
    osm_path = _raw_osm_temp(cfg) / "osm_pois.parquet"
    silver_path = cfg.local.silver / cfg.silver_venues_filename()
    conflate_parquet(
        overture_path,
        osm_path,
        silver_path,
        radius_m=50.0,
        output_format=cfg.local_output_format,
    )


def task_build_gold(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    cfg.local.gold.mkdir(parents=True, exist_ok=True)
    silver_path = cfg.local.silver / cfg.silver_venues_filename()
    gold_path = cfg.local.gold / cfg.gold_venues_filename()
    silver_to_gold(silver_path, gold_path, output_format=cfg.local_output_format)


def task_cleanup_raw_temp(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """Remove temp raw files (data/raw/overture/temp, data/raw/osm/temp) after pipeline. Set RPG_CLEANUP_RAW_TEMP=false to keep them."""
    if getenv("RPG_CLEANUP_RAW_TEMP", "true").lower() in {"0", "false", "no", "n"}:
        return
    cfg = config or get_validated_config()
    for temp_dir in (_raw_overture_temp(cfg), _raw_osm_temp(cfg)):
        if temp_dir.exists():
            for p in temp_dir.iterdir():
                if p.is_file():
                    p.unlink()


with DAG(
    dag_id="rpg_data_foundation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rpg", "data-foundation"],
) as dag:
    overture_sample = PythonOperator(
        task_id="overture_sample",
        python_callable=task_overture_sample,
    )

    osm_extract = PythonOperator(
        task_id="osm_extract",
        python_callable=task_osm_extract,
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=task_build_silver,
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=task_build_gold,
    )

    cleanup_raw_temp = PythonOperator(
        task_id="cleanup_raw_temp",
        python_callable=task_cleanup_raw_temp,
    )

    (overture_sample >> osm_extract >> build_silver >> build_gold >> cleanup_raw_temp)
