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
from src.io import gcs as gcs_io

if TYPE_CHECKING:
    pass  # Config used for type hints


def _default_data_dir() -> Path:
    return Path(".") / "data"


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
    raw_dir = data_root / "raw" / "overture"
    raw_dir.mkdir(parents=True, exist_ok=True)

    source = _overture_source(cfg, data_root)
    bbox = BBox(minx=-122.5, maxx=-122.3, miny=37.7, maxy=37.9)
    output = cfg.local.raw / "overture_sample.parquet"
    sample_overture_places_by_bbox(
        source, bbox, output, limit=cfg.overture_sample_limit
    )


def task_osm_extract(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    data_root = _default_data_dir()
    raw_dir = data_root / "raw" / "osm"
    raw_dir.mkdir(parents=True, exist_ok=True)

    source = (
        cfg.datasets.osm_extract
        if cfg.datasets.osm_extract
        else raw_dir / "mini_region.parquet"
    )
    output = cfg.local.raw / "osm_pois.parquet"
    extract_osm_pois(source, output)


def task_build_silver(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    overture_path = cfg.local.raw / "overture_sample.parquet"
    osm_path = cfg.local.raw / "osm_pois.parquet"
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
    silver_path = cfg.local.silver / cfg.silver_venues_filename()
    gold_path = cfg.local.gold / cfg.gold_venues_filename()
    silver_to_gold(silver_path, gold_path, output_format=cfg.local_output_format)


def task_upload_gold_to_gcs(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """Optional: sync local Gold to GCS. Controlled via RPG_ENABLE_LOCAL_GCS_SYNC and RPG_GCS_GOLD_URI."""
    if getenv("RPG_ENABLE_LOCAL_GCS_SYNC", "").lower() not in {"1", "true", "yes", "y"}:
        return

    cfg = config or get_validated_config()
    local_gold = cfg.local.gold / cfg.gold_venues_filename()
    gcs_uri = getenv("RPG_GCS_GOLD_URI", cfg.gcs.gold)
    if not gcs_uri:
        return
    gcs_io.sync_local_to_gcs(local_gold, gcs_uri)


with DAG(
    dag_id="rpg_data_foundation",
    start_date=datetime(2024, 1, 1),
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

    upload_gold_to_gcs = PythonOperator(
        task_id="upload_gold_to_gcs",
        python_callable=task_upload_gold_to_gcs,
    )

    overture_sample >> osm_extract >> build_silver >> build_gold >> upload_gold_to_gcs
