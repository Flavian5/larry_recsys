from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from config.data_foundation import load_config
from data.conflation import conflate_parquet, silver_to_gold
from src.io import gcs as gcs_io
from data.osm_ingest import extract_osm_pois
from data.overture_ingest import BBox, sample_overture_places_by_bbox


def _default_data_dir() -> Path:
    return Path(".") / "data"


def task_overture_sample(**_context) -> None:
    cfg = load_config()
    data_root = _default_data_dir()
    raw_dir = data_root / "raw" / "overture"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # For local runs this expects a pre-downloaded Parquet in raw/overture.
    # The bbox is chosen to be small for fixture-style development.
    source = str(raw_dir / "places.parquet")
    bbox = BBox(minx=-122.5, maxx=-122.3, miny=37.7, maxy=37.9)
    output = cfg.local.raw / "overture_sample.parquet"
    sample_overture_places_by_bbox(source, bbox, output)


def task_osm_extract(**_context) -> None:
    cfg = load_config()
    data_root = _default_data_dir()
    raw_dir = data_root / "raw" / "osm"
    raw_dir.mkdir(parents=True, exist_ok=True)

    source = raw_dir / "mini_region.parquet"
    output = cfg.local.raw / "osm_pois.parquet"
    extract_osm_pois(source, output)


def task_build_silver(**_context) -> None:
    cfg = load_config()
    overture_path = cfg.local.raw / "overture_sample.parquet"
    osm_path = cfg.local.raw / "osm_pois.parquet"
    silver_path = cfg.local.silver / "venues.parquet"
    conflate_parquet(overture_path, osm_path, silver_path, radius_m=50.0)


def task_build_gold(**_context) -> None:
    cfg = load_config()
    silver_path = cfg.local.silver / "venues.parquet"
    gold_path = cfg.local.gold / "venues.parquet"
    silver_to_gold(silver_path, gold_path)


def task_upload_gold_to_gcs(**_context) -> None:
    """
    Optional helper to sync locally generated Gold data to GCS.

    Controlled via RPG_ENABLE_LOCAL_GCS_SYNC and RPG_GCS_GOLD_URI.
    """
    from os import getenv

    if getenv("RPG_ENABLE_LOCAL_GCS_SYNC", "").lower() not in {"1", "true", "yes", "y"}:
        return

    cfg = load_config()
    local_gold = cfg.local.gold / "venues.parquet"
    gcs_uri = getenv("RPG_GCS_GOLD_URI", cfg.gcs.gold)
    if not gcs_uri:
        return
    gcs_io.sync_local_to_gcs(local_gold, gcs_uri)


with DAG(
    dag_id="rpg_data_foundation",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
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
