"""
Composer-only tiled DAG: conflate Overture + OSM for North America by H3 tile, then merge and build gold.

Use this DAG for continental-scale runs. For local/single-bbox runs use rpg_data_foundation DAG instead.
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from os import getenv
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from config.data_foundation import Config
from data.conflation import silver_to_gold
from data.na_tiles import get_na_tiles
from data.overture_ingest import build_overture_parquet_url
from data.tiled_conflation import run_conflate_for_tile
from pipelines.airflow.validation import get_validated_config
from src.io import gcs as gcs_io


def _get_tiles() -> list[str]:
    """NA tile list (res 3), optionally limited by env for testing."""
    na_tiles = get_na_tiles(res=3)
    limit_str = getenv("RPG_NA_TILE_LIMIT", "").strip()
    if limit_str and limit_str.isdigit():
        na_tiles = na_tiles[: int(limit_str)]
    return na_tiles


def _overture_source() -> str:
    release = (getenv("RPG_OVERTURE_RELEASE_DATE", "") or "").strip()
    if not release:
        raise ValueError("RPG_OVERTURE_RELEASE_DATE is required for tiled NA DAG")
    return build_overture_parquet_url(release)


def _osm_source(cfg: Config) -> str:
    osm = (cfg.datasets.osm_extract or getenv("RPG_OSM_EXTRACT_URI", "") or "").strip()
    if not osm:
        raise ValueError(
            "OSM extract URI (RPG_OSM_EXTRACT_URI or config) is required for tiled NA DAG"
        )
    return osm


def task_conflate_tile(
    tile_id: str,
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """Run conflation for one tile; write tile silver to GCS."""
    cfg = config or get_validated_config()
    release = (
        cfg.overture_release_date or getenv("RPG_OVERTURE_RELEASE_DATE", "") or ""
    ).strip()
    if not release:
        release = "unknown"
    overture_source = _overture_source()
    osm_source = _osm_source(cfg)
    # Normalize tile_id for use in paths (H3 hex can contain chars safe for GCS)
    safe_id = tile_id.replace("/", "_")
    gcs_silver_tile = (
        f"{cfg.gcs.silver.rstrip('/')}/{release}/tiles/silver_{safe_id}.parquet"
    )
    with tempfile.TemporaryDirectory() as td:
        temp_dir = Path(td)
        out_path = temp_dir / "silver_tile.parquet"
        run_conflate_for_tile(
            tile_id,
            overture_source,
            osm_source,
            out_path,
            radius_m=50.0,
            limit=cfg.overture_sample_limit,
            temp_dir=temp_dir,
        )
        gcs_io.sync_local_to_gcs(out_path, gcs_silver_tile)


def task_merge_silver_and_build_gold(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """List tile silvers from GCS, concatenate, write merged silver and gold to GCS."""
    import pandas as pd

    cfg = config or get_validated_config()
    release = (
        cfg.overture_release_date or getenv("RPG_OVERTURE_RELEASE_DATE", "") or ""
    ).strip()
    if not release:
        release = "unknown"
    base_uri = f"{cfg.gcs.silver.rstrip('/')}/{release}/tiles"
    bucket_name, prefix = gcs_io.parse_gcs_uri(base_uri)
    client = gcs_io.get_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]

    with tempfile.TemporaryDirectory() as td:
        temp_dir = Path(td)
        dfs = []
        for blob in parquet_blobs:
            local_path = temp_dir / blob.name.replace("/", "_")
            blob.download_to_filename(str(local_path))
            dfs.append(pd.read_parquet(local_path))
        merged = pd.DataFrame() if not dfs else pd.concat(dfs, ignore_index=True)

        silver_local = temp_dir / "venues.parquet"
        merged.to_parquet(silver_local, index=False)
        silver_gcs = f"{cfg.gcs.silver.rstrip('/')}/{release}/venues.parquet"
        gcs_io.sync_local_to_gcs(silver_local, silver_gcs)

        gold_local = temp_dir / "gold.parquet"
        silver_to_gold(silver_local, gold_local)
        gold_gcs = f"{cfg.gcs.gold.rstrip('/')}/{release}/venues.parquet"
        gcs_io.sync_local_to_gcs(gold_local, gold_gcs)


with DAG(
    dag_id="rpg_data_foundation_na",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rpg", "data-foundation", "na-tiled"],
) as dag:
    tiles = _get_tiles()

    conflate_tile = PythonOperator.partial(
        task_id="conflate_tile",
        python_callable=task_conflate_tile,
    ).expand(op_kwargs=[{"tile_id": t} for t in tiles])

    merge_and_gold = PythonOperator(
        task_id="merge_silver_and_build_gold",
        python_callable=task_merge_silver_and_build_gold,
    )

    conflate_tile >> merge_and_gold  # pylint: disable=pointless-statement
