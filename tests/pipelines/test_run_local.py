"""Tests for the local pipeline runner (run_local)."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from config.data_foundation import Config
from pipelines.run_local import main
from pipelines.airflow.dags.rpg_data_foundation_dag import (
    task_build_gold,
    task_build_silver,
    task_overture_sample,
    task_osm_extract,
)

# Load fetch_osm script for integration test (mocked Overpass)
_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "fetch_osm_mini_region.py"
)
_spec = importlib.util.spec_from_file_location("fetch_osm_mini_region", _SCRIPT_PATH)
_fetch_osm = importlib.util.module_from_spec(_spec) if _spec and _spec.loader else None
if _spec and _spec.loader:
    _spec.loader.exec_module(_fetch_osm)


def test_run_local_full_pipeline_with_local_sources(
    tmp_path: Path, monkeypatch
) -> None:
    """Run the pipeline using local Parquet files (no network)."""
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "overture").mkdir()
    (raw / "osm").mkdir()

    # Seed Overture-style and OSM-style Parquet so the DAG tasks can read them
    overture_src = raw / "overture" / "places.parquet"
    overture_df = pd.DataFrame(
        [
            {"id": "a", "gers_id": "G1", "lat": 37.78, "lon": -122.40},
            {"id": "b", "gers_id": "G2", "lat": 37.79, "lon": -122.41},
        ]
    )
    if "gers_id" not in overture_df.columns:
        overture_df["gers_id"] = overture_df["id"]
    overture_df.to_parquet(overture_src, index=False)

    osm_src = raw / "osm" / "mini_region.parquet"
    osm_df = pd.DataFrame(
        [
            {"osm_id": 1, "lat": 37.7801, "lon": -122.4001, "amenity": "cafe"},
            {"osm_id": 2, "lat": 37.7902, "lon": -122.4102, "amenity": "restaurant"},
        ]
    )
    osm_df.to_parquet(osm_src, index=False)

    cfg = Config.for_test(
        tmp_path,
        overture_places=str(overture_src),
        overture_sample_limit=100,
        osm_extract=str(osm_src),
    )

    task_overture_sample(config=cfg)
    task_osm_extract(config=cfg)
    task_build_silver(config=cfg)
    task_build_gold(config=cfg)

    assert (cfg.local.raw / "overture" / "temp" / "overture_sample.parquet").exists()
    assert (cfg.local.raw / "osm" / "temp" / "osm_pois.parquet").exists()
    assert (cfg.local.silver / "venues.parquet").exists()
    assert (cfg.local.gold / "venues.parquet").exists()
    gold_df = pd.read_parquet(cfg.local.gold / "venues.parquet")
    assert "gold_text" in gold_df.columns
    assert len(gold_df) >= 1


def test_run_local_cli_exits_zero_with_local_sources(tmp_path: Path) -> None:
    """CLI runs and exits 0 when overture and osm sources are local files."""
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "overture").mkdir()
    (raw / "osm").mkdir()

    overture_src = raw / "overture" / "places.parquet"
    pd.DataFrame(
        [{"id": "a", "gers_id": "G1", "lat": 37.78, "lon": -122.40}]
    ).to_parquet(overture_src, index=False)

    osm_src = raw / "osm" / "mini.parquet"
    pd.DataFrame(
        [{"osm_id": 1, "lat": 37.78, "lon": -122.40, "amenity": "cafe"}]
    ).to_parquet(osm_src, index=False)

    import sys
    from io import StringIO

    orig_argv = sys.argv
    orig_stdout, orig_stderr = sys.stdout, sys.stderr
    try:
        sys.argv = [
            "run_local",
            "--date",
            "2024-01-01",  # ignored when overture-source is set
            "--sample-size",
            "100",
            "--data-dir",
            str(tmp_path),
            "--osm-source",
            str(osm_src),
            "--overture-source",
            str(overture_src),
        ]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        exit_code = main()
    finally:
        sys.argv = orig_argv
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr

    assert exit_code == 0
    assert (tmp_path / "data" / "gold" / "venues.parquet").exists()


def test_run_local_with_default_osm_path_after_fetch(tmp_path: Path) -> None:
    """Simulate make pull-data: fetch script writes mini_region.parquet, then pipeline uses default OSM path."""
    if _fetch_osm is None:
        return
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "overture").mkdir()
    (raw / "osm").mkdir()

    # Overture: local source
    overture_src = raw / "overture" / "places.parquet"
    pd.DataFrame(
        [{"id": "a", "gers_id": "G1", "lat": 37.78, "lon": -122.40}]
    ).to_parquet(overture_src, index=False)

    # OSM: run fetch script (mocked) to create default mini_region.parquet
    osm_default = raw / "osm" / "mini_region.parquet"
    fake_overpass = {
        "elements": [
            {
                "type": "node",
                "id": 1,
                "lat": 37.78,
                "lon": -122.40,
                "tags": {"amenity": "cafe"},
            },
            {
                "type": "node",
                "id": 2,
                "lat": 37.79,
                "lon": -122.41,
                "tags": {"amenity": "restaurant"},
            },
        ]
    }
    with patch.object(_fetch_osm, "overpass_query", return_value=fake_overpass):
        with patch.object(sys, "argv", ["fetch_osm", "-o", str(osm_default)]):
            assert _fetch_osm.main() == 0
    assert osm_default.exists()

    # Run pipeline with overture-source only (no osm-source) so it uses default data/raw/osm/mini_region.parquet
    with patch.object(
        sys,
        "argv",
        [
            "run_local",
            "--date",
            "2024-01-01",
            "--sample-size",
            "10",
            "--data-dir",
            str(tmp_path),
            "--overture-source",
            str(overture_src),
        ],
    ):
        exit_code = main()
    assert exit_code == 0
    # cleanup_raw_temp removes temp files after success; gold is the final output
    assert (tmp_path / "data" / "gold" / "venues.parquet").exists()
