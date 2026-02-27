"""Tests for scripts/fetch_osm_mini_region.py (OSM mini-region fetch for pipeline)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from data.osm_ingest import nodes_to_rows

# Load the script as a module (it lives outside src/)
_SCRIPT_DIR = Path(__file__).resolve().parents[2] / "scripts"
_SCRIPT_PATH = _SCRIPT_DIR / "fetch_osm_mini_region.py"
spec = importlib.util.spec_from_file_location("fetch_osm_mini_region", _SCRIPT_PATH)
assert spec and spec.loader, f"Could not load {_SCRIPT_PATH}"
fetch_osm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_osm)


def test_nodes_to_rows_builds_expected_columns() -> None:
    """nodes_to_rows maps Overpass node elements to rows with osm_id, lat, lon, amenity, cuisine, dog_friendly."""
    elements = [
        {
            "type": "node",
            "id": 101,
            "lat": 37.78,
            "lon": -122.40,
            "tags": {"amenity": "cafe", "cuisine": "coffee"},
        },
        {
            "type": "node",
            "id": 102,
            "lat": 37.79,
            "lon": -122.41,
            "tags": {"amenity": "restaurant", "cuisine": "thai", "dog_friendly": "yes"},
        },
        {"type": "node", "id": 103, "lat": 37.0, "lon": -122.0},
    ]
    rows = nodes_to_rows(elements)
    assert len(rows) == 3
    assert rows[0]["osm_id"] == 101
    assert rows[0]["lat"] == 37.78
    assert rows[0]["lon"] == -122.40
    assert rows[0]["amenity"] == "cafe"
    assert rows[0]["cuisine"] == "coffee"
    assert rows[0]["dog_friendly"] is None
    assert rows[1]["dog_friendly"] is True
    assert rows[2]["amenity"] is None


def test_nodes_to_rows_skips_non_nodes_and_missing_coords() -> None:
    elements = [
        {"type": "way", "id": 1},
        {"type": "node", "id": 2},
    ]
    rows = nodes_to_rows(elements)
    assert len(rows) == 0  # way skipped; node has no lat/lon


def test_main_writes_parquet_compatible_with_osm_ingest(tmp_path: Path) -> None:
    """Script output can be read by extract_osm_pois (pipeline OSM step)."""
    out_parquet = tmp_path / "osm" / "mini_region.parquet"
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    fake_response = {
        "elements": [
            {
                "type": "node",
                "id": 1,
                "lat": 37.78,
                "lon": -122.40,
                "tags": {"amenity": "cafe"},
            },
        ]
    }
    with patch("data.osm_ingest.overpass_query", return_value=fake_response):
        with patch.object(
            sys,
            "argv",
            [
                "fetch_osm",
                "--bbox",
                "37.77,-122.41,37.79,-122.39",
                "-o",
                str(out_parquet),
            ],
        ):
            exit_code = fetch_osm.main()
    assert exit_code == 0
    assert out_parquet.exists()
    df = pd.read_parquet(out_parquet)
    assert list(df.columns) == [
        "osm_id",
        "lat",
        "lon",
        "amenity",
        "cuisine",
        "dog_friendly",
    ]
    assert len(df) == 1
    assert int(df.iloc[0]["osm_id"]) == 1
    assert df.iloc[0]["amenity"] == "cafe"

    # Pipeline's extract_osm_pois expects osm_id, lat, lon and optional tag columns
    from data.osm_ingest import extract_osm_pois

    extracted = tmp_path / "osm_pois.parquet"
    extract_osm_pois(out_parquet, extracted)
    assert extracted.exists()
    out_df = pd.read_parquet(extracted)
    assert (
        "osm_id" in out_df.columns
        and "lat" in out_df.columns
        and "lon" in out_df.columns
    )


def test_main_exits_nonzero_when_no_elements(tmp_path: Path) -> None:
    with patch("data.osm_ingest.overpass_query", return_value={"elements": []}):
        with patch.object(
            sys, "argv", ["fetch_osm", "-o", str(tmp_path / "out.parquet")]
        ):
            exit_code = fetch_osm.main()
    assert exit_code == 1
