from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from data.osm_ingest import extract_osm_pois, fetch_osm_pois_via_overpass, nodes_to_rows


def test_extract_osm_pois_basic_parquet_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "mini_osm.parquet"
    df = pd.DataFrame(
        [
            {
                "osm_id": 1,
                "lat": 37.78,
                "lon": -122.40,
                "amenity": "restaurant",
                "cuisine": "thai",
                "dog_friendly": True,
                "raw_tags": '{"amenity": "restaurant", "cuisine": "thai", "dog_friendly": "yes"}',
            },
            {
                "osm_id": 2,
                "lat": 37.79,
                "lon": -122.41,
                "amenity": "cafe",
                "cuisine": "coffee",
                "dog_friendly": False,
                "raw_tags": '{"amenity": "cafe", "cuisine": "coffee"}',
            },
        ]
    )
    df.to_parquet(src)

    out = tmp_path / "pois" / "osm_pois.parquet"
    result = extract_osm_pois(src, out)

    assert result == out
    assert out.exists()

    pois = pd.read_parquet(out)
    assert list(pois["osm_id"]) == [1, 2]
    assert list(pois["amenity"]) == ["restaurant", "cafe"]
    assert "raw_tags" in pois.columns


def test_extract_osm_pois_raises_when_core_columns_missing(tmp_path: Path) -> None:
    src = tmp_path / "bad_osm.parquet"
    df = pd.DataFrame([{"lat": 0.0, "lon": 0.0}])
    df.to_parquet(src)

    out = tmp_path / "out.parquet"
    with pytest.raises(ValueError):
        extract_osm_pois(src, out)


def test_nodes_to_rows_builds_expected_columns() -> None:
    """nodes_to_rows maps Overpass node elements to rows with osm_id, lat, lon, amenity, cuisine, dog_friendly."""
    elements = [
        {"type": "node", "id": 101, "lat": 37.78, "lon": -122.40, "tags": {"amenity": "cafe", "cuisine": "coffee"}},
        {"type": "node", "id": 102, "lat": 37.79, "lon": -122.41, "tags": {"amenity": "restaurant", "cuisine": "thai", "dog_friendly": "yes"}},
        {"type": "node", "id": 103, "lat": 37.0, "lon": -122.0},
    ]
    rows = nodes_to_rows(elements)
    assert len(rows) == 3
    assert rows[0]["osm_id"] == 101
    assert rows[0]["amenity"] == "cafe"
    assert rows[1]["dog_friendly"] is True
    assert rows[2]["amenity"] is None


def test_nodes_to_rows_skips_non_nodes_and_missing_coords() -> None:
    elements = [{"type": "way", "id": 1}, {"type": "node", "id": 2}]
    rows = nodes_to_rows(elements)
    assert len(rows) == 0


def test_fetch_osm_pois_via_overpass_writes_parquet(tmp_path: Path) -> None:
    """fetch_osm_pois_via_overpass writes Parquet compatible with extract_osm_pois when overpass is mocked."""
    out = tmp_path / "osm" / "mini.parquet"
    fake = {
        "elements": [
            {"type": "node", "id": 1, "lat": 37.78, "lon": -122.40, "tags": {"amenity": "cafe"}},
        ]
    }
    with patch("data.osm_ingest.overpass_query", return_value=fake):
        fetch_osm_pois_via_overpass((37.7, -122.5, 37.9, -122.3), out)
    assert out.exists()
    df = pd.read_parquet(out)
    assert list(df.columns) == ["osm_id", "lat", "lon", "amenity", "cuisine", "dog_friendly"]
    assert len(df) == 1
    extract_osm_pois(out, tmp_path / "pois.parquet")
    assert (tmp_path / "pois.parquet").exists()


def test_fetch_osm_pois_via_overpass_raises_when_no_elements(tmp_path: Path) -> None:
    with patch("data.osm_ingest.overpass_query", return_value={"elements": []}):
        with pytest.raises(ValueError, match="No nodes"):
            fetch_osm_pois_via_overpass((37.7, -122.5, 37.9, -122.3), tmp_path / "out.parquet")
