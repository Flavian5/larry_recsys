from pathlib import Path

import pandas as pd
import pytest

from data.osm_ingest import extract_osm_pois


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

