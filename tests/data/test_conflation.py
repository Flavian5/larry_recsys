from pathlib import Path

import pandas as pd

from data.conflation import (
    _gold_text_vectorized,
    conflate_parquet,
    silver_to_gold,
    spatial_conflate,
    standardize_osm,
    standardize_overture,
)


def test_standardize_overture_normalises_gers_id_and_types() -> None:
    df = pd.DataFrame(
        [
            {"gers_id": "  abc123  ", "lat": 37.78, "lon": -122.40, "city": "SF"},
            {"gers_id": "def456", "lat": 37.79, "lon": -122.41, "city": None},
        ]
    )
    out = standardize_overture(df)
    assert list(out["gers_id"]) == ["ABC123", "DEF456"]
    assert out["lat"].dtype.kind == "f"
    assert out["lon"].dtype.kind == "f"
    assert list(out["city"]) == ["SF", ""]


def test_standardize_osm_normalises_booleans() -> None:
    df = pd.DataFrame(
        [
            {"osm_id": 1, "lat": 37.78, "lon": -122.40, "dog_friendly": "yes"},
            {"osm_id": 2, "lat": 37.79, "lon": -122.41, "dog_friendly": "no"},
        ]
    )
    out = standardize_osm(df)
    assert list(out["dog_friendly"]) == [True, False]


def test_spatial_conflate_matches_within_radius() -> None:
    overture_df = pd.DataFrame(
        [
            {"gers_id": "A1", "lat": 37.78, "lon": -122.40, "city": "SF"},
            {"gers_id": "B2", "lat": 40.0, "lon": -120.0, "city": "Nowhere"},
        ]
    )
    osm_df = pd.DataFrame(
        [
            {
                "osm_id": 10,
                "lat": 37.7801,
                "lon": -122.4001,
                "amenity": "restaurant",
                "dog_friendly": True,
            },
            {
                "osm_id": 20,
                "lat": 41.0,
                "lon": -121.0,
                "amenity": "cafe",
                "dog_friendly": False,
            },
        ]
    )

    silver = spatial_conflate(overture_df, osm_df, radius_m=50)  # 50m radius

    assert list(silver["gers_id"]) == ["A1", "B2"]
    # First record should have one nearby OSM POI
    assert silver.iloc[0]["osm_ids"] == [10]
    assert silver.iloc[0]["osm_amenities"] == ["restaurant"]
    assert bool(silver.iloc[0]["has_dog_friendly"]) is True
    # Second record should have no matches
    assert silver.iloc[1]["osm_ids"] == []
    assert bool(silver.iloc[1]["has_dog_friendly"]) is False


def test_conflate_parquet_roundtrip(tmp_path: Path) -> None:
    overture_path = tmp_path / "overture.parquet"
    osm_path = tmp_path / "osm.parquet"

    pd.DataFrame(
        [
            {"gers_id": "Z1", "lat": 37.78, "lon": -122.40, "city": "SF"},
        ]
    ).to_parquet(overture_path)

    pd.DataFrame(
        [
            {
                "osm_id": 99,
                "lat": 37.78005,
                "lon": -122.40005,
                "amenity": "bar",
                "dog_friendly": False,
            },
        ]
    ).to_parquet(osm_path)

    out = tmp_path / "silver" / "venues.parquet"
    result_path = conflate_parquet(overture_path, osm_path, out, radius_m=30)

    assert result_path == out
    assert out.exists()
    df = pd.read_parquet(out)
    assert list(df["gers_id"]) == ["Z1"]
    assert df.iloc[0]["osm_ids"] == [99]


def test_gold_text_vectorized_single_row() -> None:
    """_gold_text_vectorized produces expected gold text for one row with amenities and city."""
    df = pd.DataFrame(
        [
            {
                "name": "Golden Curry",
                "category": "restaurant",
                "city": "San Francisco",
                "osm_amenities": ["restaurant", "outdoor_seating"],
                "lat": 37.78,
                "lon": -122.40,
            }
        ]
    )
    result = _gold_text_vectorized(df)
    assert len(result) == 1
    text = result.iloc[0]
    assert "Golden Curry is a restaurant in San Francisco." in text
    assert "It features restaurant, outdoor_seating and is located at" in text


def test_gold_text_vectorized_multiple_rows() -> None:
    """_gold_text_vectorized produces correct gold text per row (with and without city/amenities)."""
    df = pd.DataFrame(
        [
            {
                "name": "Cafe A",
                "category": "cafe",
                "city": "SF",
                "osm_amenities": ["cafe"],
                "lat": 37.78,
                "lon": -122.40,
            },
            {
                "name": "Bar B",
                "category": "bar",
                "city": "",
                "osm_amenities": [],
                "lat": 37.79,
                "lon": -122.41,
            },
        ]
    )
    result = _gold_text_vectorized(df)
    assert len(result) == 2
    assert "Cafe A is a cafe in SF." in result.iloc[0]
    assert "It features cafe and is located at" in result.iloc[0]
    assert "Bar B is a bar." in result.iloc[1]
    assert "It is located at (37.79000, -122.41000)." in result.iloc[1]


def test_silver_to_gold_roundtrip(tmp_path: Path) -> None:
    silver_path = tmp_path / "silver.parquet"
    df = pd.DataFrame(
        [
            {
                "gers_id": "G1",
                "name": "Cafe Example",
                "category": "cafe",
                "city": "SF",
                "osm_amenities": ["cafe"],
                "lat": 37.78,
                "lon": -122.40,
            }
        ]
    )
    df.to_parquet(silver_path)

    gold_path = tmp_path / "gold" / "venues.parquet"
    result = silver_to_gold(silver_path, gold_path)

    assert result == gold_path
    assert gold_path.exists()
    gold_df = pd.read_parquet(gold_path)
    assert "gold_text" in gold_df.columns
    assert "Cafe Example is a cafe in SF." in gold_df.iloc[0]["gold_text"]


def test_conflate_parquet_text_mode(tmp_path: Path) -> None:
    overture_path = tmp_path / "overture.parquet"
    osm_path = tmp_path / "osm.parquet"
    pd.DataFrame(
        [{"gers_id": "Z1", "lat": 37.78, "lon": -122.40, "city": "SF"}]
    ).to_parquet(overture_path)
    pd.DataFrame(
        [
            {
                "osm_id": 99,
                "lat": 37.78005,
                "lon": -122.40005,
                "amenity": "bar",
                "dog_friendly": False,
            },
        ]
    ).to_parquet(osm_path)

    out = tmp_path / "venues.jsonl"
    result_path = conflate_parquet(
        overture_path, osm_path, out, radius_m=30, output_format="text"
    )
    assert result_path == out
    assert out.exists()
    content = out.read_text()
    assert "gers_id" in content and "Z1" in content
    assert "osm_ids" in content


def test_silver_to_gold_text_mode(tmp_path: Path) -> None:
    silver_path = tmp_path / "silver.parquet"
    df = pd.DataFrame(
        [
            {
                "gers_id": "G1",
                "name": "Cafe Example",
                "category": "cafe",
                "city": "SF",
                "osm_amenities": ["cafe"],
                "lat": 37.78,
                "lon": -122.40,
            }
        ]
    )
    df.to_parquet(silver_path)

    gold_path = tmp_path / "venues.txt"
    result = silver_to_gold(silver_path, gold_path, output_format="text")

    assert result == gold_path
    assert gold_path.exists()
    lines = gold_path.read_text().strip().split("\n")
    assert len(lines) == 1
    assert "Cafe Example is a cafe in SF." in lines[0]


def test_silver_to_gold_reads_jsonl(tmp_path: Path) -> None:
    silver_path = tmp_path / "silver.jsonl"
    df = pd.DataFrame(
        [
            {
                "gers_id": "G1",
                "name": "Cafe Example",
                "category": "cafe",
                "city": "SF",
                "osm_amenities": ["cafe"],
                "lat": 37.78,
                "lon": -122.40,
            }
        ]
    )
    df.to_json(silver_path, orient="records", lines=True)

    gold_path = tmp_path / "gold.txt"
    result = silver_to_gold(silver_path, gold_path, output_format="text")
    assert result == gold_path
    assert gold_path.exists()
    assert "Cafe Example is a cafe in SF." in gold_path.read_text()
