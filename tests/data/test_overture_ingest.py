from pathlib import Path

import pandas as pd

from data.overture_ingest import (
    BBox,
    build_overture_parquet_url,
    sample_overture_places_by_bbox,
)


def test_build_overture_parquet_url_shape() -> None:
    """Default use_s3=True returns s3:// so DuckDB can list/read (glob over https 404s)."""
    url = build_overture_parquet_url("2024-01-01")
    assert url.startswith("s3://overturemaps-us-west-2/")
    assert "/release/2024-01-01.0/" in url
    assert "theme=places" in url
    assert "type=place" in url
    assert url.endswith("*.parquet")


def test_build_overture_parquet_url_appends_dot_zero_for_plain_date() -> None:
    """Plain YYYY-MM-DD gets .0 suffix (Overture release format)."""
    url = build_overture_parquet_url("2024-11-14")
    assert "/release/2024-11-14.0/" in url


def test_build_overture_parquet_url_preserves_existing_version_suffix() -> None:
    """If release already contains a dot (e.g. 2024-11-14.0), do not double-append."""
    url = build_overture_parquet_url("2024-03-12.0")
    assert "/release/2024-03-12.0/" in url


def test_build_overture_parquet_url_use_s3_false_returns_https() -> None:
    """use_s3=False returns https URL (e.g. for docs)."""
    url = build_overture_parquet_url(
        "2024-03-12",
        base_url="https://custom-overture.example.com",
        use_s3=False,
    )
    assert url == (
        "https://custom-overture.example.com/release/2024-03-12.0/theme=places/type=place/*.parquet"
    )


def test_sample_overture_places_by_bbox_filters_and_writes(tmp_path: Path) -> None:
    source = tmp_path / "overture_places.parquet"
    df = pd.DataFrame(
        [
            {"id": "a", "name": "Inside", "lon": -122.40, "lat": 37.78},
            {"id": "b", "name": "Outside West", "lon": -123.0, "lat": 37.78},
            {"id": "c", "name": "Outside North", "lon": -122.40, "lat": 38.5},
        ]
    )
    df.to_parquet(source)

    bbox = BBox(minx=-122.5, maxx=-122.3, miny=37.7, maxy=37.9)
    out_path = tmp_path / "sampled" / "places_sf.parquet"

    result_path = sample_overture_places_by_bbox(str(source), bbox, out_path)

    assert result_path == out_path
    assert result_path.exists()

    sampled = pd.read_parquet(result_path)
    assert "gers_id" in sampled.columns  # id is aliased to gers_id for conflation
    assert list(sampled["gers_id"]) == ["a"]
    assert sampled.iloc[0]["name"] == "Inside"
