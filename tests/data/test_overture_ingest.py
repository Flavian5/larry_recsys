from pathlib import Path

import pandas as pd

from data.overture_ingest import BBox, build_overture_parquet_url, sample_overture_places_by_bbox


def test_build_overture_parquet_url_shape() -> None:
    url = build_overture_parquet_url("2024-01-01")
    assert url.startswith("https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-01-01/")
    assert "theme=places" in url
    assert "type=place" in url
    assert url.endswith("*.parquet")


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
    assert list(sampled["id"]) == ["a"]
    assert sampled.iloc[0]["name"] == "Inside"

