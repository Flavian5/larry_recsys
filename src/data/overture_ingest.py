from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import duckdb
import pandas as pd


@dataclass(frozen=True)
class BBox:
    minx: float
    maxx: float
    miny: float
    maxy: float


class _PathLike(Protocol):
    def __fspath__(self) -> str:  # pragma: no cover - structural protocol
        ...


def build_overture_parquet_url(release_date: str) -> str:
    """
    Build the public HTTP URL pattern for Overture Places GeoParquet.

    This follows the documented layout, but the function is kept pure so that
    tests only need to assert on the returned string, not on network access.
    """
    release = release_date.strip()
    base = "https://overturemaps-us-west-2.s3.amazonaws.com"
    return f"{base}/release/{release}/theme=places/type=place/*.parquet"


def sample_overture_places_by_bbox(
    source: str,
    bbox: BBox,
    output_path: _PathLike,
) -> Path:
    """
    Sample Overture Places within a bounding box and write them to Parquet.

    For local development and tests, `source` can be a local Parquet file or
    glob pattern. For cloud-scale runs, the same function can be used with the
    HTTP URL pattern built by `build_overture_parquet_url` when DuckDB's
    httpfs extension is enabled.

    The input is expected to expose numeric columns `lon` and `lat`.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE overture_src AS
            SELECT * FROM read_parquet(?)
            """,
            [source],
        )

        df: pd.DataFrame = con.execute(
            """
            SELECT *
            FROM overture_src
            WHERE lon BETWEEN ? AND ?
              AND lat BETWEEN ? AND ?
            """,
            [bbox.minx, bbox.maxx, bbox.miny, bbox.maxy],
        ).df()
    finally:
        con.close()

    df.to_parquet(out)
    return out

