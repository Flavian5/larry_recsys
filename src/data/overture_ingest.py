from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

import duckdb
import pandas as pd
from pydantic import BaseModel, model_validator

# HTTPS base (for docs); DuckDB reads via S3 protocol so we use S3 URI for remote pulls
DEFAULT_OVERTURE_PLACES_BASE = "https://overturemaps-us-west-2.s3.amazonaws.com"
DEFAULT_OVERTURE_S3_BUCKET = "overturemaps-us-west-2"


class BBox(BaseModel, frozen=True):
    """Bounding box (minx, maxx, miny, maxy). Validates min <= max for x and y."""

    minx: float
    maxx: float
    miny: float
    maxy: float

    @property
    def width(self) -> float:
        return self.maxx - self.minx

    @property
    def height(self) -> float:
        return self.maxy - self.miny

    @model_validator(mode="after")
    def check_min_max(self) -> "BBox":
        if self.minx > self.maxx:
            raise ValueError("minx must be <= maxx")
        if self.miny > self.maxy:
            raise ValueError("miny must be <= maxy")
        return self


class _PathLike(Protocol):
    def __fspath__(self) -> str:  # pragma: no cover - structural protocol
        ...


def build_overture_parquet_url(
    release_date: str,
    base_url: str | None = None,
    *,
    use_s3: bool = True,
) -> str:
    """
    Build the Overture Places GeoParquet source path for DuckDB.

    use_s3=True (default): returns s3://bucket/release/... so DuckDB can list
    and read multiple parquet files. use_s3=False: returns https://... (for
    docs; single-URL GET with a glob does not work on S3).

    base_url (when use_s3=False) defaults to RPG_OVERTURE_PLACES_BASE_URL then
    DEFAULT_OVERTURE_PLACES_BASE. For S3, bucket is from env or
    DEFAULT_OVERTURE_S3_BUCKET.

    Overture release identifiers use a version suffix (e.g. 2024-11-14.0). If
    release_date is a plain date (YYYY-MM-DD with no dot), .0 is appended.
    """
    release = release_date.strip()
    if release and "." not in release:
        release = f"{release}.0"
    path_suffix = f"release/{release}/theme=places/type=place/*.parquet"
    if use_s3:
        bucket = os.getenv(
            "RPG_OVERTURE_S3_BUCKET", DEFAULT_OVERTURE_S3_BUCKET
        ).strip().rstrip("/")
        return f"s3://{bucket}/{path_suffix}"
    base = base_url or os.getenv(
        "RPG_OVERTURE_PLACES_BASE_URL", DEFAULT_OVERTURE_PLACES_BASE
    )
    return f"{base.rstrip('/')}/{path_suffix}"


def sample_overture_places_by_bbox(
    source: str,
    bbox: BBox,
    output_path: _PathLike,
    *,
    limit: int | None = None,
) -> Path:
    """
    Sample Overture Places within a bounding box and write them to Parquet.

    For local development and tests, `source` can be a local Parquet file or
    glob pattern. For cloud-scale runs, the same function can be used with the
    HTTP URL pattern built by `build_overture_parquet_url` when DuckDB's
    httpfs extension is enabled.

    The input is expected to expose numeric columns `lon` and `lat`.
    If limit is set, at most that many rows are returned (for local sampling).
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        src = source.strip()
        if src.lower().startswith("http") or src.startswith("s3://"):
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
            if src.startswith("s3://"):
                con.execute("SET s3_region='us-west-2'")
        con.execute(
            """
            CREATE TABLE overture_src AS
            SELECT * FROM read_parquet(?)
            """,
            [source],
        )

        sql = """
            SELECT *
            FROM overture_src
            WHERE lon BETWEEN ? AND ?
              AND lat BETWEEN ? AND ?
            """
        params: list[object] = [bbox.minx, bbox.maxx, bbox.miny, bbox.maxy]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        df: pd.DataFrame = con.execute(sql, params).df()
    finally:
        con.close()

    df.to_parquet(out)
    return out
