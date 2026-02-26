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

    For local development and tests, `source` can be a local Parquet file with
    columns `lon`, `lat`, and `id` (or `gers_id`). For remote Overture data,
    the schema uses `bbox` (struct: xmin, xmax, ymin, ymax) and `id`; we derive
    lon/lat from bbox centroid and alias id -> gers_id so the output matches
    what the conflation pipeline expects (gers_id, lat, lon).
    """
    import time

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    src = source.strip()
    if src.startswith("s3://"):
        print(f"[overture] Reading from S3 (this may take several minutes)...", flush=True)
        print(f"[overture] Source: {src}", flush=True)
    else:
        print(f"[overture] Reading from local: {src}", flush=True)
    t0 = time.perf_counter()

    con = duckdb.connect()
    try:
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
        load_sec = time.perf_counter() - t0
        n_total = con.execute("SELECT COUNT(*) FROM overture_src").fetchone()[0]
        print(f"[overture] Loaded {n_total} rows in {load_sec:.1f}s", flush=True)

        # Overture Places schema: bbox (xmin,xmax=lon; ymin,ymax=lat), id. No lon/lat columns.
        # If source has bbox, derive lon/lat from centroid; else use lon/lat (local fixtures).
        cols = [c[0] for c in con.execute("DESCRIBE overture_src").fetchall()]
        if "bbox" in cols:
            sql = """
                SELECT
                    id AS gers_id,
                    (bbox.xmin + bbox.xmax) / 2 AS lon,
                    (bbox.ymin + bbox.ymax) / 2 AS lat
                FROM overture_src
                WHERE bbox IS NOT NULL
                  AND (bbox.xmin + bbox.xmax) / 2 BETWEEN ? AND ?
                  AND (bbox.ymin + bbox.ymax) / 2 BETWEEN ? AND ?
                """
        else:
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
        if "gers_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "gers_id"})
    finally:
        con.close()

    n_out = len(df)
    print(f"[overture] Filtered to {n_out} rows (bbox + limit)", flush=True)
    print(f"[overture] Writing to {out} ...", flush=True)
    df.to_parquet(out)
    print(f"[overture] Wrote {out} ({n_out} rows)", flush=True)
    return out
