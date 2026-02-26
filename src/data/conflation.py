from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Protocol

import numpy as np
import pandas as pd
from pydantic import BaseModel


class _PathLike(Protocol):
    def __fspath__(self) -> str:  # pragma: no cover - structural protocol
        ...


def standardize_overture(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise Overture schema for conflation.

    - Ensure `gers_id` is uppercase, stripped string.
    - Keep core columns: `gers_id`, `lat`, `lon`, `city` (if present) and all others unchanged.
    """
    out = df.copy()
    if "gers_id" not in out.columns:
        raise ValueError("Overture input is missing required column 'gers_id'")

    out["gers_id"] = out["gers_id"].astype(str).str.strip().str.upper()

    for col in ("lat", "lon"):
        if col not in out.columns:
            raise ValueError(f"Overture input is missing required column '{col}'")
        out[col] = out[col].astype(float)

    if "city" in out.columns:
        out["city"] = out["city"].fillna("").astype(str)

    return out


def standardize_osm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise OSM schema for conflation.

    - Ensure `osm_id`, `lat`, `lon` exist.
    - Normalise boolean `dog_friendly` if present.
    """
    out = df.copy()
    for col in ("osm_id", "lat", "lon"):
        if col not in out.columns:
            raise ValueError(f"OSM input is missing required column '{col}'")
    out["osm_id"] = out["osm_id"]
    out["lat"] = out["lat"].astype(float)
    out["lon"] = out["lon"].astype(float)

    if "dog_friendly" in out.columns:
        # Accept a mix of bools and truthy strings
        def _to_bool(val: object) -> bool:
            if isinstance(val, bool):
                return val
            if val is None:
                return False
            s = str(val).strip().lower()
            return s in {"1", "true", "yes", "y"}

        out["dog_friendly"] = out["dog_friendly"].map(_to_bool)

    return out


def _haversine_m(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    # Simple haversine distance in metres
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def spatial_conflate(
    overture_df: pd.DataFrame,
    osm_df: pd.DataFrame,
    radius_m: float,
) -> pd.DataFrame:
    """
    Spatially join Overture centroids to nearby OSM POIs within `radius_m`.

    Output schema (minimal for this POC):
    - gers_id
    - lat, lon, city (from Overture)
    - osm_ids: list of matched OSM IDs
    - osm_amenities: list of amenities
    - has_dog_friendly: bool indicating whether any matched POI is dog-friendly
    """
    o_df = standardize_overture(overture_df)
    os_df = standardize_osm(osm_df)

    records: list[dict] = []
    for _, o_row in o_df.iterrows():
        o_lat = float(o_row["lat"])
        o_lon = float(o_row["lon"])
        matched_osm_ids: list[object] = []
        matched_amenities: list[object] = []
        any_dog_friendly = False

        for _, s_row in os_df.iterrows():
            dist = _haversine_m(o_lat, o_lon, float(s_row["lat"]), float(s_row["lon"]))
            if dist <= radius_m:
                matched_osm_ids.append(s_row["osm_id"])
                if "amenity" in s_row:
                    matched_amenities.append(s_row["amenity"])
                if "dog_friendly" in s_row and bool(s_row["dog_friendly"]):
                    any_dog_friendly = True

        rec: dict = {
            "gers_id": o_row["gers_id"],
            "lat": o_lat,
            "lon": o_lon,
            "osm_ids": matched_osm_ids,
            "osm_amenities": matched_amenities,
            "has_dog_friendly": any_dog_friendly,
        }
        if "city" in o_row:
            rec["city"] = o_row["city"]
        records.append(rec)

    df = pd.DataFrame.from_records(records)
    # Ensure `has_dog_friendly` is a plain Python bool dtype where possible
    if "has_dog_friendly" in df.columns:
        df["has_dog_friendly"] = df["has_dog_friendly"].astype(bool)
    return df


class SilverPaths(BaseModel, frozen=True):
    """Validated paths for silver conflation step."""

    overture: Path
    osm: Path
    conflated: Path


def conflate_parquet(
    overture_path: _PathLike,
    osm_path: _PathLike,
    output_path: _PathLike,
    radius_m: float,
    *,
    output_format: str = "parquet",
) -> Path:
    """
    Convenience function: load Overture/OSM Parquet files, conflate, and write Silver.

    output_format: "parquet" (default) or "text". When "text", writes JSONL for local inspection.
    """
    overture_df = pd.read_parquet(overture_path)
    osm_df = pd.read_parquet(osm_path)
    silver_df = spatial_conflate(overture_df, osm_df, radius_m)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "text":
        silver_df.to_json(out, orient="records", lines=True, date_format="iso")
    else:
        silver_df.to_parquet(out)
    return out


def _format_amenities_list(amenities: object) -> str:
    """Format a single row's amenities list for vectorized use (one apply on list column)."""
    if amenities is None:
        return ""
    if not isinstance(amenities, (list, tuple)):
        return str(amenities) if amenities else ""
    return ", ".join(str(a) for a in amenities if a)


def _gold_text_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Build gold_text column with vectorized ops. Uses one .apply only for the
    list column (osm_amenities); rest is vectorized for better performance on large datasets.
    """
    name = df.get("name", pd.Series("", index=df.index)).fillna("").astype(str)
    category = df.get("category", pd.Series("", index=df.index)).fillna("").astype(str)
    city = (
        df.get("city", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    )
    lat = pd.to_numeric(df["lat"], errors="coerce").fillna(0)
    lon = pd.to_numeric(df["lon"], errors="coerce").fillna(0)
    osm_amenities = df.get(
        "osm_amenities",
        pd.Series([[]] * len(df), index=df.index),
    )

    sentence1 = name + " is a " + category
    has_city = city != ""
    sentence1 = sentence1 + np.where(has_city, " in " + city, "")
    sentence1 = sentence1 + "."

    amenities_str = osm_amenities.apply(_format_amenities_list)
    # .5f formatting: 2 column-wise .apply (cheap); only list column is per-row
    lat_s = lat.round(5).apply("{:.5f}".format)
    lon_s = lon.round(5).apply("{:.5f}".format)
    located_at = "(" + lat_s + ", " + lon_s + ")"
    has_amenities = amenities_str.str.len() > 0
    sentence2 = (
        "It features " + amenities_str + " and is located at " + located_at + "."
    ).where(has_amenities, "It is located at " + located_at + ".")

    return sentence1 + " " + sentence2


def _read_silver(path: Path) -> pd.DataFrame:
    """Load silver from Parquet or JSONL."""
    if str(path).endswith(".jsonl"):
        return pd.read_json(path, lines=True)
    return pd.read_parquet(path)


def silver_to_gold(
    silver_path: _PathLike,
    output_path: _PathLike,
    *,
    output_format: str = "parquet",
) -> Path:
    """
    Convert a Silver file (Parquet or JSONL) into Gold with `gold_text`.

    Uses vectorized string ops for large datasets; only the list column (osm_amenities)
    uses a single .apply. output_format: "parquet" (default) or "text".
    """
    silver_df = _read_silver(Path(silver_path))
    gold_df = silver_df.copy()
    gold_df["gold_text"] = _gold_text_vectorized(gold_df)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "text":
        out.write_text("\n".join(gold_df["gold_text"].astype(str)), encoding="utf-8")
    else:
        gold_df.to_parquet(out)
    return out
