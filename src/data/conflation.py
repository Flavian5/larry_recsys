from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol

import pandas as pd


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

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
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


@dataclass(frozen=True)
class SilverPaths:
    overture: Path
    osm: Path
    conflated: Path


def conflate_parquet(
    overture_path: _PathLike,
    osm_path: _PathLike,
    output_path: _PathLike,
    radius_m: float,
) -> Path:
    """
    Convenience function: load Overture/OSM Parquet files, conflate, and write Silver.
    """
    overture_df = pd.read_parquet(overture_path)
    osm_df = pd.read_parquet(osm_path)
    silver_df = spatial_conflate(overture_df, osm_df, radius_m)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(out)
    return out


def format_gold_record(row: pd.Series) -> str:
    """
    Format a single Silver row into a natural language description.

    Expected columns:
    - name
    - category
    - city (optional; may be empty)
    - osm_amenities (list-like; optional)
    - lat, lon
    """
    name = row.get("name", "")
    category = row.get("category", "")
    city = row.get("city", "")
    lat = float(row.get("lat"))
    lon = float(row.get("lon"))
    amenities = row.get("osm_amenities") or []

    parts = [f"{name} is a {category}"]
    city_str = str(city).strip()
    if city_str:
        parts[0] += f" in {city_str}"
    sentence1 = parts[0] + "."

    tags = [str(a) for a in amenities if a]
    if tags:
        tag_str = ", ".join(tags)
        sentence2 = f"It features {tag_str} and is located at ({lat:.5f}, {lon:.5f})."
    else:
        sentence2 = f"It is located at ({lat:.5f}, {lon:.5f})."

    return f"{sentence1} {sentence2}"


def silver_to_gold(silver_path: _PathLike, output_path: _PathLike) -> Path:
    """
    Convert a Silver Parquet file into a Gold Parquet with `gold_text`.
    """
    silver_df = pd.read_parquet(silver_path)
    gold_df = silver_df.copy()
    gold_df["gold_text"] = gold_df.apply(format_gold_record, axis=1)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    gold_df.to_parquet(out)
    return out

