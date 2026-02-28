from __future__ import annotations

import math
from pathlib import Path
from collections import Counter
from typing import Iterable, Protocol, Sequence, cast

import h3
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
    - Keep core columns: `gers_id`, `lat`, `lon`, `city` (if present), `theme` (if present) and all others unchanged.
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

    if "theme" in out.columns:
        out["theme"] = out["theme"].fillna("").astype(str)

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
    """Haversine distance in metres (scalar version)."""
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


def _haversine_m_vectorized(
    lat1: np.ndarray,
    lon1: np.ndarray,
    lat2: np.ndarray,
    lon2: np.ndarray,
) -> np.ndarray:
    """Vectorized haversine distance in metres."""
    r = 6371000.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = (
        np.sin(dphi / 2) ** 2
        + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return r * c


def _h3_cell(lat: float, lon: float, res: int) -> str:
    """Return H3 cell (hex string) at given resolution. lat/lon in degrees."""
    return h3.latlng_to_cell(lat, lon, res)


def _h3_ring_cells(cell: str, k: int = 1) -> set[str]:
    """Return cell and its k-ring (cell + neighbors within distance k)."""
    return set(h3.grid_disk(cell, k))


def _aggregate_candidates_to_silver(
    o_df: pd.DataFrame,
    candidates: Sequence[tuple[object, object, object, object]],
) -> pd.DataFrame:
    """Build one row per Overture place with lists of osm_ids, osm_amenities, has_dog_friendly.
    candidates: list of (gers_id, osm_id, amenity, dog_friendly).
    """
    from collections import defaultdict

    # gers_id -> (osm_ids, amenities, any_dog_friendly)
    agg: dict[object, tuple[list[object], list[object], bool]] = defaultdict(
        lambda: ([], [], False)
    )
    for gers_id, osm_id, amenity, dog_friendly in candidates:
        osm_ids, amenities, any_dog = agg[gers_id]
        osm_ids.append(osm_id)
        if amenity is not None:
            amenities.append(amenity)
        if dog_friendly:
            agg[gers_id] = (osm_ids, amenities, True)
        else:
            agg[gers_id] = (osm_ids, amenities, any_dog)

    records: list[dict] = []
    for _, o_row in o_df.iterrows():
        gers_id = o_row["gers_id"]
        osm_ids, amenities, any_dog = agg.get(gers_id, ([], [], False))
        rec: dict = {
            "gers_id": gers_id,
            "lat": float(o_row["lat"]),
            "lon": float(o_row["lon"]),
            "osm_ids": osm_ids,
            "osm_amenities": amenities,
            "has_dog_friendly": any_dog,
        }
        if "city" in o_row:
            rec["city"] = o_row["city"]
        if "theme" in o_row and o_row.get("theme"):
            rec["theme"] = o_row["theme"]
        records.append(rec)

    df = pd.DataFrame.from_records(records)
    if "has_dog_friendly" in df.columns:
        df["has_dog_friendly"] = df["has_dog_friendly"].astype(bool)
    if "theme" in df.columns:
        df["theme"] = df["theme"].fillna("").astype(str)
    return df


def spatial_conflate(
    overture_df: pd.DataFrame,
    osm_df: pd.DataFrame,
    radius_m: float,
    *,
    h3_res: int = 8,
) -> pd.DataFrame:
    """
    Spatially join Overture centroids to nearby OSM POIs within `radius_m`.

    Uses H3 bucketing (res 7 or 8 for POIs) and k-ring(1) for boundary safety;
    candidate pairs are then filtered by haversine distance.

    Output schema (minimal for this POC):
    - gers_id
    - lat, lon, city (from Overture)
    - osm_ids: list of matched OSM IDs
    - osm_amenities: list of amenities
    - has_dog_friendly: bool indicating whether any matched POI is dog-friendly
    """
    o_df = standardize_overture(overture_df)
    os_df = standardize_osm(osm_df)

    # 1. Add primary H3 cell to Overture (using list comprehension for speed)
    o_df = o_df.copy()
    lat_vals = o_df["lat"].values
    lon_vals = o_df["lon"].values
    o_df["_h3_cell"] = [h3.latlng_to_cell(lat, lon, h3_res) for lat, lon in zip(lat_vals, lon_vals)]

    # 2. OSM: add primary cell, then explode to one row per cell in k-ring(1)
    os_df = os_df.copy()
    lat_vals = os_df["lat"].values
    lon_vals = os_df["lon"].values
    os_df["_h3_primary"] = [h3.latlng_to_cell(lat, lon, h3_res) for lat, lon in zip(lat_vals, lon_vals)]
    osm_exploded_rows: list[dict] = []
    for _, row in os_df.iterrows():
        primary = row["_h3_primary"]
        row_dict = row.drop("_h3_primary").to_dict()
        for cell in _h3_ring_cells(primary, 1):
            osm_exploded_rows.append({**row_dict, "_h3_cell": cell})
    osm_exploded = (
        pd.DataFrame(osm_exploded_rows) if osm_exploded_rows else pd.DataFrame()
    )

    if osm_exploded.empty:
        # No OSM data: return one row per Overture with empty matches
        return _aggregate_candidates_to_silver(o_df, [])

    # 3. Join on H3 cell -> candidate pairs (Overture lat/lon, OSM gets lat_osm/lon_osm)
    merged = o_df.merge(
        osm_exploded,
        on="_h3_cell",
        how="left",
        suffixes=("", "_osm"),
    )
    merged = merged.dropna(subset=["osm_id"])  # keep only rows that had a match in join

    if merged.empty:
        return _aggregate_candidates_to_silver(o_df, [])

    # 4. Haversine filter (vectorized)
    merged["_dist_m"] = _haversine_m_vectorized(
        np.asarray(merged["lat"]),
        np.asarray(merged["lon"]),
        np.asarray(merged["lat_osm"]),
        np.asarray(merged["lon_osm"]),
    )
    within = merged[merged["_dist_m"] <= radius_m].copy()
    within = within.drop_duplicates(subset=["gers_id", "osm_id"])

    # 5. Aggregate to one row per Overture place
    candidates = list(
        zip(
            within["gers_id"],
            within["osm_id"],
            within.get("amenity", pd.Series([None] * len(within))),
            within.get("dog_friendly", pd.Series([False] * len(within))),
        )
    )
    return _aggregate_candidates_to_silver(o_df, candidates)


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
    print(f"[conflation] Loading overture: {overture_path}", flush=True)
    overture_df = pd.read_parquet(overture_path)
    print(f"[conflation] Loading OSM: {osm_path}", flush=True)
    osm_df = pd.read_parquet(osm_path)
    print("[conflation] Inputs:", flush=True)
    print(f"  - overture rows: {len(overture_df)}", flush=True)
    if "gers_id" in overture_df.columns:
        try:
            n_unique = (
                overture_df["gers_id"]
                .astype(str)
                .str.strip()
                .str.upper()
                .nunique(dropna=True)
            )
            print(f"  - overture unique gers_id: {n_unique}", flush=True)
        except (KeyError, ValueError, TypeError):
            pass
    print(f"  - osm rows: {len(osm_df)}", flush=True)
    if "osm_id" in osm_df.columns:
        try:
            n_unique = osm_df["osm_id"].nunique(dropna=True)
            print(f"  - osm unique osm_id: {n_unique}", flush=True)
        except (KeyError, ValueError, TypeError):
            pass
    print(f"[conflation] Conflating with radius_m={radius_m} ...", flush=True)
    silver_df = spatial_conflate(overture_df, osm_df, radius_m)
    print(
        f"[conflation] Silver: {len(silver_df)} rows (1 row per Overture place)",
        flush=True,
    )

    # Match coverage summary (kept lightweight; avoids per-row logging)
    if "osm_ids" in silver_df.columns:

        def _list_len(x: object) -> int:
            if x is None:
                return 0
            if isinstance(x, np.ndarray):
                return int(len(x))
            if isinstance(x, (list, tuple)):
                return int(len(x))
            # Unexpected scalar (shouldn't happen); treat as single match
            return 1

        match_counts = silver_df["osm_ids"].map(_list_len)
        n_overture = int(len(silver_df))
        n_matched_overture = int((match_counts > 0).sum())
        pct = (100.0 * n_matched_overture / n_overture) if n_overture else 0.0
        print("[conflation] Match coverage:", flush=True)
        print(
            f"  - overture rows with ≥1 OSM match: {n_matched_overture}/{n_overture} ({pct:.1f}%)",
            flush=True,
        )
        if n_overture:
            try:
                p50 = float(match_counts.quantile(0.50))
                p90 = float(match_counts.quantile(0.90))
                p99 = float(match_counts.quantile(0.99))
                print(
                    "  - matches per overture row: "
                    f"mean={match_counts.mean():.2f}, p50={p50:.0f}, p90={p90:.0f}, p99={p99:.0f}, max={int(match_counts.max())}",
                    flush=True,
                )
            except (KeyError, ValueError, TypeError):
                pass

        # Unique OSM POIs that got attached anywhere
        try:
            matched_osm_ids: set[object] = set()
            for ids in silver_df["osm_ids"]:
                if ids is None:
                    continue
                if isinstance(ids, np.ndarray):
                    ids = ids.tolist()
                if isinstance(ids, (list, tuple)):
                    matched_osm_ids.update(ids)
                else:
                    matched_osm_ids.add(ids)
            n_unique_matched_osm = len(matched_osm_ids)
            print(
                f"  - unique OSM POIs matched to any Overture row: {n_unique_matched_osm}",
                flush=True,
            )
        except (KeyError, ValueError, TypeError):
            pass

    if "osm_amenities" in silver_df.columns:
        try:
            c: Counter[str] = Counter()
            for ams in silver_df["osm_amenities"]:
                if ams is None:
                    continue
                if isinstance(ams, np.ndarray):
                    ams = ams.tolist()
                if isinstance(ams, (list, tuple)):
                    c.update(str(a) for a in ams if a)
                else:
                    if ams:
                        c.update([str(ams)])
            if c:
                top = ", ".join(f"{k}({v})" for k, v in c.most_common(10))
                print(f"[conflation] Top matched OSM amenities: {top}", flush=True)
        except (KeyError, ValueError, TypeError):
            pass

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "text":
        silver_df.to_json(out, orient="records", lines=True, date_format="iso")
    else:
        silver_df.to_parquet(out)
    print(f"[conflation] Wrote {out}", flush=True)
    return out


def _format_amenities_list(amenities: object) -> str:
    """Format a single row's amenities list for vectorized use (one apply on list column)."""
    if amenities is None:
        return ""
    if isinstance(amenities, np.ndarray):
        amenities = amenities.tolist()
    if not isinstance(amenities, (list, tuple)):
        return str(amenities) if amenities else ""
    return ", ".join(str(a) for a in amenities if a)


def _gold_text_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Build gold_text column with vectorized ops. Uses one .apply only for the
    list column (osm_amenities); rest is vectorized for better performance on large datasets.

    Incorporates theme information when present (e.g., "administrative boundary", "land", "water").
    """
    name = df.get("name", pd.Series("", index=df.index)).fillna("").astype(str)
    category = df.get("category", pd.Series("", index=df.index)).fillna("").astype(str)
    city = (
        df.get("city", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    )
    theme = df.get("theme", pd.Series("", index=df.index)).fillna("").astype(str)
    lat = pd.to_numeric(df["lat"], errors="coerce").fillna(0)
    lon = pd.to_numeric(df["lon"], errors="coerce").fillna(0)
    osm_amenities = df.get(
        "osm_amenities",
        pd.Series([[]] * len(df), index=df.index),
    )

    # Build item type description based on theme and category
    # Theme overrides category for non-place themes
    item_type = np.where(theme != "", theme + " " + category, category).astype(str)
    # Clean up double spaces
    item_type = pd.Series(item_type, index=df.index).str.replace(r"\s+", " ", regex=True)

    sentence1 = name + " is a " + item_type
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
    print(f"[gold] Loading silver: {silver_path}", flush=True)
    silver_df = _read_silver(Path(silver_path))
    print(f"[gold] Building gold_text for {len(silver_df)} rows ...", flush=True)
    gold_df = silver_df.copy()
    gold_df["gold_text"] = _gold_text_vectorized(gold_df)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "text":
        out.write_text("\n".join(gold_df["gold_text"].astype(str)), encoding="utf-8")
    else:
        gold_df.to_parquet(out)
    print(f"[gold] Wrote {out}", flush=True)
    return out
