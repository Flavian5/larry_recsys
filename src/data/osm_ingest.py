from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Iterable, Protocol

import pandas as pd


def overpass_query(
    bbox_swne: tuple[float, float, float, float],
    *,
    overpass_url: str = "https://overpass-api.de/api/interpreter",
    timeout_sec: int = 120,
) -> dict:
    """Query Overpass API for nodes with amenity in bbox. bbox_swne = (south, west, north, east)."""
    south, west, north, east = bbox_swne
    query = f"""
    [out:json][timeout:{timeout_sec}];
    node["amenity"]({south},{west},{north},{east});
    out body;
    """
    req = urllib.request.Request(
        overpass_url,
        data=query.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def nodes_to_rows(elements: list) -> list[dict]:
    """Convert Overpass node elements to rows with osm_id, lat, lon, amenity, cuisine, dog_friendly."""
    rows = []
    for el in elements:
        if el.get("type") != "node":
            continue
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags") or {}
        dog = tags.get("dog_friendly") or tags.get("dogs") or None
        rows.append(
            {
                "osm_id": el["id"],
                "lat": float(lat),
                "lon": float(lon),
                "amenity": tags.get("amenity"),
                "cuisine": tags.get("cuisine"),
                "dog_friendly": (
                    str(dog).lower() in ("yes", "true", "1") if dog else None
                ),
            }
        )
    return rows


def fetch_osm_pois_via_overpass(
    bbox_swne: tuple[float, float, float, float],
    output_path: Path | str,
    *,
    overpass_url: str = "https://overpass-api.de/api/interpreter",
) -> Path:
    """
    Fetch OSM POIs for a bbox via Overpass API and write Parquet.
    bbox_swne = (south, west, north, east). Same schema as extract_osm_pois input.
    """
    data = overpass_query(bbox_swne, overpass_url=overpass_url)
    elements = data.get("elements") or []
    rows = nodes_to_rows(elements)
    if not rows:
        raise ValueError(
            "No nodes in bbox; try a larger area or check coordinates."
        )
    dst = Path(output_path)
    dst.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(dst, index=False)
    return dst


class _PathLike(Protocol):
    def __fspath__(self) -> str:  # pragma: no cover - structural protocol
        ...


def extract_osm_pois(
    input_path: _PathLike,
    output_path: _PathLike,
    tag_keys: Iterable[str] | None = None,
) -> Path:
    """
    Extract OSM POIs into a tabular form suitable for conflation.

    For this POC implementation we assume that `input_path` is already a
    small, local Parquet file containing OSM-like records with at least:
    - `osm_id`
    - `lat`
    - `lon`
    - optional tag columns such as `amenity`, `cuisine`, `dog_friendly`
    - optional `raw_tags` column with the original tag map encoded as JSON.

    In a future iteration this function can be extended to support `.osm.pbf`
    or `.osm.xml` via `pyosmium` or `osmium-tool`. The tests drive the current
    behaviour using Parquet fixtures to keep the dependency surface small.
    """
    src = Path(input_path)
    dst = Path(output_path)
    dst.parent.mkdir(parents=True, exist_ok=True)

    print(f"[osm] Reading from {src} ...", flush=True)
    df = pd.read_parquet(src)
    print(f"[osm] Read {len(df)} rows", flush=True)

    keys = (
        list(tag_keys)
        if tag_keys is not None
        else ["amenity", "cuisine", "dog_friendly"]
    )

    for key in keys:
        if key not in df.columns:
            df[key] = None

    # Ensure required core columns exist
    for col in ("osm_id", "lat", "lon"):
        if col not in df.columns:
            raise ValueError(f"Expected column '{col}' in OSM input Parquet at {src}")

    # Normalise column order
    base_cols: list[str] = ["osm_id", "lat", "lon"]
    ordered_cols: list[str] = base_cols + keys
    if "raw_tags" in df.columns:
        ordered_cols.append("raw_tags")

    out_df = df[ordered_cols]
    print(f"[osm] Writing to {dst} ...", flush=True)
    out_df.to_parquet(dst)
    print(f"[osm] Wrote {dst} ({len(out_df)} rows)", flush=True)
    return dst
