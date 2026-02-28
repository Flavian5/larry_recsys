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
    timeout_sec: int = 180,
) -> dict:
    """Query Overpass API for nodes and ways with POI tags in bbox. bbox_swne = (south, west, north, east)."""
    south, west, north, east = bbox_swne
    # Expanded query: nodes and ways with amenity, shop, leisure, tourism tags
    query = f"""
    [out:json][timeout:{timeout_sec}][maxsize:536870912];
    (
      node["amenity"]({south},{west},{north},{east});
      node["shop"]({south},{west},{north},{east});
      node["leisure"]({south},{west},{north},{east});
      node["tourism"]({south},{west},{north},{east});
      way["amenity"]({south},{west},{north},{east});
      way["shop"]({south},{west},{north},{east});
      way["leisure"]({south},{west},{north},{east});
      way["tourism"]({south},{west},{north},{east});
    );
    out center;
    """
    req = urllib.request.Request(
        overpass_url,
        data=query.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def _get_center(el: dict) -> tuple[float, float] | None:
    """Extract center point from Overpass element (node or way)."""
    # For nodes, use lat/lon directly
    lat = el.get("lat")
    lon = el.get("lon")
    if lat is not None and lon is not None:
        return float(lat), float(lon)
    
    # For ways, use center from geometry (provided by "out center")
    center = el.get("center")
    if center:
        return float(center["lat"]), float(center["lon"])
    
    return None


def elements_to_rows(elements: list) -> list[dict]:
    """Convert Overpass elements (nodes and ways) to rows with osm_id, lat, lon, amenity, shop, leisure, tourism."""
    rows = []
    for el in elements:
        el_type = el.get("type")
        
        # Extract center point
        center = _get_center(el)
        if center is None:
            continue
        lat, lon = center
        
        tags = el.get("tags") or {}
        
        # Determine the primary POI type (check multiple tag keys)
        amenity = tags.get("amenity")
        shop = tags.get("shop")
        leisure = tags.get("leisure")
        tourism = tags.get("tourism")
        
        # Determine main category for conflation (use any available POI tag)
        category = amenity or shop or leisure or tourism or None
        
        # For backward compatibility with conflation code, set amenity to category
        # This ensures shop/leisure/tourism POIs are included in osm_amenities
        amenity_for_matching = category
        
        dog = tags.get("dog_friendly") or tags.get("dogs") or None
        
        rows.append(
            {
                "osm_id": el["id"],
                "osm_type": el_type,  # 'node' or 'way'
                "lat": lat,
                "lon": lon,
                "category": category,
                "amenity": amenity_for_matching,  # Include all POI types for conflation
                "shop": shop,
                "leisure": leisure,
                "tourism": tourism,
                "name": tags.get("name"),
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
    rows = elements_to_rows(elements)
    if not rows:
        raise ValueError(
            "No elements in bbox; try a larger area or check coordinates."
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
