#!/usr/bin/env python3
"""
Fetch a small OSM region (POIs) via Overpass API and save as Parquet for the pipeline.

Usage:
  python scripts/fetch_osm_mini_region.py -o data/raw/osm/mini_region.parquet
  python scripts/fetch_osm_mini_region.py --bbox 37.2,-122.52,37.82,-122.35 -o data/raw/osm/mini_region.parquet

Default bbox is SF Bay Area (approx). Output has osm_id, lat, lon, amenity, cuisine, dog_friendly
so it matches what data/osm_ingest.py and conflation expect.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

# Optional: use pandas if available (we have it in the project)
try:
    import pandas as pd
except ImportError:
    pd = None


OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# SF Bay Area (south, west, north, east) - roughly San Francisco + Oakland/Berkeley
DEFAULT_BBOX = (37.2, -122.52, 37.82, -122.35)


def overpass_query(bbox: tuple[float, float, float, float]) -> dict:
    south, west, north, east = bbox
    query = f"""
    [out:json][timeout:120];
    node["amenity"]({south},{west},{north},{east});
    out body;
    """
    req = urllib.request.Request(
        OVERPASS_URL,
        data=query.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def nodes_to_rows(elements: list) -> list[dict]:
    rows = []
    for el in elements:
        if el.get("type") != "node":
            continue
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags") or {}
        # dog_friendly: common OSM tags (not standard; we map common ones)
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


def main() -> int:
    p = argparse.ArgumentParser(
        description="Fetch OSM POIs for a bbox via Overpass and write Parquet."
    )
    p.add_argument(
        "--bbox",
        type=str,
        default=None,
        help="Bbox as south,west,north,east (default: SF Bay Area). Example: 37.2,-122.52,37.82,-122.35",
    )
    p.add_argument(
        "-o",
        "--output",
        type=str,
        default="data/raw/osm/mini_region.parquet",
        help="Output Parquet path (default: data/raw/osm/mini_region.parquet)",
    )
    args = p.parse_args()

    if args.bbox:
        parts = [float(x.strip()) for x in args.bbox.split(",")]
        if len(parts) != 4:
            print("bbox must be south,west,north,east (4 numbers)", file=sys.stderr)
            return 1
        bbox = tuple(parts)
    else:
        bbox = DEFAULT_BBOX

    print(f"Querying Overpass for bbox {bbox} ...", flush=True)
    try:
        data = overpass_query(bbox)
    except Exception as e:
        print(f"Overpass request failed: {e}", file=sys.stderr)
        return 1

    elements = data.get("elements") or []
    rows = nodes_to_rows(elements)
    print(f"Got {len(rows)} POI nodes", flush=True)

    if not rows:
        print(
            "No nodes in bbox; try a larger area or check coordinates.", file=sys.stderr
        )
        return 1

    if pd is None:
        print(
            "pandas required to write Parquet. pip install pandas pyarrow",
            file=sys.stderr,
        )
        return 1

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(out, index=False)
    print(f"Wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
