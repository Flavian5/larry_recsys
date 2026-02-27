#!/usr/bin/env python3
"""
Fetch a small OSM region (POIs) via Overpass API and save as Parquet for the pipeline.

Uses RPG_BBOX and RPG_OVERPASS_URL from environment (.env supported). Same bbox
is used for Overture sample when running the full pipeline.

Usage:
  python scripts/fetch_osm_mini_region.py -o data/raw/osm/mini_region.parquet
  RPG_BBOX=37.2,-122.52,37.82,-122.35 python scripts/fetch_osm_mini_region.py -o data/raw/osm/mini_region.parquet
  python scripts/fetch_osm_mini_region.py --bbox 37.2,-122.52,37.82,-122.35 -o data/raw/osm/mini_region.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Load from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config.data_foundation import Config
from data.osm_ingest import fetch_osm_pois_via_overpass


def main() -> int:
    p = argparse.ArgumentParser(
        description="Fetch OSM POIs for a bbox via Overpass (uses RPG_BBOX from env by default)."
    )
    p.add_argument(
        "--bbox",
        type=str,
        default=None,
        help="Override bbox as south,west,north,east (default: from RPG_BBOX in .env). Example: 37.2,-122.52,37.82,-122.35",
    )
    p.add_argument(
        "-o",
        "--output",
        type=str,
        default="data/raw/osm/mini_region.parquet",
        help="Output Parquet path (default: data/raw/osm/mini_region.parquet)",
    )
    args = p.parse_args()

    cfg = Config.from_env(Path(".").resolve())
    if args.bbox:
        parts = [float(x.strip()) for x in args.bbox.split(",")]
        if len(parts) != 4:
            print("bbox must be south,west,north,east (4 numbers)", file=sys.stderr)
            return 1
        bbox_swne = tuple(parts)
    else:
        # Config has (minx, maxx, miny, maxy) -> (south, west, north, east) = (miny, minx, maxy, maxx)
        bbox_swne = (cfg.bbox_miny, cfg.bbox_minx, cfg.bbox_maxy, cfg.bbox_maxx)
    overpass_url = cfg.datasets.overpass_url

    print(f"Querying Overpass for bbox {bbox_swne} ...", flush=True)
    try:
        out = fetch_osm_pois_via_overpass(
            bbox_swne,
            args.output,
            overpass_url=overpass_url,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Overpass request failed: {e}", file=sys.stderr)
        return 1
    print(f"Wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
