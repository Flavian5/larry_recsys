from __future__ import annotations

from pathlib import Path
from typing import Iterable, Protocol

import pandas as pd


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
