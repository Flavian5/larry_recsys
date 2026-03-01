#!/usr/bin/env python3
"""
Data inspection script for sanity checking pipeline outputs.

Verifies that data files exist in raw/silver/gold directories and reports
on their schema and row counts.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.data_foundation import Config


def list_parquet_files(directory: Path) -> list[Path]:
    """Recursively find all parquet files in a directory."""
    if not directory.exists():
        return []
    return list(directory.rglob("*.parquet"))


def inspect_parquet(path: Path) -> dict:
    """Inspect a parquet file and return summary info."""
    try:
        df = pd.read_parquet(path)
        return {
            "path": path,
            "exists": True,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict(),
        }
    except Exception as e:
        return {
            "path": path,
            "exists": True,
            "error": str(e),
        }


def print_file_info(info: dict, verbose: bool = False) -> None:
    """Print information about a parquet file."""
    path = info["path"]
    rel_path = path.relative_to(Path.cwd()) if path.is_absolute() else path

    if "error" in info:
        print(f"  {rel_path}: ERROR - {info['error']}")
        return

    print(f"  {rel_path}")
    print(f"    Rows: {info['rows']}")
    print(
        f"    Columns ({len(info['columns'])}): {info['columns'][:5]}{'...' if len(info['columns']) > 5 else ''}"
    )
    if verbose:
        print("    Types:")
        for col, dtype in list(info["dtypes"].items())[:10]:
            print(f"      {col}: {dtype}")


def validate_pipeline_counts(config: Config) -> dict:
    """Check row counts across standard pipeline files."""
    raw = config.local.raw
    silver = config.local.silver
    gold = config.local.gold

    paths = {
        "overture_sample": raw / "overture" / "temp" / "overture_sample.parquet",
        "overture_combined": raw / "overture" / "temp" / "overture_combined.parquet",
        "osm_pois": raw / "osm" / "temp" / "osm_pois.parquet",
        "silver": silver / "venues.parquet",
        "gold": gold / "venues.parquet",
    }

    counts = {}
    for name, path in paths.items():
        if path.exists():
            try:
                counts[name] = len(pd.read_parquet(path))
            except Exception:
                counts[name] = None
        else:
            counts[name] = None

    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Inspect data foundation pipeline outputs."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("."),
        help="Data directory (default: current directory)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed output including dtypes",
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Only show config and paths, don't inspect files",
    )

    args = parser.parse_args()

    # Resolve data directory
    data_dir = args.data_dir.resolve()

    # Load config
    try:
        config = Config.from_env(data_dir)
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        return 1

    print("=" * 60)
    print("DATA INSPECTION")
    print("=" * 60)
    print(f"Data dir: {data_dir}")
    print(f"Config env: {config.env}")
    print(f"Output format: {config.local_output_format}")
    print(
        f"BBOX: ({config.bbox_minx}, {config.bbox_miny}) to ({config.bbox_maxx}, {config.bbox_maxy})"
    )
    print()

    # Directory paths
    raw_dir = config.local.raw
    silver_dir = config.local.silver
    gold_dir = config.local.gold

    print("Directories:")
    print(f"  raw:    {raw_dir}")
    print(f"  silver: {silver_dir}")
    print(f"  gold:   {gold_dir}")
    print()

    if args.config_only:
        return 0

    # List parquet files in each directory
    for name, directory in [
        ("RAW", raw_dir),
        ("SILVER", silver_dir),
        ("GOLD", gold_dir),
    ]:
        print(f"{name} FILES:")
        files = list_parquet_files(directory)
        if not files:
            print("  (none found)")
        else:
            for f in files:
                info = inspect_parquet(f)
                print_file_info(info, args.verbose)
        print()

    # Validate pipeline counts
    print("PIPELINE COUNTS:")
    counts = validate_pipeline_counts(config)
    for name, count in counts.items():
        status = str(count) if count is not None else "N/A"
        print(f"  {name}: {status}")

    # Check consistency
    if counts.get("overture_sample") and counts.get("silver"):
        match = counts["silver"] == counts["overture_sample"]
        print(f"  silver == overture_sample: {match}")
    if counts.get("silver") and counts.get("gold"):
        match = counts["gold"] == counts["silver"]
        print(f"  gold == silver: {match}")

    print()
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
