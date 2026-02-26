"""
Local runner: pull from actual Overture/OSM sources (date + sample size) and run the DAG in process.

Usage:
  python -m pipelines.run_local --date 2024-01-01 --sample-size 5000 --data-dir ./data
  # Or after pip install -e .:
  rpg-run-local --date 2024-01-01 --sample-size 5000

For Overture, the release date selects the Overture Maps release. For OSM, pass a path or URI
to a Parquet extract via --osm-source (default: data/raw/osm/mini_region.parquet if present).
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from config.data_foundation import Config

# Import task callables so we run the same logic as the DAG
from pipelines.airflow.dags.rpg_data_foundation_dag import (
    task_build_gold,
    task_build_silver,
    task_cleanup_raw_temp,
    task_overture_sample,
    task_osm_extract,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run data foundation pipeline locally: pull Overture (by date + sample size) and OSM, then build silver and gold."
    )
    p.add_argument(
        "--date",
        required=True,
        help="Overture Maps release date (e.g. 2024-01-01). Used to build the Overture Places Parquet URL.",
    )
    p.add_argument(
        "--sample-size",
        type=int,
        default=10_000,
        help="Max number of Overture places to sample (default: 10000).",
    )
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path("."),
        help="Base directory for data/raw, data/silver, data/gold (default: current dir).",
    )
    p.add_argument(
        "--osm-source",
        type=str,
        default="",
        help="Path or URI to OSM extract Parquet. If not set, uses data/raw/osm/mini_region.parquet.",
    )
    p.add_argument(
        "--overture-source",
        type=str,
        default="",
        help="Override Overture source (local path or URL). If not set, uses --date to build the release URL.",
    )
    p.add_argument(
        "--output-format",
        choices=("parquet", "text"),
        default="parquet",
        help="Output format for silver/gold (default: parquet).",
    )
    p.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma-separated task names to run only (e.g. overture_sample, osm_extract). If not set, runs full pipeline.",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    base_dir = args.data_dir.resolve()

    cfg = Config.for_test(
        base_dir,
        overture_release_date=args.date.strip() if not args.overture_source else "",
        overture_sample_limit=args.sample_size,
        overture_places=args.overture_source.strip(),
        osm_extract=args.osm_source.strip() or "",
        output_format=args.output_format,
    )

    all_tasks = [
        ("overture_sample", task_overture_sample),
        ("osm_extract", task_osm_extract),
        ("build_silver", task_build_silver),
        ("build_gold", task_build_gold),
        ("cleanup_raw_temp", task_cleanup_raw_temp),
    ]
    only_names = [s.strip() for s in args.only.split(",") if s.strip()]
    if only_names:
        name_to_task = dict(all_tasks)
        tasks = []
        for name in only_names:
            if name not in name_to_task:
                print(
                    f"Unknown task: {name}. Valid: {list(name_to_task)}",
                    file=sys.stderr,
                )
                return 1
            tasks.append((name, name_to_task[name]))
    else:
        tasks = all_tasks
    for name, task_fn in tasks:
        print(f"\n--- {name} ---", flush=True)
        t0 = time.perf_counter()
        try:
            task_fn(config=cfg)
        except Exception as e:
            print(
                f"Task {name} failed after {time.perf_counter() - t0:.1f}s: {e}",
                file=sys.stderr,
            )
            return 1
        elapsed = time.perf_counter() - t0
        print(f"{name} finished in {elapsed:.1f}s", flush=True)
        if name == "overture_sample":
            p = cfg.local.raw / "overture" / "temp" / "overture_sample.parquet"
            print(f"  -> {p} (exists: {p.exists()})", flush=True)
        elif name == "osm_extract":
            p = cfg.local.raw / "osm" / "temp" / "osm_pois.parquet"
            print(f"  -> {p} (exists: {p.exists()})", flush=True)
        elif name == "build_silver":
            print(f"  -> {cfg.local.silver / cfg.silver_venues_filename()}", flush=True)
        elif name == "build_gold":
            print(f"  -> {cfg.local.gold / cfg.gold_venues_filename()}", flush=True)
    print("\nDone.", flush=True)
    print("Silver:", cfg.local.silver / cfg.silver_venues_filename())
    print("Gold:", cfg.local.gold / cfg.gold_venues_filename())
    return 0


if __name__ == "__main__":
    sys.exit(main())
