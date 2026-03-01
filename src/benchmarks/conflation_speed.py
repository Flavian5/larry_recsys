"""
Benchmark script for conflation speed testing.

Usage:
    python -m benchmarks.conflation_speed --sample-size 10000
    python -m benchmarks.conflation_speed --sample-sizes 10000,100000,250000

This runs the full pipeline with different sample sizes and reports timing for each step.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.data_foundation import Config
from pipelines.airflow.dags.rpg_data_foundation_dag import (
    task_build_gold,
    task_build_silver,
    task_overture_sample,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark conflation speed with different sample sizes"
    )
    p.add_argument(
        "--sample-sizes",
        type=str,
        default="10000",
        help="Comma-separated sample sizes to test (default: 10000)",
    )
    p.add_argument(
        "--date",
        default="2026-01-21",
        help="Overture release date (default: 2026-01-21)",
    )
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Base directory for data (default: data)",
    )
    return p.parse_args()


def run_benchmark(sample_size: int, date: str, data_dir: Path) -> dict:
    """Run a single benchmark and return timing results."""
    print(f"\n{'='*60}")
    print(f"Benchmark: sample_size={sample_size:,}, date={date}")
    print(f"{'='*60}")

    base_dir = data_dir.resolve()
    cfg = Config.from_env(base_dir)
    cfg = cfg.model_copy(
        update={
            "overture_release_date": date,
            "overture_sample_limit": sample_size,
        }
    )

    results = {"sample_size": sample_size, "tasks": {}}

    # Task: overture_sample
    print("\n--- overture_sample ---")
    t0 = time.perf_counter()
    task_overture_sample(config=cfg)
    elapsed = time.perf_counter() - t0
    results["tasks"]["overture_sample"] = elapsed
    print(f"overture_sample finished in {elapsed:.1f}s")

    # Task: build_silver (conflation)
    print("\n--- build_silver (conflation) ---")
    t0 = time.perf_counter()
    task_build_silver(config=cfg)
    elapsed = time.perf_counter() - t0
    results["tasks"]["build_silver"] = elapsed
    print(f"build_silver finished in {elapsed:.1f}s")

    # Task: build_gold
    print("\n--- build_gold ---")
    t0 = time.perf_counter()
    task_build_gold(config=cfg)
    elapsed = time.perf_counter() - t0
    results["tasks"]["build_gold"] = elapsed
    print(f"build_gold finished in {elapsed:.1f}s")

    # Total
    results["total"] = sum(results["tasks"].values())
    print(f"\n>>> Total time: {results['total']:.1f}s")

    return results


def main() -> int:
    args = _parse_args()

    # Parse sample sizes
    sample_sizes = [int(s.strip()) for s in args.sample_sizes.split(",") if s.strip()]

    print(f"Running benchmarks for sample sizes: {sample_sizes}")
    print(f"Date: {args.date}")
    print(f"Data dir: {args.data_dir}")

    all_results = []
    for size in sample_sizes:
        results = run_benchmark(size, args.date, args.data_dir)
        all_results.append(results)

    # Summary table
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(
        f"{'Sample Size':>12} | {'Overture':>10} | {'Silver':>10} | {'Gold':>10} | {'Total':>10}"
    )
    print("-" * 80)
    for r in all_results:
        print(
            f"{r['sample_size']:>12,} | "
            f"{r['tasks'].get('overture_sample', 0):>10.1f}s | "
            f"{r['tasks'].get('build_silver', 0):>10.1f}s | "
            f"{r['tasks'].get('build_gold', 0):>10.1f}s | "
            f"{r['total']:>10.1f}s"
        )

    # Calculate scaling factor (assuming first result is baseline)
    if len(all_results) > 1:
        baseline = all_results[0]
        print(
            f"\nScaling from {baseline['sample_size']:,} to {all_results[-1]['sample_size']:,}:"
        )
        silver_baseline = baseline["tasks"].get("build_silver", 1)
        silver_final = all_results[-1]["tasks"].get("build_silver", 1)
        size_ratio = all_results[-1]["sample_size"] / baseline["sample_size"]
        time_ratio = silver_final / silver_baseline if silver_baseline > 0 else 0
        print(f"  - Size ratio: {size_ratio:.1f}x")
        print(f"  - Silver time ratio: {time_ratio:.1f}x")
        print(f"  - Expected if O(n): {size_ratio:.1f}x, Actual: {time_ratio:.1f}x")

    return 0


if __name__ == "__main__":
    sys.exit(main())
