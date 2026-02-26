"""
Centralized config for the data foundation pipeline.

Use Config.from_env() in production/DAGs; use Config.for_test() in tests and inject
into tasks for dependency injection.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

EnvKind = Literal["local", "composer-dev", "composer-prod"]
OutputFormat = Literal["parquet", "text"]


class LocalPaths(BaseModel, frozen=True):
    raw: Path
    silver: Path
    gold: Path


class GCSUris(BaseModel, frozen=True):
    raw: str = ""
    silver: str = ""
    gold: str = ""


DEFAULT_OVERTURE_PLACES_BASE = "https://overturemaps-us-west-2.s3.amazonaws.com"


class DatasetUris(BaseModel, frozen=True):
    """
    URIs or URLs for upstream dataset sources. Empty means use DAG default local paths.
    """

    overture_places_base: str = DEFAULT_OVERTURE_PLACES_BASE
    overture_places: str = ""
    osm_extract: str = ""


class Config(BaseModel, frozen=True):
    """
    Data foundation config. Create via Config.from_env() or Config.for_test().
    """

    env: EnvKind
    gcp_project: str | None = None
    local: LocalPaths
    gcs: GCSUris
    local_output_format: OutputFormat = Field(default="parquet")
    datasets: DatasetUris
    # Local / run overrides: Overture release date and row limit for sampling
    overture_release_date: str = Field(default="")
    overture_sample_limit: int | None = Field(default=None)

    # --- Factory: from environment (production/DAG) ---

    @classmethod
    def from_env(cls, base_dir: str | Path = ".") -> Config:
        """Build config from environment. Raises ValidationError if invalid."""
        env = _detect_env()
        project = _get_env_var("RPG_GCP_PROJECT")
        local_paths = _build_local_paths(base_dir)
        gcs_uris = _build_gcs_uris(project=project)
        output_format = _detect_local_output_format()
        datasets = _build_dataset_uris()
        release_date = (_get_env_var("RPG_OVERTURE_RELEASE_DATE") or "").strip()
        sample_limit = _get_env_var("RPG_OVERTURE_SAMPLE_LIMIT")
        overture_sample_limit = (
            int(sample_limit) if sample_limit and sample_limit.isdigit() else None
        )
        return cls(
            env=env,
            gcp_project=project,
            local=local_paths,
            gcs=gcs_uris,
            local_output_format=output_format,
            datasets=datasets,
            overture_release_date=release_date,
            overture_sample_limit=overture_sample_limit,
        )

    # --- Factory: for tests (inject this into tasks) ---

    @classmethod
    def for_test(
        cls,
        base_dir: str | Path,
        *,
        env: EnvKind = "local",
        gcp_project: str | None = None,
        gcs_raw: str = "",
        gcs_silver: str = "",
        gcs_gold: str = "",
        output_format: OutputFormat = "parquet",
        overture_places_base: str = DEFAULT_OVERTURE_PLACES_BASE,
        overture_places: str = "",
        osm_extract: str = "",
        overture_release_date: str = "",
        overture_sample_limit: int | None = None,
    ) -> Config:
        """Build a valid config with overrides for testing. No env vars required."""
        base = Path(base_dir)
        local = LocalPaths(
            raw=base / "data" / "raw",
            silver=base / "data" / "silver",
            gold=base / "data" / "gold",
        )
        gcs = GCSUris(raw=gcs_raw, silver=gcs_silver, gold=gcs_gold)
        datasets = DatasetUris(
            overture_places_base=overture_places_base,
            overture_places=overture_places,
            osm_extract=osm_extract,
        )
        return cls(
            env=env,
            gcp_project=gcp_project,
            local=local,
            gcs=gcs,
            local_output_format=output_format,
            datasets=datasets,
            overture_release_date=overture_release_date,
            overture_sample_limit=overture_sample_limit,
        )

    # --- Helpers (convenience on config instance) ---

    def silver_venues_filename(self) -> str:
        """Filename for silver venues output (e.g. venues.parquet or venues.jsonl)."""
        return silver_venues_filename(self.local_output_format)

    def gold_venues_filename(self) -> str:
        """Filename for gold venues output (e.g. venues.parquet or venues.txt)."""
        return gold_venues_filename(self.local_output_format)

    def build_dated_gcs_path(
        self, base_uri: str, date_str: str, *components: str
    ) -> str:
        """Build {base_uri}/{date_str}/{components...}."""
        return build_dated_gcs_path(base_uri, date_str, *components)


# --- Module-level helpers (used by Config.from_env and by callers that have only format) ---


def _get_env_var(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def _detect_env() -> EnvKind:
    raw = (_get_env_var("RPG_ENV", "local") or "local").lower()
    if raw in {"local", "composer-dev", "composer-prod"}:
        return raw  # type: ignore[return-value]
    return "local"


def _build_local_paths(base_dir: str | Path = ".") -> LocalPaths:
    base = Path(base_dir)
    return LocalPaths(
        raw=base / "data" / "raw",
        silver=base / "data" / "silver",
        gold=base / "data" / "gold",
    )


def _build_gcs_uris(
    project: str | None = None,  # accepted for API compat; buckets from env/args
    raw_bucket: str | None = None,
    silver_bucket: str | None = None,
    gold_bucket: str | None = None,
) -> GCSUris:
    raw_b = raw_bucket or _get_env_var("RPG_GCS_BUCKET_RAW", "") or ""
    silver_b = silver_bucket or _get_env_var("RPG_GCS_BUCKET_SILVER", "") or ""
    gold_b = gold_bucket or _get_env_var("RPG_GCS_BUCKET_GOLD", "") or ""

    def to_uri(bucket: str) -> str:
        return f"gs://{bucket}" if bucket else ""

    return GCSUris(raw=to_uri(raw_b), silver=to_uri(silver_b), gold=to_uri(gold_b))


def _build_dataset_uris() -> DatasetUris:
    overture_base = (
        _get_env_var("RPG_OVERTURE_PLACES_BASE_URL") or DEFAULT_OVERTURE_PLACES_BASE
    )
    return DatasetUris(
        overture_places_base=overture_base,
        overture_places=_get_env_var("RPG_OVERTURE_PLACES_URI", "") or "",
        osm_extract=_get_env_var("RPG_OSM_EXTRACT_URI", "") or "",
    )


def _detect_local_output_format() -> OutputFormat:
    raw = (_get_env_var("RPG_LOCAL_OUTPUT_FORMAT", "parquet") or "parquet").lower()
    return "text" if raw == "text" else "parquet"


def silver_venues_filename(output_format: OutputFormat) -> str:
    """Filename for silver venues output (e.g. venues.parquet or venues.jsonl)."""
    return "venues.jsonl" if output_format == "text" else "venues.parquet"


def gold_venues_filename(output_format: OutputFormat) -> str:
    """Filename for gold venues output (e.g. venues.parquet or venues.txt)."""
    return "venues.txt" if output_format == "text" else "venues.parquet"


def build_dated_gcs_path(base_uri: str, date_str: str, *components: str) -> str:
    """Build {base_uri}/{date_str}/{component1}/{component2}/..."""
    if not base_uri:
        return ""
    parts = "/".join(c.strip("/") for c in components) if components else ""
    if parts:
        return f"{base_uri}/{date_str}/{parts}"
    return f"{base_uri}/{date_str}"


# --- Public builders (for tests that assert on paths/uris in isolation) ---


def detect_env() -> EnvKind:
    """Detect env from RPG_ENV. Public for tests."""
    return _detect_env()


def build_local_paths(base_dir: str | Path = ".") -> LocalPaths:
    """Build local paths. Public for tests."""
    return _build_local_paths(base_dir)


def build_gcs_uris(
    project: str | None = None,
    raw_bucket: str | None = None,
    silver_bucket: str | None = None,
    gold_bucket: str | None = None,
) -> GCSUris:
    """Build GCS URIs from env or args. Public for tests."""
    return _build_gcs_uris(
        project=project,
        raw_bucket=raw_bucket,
        silver_bucket=silver_bucket,
        gold_bucket=gold_bucket,
    )
