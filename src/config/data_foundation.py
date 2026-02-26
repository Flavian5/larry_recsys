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
    - overture_places_base: Base URL for Overture Places (used with release date). Env: RPG_OVERTURE_PLACES_BASE_URL.
    - overture_places: Full Overture Places URI (overrides base+release when set). Set RPG_OVERTURE_PLACES_URI,
      or use RPG_OVERTURE_RELEASE_DATE with overture_places_base.
    - osm_extract: OSM extract (URL or path). Set RPG_OSM_EXTRACT_URI when non-local.
    """

    overture_places_base: str = DEFAULT_OVERTURE_PLACES_BASE
    overture_places: str = ""
    osm_extract: str = ""


class DataFoundationConfig(BaseModel, frozen=True):
    env: EnvKind
    gcp_project: str | None = None
    local: LocalPaths
    gcs: GCSUris
    """When 'text', silver/gold are written as plain text (jsonl / txt) for local inspection."""
    local_output_format: OutputFormat = Field(default="parquet")
    """Upstream dataset sources. Empty strings = use DAG default local paths."""
    datasets: DatasetUris


def _get_env_var(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value


def detect_env() -> EnvKind:
    raw = (_get_env_var("RPG_ENV", "local") or "local").lower()
    if raw in {"local", "composer-dev", "composer-prod"}:
        return raw  # type: ignore[return-value]
    # Fall back to local for unknown values to keep behaviour predictable in dev.
    return "local"


def build_local_paths(base_dir: str | Path = ".") -> LocalPaths:
    base = Path(base_dir)
    return LocalPaths(
        raw=base / "data" / "raw",
        silver=base / "data" / "silver",
        gold=base / "data" / "gold",
    )


def build_gcs_uris(
    project: str | None = None,
    raw_bucket: str | None = None,
    silver_bucket: str | None = None,
    gold_bucket: str | None = None,
) -> GCSUris:
    """
    Build base GCS URIs from bucket names.

    Buckets can be passed explicitly or read from environment:
    - RPG_GCS_BUCKET_RAW
    - RPG_GCS_BUCKET_SILVER
    - RPG_GCS_BUCKET_GOLD
    """
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
    overture = _get_env_var("RPG_OVERTURE_PLACES_URI", "") or ""
    osm = _get_env_var("RPG_OSM_EXTRACT_URI", "") or ""
    return DatasetUris(
        overture_places_base=overture_base,
        overture_places=overture,
        osm_extract=osm,
    )


def _detect_local_output_format() -> OutputFormat:
    raw = (_get_env_var("RPG_LOCAL_OUTPUT_FORMAT", "parquet") or "parquet").lower()
    if raw == "text":
        return "text"
    return "parquet"


def silver_venues_filename(output_format: OutputFormat) -> str:
    """Filename for silver venues output (e.g. venues.parquet or venues.jsonl)."""
    return "venues.jsonl" if output_format == "text" else "venues.parquet"


def gold_venues_filename(output_format: OutputFormat) -> str:
    """Filename for gold venues output (e.g. venues.parquet or venues.txt)."""
    return "venues.txt" if output_format == "text" else "venues.parquet"


def load_config(base_dir: str | Path = ".") -> DataFoundationConfig:
    """Load and validate DataFoundationConfig from environment. Raises ValidationError if invalid."""
    env = detect_env()
    project = _get_env_var("RPG_GCP_PROJECT")
    local_paths = build_local_paths(base_dir)
    gcs_uris = build_gcs_uris(project=project)
    local_output_format = _detect_local_output_format()
    datasets = _build_dataset_uris()
    return DataFoundationConfig(
        env=env,
        gcp_project=project,
        local=local_paths,
        gcs=gcs_uris,
        local_output_format=local_output_format,
        datasets=datasets,
    )


def build_dated_gcs_path(base_uri: str, date_str: str, *components: str) -> str:
    """
    Build a dated GCS URI following the convention:

    {base_uri}/{date}/{component1}/{component2}/...
    """
    if not base_uri:
        return ""
    parts = "/".join(c.strip("/") for c in components) if components else ""
    if parts:
        return f"{base_uri}/{date_str}/{parts}"
    return f"{base_uri}/{date_str}"
