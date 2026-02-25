import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

EnvKind = Literal["local", "composer-dev", "composer-prod"]


@dataclass(frozen=True)
class LocalPaths:
    raw: Path
    silver: Path
    gold: Path


@dataclass(frozen=True)
class GCSUris:
    raw: str
    silver: str
    gold: str


@dataclass(frozen=True)
class DataFoundationConfig:
    env: EnvKind
    gcp_project: str | None
    local: LocalPaths
    gcs: GCSUris


def _get_env_var(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value


def detect_env() -> EnvKind:
    raw = _get_env_var("RPG_ENV", "local").lower()
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


def load_config(base_dir: str | Path = ".") -> DataFoundationConfig:
    env = detect_env()
    project = _get_env_var("RPG_GCP_PROJECT")
    local_paths = build_local_paths(base_dir)
    gcs_uris = build_gcs_uris(project=project)
    return DataFoundationConfig(
        env=env,
        gcp_project=project,
        local=local_paths,
        gcs=gcs_uris,
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
