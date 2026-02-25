import os
from pathlib import Path

import pytest

from config.data_foundation import (
    DataFoundationConfig,
    build_dated_gcs_path,
    build_gcs_uris,
    build_local_paths,
    detect_env,
    load_config,
)


def test_detect_env_defaults_to_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RPG_ENV", raising=False)
    assert detect_env() == "local"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("local", "local"),
        ("LOCAL", "local"),
        ("composer-dev", "composer-dev"),
        ("composer-prod", "composer-prod"),
        ("unknown-env", "local"),
    ],
)
def test_detect_env_variants(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: str
) -> None:
    monkeypatch.setenv("RPG_ENV", value)
    assert detect_env() == expected


def test_build_local_paths_base_dir(tmp_path: Path) -> None:
    paths = build_local_paths(tmp_path)
    assert paths.raw == tmp_path / "data" / "raw"
    assert paths.silver == tmp_path / "data" / "silver"
    assert paths.gold == tmp_path / "data" / "gold"


def test_build_gcs_uris_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RPG_GCS_BUCKET_RAW", "larry-rpg-dev-raw")
    monkeypatch.setenv("RPG_GCS_BUCKET_SILVER", "larry-rpg-dev-silver")
    monkeypatch.setenv("RPG_GCS_BUCKET_GOLD", "larry-rpg-dev-gold")

    uris = build_gcs_uris(project="larry-rpg-dev")
    assert uris.raw == "gs://larry-rpg-dev-raw"
    assert uris.silver == "gs://larry-rpg-dev-silver"
    assert uris.gold == "gs://larry-rpg-dev-gold"


def test_build_gcs_uris_empty_when_no_buckets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RPG_GCS_BUCKET_RAW", raising=False)
    monkeypatch.delenv("RPG_GCS_BUCKET_SILVER", raising=False)
    monkeypatch.delenv("RPG_GCS_BUCKET_GOLD", raising=False)

    uris = build_gcs_uris()
    assert uris.raw == ""
    assert uris.silver == ""
    assert uris.gold == ""


def test_build_dated_gcs_path_with_components() -> None:
    base = "gs://larry-rpg-dev-silver"
    path = build_dated_gcs_path(base, "2024-01-01", "overture", "places.parquet")
    assert path == "gs://larry-rpg-dev-silver/2024-01-01/overture/places.parquet"


def test_build_dated_gcs_path_without_components() -> None:
    base = "gs://larry-rpg-dev-gold"
    path = build_dated_gcs_path(base, "2024-01-01")
    assert path == "gs://larry-rpg-dev-gold/2024-01-01"


def test_load_config_wires_env_and_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RPG_ENV", "composer-dev")
    monkeypatch.setenv("RPG_GCP_PROJECT", "larry-rpg-dev")
    monkeypatch.setenv("RPG_GCS_BUCKET_RAW", "larry-rpg-dev-raw")
    monkeypatch.setenv("RPG_GCS_BUCKET_SILVER", "larry-rpg-dev-silver")
    monkeypatch.setenv("RPG_GCS_BUCKET_GOLD", "larry-rpg-dev-gold")

    cfg = load_config(base_dir=tmp_path)
    assert isinstance(cfg, DataFoundationConfig)
    assert cfg.env == "composer-dev"
    assert cfg.gcp_project == "larry-rpg-dev"
    assert cfg.local.raw == tmp_path / "data" / "raw"
    assert cfg.gcs.raw == "gs://larry-rpg-dev-raw"
    assert cfg.gcs.silver == "gs://larry-rpg-dev-silver"
    assert cfg.gcs.gold == "gs://larry-rpg-dev-gold"
