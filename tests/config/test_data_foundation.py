from pathlib import Path

import pytest

from config.data_foundation import (
    DEFAULT_OVERTURE_PLACES_BASE,
    Config,
    build_dated_gcs_path,
    build_gcs_uris,
    build_local_paths,
    detect_env,
    gold_venues_filename,
    silver_venues_filename,
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


def test_config_from_env_wires_env_and_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RPG_ENV", "composer-dev")
    monkeypatch.setenv("RPG_GCP_PROJECT", "larry-rpg-dev")
    monkeypatch.setenv("RPG_GCS_BUCKET_RAW", "larry-rpg-dev-raw")
    monkeypatch.setenv("RPG_GCS_BUCKET_SILVER", "larry-rpg-dev-silver")
    monkeypatch.setenv("RPG_GCS_BUCKET_GOLD", "larry-rpg-dev-gold")

    cfg = Config.from_env(base_dir=tmp_path)
    assert isinstance(cfg, Config)
    assert cfg.env == "composer-dev"
    assert cfg.gcp_project == "larry-rpg-dev"
    assert cfg.local.raw == tmp_path / "data" / "raw"
    assert cfg.gcs.raw == "gs://larry-rpg-dev-raw"
    assert cfg.gcs.silver == "gs://larry-rpg-dev-silver"
    assert cfg.gcs.gold == "gs://larry-rpg-dev-gold"
    assert cfg.local_output_format == "parquet"
    assert cfg.datasets.overture_places_base == DEFAULT_OVERTURE_PLACES_BASE
    assert cfg.datasets.overture_places == ""
    assert cfg.datasets.osm_extract == ""


def test_config_from_env_dataset_uris(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "RPG_OVERTURE_PLACES_URI", "https://example.com/overture/*.parquet"
    )
    monkeypatch.setenv("RPG_OSM_EXTRACT_URI", "https://example.com/osm.parquet")
    cfg = Config.from_env(base_dir=Path("."))
    assert cfg.datasets.overture_places == "https://example.com/overture/*.parquet"
    assert cfg.datasets.osm_extract == "https://example.com/osm.parquet"


def test_config_from_env_overture_base(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "RPG_OVERTURE_PLACES_BASE_URL", "https://custom-overture.example.com"
    )
    cfg = Config.from_env(base_dir=Path("."))
    assert cfg.datasets.overture_places_base == "https://custom-overture.example.com"


def test_config_from_env_local_output_format_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RPG_LOCAL_OUTPUT_FORMAT", "text")
    cfg = Config.from_env(base_dir=Path("."))
    assert cfg.local_output_format == "text"


def test_config_from_env_overture_release_date_and_sample_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RPG_OVERTURE_RELEASE_DATE", " 2024-03-15 ")
    monkeypatch.setenv("RPG_OVERTURE_SAMPLE_LIMIT", "5000")
    cfg = Config.from_env(base_dir=Path("."))
    assert cfg.overture_release_date == "2024-03-15"
    assert cfg.overture_sample_limit == 5000


def test_config_from_env_overture_sample_limit_ignored_when_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RPG_OVERTURE_SAMPLE_LIMIT", "not-a-number")
    cfg = Config.from_env(base_dir=Path("."))
    assert cfg.overture_sample_limit is None


def test_config_from_env_bbox_and_overpass(monkeypatch: pytest.MonkeyPatch) -> None:
    """RPG_BBOX (south,west,north,east) and RPG_OVERPASS_URL are applied."""
    monkeypatch.setenv("RPG_BBOX", "37.1,-122.6,37.9,-122.2")
    monkeypatch.setenv("RPG_OVERPASS_URL", "https://overpass.example.com/api")
    cfg = Config.from_env(base_dir=Path("."))
    assert cfg.bbox_miny == 37.1
    assert cfg.bbox_minx == -122.6
    assert cfg.bbox_maxy == 37.9
    assert cfg.bbox_maxx == -122.2
    assert cfg.datasets.overpass_url == "https://overpass.example.com/api"


def test_config_for_test_injectable(tmp_path: Path) -> None:
    """Config.for_test() builds a valid config without env vars (for DI in tests)."""
    cfg = Config.for_test(tmp_path)
    assert cfg.env == "local"
    assert cfg.local.raw == tmp_path / "data" / "raw"
    assert cfg.gcs.raw == ""
    assert cfg.silver_venues_filename() == "venues.parquet"
    assert cfg.gold_venues_filename() == "venues.parquet"

    cfg_text = Config.for_test(tmp_path, output_format="text")
    assert cfg_text.silver_venues_filename() == "venues.jsonl"
    assert cfg_text.gold_venues_filename() == "venues.txt"


def test_config_for_test_overrides(tmp_path: Path) -> None:
    cfg = Config.for_test(
        tmp_path,
        env="composer-dev",
        gcp_project="my-project",
        gcs_raw="gs://r",
        gcs_silver="gs://s",
        gcs_gold="gs://g",
    )
    assert cfg.env == "composer-dev"
    assert cfg.gcp_project == "my-project"
    assert cfg.gcs.raw == "gs://r"
    assert cfg.gcs.silver == "gs://s"
    assert cfg.gcs.gold == "gs://g"


def test_silver_gold_venues_filename() -> None:
    assert silver_venues_filename("parquet") == "venues.parquet"
    assert silver_venues_filename("text") == "venues.jsonl"
    assert gold_venues_filename("parquet") == "venues.parquet"
    assert gold_venues_filename("text") == "venues.txt"
