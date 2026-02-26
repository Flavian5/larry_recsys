"""Tests for Airflow task validation (get_validated_config)."""

from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from config.data_foundation import DataFoundationConfig
from pipelines.airflow.validation import get_validated_config


def test_get_validated_config_returns_config_when_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RPG_ENV", "local")
    monkeypatch.delenv("RPG_GCP_PROJECT", raising=False)
    monkeypatch.delenv("RPG_GCS_BUCKET_RAW", raising=False)
    monkeypatch.delenv("RPG_GCS_BUCKET_SILVER", raising=False)
    monkeypatch.delenv("RPG_GCS_BUCKET_GOLD", raising=False)

    cfg = get_validated_config(base_dir=tmp_path)

    assert isinstance(cfg, DataFoundationConfig)
    assert cfg.env == "local"
    assert cfg.local.raw == tmp_path / "data" / "raw"


def test_get_validated_config_raises_runtime_error_on_validation_error() -> None:
    # Trigger a real ValidationError (wrong type for env)
    try:
        DataFoundationConfig.model_validate(
            {"env": 123, "local": {}, "gcs": {}, "datasets": {}}
        )
    except ValidationError as e:
        validation_error = e
    else:
        pytest.fail("Expected ValidationError from invalid env type")

    with patch(
        "pipelines.airflow.validation.load_config",
        side_effect=validation_error,
    ):
        with pytest.raises(RuntimeError) as exc_info:
            get_validated_config()

    assert "Config validation failed" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, ValidationError)
