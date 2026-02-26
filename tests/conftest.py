"""Shared pytest fixtures. Config is dependency-injected for tests and DAG tasks."""

from pathlib import Path

import pytest

from config.data_foundation import Config


@pytest.fixture
def config(tmp_path: Path) -> Config:
    """Injectable config for tests. Use this instead of env when testing tasks or pipeline code."""
    return Config.for_test(tmp_path)
