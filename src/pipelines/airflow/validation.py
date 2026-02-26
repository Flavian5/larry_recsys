"""
Pydantic-backed validation for Airflow tasks.

- Use get_validated_config() at the start of each task to load and validate
  DataFoundationConfig; invalid env/config will raise ValidationError and fail the task.
- All task inputs that come from config are therefore validated. For task outputs
  (e.g. XCom payloads), define a Pydantic model and validate before pushing.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError

from config.data_foundation import load_config

if TYPE_CHECKING:
    from config.data_foundation import DataFoundationConfig


def get_validated_config(base_dir: str | Path = ".") -> "DataFoundationConfig":
    """
    Load and validate DataFoundationConfig for use in Airflow tasks.

    Raises pydantic.ValidationError if config is invalid (e.g. bad env vars or types).
    Catch ValidationError in task code if you want to handle it; otherwise the task
    will fail with a clear validation message.
    """
    try:
        return load_config(base_dir=base_dir)
    except ValidationError as e:
        # Re-raise with a short summary so Airflow UI shows a clear failure reason
        msg = f"Config validation failed: {e.error_count()} error(s). {e.errors()!s}"
        raise RuntimeError(msg) from e
