"""
Pydantic-backed validation for Airflow tasks.

- Use get_validated_config() at the start of each task to load and validate
  Config; invalid env/config will raise ValidationError and fail the task.
- For dependency injection, pass config into the task (e.g. op_kwargs={"config": cfg});
  if config is None, the task calls get_validated_config().
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError

from config.data_foundation import Config

if TYPE_CHECKING:
    pass  # Config already imported


def get_validated_config(base_dir: str | Path = ".") -> Config:
    """
    Load and validate Config from environment for use in Airflow tasks.

    Raises RuntimeError with a clear message (wrapping ValidationError) if invalid.
    """
    try:
        return Config.from_env(base_dir=base_dir)
    except ValidationError as e:
        msg = f"Config validation failed: {e.error_count()} error(s). {e.errors()!s}"
        raise RuntimeError(msg) from e
