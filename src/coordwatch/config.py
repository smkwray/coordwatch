
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from coordwatch.paths import CONFIGS_DIR


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise TypeError(f"Expected dict-like YAML at {path}, got {type(data)!r}")
    return data


@lru_cache(maxsize=None)
def load_source_manifest() -> dict[str, Any]:
    return _load_yaml(CONFIGS_DIR / "source_manifest.yml")


@lru_cache(maxsize=None)
def load_variables() -> dict[str, Any]:
    return _load_yaml(CONFIGS_DIR / "variables.yml")


@lru_cache(maxsize=None)
def load_model_specs() -> dict[str, Any]:
    return _load_yaml(CONFIGS_DIR / "model_specs.yml")
