
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = REPO_ROOT / "configs"
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
PUBLISH_DIR = DATA_DIR / "publish"
MANUAL_DIR = DATA_DIR / "manual"
REFERENCE_DIR = DATA_DIR / "reference"
OUTPUTS_DIR = REPO_ROOT / "outputs"
OUTPUTS_FIGURES_DIR = OUTPUTS_DIR / "figures"
OUTPUTS_TABLES_DIR = OUTPUTS_DIR / "tables"
OUTPUTS_LOGS_DIR = OUTPUTS_DIR / "logs"
SITE_DIR = REPO_ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
SITE_FIGURES_DIR = SITE_DIR / "figures"

EXPECTED_DIRS = [
    RAW_DIR,
    RAW_DIR / "downloads",
    RAW_DIR / "demo",
    RAW_DIR / "demo" / "fred",
    RAW_DIR / "demo" / "treasury",
    RAW_DIR / "demo" / "nyfed",
    INTERIM_DIR,
    PROCESSED_DIR,
    PUBLISH_DIR,
    OUTPUTS_DIR,
    OUTPUTS_FIGURES_DIR,
    OUTPUTS_TABLES_DIR,
    OUTPUTS_LOGS_DIR,
    SITE_DIR,
    SITE_DATA_DIR,
    SITE_FIGURES_DIR,
]


def ensure_repo_dirs() -> None:
    for path in EXPECTED_DIRS:
        path.mkdir(parents=True, exist_ok=True)
