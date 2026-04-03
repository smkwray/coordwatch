
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from coordwatch.io import write_json
from coordwatch.paths import PUBLISH_DIR, SITE_DATA_DIR, SITE_FIGURES_DIR


def copy_figure(src: Path, dst_name: str | None = None) -> str:
    SITE_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    dst = SITE_FIGURES_DIR / (dst_name or src.name)
    if src.exists():
        shutil.copy2(src, dst)
    return str(dst.relative_to(SITE_FIGURES_DIR.parent))


def publish_table(df: pd.DataFrame, name: str) -> None:
    write_json(PUBLISH_DIR / f"{name}.json", df.to_dict(orient="records"))
    write_json(SITE_DATA_DIR / f"{name}.json", df.to_dict(orient="records"))
