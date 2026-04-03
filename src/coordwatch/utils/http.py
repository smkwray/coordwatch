
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import requests

from coordwatch.io import ensure_parent

DEFAULT_HEADERS = {
    "User-Agent": "CoordWatch/0.1 (open reproducible research)",
    "Accept": "*/*",
}


def get(
    url: str,
    timeout: int = 60,
    headers: dict[str, str] | None = None,
    retries: int = 3,
    backoff: float = 5.0,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout, headers=headers or DEFAULT_HEADERS)
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    raise last_exc  # type: ignore[misc]


def get_text(url: str, timeout: int = 60) -> str:
    response = get(url, timeout=timeout)
    response.encoding = response.encoding or "utf-8"
    return response.text


def get_json(url: str, timeout: int = 60) -> Any:
    return get(url, timeout=timeout).json()


def download_to_path(url: str, path: Path, timeout: int = 60, overwrite: bool = False, sleep: float = 0.0) -> Path:
    if path.exists() and not overwrite:
        return path
    response = get(url, timeout=timeout)
    ensure_parent(path)
    path.write_bytes(response.content)
    if sleep:
        time.sleep(sleep)
    return path
