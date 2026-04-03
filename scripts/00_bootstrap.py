
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import ensure_repo_dirs

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    ensure_repo_dirs()
    logger.info("Ensured repo directory structure exists")


if __name__ == "__main__":
    main()
