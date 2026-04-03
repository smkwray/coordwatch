
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.construct.refunding import build_refunding_panel
from coordwatch.logging_utils import configure_logging

configure_logging()


def main() -> None:
    build_refunding_panel(prefer_real=True)


if __name__ == "__main__":
    main()
