#!/usr/bin/env python3
"""CLI wrapper for runtime database preparation and initialization."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.database_bootstrap import main


if __name__ == "__main__":
    raise SystemExit(main())
