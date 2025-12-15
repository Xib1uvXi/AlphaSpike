"""Test configuration and common fixtures."""

import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TEST_HOME = _PROJECT_ROOT / ".data" / "home"

# Ensure Tushare writes token/cache inside the repo instead of the real HOME.
_TEST_HOME.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("TUSHARE_HOME", str(_TEST_HOME))
os.environ.setdefault("HOME", str(_TEST_HOME))
os.environ.setdefault("TUSHARE_TOKEN", "test-token")
