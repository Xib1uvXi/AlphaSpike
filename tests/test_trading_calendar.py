"""Tests for trading calendar helpers."""

from datetime import datetime

from src.datahub.trading_calendar import get_last_trading_day, is_trading_day


def test_returns_same_day_when_trading():
    """Should return the day itself when it is a trading day."""
    assert is_trading_day("2025-03-07") is True
    assert get_last_trading_day("2025-03-07") == "20250307"


def test_skips_non_trading_holiday():
    """Should find the previous trading day for a holiday/non-trading date."""
    assert is_trading_day("2025-10-07") is False
    assert get_last_trading_day("2025-10-07") == "20250930"


def test_cross_year_new_years_day():
    """2026-01-01 is marked closed; should return last trading day of 2025."""
    assert is_trading_day("2026-01-01") is False
    assert get_last_trading_day("2026-01-01") == "20251231"


def test_2026_regular_trading_day():
    """Should accept a known 2026 trading day."""
    assert is_trading_day("2026-01-02") is True
    assert get_last_trading_day("2026-01-02") == "20260102"


def test_future_year_falls_back_to_known_last_date():
    """Dates beyond calendar should fall back to the latest known trading day."""
    result = get_last_trading_day("2027-01-03")  # beyond available calendar
    assert result == "20261231"


def test_is_trading_day():
    assert is_trading_day("2026-01-04") is False
    assert is_trading_day("2026-01-05") is True
    assert is_trading_day("2025-01-01") is False
    assert is_trading_day("2025-01-02") is True
    assert is_trading_day("2025-01-04") is False
    assert is_trading_day("2025-01-05") is False
    assert is_trading_day("2025-01-06") is True
