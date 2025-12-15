"""Tests for the daily_bar module."""

from unittest.mock import patch

import pandas as pd
import pytest

from src.datahub.daily_bar import (
    _get_latest_trade_date,
    _get_next_date,
    _get_symbol_list_date,
    _get_today,
    _save_to_db,
    get_daily_bar_from_db,
    get_date_range,
    sync_daily_bar,
)
from src.datahub.db import get_connection, init_db


@pytest.fixture
def temp_db(monkeypatch, tmp_path):
    """Create a temporary database for testing."""
    temp_path = tmp_path / "test.db"

    monkeypatch.setenv("SQLITE_PATH", str(temp_path))
    init_db()

    yield str(temp_path)

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_daily_bar_df():
    """Create a sample daily bar DataFrame."""
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["20231201", "20231204", "20231205"],
            "open": [10.0, 10.5, 10.3],
            "high": [10.5, 10.8, 10.6],
            "low": [9.8, 10.2, 10.1],
            "close": [10.2, 10.6, 10.4],
            "pre_close": [10.0, 10.2, 10.6],
            "change": [0.2, 0.4, -0.2],
            "pct_chg": [2.0, 3.9, -1.9],
            "vol": [1000000.0, 1200000.0, 900000.0],
            "amount": [10200000.0, 12720000.0, 9360000.0],
        }
    )


class TestGetNextDate:
    """Tests for _get_next_date function."""

    def test_normal_date(self):
        """Should return next day."""
        assert _get_next_date("20231201") == "20231202"

    def test_end_of_month(self):
        """Should handle end of month correctly."""
        assert _get_next_date("20231231") == "20240101"

    def test_end_of_year(self):
        """Should handle end of year correctly."""
        assert _get_next_date("20231231") == "20240101"


class TestGetToday:
    """Tests for _get_today function."""

    def test_returns_today(self):
        """Should return today's date in YYYYMMDD format."""
        result = _get_today()
        assert len(result) == 8
        assert result.isdigit()


class TestGetSymbolListDate:
    """Tests for _get_symbol_list_date function."""

    def test_returns_list_date_for_valid_symbol(self):
        """Should return list date for a valid symbol."""
        # 000001.SZ (平安银行) is a well-known stock
        result = _get_symbol_list_date("000001.SZ")
        assert result is not None
        assert len(result) == 8
        assert result.isdigit()

    def test_returns_none_for_invalid_symbol(self):
        """Should return None for invalid symbol."""
        result = _get_symbol_list_date("999999.SZ")
        assert result is None


class TestSaveToDb:
    """Tests for _save_to_db function."""

    def test_saves_data(self, temp_db, sample_daily_bar_df):
        """Should save data to database."""
        _save_to_db(sample_daily_bar_df)

        with get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM daily_bar")
            assert cursor.fetchone()[0] == 3

    def test_handles_empty_df(self, temp_db):
        """Should handle empty DataFrame."""
        _save_to_db(pd.DataFrame())

        with get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM daily_bar")
            assert cursor.fetchone()[0] == 0

    def test_replaces_duplicates(self, temp_db, sample_daily_bar_df):
        """Should replace existing records with same key."""
        _save_to_db(sample_daily_bar_df)

        # Modify and save again
        sample_daily_bar_df["close"] = [20.0, 20.0, 20.0]
        _save_to_db(sample_daily_bar_df)

        with get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM daily_bar")
            assert cursor.fetchone()[0] == 3

            cursor = conn.execute("SELECT close FROM daily_bar WHERE trade_date = '20231201'")
            assert cursor.fetchone()[0] == 20.0


class TestGetLatestTradeDate:
    """Tests for _get_latest_trade_date function."""

    def test_returns_latest_date(self, temp_db, sample_daily_bar_df):
        """Should return the latest trade date."""
        _save_to_db(sample_daily_bar_df)

        result = _get_latest_trade_date("000001.SZ")
        assert result == "20231205"

    def test_returns_none_when_no_data(self, temp_db):
        """Should return None when no data exists."""
        result = _get_latest_trade_date("000001.SZ")
        assert result is None


@patch("src.datahub.daily_bar.set_daily_bar_cache")
@patch("src.datahub.daily_bar.get_daily_bar_cache", return_value=None)
class TestGetDailyBarFromDb:
    """Tests for get_daily_bar_from_db function."""

    def test_returns_all_data(self, mock_get_cache, mock_set_cache, temp_db, sample_daily_bar_df):
        """Should return all data for a stock."""
        _save_to_db(sample_daily_bar_df)

        result = get_daily_bar_from_db("000001.SZ")
        assert len(result) == 3

    def test_filters_by_start_date(self, mock_get_cache, mock_set_cache, temp_db, sample_daily_bar_df):
        """Should filter by start date."""
        _save_to_db(sample_daily_bar_df)

        result = get_daily_bar_from_db("000001.SZ", start_date="20231204")
        assert len(result) == 2

    def test_filters_by_end_date(self, mock_get_cache, mock_set_cache, temp_db, sample_daily_bar_df):
        """Should filter by end date."""
        _save_to_db(sample_daily_bar_df)

        result = get_daily_bar_from_db("000001.SZ", end_date="20231204")
        assert len(result) == 2

    def test_filters_by_date_range(self, mock_get_cache, mock_set_cache, temp_db, sample_daily_bar_df):
        """Should filter by date range."""
        _save_to_db(sample_daily_bar_df)

        result = get_daily_bar_from_db("000001.SZ", start_date="20231201", end_date="20231204")
        assert len(result) == 2

    def test_returns_empty_for_no_data(self, mock_get_cache, mock_set_cache, temp_db):
        """Should return empty DataFrame when no data."""
        result = get_daily_bar_from_db("000001.SZ")
        assert len(result) == 0

    def test_ordered_by_trade_date(self, mock_get_cache, mock_set_cache, temp_db, sample_daily_bar_df):
        """Should return data ordered by trade_date."""
        _save_to_db(sample_daily_bar_df)

        result = get_daily_bar_from_db("000001.SZ")
        dates = result["trade_date"].tolist()
        assert dates == sorted(dates)


class TestGetDateRange:
    """Tests for get_date_range function."""

    def test_returns_date_range(self, temp_db, sample_daily_bar_df):
        """Should return min and max dates."""
        _save_to_db(sample_daily_bar_df)

        min_date, max_date = get_date_range("000001.SZ")
        assert min_date == "20231201"
        assert max_date == "20231205"

    def test_returns_none_when_no_data(self, temp_db):
        """Should return (None, None) when no data."""
        min_date, max_date = get_date_range("000001.SZ")
        assert min_date is None
        assert max_date is None


class TestSyncDailyBar:
    """Tests for sync_daily_bar function."""

    @patch("src.datahub.daily_bar.get_daily_bar")
    @patch("src.datahub.daily_bar.get_last_trading_day")
    def test_incremental_update(self, mock_last_trading_day, mock_get_daily_bar, temp_db, sample_daily_bar_df):
        """Should perform incremental update."""
        # Setup existing data
        _save_to_db(sample_daily_bar_df)

        # Mock today and new data
        mock_last_trading_day.return_value = "20231210"
        new_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "trade_date": ["20231206", "20231207"],
                "open": [10.5, 10.6],
                "high": [10.8, 10.9],
                "low": [10.3, 10.4],
                "close": [10.7, 10.8],
                "pre_close": [10.4, 10.7],
                "change": [0.3, 0.1],
                "pct_chg": [2.9, 0.9],
                "vol": [1100000.0, 1000000.0],
                "amount": [11770000.0, 10800000.0],
            }
        )
        mock_get_daily_bar.return_value = new_data

        # Sync
        count = sync_daily_bar("000001.SZ")

        # Verify
        assert count == 2
        mock_get_daily_bar.assert_called_once_with("000001.SZ", "20231206", "20231210")

        # Check database
        with get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM daily_bar")
            assert cursor.fetchone()[0] == 5

    @patch("src.datahub.daily_bar.get_daily_bar")
    @patch("src.datahub.daily_bar.get_last_trading_day")
    @patch("src.datahub.daily_bar._get_symbol_list_date")
    def test_first_sync(self, mock_list_date, mock_last_trading_day, mock_get_daily_bar, temp_db, sample_daily_bar_df):
        """Should fetch from list date on first sync."""
        mock_list_date.return_value = "20231201"
        mock_last_trading_day.return_value = "20231205"
        mock_get_daily_bar.return_value = sample_daily_bar_df

        count = sync_daily_bar("000001.SZ")

        assert count == 3
        mock_get_daily_bar.assert_called_once_with("000001.SZ", "20231201", "20231205")

    @patch("src.datahub.daily_bar.get_last_trading_day")
    def test_already_up_to_date(self, mock_last_trading_day, temp_db, sample_daily_bar_df):
        """Should return 0 when already up to date."""
        _save_to_db(sample_daily_bar_df)
        mock_last_trading_day.return_value = "20231205"

        count = sync_daily_bar("000001.SZ")

        assert count == 0

    @patch("src.datahub.daily_bar._get_symbol_list_date")
    def test_raises_error_for_unknown_symbol(self, mock_list_date, temp_db):
        """Should raise error when symbol not found."""
        mock_list_date.return_value = None

        with pytest.raises(ValueError, match="Cannot find list date"):
            sync_daily_bar("999999.SZ")
