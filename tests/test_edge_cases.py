"""Edge case tests for feature detection and return calculation."""

import numpy as np
import pandas as pd
import pytest

from src.common.returns import calculate_period_returns
from src.feature.bullish_cannon import bullish_cannon
from src.feature.consolidation_breakout import consolidation_breakout
from src.feature.volume_stagnation import volume_stagnation
from src.feature.volume_upper_shadow import volume_upper_shadow


def is_bool_like(value) -> bool:
    """Check if value is a boolean or numpy boolean."""
    return isinstance(value, (bool, np.bool_))


class TestEmptyDataFrame:
    """Test feature functions with empty DataFrames."""

    def test_bullish_cannon_empty_df(self):
        df = pd.DataFrame()
        assert bullish_cannon(df) is False

    def test_consolidation_breakout_empty_df(self):
        df = pd.DataFrame()
        assert consolidation_breakout(df) is False

    def test_volume_upper_shadow_empty_df(self):
        df = pd.DataFrame()
        assert volume_upper_shadow(df) is False

    def test_volume_stagnation_empty_df(self):
        df = pd.DataFrame()
        assert volume_stagnation(df) is False


class TestMinimumDataBoundary:
    """Test features with exactly minimum required data."""

    @pytest.fixture
    def base_row(self):
        """Base row data for creating DataFrames."""
        return {
            "ts_code": "000001.SZ",
            "trade_date": "20240101",
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "vol": 1000000,
            "amount": 10000000,
            "pct_chg": 1.0,
            "pre_close": 10.0,
        }

    def test_bullish_cannon_exactly_min_days(self, base_row):
        """Bullish cannon requires 30 days minimum."""
        rows = []
        for i in range(30):
            row = base_row.copy()
            row["trade_date"] = f"2024{i // 30 + 1:02d}{i % 28 + 1:02d}"
            rows.append(row)
        df = pd.DataFrame(rows)
        # Should not crash, may return False (no signal)
        result = bullish_cannon(df)
        assert isinstance(result, bool)

    def test_bullish_cannon_below_min_days(self, base_row):
        """Bullish cannon with insufficient data returns False."""
        rows = []
        for i in range(29):  # One less than minimum
            row = base_row.copy()
            row["trade_date"] = f"2024{i // 28 + 1:02d}{i % 28 + 1:02d}"
            rows.append(row)
        df = pd.DataFrame(rows)
        assert bullish_cannon(df) is False

    def test_consolidation_breakout_exactly_min_days(self, base_row):
        """Consolidation breakout requires 60 days minimum."""
        rows = []
        for i in range(60):
            row = base_row.copy()
            row["trade_date"] = f"2024{i // 28 + 1:02d}{i % 28 + 1:02d}"
            rows.append(row)
        df = pd.DataFrame(rows)
        result = consolidation_breakout(df)
        assert is_bool_like(result)

    def test_consolidation_breakout_below_min_days(self, base_row):
        """Consolidation breakout with insufficient data returns False."""
        rows = []
        for i in range(59):  # One less than minimum
            row = base_row.copy()
            row["trade_date"] = f"2024{i // 28 + 1:02d}{i % 28 + 1:02d}"
            rows.append(row)
        df = pd.DataFrame(rows)
        assert consolidation_breakout(df) is False


class TestInvalidData:
    """Test handling of invalid/unusual data values."""

    @pytest.fixture
    def valid_df(self):
        """Create a valid DataFrame with required columns."""
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 250,
                "trade_date": [f"2024{i // 28 + 1:02d}{i % 28 + 1:02d}" for i in range(250)],
                "open": [10.0] * 250,
                "high": [11.0] * 250,
                "low": [9.5] * 250,
                "close": [10.5] * 250,
                "vol": [1000000] * 250,
                "amount": [10000000] * 250,
                "pct_chg": [1.0] * 250,
                "pre_close": [10.0] * 250,
            }
        )

    def test_volume_upper_shadow_with_nan(self, valid_df):
        """Test volume_upper_shadow handles NaN values."""
        valid_df.loc[100, "close"] = float("nan")
        # dropna() should handle this
        result = volume_upper_shadow(valid_df)
        assert is_bool_like(result)

    def test_volume_stagnation_with_nan(self, valid_df):
        """Test volume_stagnation handles NaN values."""
        valid_df = pd.concat([valid_df, valid_df, valid_df])  # Need 550+ rows
        valid_df = valid_df.reset_index(drop=True)
        valid_df.loc[100, "vol"] = float("nan")
        result = volume_stagnation(valid_df)
        assert is_bool_like(result)


class TestReturnCalculation:
    """Test return calculation edge cases."""

    @pytest.fixture
    def future_df(self):
        """Create a DataFrame with future trading days."""
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 10,
                "trade_date": [
                    "20240101",
                    "20240102",
                    "20240103",
                    "20240104",
                    "20240105",
                    "20240108",
                    "20240109",
                    "20240110",
                    "20240111",
                    "20240112",
                ],
                "open": [10.0, 10.5, 11.0, 10.8, 11.2, 11.5, 11.3, 11.8, 12.0, 12.2],
                "close": [10.5, 10.8, 10.6, 11.0, 11.4, 11.2, 11.6, 11.9, 12.1, 12.0],
            }
        )

    def test_returns_none_for_empty_df(self):
        """Empty DataFrame returns None."""
        df = pd.DataFrame()
        result = calculate_period_returns(df, "20240101", [1, 2, 3])
        assert result is None

    def test_returns_none_for_missing_ts_code(self):
        """DataFrame without ts_code returns None."""
        df = pd.DataFrame({"trade_date": ["20240102"], "open": [10.0], "close": [10.5]})
        result = calculate_period_returns(df, "20240101", [1])
        assert result is None

    def test_returns_none_for_no_future_data(self, future_df):
        """Signal date after all data returns None."""
        result = calculate_period_returns(future_df, "20240115", [1, 2, 3])
        assert result is None

    def test_returns_partial_periods(self, future_df):
        """Returns available periods when not all periods have data."""
        # Signal on 20240110 - only 2 future days available
        result = calculate_period_returns(future_df, "20240110", [1, 2, 3, 5])
        assert result is not None
        assert result["returns"][1] is not None
        assert result["returns"][2] is not None
        assert result["returns"][3] is None  # Not enough data
        assert result["returns"][5] is None  # Not enough data

    def test_returns_zero_entry_price(self, future_df):
        """Zero entry price returns None."""
        future_df.loc[1, "open"] = 0.0
        result = calculate_period_returns(future_df, "20240101", [1])
        assert result is None

    def test_returns_negative_entry_price(self, future_df):
        """Negative entry price returns None."""
        future_df.loc[1, "open"] = -10.0
        result = calculate_period_returns(future_df, "20240101", [1])
        assert result is None

    def test_correct_return_calculation(self, future_df):
        """Verify return calculation is correct."""
        result = calculate_period_returns(future_df, "20240101", [1])
        assert result is not None
        # Entry: 20240102 open = 10.5
        # Exit: 20240102 close = 10.8
        # Return: (10.8 - 10.5) / 10.5 * 100 = 2.857...
        assert result["entry_price"] == 10.5
        assert abs(result["returns"][1] - 2.86) < 0.01

    def test_max_return_calculation(self, future_df):
        """Verify max return during holding period."""
        result = calculate_period_returns(future_df, "20240101", [3])
        assert result is not None
        # Entry: 20240102 open = 10.5
        # Days 1-3 closes: 10.8, 10.6, 11.0
        # Max close = 11.0
        # Max return = (11.0 - 10.5) / 10.5 * 100 = 4.76%
        assert abs(result["max_return"] - 4.76) < 0.01
