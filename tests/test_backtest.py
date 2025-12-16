"""Tests for backtest module."""

import pandas as pd
import pytest
from dotenv import load_dotenv

load_dotenv()

from src.backtest.backtest import (
    BacktestResult,
    backtest_feature,
    calculate_future_returns,
)


class TestCalculateFutureReturns:
    """Tests for calculate_future_returns function."""

    def test_basic_return_calculation(self):
        """Test basic return calculation with mock data."""
        # Create mock data - 7 trading days
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 7,
                "trade_date": [
                    "20251208",  # Signal date (Sunday in real life, but we use DataFrame data)
                    "20251209",  # Entry day (T+1)
                    "20251210",  # T+2
                    "20251211",  # T+3
                    "20251212",  # T+4
                    "20251213",  # T+5 (Exit)
                    "20251214",  # T+6
                ],
                "open": [10.0, 10.5, 10.8, 11.0, 11.2, 11.0, 10.8],
                "high": [10.8, 11.0, 11.2, 11.5, 11.8, 11.5, 11.2],
                "low": [9.8, 10.2, 10.5, 10.8, 11.0, 10.8, 10.5],
                "close": [10.4, 10.7, 11.0, 11.3, 11.5, 11.2, 10.9],
            }
        )

        # Signal on 20251208, entry on 20251209
        result = calculate_future_returns(df, "20251208", holding_days=5)

        assert result is not None
        assert result.ts_code == "000001.SZ"
        assert result.signal_date == "20251208"
        assert result.entry_date == "20251209"
        assert result.entry_price == 10.5  # Open on entry day
        assert result.exit_date == "20251213"  # 5th trading day
        assert result.exit_price == 11.2  # Close on exit day
        assert result.holding_days == 5

        # Total return: (11.2 - 10.5) / 10.5 * 100 = 6.67%
        assert abs(result.total_return - 6.67) < 0.01

        # Max return: max close in holding period is 11.5 on 20251212
        # (11.5 - 10.5) / 10.5 * 100 = 9.52%
        assert abs(result.max_return - 9.52) < 0.01

    def test_returns_none_for_empty_df(self):
        """Test that empty DataFrame returns None."""
        df = pd.DataFrame()
        result = calculate_future_returns(df, "20251210", holding_days=5)
        assert result is None

    def test_returns_none_for_missing_ts_code(self):
        """Test that missing ts_code column returns None."""
        df = pd.DataFrame({"trade_date": ["20251210"], "close": [10.0]})
        result = calculate_future_returns(df, "20251210", holding_days=5)
        assert result is None

    def test_returns_none_for_insufficient_future_data(self):
        """Test returns None when not enough future data."""
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 3,
                "trade_date": ["20251208", "20251209", "20251210"],
                "open": [10.0, 10.5, 10.8],
                "close": [10.4, 10.7, 11.0],
            }
        )
        # Signal on last day, only 0 future days available
        result = calculate_future_returns(df, "20251210", holding_days=5)
        assert result is None

    def test_uses_dataframe_trading_days(self):
        """Test that function uses DataFrame rows as trading days."""
        # Simulate a gap in trading days (e.g., weekend)
        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 6,
                "trade_date": [
                    "20251205",  # Friday (signal)
                    "20251208",  # Monday (entry) - weekend skipped
                    "20251209",
                    "20251210",
                    "20251211",
                    "20251212",  # Exit
                ],
                "open": [10.0, 10.5, 10.8, 11.0, 11.2, 11.0],
                "close": [10.4, 10.7, 11.0, 11.3, 11.5, 11.2],
            }
        )

        result = calculate_future_returns(df, "20251205", holding_days=5)

        assert result is not None
        assert result.entry_date == "20251208"  # First row after signal
        assert result.exit_date == "20251212"  # 5th row after signal


@pytest.mark.skip(reason="Requires database with real data")
class TestBacktestFeature:
    """Integration tests for backtest_feature function."""

    def test_backtest_bullish_cannon(self):
        """Test backtesting bullish_cannon feature."""
        results = backtest_feature("bullish_cannon", "20251210", holding_days=5)

        print(f"\nBacktest results for bullish_cannon on 20251210:")
        print(f"Total signals: {len(results)}")

        if results:
            print(f"\n{'Stock':<12} {'Entry':<10} {'Entry$':<8} {'Exit$':<8} {'Return%':<10} {'MaxRet%':<10}")
            print("-" * 70)
            for r in results:
                print(
                    f"{r.ts_code:<12} {r.entry_date:<10} {r.entry_price:<8.2f} "
                    f"{r.exit_price:<8.2f} {r.total_return:<10.2f} {r.max_return:<10.2f}"
                )

            avg_return = sum(r.total_return for r in results) / len(results)
            avg_max_return = sum(r.max_return for r in results) / len(results)
            win_rate = len([r for r in results if r.total_return > 0]) / len(results) * 100

            print(f"\nStatistics:")
            print(f"  Average return: {avg_return:.2f}%")
            print(f"  Average max return: {avg_max_return:.2f}%")
            print(f"  Win rate: {win_rate:.1f}%")

        assert True


@pytest.mark.skip(reason="Requires database with real data")
class TestBacktestWithRealData:
    """Real data tests for backtest module."""

    def test_backtest_known_signal(self):
        """Test backtest with a known signal stock."""
        from src.datahub.daily_bar import get_daily_bar_from_db

        ts_code = "300043.SZ"
        signal_date = "20250609"

        df = get_daily_bar_from_db(ts_code)
        result = calculate_future_returns(df, signal_date, holding_days=5)

        print(f"\nBacktest result for {ts_code} signal on {signal_date}:")
        if result:
            print(f"  Entry date: {result.entry_date}")
            print(f"  Entry price: {result.entry_price:.2f}")
            print(f"  Exit date: {result.exit_date}")
            print(f"  Exit price: {result.exit_price:.2f}")
            print(f"  Total return: {result.total_return:.2f}%")
            print(f"  Max return: {result.max_return:.2f}%")
        else:
            print("  No result (insufficient data)")

        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
