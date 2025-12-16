"""Backtest module for feature signal evaluation."""

from src.backtest.backtest import (
    BacktestResult,
    YearlyBacktestStats,
    backtest_feature,
    backtest_year,
    calculate_future_returns,
    get_year_trading_days,
)

__all__ = [
    "BacktestResult",
    "YearlyBacktestStats",
    "backtest_feature",
    "backtest_year",
    "calculate_future_returns",
    "get_year_trading_days",
]
