"""Core backtest module for evaluating feature signals."""

from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from io import StringIO

import pandas as pd

from src.alphaspike.scanner import FEATURES
from src.datahub.daily_bar import batch_load_daily_bars, get_daily_bar_from_db_no_cache
from src.datahub.symbol import get_ts_codes
from src.datahub.trading_calendar import _load_calendar
from src.feature.bbc import bbc
from src.feature.bullish_cannon import bullish_cannon
from src.feature.consolidation_breakout import consolidation_breakout
from src.feature.high_retracement import high_retracement
from src.feature.volume_stagnation import volume_stagnation
from src.feature.volume_upper_shadow import volume_upper_shadow

# Feature name to function mapping for worker processes
_FEATURE_FUNCS = {
    "bbc": bbc,
    "volume_upper_shadow": volume_upper_shadow,
    "volume_stagnation": volume_stagnation,
    "high_retracement": high_retracement,
    "consolidation_breakout": consolidation_breakout,
    "bullish_cannon": bullish_cannon,
}


@dataclass
class BacktestResult:
    """Backtest result for a single stock."""

    ts_code: str  # Stock code
    signal_date: str  # Signal date (YYYYMMDD)
    entry_date: str  # Entry date (next trading day)
    entry_price: float  # Entry price (open price on entry date)
    exit_date: str  # Exit date (last holding day)
    exit_price: float  # Exit price (close price on exit date)
    total_return: float  # Total return percentage
    max_return: float  # Maximum return percentage during holding period
    holding_days: int  # Actual holding days


def _calculate_future_returns_from_df(
    df: pd.DataFrame,
    signal_date: str,
    holding_days: int = 5,
) -> dict | None:
    """
    Calculate future returns after a signal date.

    Returns dict instead of BacktestResult for pickling in multiprocessing.
    """
    if df.empty or "ts_code" not in df.columns:
        return None

    ts_code = df.iloc[0]["ts_code"]

    # Ensure df is sorted by trade_date
    df = df.sort_values("trade_date").reset_index(drop=True)

    # Find rows after signal_date (these are the future trading days)
    future_df = df[df["trade_date"] > signal_date].reset_index(drop=True)

    # Need at least holding_days rows
    if len(future_df) < holding_days:
        return None

    # Entry is the first day after signal
    entry_row = future_df.iloc[0]
    entry_date = str(entry_row["trade_date"])
    entry_price = float(entry_row["open"])

    if entry_price <= 0:
        return None

    # Exit is the Nth day (index = holding_days - 1)
    exit_row = future_df.iloc[holding_days - 1]
    exit_date = str(exit_row["trade_date"])
    exit_price = float(exit_row["close"])

    # Holding period data (first holding_days rows)
    holding_df = future_df.iloc[:holding_days]

    # Calculate total return
    total_return = (exit_price - entry_price) / entry_price * 100

    # Calculate max return (based on close prices during holding period)
    max_close = float(holding_df["close"].max())
    max_return = (max_close - entry_price) / entry_price * 100

    return {
        "ts_code": ts_code,
        "signal_date": signal_date,
        "entry_date": entry_date,
        "entry_price": entry_price,
        "exit_date": exit_date,
        "exit_price": exit_price,
        "total_return": round(total_return, 2),
        "max_return": round(max_return, 2),
        "holding_days": len(holding_df),
    }


def calculate_future_returns(
    df: pd.DataFrame,
    signal_date: str,
    holding_days: int = 5,
) -> BacktestResult | None:
    """
    Calculate future returns after a signal date.

    Entry: Buy at next trading day's open price
    Exit: Sell at the Nth trading day's close price

    Args:
        df: Daily bar data with columns: ts_code, trade_date, open, close
        signal_date: Signal trigger date (YYYYMMDD)
        holding_days: Number of days to hold (default: 5)

    Returns:
        BacktestResult or None if insufficient data.
    """
    result_dict = _calculate_future_returns_from_df(df, signal_date, holding_days)
    if result_dict:
        return BacktestResult(**result_dict)
    return None


def _backtest_day_worker(args: tuple) -> list[dict]:
    """
    Worker function for parallel backtesting a single day.

    Must be defined at module level to be picklable for ProcessPoolExecutor.

    Args:
        args: (signal_date, ts_codes, feature_name, min_days, holding_days)

    Returns:
        List of result dicts for signals found on this day.
    """
    signal_date, ts_codes, feature_name, min_days, holding_days = args

    feature_func = _FEATURE_FUNCS.get(feature_name)
    if feature_func is None:
        return []

    results = []
    for ts_code in ts_codes:
        try:
            df = get_daily_bar_from_db_no_cache(ts_code, end_date=signal_date)
            if len(df) < min_days:
                continue

            # Check if signal triggered
            if feature_func(df):
                # Need full data for backtest calculation
                full_df = get_daily_bar_from_db_no_cache(ts_code)
                result = _calculate_future_returns_from_df(full_df, signal_date, holding_days)
                if result:
                    results.append(result)
        except Exception:
            continue

    return results


def _backtest_stock_worker(args: tuple) -> list[dict]:
    """
    Worker function for parallel backtesting a single stock across all trading days.

    This is more efficient than _backtest_day_worker because:
    - Only ONE database/data load per stock (vs. one per day)
    - All signal checks happen in memory

    Args:
        args: (ts_code, df_json, trading_days, feature_name, min_days, holding_days)

    Returns:
        List of result dicts for all signals found for this stock.
    """
    _ts_code, df_json, trading_days, feature_name, min_days, holding_days = args

    feature_func = _FEATURE_FUNCS.get(feature_name)
    if feature_func is None:
        return []

    try:
        # Reconstruct DataFrame from JSON (only once per stock)
        df = pd.read_json(StringIO(df_json))
        if len(df) < min_days:
            return []

        # Restore trade_date to string format (JSON converts to int)
        df["trade_date"] = df["trade_date"].astype(str)

        # Ensure sorted by trade_date
        df = df.sort_values("trade_date").reset_index(drop=True)

        # Convert trade_date to set for O(1) lookup
        df_dates = set(df["trade_date"].tolist())

        results = []
        for signal_date in trading_days:
            # Skip if signal_date not in this stock's data
            if signal_date not in df_dates:
                continue

            # Get data up to signal_date for feature check
            df_slice = df[df["trade_date"] <= signal_date]
            if len(df_slice) < min_days:
                continue

            # Check if signal triggered
            if feature_func(df_slice):
                # Calculate returns using full df (includes future data)
                result = _calculate_future_returns_from_df(df, signal_date, holding_days)
                if result:
                    results.append(result)

        return results
    except Exception:
        return []


def backtest_feature(
    feature_name: str,
    signal_date: str,
    holding_days: int = 5,
) -> list[BacktestResult]:
    """
    Backtest a feature on a specific signal date.

    Args:
        feature_name: Feature name (e.g., 'bbc', 'bullish_cannon')
        signal_date: Signal date to backtest (YYYYMMDD)
        holding_days: Number of days to hold (default: 5)

    Returns:
        List of BacktestResult for all stocks with signals.
    """
    feature_config = _get_feature_config(feature_name)
    if feature_config is None:
        return []

    ts_codes = get_ts_codes()
    results = _backtest_day_worker((signal_date, ts_codes, feature_name, feature_config.min_days, holding_days))

    return [BacktestResult(**r) for r in results]


def _get_feature_config(feature_name: str):
    """Get feature config by name."""
    for f in FEATURES:
        if f.name == feature_name:
            return f
    return None


@dataclass
class YearlyBacktestStats:
    """Yearly backtest statistics."""

    feature_name: str  # Feature name
    year: int  # Year
    total_signals: int  # Total signal count
    win_count: int  # Win count (total_return > 0)
    loss_count: int  # Loss count (total_return <= 0)
    win_rate: float  # Win rate (%)
    max_win_count: int  # Win count based on max_return > 0
    max_win_rate: float  # Win rate based on max_return (%)
    total_return_sum: float  # Sum of all 5-day returns (%)
    win_return_sum: float  # Sum of winning returns (%)
    loss_return_sum: float  # Sum of losing returns (%)
    max_return_sum: float  # Sum of max returns during holding period (%)
    avg_return: float  # Average 5-day return (%)
    max_return: float  # Maximum return (%)
    min_return: float  # Minimum return (%)
    trading_days_count: int  # Number of trading days backtested


def get_year_trading_days(year: int) -> list[str]:
    """
    Get all trading days for a given year.

    Args:
        year: Year (e.g., 2025)

    Returns:
        List of trading dates in YYYYMMDD format.
    """
    try:
        calendar = _load_calendar()
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)

        year_df = calendar[
            (calendar["trade_date"] >= year_start)
            & (calendar["trade_date"] <= year_end)
            & (calendar["trade_status"] == 1)
        ]

        return [d.strftime("%Y%m%d") for d in year_df["trade_date"].tolist()]
    except FileNotFoundError:
        return []


def _extract_year_trading_days(data_cache: dict[str, pd.DataFrame], year: int) -> list[str]:
    """
    Extract trading days for a specific year from loaded data.

    Args:
        data_cache: Dict mapping ts_code to DataFrame
        year: Year to extract trading days for

    Returns:
        Sorted list of trading dates in YYYYMMDD format.
    """
    year_prefix = str(year)
    all_dates = set()

    for df in data_cache.values():
        if "trade_date" in df.columns:
            dates = df["trade_date"].astype(str).tolist()
            for d in dates:
                if d.startswith(year_prefix):
                    all_dates.add(d)

    return sorted(all_dates)


def backtest_year(
    feature_name: str,
    year: int,
    holding_days: int = 5,
    progress_callback: Callable[[int, int], None] | None = None,
    max_workers: int = 6,
) -> tuple[YearlyBacktestStats, list[BacktestResult]]:
    """
    Backtest a feature for an entire year.

    Uses stock-level parallelization for efficiency:
    - Phase 1: Batch load all stock data (single DB query)
    - Phase 2: Parallel process each stock across all trading days
    - Phase 3: Aggregate results

    Args:
        feature_name: Feature name (e.g., 'bullish_cannon')
        year: Year to backtest (e.g., 2025)
        holding_days: Number of days to hold (default: 5)
        progress_callback: Callback function for progress (current, total)
        max_workers: Number of parallel workers (default: 6)

    Returns:
        Tuple of (YearlyBacktestStats, list of all BacktestResult)
    """
    feature_config = _get_feature_config(feature_name)
    if feature_config is None:
        return _empty_stats(feature_name, year, 0), []

    ts_codes = get_ts_codes()
    total_stocks = len(ts_codes)

    # Phase 1: Batch load all stock data (single DB query)
    data_cache = batch_load_daily_bars(ts_codes)

    # Extract trading days from data (no calendar dependency)
    trading_days = _extract_year_trading_days(data_cache, year)
    total_days = len(trading_days)

    if total_days == 0:
        return _empty_stats(feature_name, year, 0), []

    # Phase 2: Prepare work items (one per stock)
    work_items = []
    for ts_code in ts_codes:
        if ts_code in data_cache:
            df = data_cache[ts_code]
            work_items.append(
                (
                    ts_code,
                    df.to_json(),
                    trading_days,
                    feature_name,
                    feature_config.min_days,
                    holding_days,
                )
            )

    # Clear data_cache to free memory before spawning processes
    del data_cache

    all_results: list[BacktestResult] = []
    completed = 0

    # Phase 3: Parallel execution by stock
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_backtest_stock_worker, item): item[0] for item in work_items}

        for future in as_completed(futures):
            stock_results = future.result()
            for result_dict in stock_results:
                all_results.append(BacktestResult(**result_dict))

            completed += 1
            if progress_callback:
                progress_callback(completed, total_stocks)

    # Calculate statistics
    stats = _calculate_yearly_stats(feature_name, year, all_results, total_days)

    return stats, all_results


def _empty_stats(feature_name: str, year: int, trading_days_count: int) -> YearlyBacktestStats:
    """Return empty stats."""
    return YearlyBacktestStats(
        feature_name=feature_name,
        year=year,
        total_signals=0,
        win_count=0,
        loss_count=0,
        win_rate=0.0,
        max_win_count=0,
        max_win_rate=0.0,
        total_return_sum=0.0,
        win_return_sum=0.0,
        loss_return_sum=0.0,
        max_return_sum=0.0,
        avg_return=0.0,
        max_return=0.0,
        min_return=0.0,
        trading_days_count=trading_days_count,
    )


def _calculate_yearly_stats(
    feature_name: str,
    year: int,
    results: list[BacktestResult],
    trading_days_count: int,
) -> YearlyBacktestStats:
    """Calculate yearly backtest statistics from results."""
    if not results:
        return _empty_stats(feature_name, year, trading_days_count)

    total_signals = len(results)
    win_results = [r for r in results if r.total_return > 0]
    loss_results = [r for r in results if r.total_return <= 0]
    max_win_results = [r for r in results if r.max_return > 0]

    win_count = len(win_results)
    loss_count = len(loss_results)
    win_rate = round(win_count / total_signals * 100, 2)
    max_win_count = len(max_win_results)
    max_win_rate = round(max_win_count / total_signals * 100, 2)

    returns = [r.total_return for r in results]
    total_return_sum = round(sum(returns), 2)
    win_return_sum = round(sum(r.total_return for r in win_results), 2)
    loss_return_sum = round(sum(r.total_return for r in loss_results), 2)
    max_return_sum = round(sum(r.max_return for r in results), 2)
    avg_return = round(total_return_sum / total_signals, 2)
    max_return = round(max(returns), 2)
    min_return = round(min(returns), 2)

    return YearlyBacktestStats(
        feature_name=feature_name,
        year=year,
        total_signals=total_signals,
        win_count=win_count,
        loss_count=loss_count,
        win_rate=win_rate,
        max_win_count=max_win_count,
        max_win_rate=max_win_rate,
        total_return_sum=total_return_sum,
        win_return_sum=win_return_sum,
        loss_return_sum=loss_return_sum,
        max_return_sum=max_return_sum,
        avg_return=avg_return,
        max_return=max_return,
        min_return=min_return,
        trading_days_count=trading_days_count,
    )
