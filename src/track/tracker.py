"""Feature performance tracking module."""

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from src.alphaspike.db import (
    get_all_feature_results,
    get_distinct_feature_names,
    get_feature_results_by_name,
)
from src.common.logging import get_logger
from src.datahub.daily_bar import batch_load_daily_bars

_logger = get_logger(__name__)


@dataclass
class SignalReturn:
    """Return data for a single signal."""

    ts_code: str
    signal_date: str
    entry_date: str
    entry_price: float
    return_1d: float | None
    return_2d: float | None
    return_3d: float | None


@dataclass
class PeriodStats:
    """Statistics for a single holding period."""

    win_rate: float
    avg_return: float
    max_return: float
    max_stock: str  # ts_code with max return
    max_date: str  # signal date with max return
    min_return: float
    min_stock: str  # ts_code with min return
    min_date: str  # signal date with min return


@dataclass
class FeaturePerformance:
    """Aggregate performance for a feature."""

    feature_name: str
    total_signals: int
    valid_signals: int
    stats_1d: PeriodStats
    stats_2d: PeriodStats
    stats_3d: PeriodStats
    start_date: str
    end_date: str


def calculate_signal_returns(
    ts_code: str,
    signal_date: str,
    df: pd.DataFrame,
) -> SignalReturn | None:
    """
    Calculate 1d/2d/3d returns for a signal.

    Entry: Next trading day's open price after signal date.
    Exit: Nth trading day's close price (N=1,2,3).

    Args:
        ts_code: Stock code
        signal_date: Signal date in YYYYMMDD format
        df: Daily bar data DataFrame

    Returns:
        SignalReturn with calculated returns, or None if insufficient data.
    """
    if df.empty:
        return None

    # Ensure sorted by trade_date
    df = df.sort_values("trade_date").reset_index(drop=True)

    # Find rows after signal_date
    future_df = df[df["trade_date"] > signal_date].reset_index(drop=True)

    if len(future_df) < 1:
        return None

    # Entry is first day after signal
    entry_row = future_df.iloc[0]
    entry_date = str(entry_row["trade_date"])
    entry_price = float(entry_row["open"])

    if entry_price <= 0:
        return None

    # Calculate returns for each holding period
    return_1d = None
    return_2d = None
    return_3d = None

    if len(future_df) >= 1:
        exit_price = float(future_df.iloc[0]["close"])
        return_1d = (exit_price - entry_price) / entry_price * 100

    if len(future_df) >= 2:
        exit_price = float(future_df.iloc[1]["close"])
        return_2d = (exit_price - entry_price) / entry_price * 100

    if len(future_df) >= 3:
        exit_price = float(future_df.iloc[2]["close"])
        return_3d = (exit_price - entry_price) / entry_price * 100

    return SignalReturn(
        ts_code=ts_code,
        signal_date=signal_date,
        entry_date=entry_date,
        entry_price=round(entry_price, 2),
        return_1d=round(return_1d, 2) if return_1d is not None else None,
        return_2d=round(return_2d, 2) if return_2d is not None else None,
        return_3d=round(return_3d, 2) if return_3d is not None else None,
    )


def _calc_period_stats(
    returns: list[SignalReturn],
    period: int,
) -> PeriodStats:
    """
    Calculate statistics for a single holding period.

    Args:
        returns: List of SignalReturn objects
        period: Holding period (1, 2, or 3)

    Returns:
        PeriodStats with win rate, avg/max/min returns and corresponding stocks.
    """

    # Get return value based on period
    def get_return(r: SignalReturn) -> float | None:
        if period == 1:
            return r.return_1d
        if period == 2:
            return r.return_2d
        return r.return_3d

    # Filter valid returns with their SignalReturn objects
    valid_returns = [(r, get_return(r)) for r in returns if get_return(r) is not None]

    if not valid_returns:
        return PeriodStats(
            win_rate=0.0,
            avg_return=0.0,
            max_return=0.0,
            max_stock="",
            max_date="",
            min_return=0.0,
            min_stock="",
            min_date="",
        )

    # Calculate stats
    vals = [v for _, v in valid_returns]
    wins = sum(1 for v in vals if v > 0)
    win_rate = round(wins / len(vals) * 100, 2)
    avg_return = round(sum(vals) / len(vals), 2)

    # Find max and min
    max_item = max(valid_returns, key=lambda x: x[1])
    min_item = min(valid_returns, key=lambda x: x[1])

    return PeriodStats(
        win_rate=win_rate,
        avg_return=avg_return,
        max_return=round(max_item[1], 2),
        max_stock=max_item[0].ts_code,
        max_date=max_item[0].signal_date,
        min_return=round(min_item[1], 2),
        min_stock=min_item[0].ts_code,
        min_date=min_item[0].signal_date,
    )


def _aggregate_performance(
    feature_name: str,
    returns: list[SignalReturn],
    date_range: tuple[str, str],
) -> FeaturePerformance:
    """
    Aggregate signal returns into feature performance stats.

    Args:
        feature_name: Feature name
        returns: List of SignalReturn objects
        date_range: (start_date, end_date) tuple

    Returns:
        FeaturePerformance with aggregated stats.
    """
    total_signals = len(returns)
    valid_signals = sum(1 for r in returns if r.return_1d is not None)

    return FeaturePerformance(
        feature_name=feature_name,
        total_signals=total_signals,
        valid_signals=valid_signals,
        stats_1d=_calc_period_stats(returns, 1),
        stats_2d=_calc_period_stats(returns, 2),
        stats_3d=_calc_period_stats(returns, 3),
        start_date=date_range[0],
        end_date=date_range[1],
    )


# pylint: disable=too-many-locals,too-many-branches
def track_feature_performance(
    feature_name: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[FeaturePerformance]:
    """
    Track performance for stored feature signals.

    Args:
        feature_name: Specific feature to track, or None for all features
        progress_callback: Optional callback for progress updates

    Returns:
        List of FeaturePerformance for each feature.
    """
    # Get stored results
    if feature_name:
        stored_results = get_feature_results_by_name(feature_name)
        if not stored_results:
            return []
        feature_results = {feature_name: stored_results}
    else:
        all_results = get_all_feature_results()
        if not all_results:
            return []
        # Group by feature_name
        feature_results: dict[str, list[tuple[str, list[str]]]] = {}
        for fname, scan_date, ts_codes in all_results:
            if fname not in feature_results:
                feature_results[fname] = []
            feature_results[fname].append((scan_date, ts_codes))

    # Collect all unique (feature_name, ts_code, signal_date) tuples
    all_signals: list[tuple[str, str, str]] = []
    for fname, results in feature_results.items():
        for scan_date, ts_codes in results:
            for ts_code in ts_codes:
                all_signals.append((fname, ts_code, scan_date))

    if not all_signals:
        return []

    # Batch load price data for efficiency
    ts_codes_needed = list(set(s[1] for s in all_signals))
    _logger.info("Loading price data for %d symbols...", len(ts_codes_needed))
    data_cache = batch_load_daily_bars(ts_codes_needed)

    # Calculate returns for each signal
    signal_returns: dict[str, list[SignalReturn]] = {fname: [] for fname in feature_results}

    total = len(all_signals)
    for i, (fname, ts_code, signal_date) in enumerate(all_signals):
        if ts_code in data_cache:
            result = calculate_signal_returns(ts_code, signal_date, data_cache[ts_code])
            if result:
                signal_returns[fname].append(result)

        if progress_callback:
            progress_callback(i + 1, total)

    # Aggregate into FeaturePerformance
    performances = []
    for fname, returns in signal_returns.items():
        # Get date range from stored results
        dates = [scan_date for scan_date, _ in feature_results[fname]]
        date_range = (min(dates), max(dates)) if dates else ("", "")

        perf = _aggregate_performance(fname, returns, date_range)
        performances.append(perf)

    # Sort by feature name
    performances.sort(key=lambda p: p.feature_name)

    return performances


def get_stored_feature_names() -> list[str]:
    """
    Get list of feature names that have stored results.

    Returns:
        List of feature names.
    """
    return get_distinct_feature_names()
