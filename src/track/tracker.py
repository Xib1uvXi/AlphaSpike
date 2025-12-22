"""Feature performance tracking module."""

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from src.alphaspike.db import (
    get_all_feature_results,
    get_distinct_feature_names,
    get_feature_result_by_name_and_date,
    get_feature_results_by_date,
    get_feature_results_by_name,
)
from src.common.logging import get_logger
from src.common.returns import calculate_period_returns
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


@dataclass
class SignalDetail:
    """Detailed return data for a signal."""

    ts_code: str
    signal_date: str
    return_1d: float
    return_2d: float
    return_3d: float


# Alias for backward compatibility
AllNegativeSignal = SignalDetail


@dataclass
class SignalCategory:
    """A category of signals with statistics."""

    signals: list[SignalDetail]
    count: int
    ratio: float  # Percentage of total
    avg_1d: float
    avg_2d: float
    avg_3d: float


@dataclass
class AllNegativeAnalysis:
    """Analysis result for all-negative signals of a feature."""

    feature_name: str
    total_signals: int  # Total valid signals (with all 3 return values)
    negative_count: int  # Count of all-negative signals
    negative_ratio: float  # Percentage of all-negative signals
    avg_loss_1d: float  # Average 1d loss for all-negative signals
    avg_loss_2d: float  # Average 2d loss for all-negative signals
    avg_loss_3d: float  # Average 3d loss for all-negative signals
    signals: list[SignalDetail]  # Detailed list of all-negative signals
    # Detailed categories (populated when single feature is analyzed)
    all_positive: SignalCategory | None = None  # All 1d/2d/3d > 0
    mixed: SignalCategory | None = None  # Mixed positive and negative
    all_negative_cat: SignalCategory | None = None  # All 1d/2d/3d < 0


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
    # Use common return calculation
    result = calculate_period_returns(df, signal_date, holding_periods=[1, 2, 3])
    if result is None:
        return None

    returns = result["returns"]
    return SignalReturn(
        ts_code=ts_code,
        signal_date=signal_date,
        entry_date=result["entry_date"],
        entry_price=result["entry_price"],
        return_1d=returns.get(1),
        return_2d=returns.get(2),
        return_3d=returns.get(3),
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
    end_date: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[FeaturePerformance]:
    """
    Track performance for stored feature signals.

    Args:
        feature_name: Specific feature to track, or None for all features
        end_date: Specific scan date to track (YYYYMMDD), or None for all dates
        progress_callback: Optional callback for progress updates

    Returns:
        List of FeaturePerformance for each feature.
    """
    # Get stored results based on filters
    feature_results: dict[str, list[tuple[str, list[str]]]] = {}

    if feature_name and end_date:
        # Both feature and date specified
        stored_results = get_feature_result_by_name_and_date(feature_name, end_date)
        if not stored_results:
            return []
        feature_results = {feature_name: stored_results}
    elif feature_name:
        # Only feature specified
        stored_results = get_feature_results_by_name(feature_name)
        if not stored_results:
            return []
        feature_results = {feature_name: stored_results}
    elif end_date:
        # Only date specified
        all_results = get_feature_results_by_date(end_date)
        if not all_results:
            return []
        for fname, scan_date, ts_codes in all_results:
            if fname not in feature_results:
                feature_results[fname] = []
            feature_results[fname].append((scan_date, ts_codes))
    else:
        # Neither specified - get all
        all_results = get_all_feature_results()
        if not all_results:
            return []
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


def _is_all_negative(signal: SignalReturn) -> bool:
    """
    Check if a signal has all negative returns.

    Args:
        signal: SignalReturn object

    Returns:
        True if return_1d, return_2d, return_3d are all < 0 and not None.
    """
    return (
        signal.return_1d is not None
        and signal.return_2d is not None
        and signal.return_3d is not None
        and signal.return_1d < 0
        and signal.return_2d < 0
        and signal.return_3d < 0
    )


def _is_all_positive(signal: SignalReturn) -> bool:
    """Check if a signal has all positive returns."""
    return (
        signal.return_1d is not None
        and signal.return_2d is not None
        and signal.return_3d is not None
        and signal.return_1d > 0
        and signal.return_2d > 0
        and signal.return_3d > 0
    )


def _create_signal_category(signals: list[SignalReturn], total: int) -> SignalCategory:
    """Create a SignalCategory from a list of signals."""
    count = len(signals)
    ratio = (count / total * 100) if total > 0 else 0.0

    if count > 0:
        avg_1d = sum(r.return_1d for r in signals) / count
        avg_2d = sum(r.return_2d for r in signals) / count
        avg_3d = sum(r.return_3d for r in signals) / count
    else:
        avg_1d = avg_2d = avg_3d = 0.0

    # Convert to SignalDetail and sort
    details = [
        SignalDetail(
            ts_code=r.ts_code,
            signal_date=r.signal_date,
            return_1d=r.return_1d,
            return_2d=r.return_2d,
            return_3d=r.return_3d,
        )
        for r in signals
    ]
    details.sort(key=lambda s: (s.signal_date, s.ts_code), reverse=True)

    return SignalCategory(
        signals=details,
        count=count,
        ratio=round(ratio, 2),
        avg_1d=round(avg_1d, 2),
        avg_2d=round(avg_2d, 2),
        avg_3d=round(avg_3d, 2),
    )


def _analyze_negative_signals(
    feature_name: str,
    returns: list[SignalReturn],
    include_categories: bool = False,
) -> AllNegativeAnalysis:
    """
    Analyze all-negative signals for a feature.

    Args:
        feature_name: Feature name
        returns: List of SignalReturn objects
        include_categories: If True, include all three categories (positive/mixed/negative)

    Returns:
        AllNegativeAnalysis with statistics and detailed signal list.
    """
    # Count valid signals (those with all three return values)
    valid_signals = [
        r for r in returns if r.return_1d is not None and r.return_2d is not None and r.return_3d is not None
    ]
    total_valid = len(valid_signals)

    # Filter all-negative signals
    negative_signals = [r for r in valid_signals if _is_all_negative(r)]
    negative_count = len(negative_signals)

    # Calculate ratio
    negative_ratio = (negative_count / total_valid * 100) if total_valid > 0 else 0.0

    # Calculate average losses
    if negative_count > 0:
        avg_loss_1d = sum(r.return_1d for r in negative_signals) / negative_count
        avg_loss_2d = sum(r.return_2d for r in negative_signals) / negative_count
        avg_loss_3d = sum(r.return_3d for r in negative_signals) / negative_count
    else:
        avg_loss_1d = avg_loss_2d = avg_loss_3d = 0.0

    # Convert to SignalDetail objects
    signal_list = [
        SignalDetail(
            ts_code=r.ts_code,
            signal_date=r.signal_date,
            return_1d=r.return_1d,
            return_2d=r.return_2d,
            return_3d=r.return_3d,
        )
        for r in negative_signals
    ]

    # Sort by signal_date descending, then by ts_code
    signal_list.sort(key=lambda s: (s.signal_date, s.ts_code), reverse=True)

    # Build categories if requested
    all_positive_cat = None
    mixed_cat = None
    all_negative_cat = None

    if include_categories:
        positive_signals = [r for r in valid_signals if _is_all_positive(r)]
        mixed_signals = [r for r in valid_signals if not _is_all_positive(r) and not _is_all_negative(r)]

        all_positive_cat = _create_signal_category(positive_signals, total_valid)
        mixed_cat = _create_signal_category(mixed_signals, total_valid)
        all_negative_cat = _create_signal_category(negative_signals, total_valid)

    return AllNegativeAnalysis(
        feature_name=feature_name,
        total_signals=total_valid,
        negative_count=negative_count,
        negative_ratio=round(negative_ratio, 2),
        avg_loss_1d=round(avg_loss_1d, 2),
        avg_loss_2d=round(avg_loss_2d, 2),
        avg_loss_3d=round(avg_loss_3d, 2),
        signals=signal_list,
        all_positive=all_positive_cat,
        mixed=mixed_cat,
        all_negative_cat=all_negative_cat,
    )


# pylint: disable=too-many-locals,too-many-branches
def analyze_all_negative_signals(
    feature_name: str | None = None,
    end_date: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[AllNegativeAnalysis]:
    """
    Analyze all-negative signals for stored features.

    Entry point function for CLI --analyze option.

    Args:
        feature_name: Specific feature to analyze, or None for all features
        end_date: Specific scan date to analyze (YYYYMMDD), or None for all dates
        progress_callback: Optional callback for progress updates

    Returns:
        List of AllNegativeAnalysis for each feature.
    """
    # Get stored results based on filters (same logic as track_feature_performance)
    feature_results: dict[str, list[tuple[str, list[str]]]] = {}

    if feature_name and end_date:
        stored_results = get_feature_result_by_name_and_date(feature_name, end_date)
        if not stored_results:
            return []
        feature_results = {feature_name: stored_results}
    elif feature_name:
        stored_results = get_feature_results_by_name(feature_name)
        if not stored_results:
            return []
        feature_results = {feature_name: stored_results}
    elif end_date:
        all_results = get_feature_results_by_date(end_date)
        if not all_results:
            return []
        for fname, scan_date, ts_codes in all_results:
            if fname not in feature_results:
                feature_results[fname] = []
            feature_results[fname].append((scan_date, ts_codes))
    else:
        all_results = get_all_feature_results()
        if not all_results:
            return []
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

    # Analyze all-negative signals for each feature
    # Include detailed categories only when a single feature is specified
    include_categories = feature_name is not None
    analyses = []
    for fname, returns in signal_returns.items():
        analysis = _analyze_negative_signals(fname, returns, include_categories=include_categories)
        analyses.append(analysis)

    # Sort by feature name
    analyses.sort(key=lambda a: a.feature_name)

    return analyses
