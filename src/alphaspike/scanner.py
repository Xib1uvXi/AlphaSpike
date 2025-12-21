"""Feature scanner module."""

import pickle
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable

import pandas as pd
import redis

from src.alphaspike.cache import get_feature_cache, get_redis_client, set_feature_cache
from src.common.logging import get_logger
from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.registry import FEATURE_FUNCS, FEATURES, FeatureConfig

_logger = get_logger(__name__)

# Columns required by feature functions (reduces pickle serialization overhead by ~30%)
_FEATURE_REQUIRED_COLS = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "pct_chg", "amount"]


def scan_feature_single(feature: FeatureConfig, df: pd.DataFrame) -> bool:
    """
    Scan a single DataFrame for a feature signal.

    Args:
        feature: Feature configuration
        df: Daily bar data DataFrame

    Returns:
        True if signal detected, False otherwise.
    """
    try:
        return feature.func(df)
    except (KeyError, ValueError, IndexError, TypeError) as e:
        _logger.debug("Feature %s scan failed: %s", feature.name, e)
        return False


def _scan_symbol_worker(args: tuple) -> tuple[str, bool, str]:
    """
    Worker function for parallel scanning.

    Must be defined at module level to be picklable for ProcessPoolExecutor.

    Args:
        args: (ts_code, df_bytes, feature_name, min_days)

    Returns:
        (ts_code, has_signal, status) where status is "ok", "skip", or "error"
    """
    ts_code, df_bytes, feature_name, min_days = args

    try:
        # Reconstruct DataFrame from pickle (faster than JSON)
        df = pickle.loads(df_bytes)

        # Check minimum data requirement
        if len(df) < min_days:
            return (ts_code, False, "skip")

        # Get feature function and execute
        feature_func = FEATURE_FUNCS[feature_name]
        has_signal = feature_func(df)
        return (ts_code, has_signal, "ok")
    except (KeyError, ValueError, IndexError, TypeError, pickle.UnpicklingError):
        return (ts_code, False, "error")


@dataclass
class ScanResult:
    """Result of scanning a single feature."""

    feature_name: str
    signals: list[str]  # List of ts_codes with signals
    from_cache: bool  # Whether result was from cache
    scanned: int  # Number of symbols scanned (0 if from cache)
    skipped: int  # Number of symbols skipped (insufficient data)
    errors: int  # Number of errors during scan


def scan_feature(
    feature: FeatureConfig,
    end_date: str,
    ts_codes: list[str],
    *,
    use_cache: bool = True,
    redis_client: redis.Redis | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    data_cache: dict[str, pd.DataFrame] | None = None,
    max_workers: int = 6,
) -> ScanResult:
    """
    Scan all symbols for a single feature.

    Args:
        feature: Feature configuration
        end_date: End date in YYYYMMDD format
        ts_codes: List of stock codes to scan
        use_cache: Whether to use cached results
        redis_client: Optional Redis client for caching
        progress_callback: Optional callback(current, total) for progress updates
        data_cache: Pre-loaded data dict mapping ts_code to DataFrame
        max_workers: Number of parallel workers (default: 6)

    Returns:
        ScanResult with signals and statistics
    """
    # Try cache first (works with or without Redis)
    if use_cache:
        cached = get_feature_cache(feature.name, end_date, redis_client)
        if cached is not None:
            return ScanResult(
                feature_name=feature.name,
                signals=cached,
                from_cache=True,
                scanned=0,
                skipped=0,
                errors=0,
            )

    # Use parallel scanning if data_cache is provided
    if data_cache is not None:
        return _scan_feature_parallel(
            feature=feature,
            ts_codes=ts_codes,
            data_cache=data_cache,
            max_workers=max_workers,
            progress_callback=progress_callback,
            redis_client=redis_client,
            end_date=end_date,
        )

    # Fallback to sequential scanning (for backward compatibility)
    return _scan_feature_sequential(
        feature=feature,
        end_date=end_date,
        ts_codes=ts_codes,
        progress_callback=progress_callback,
        redis_client=redis_client,
    )


def _scan_feature_sequential(
    feature: FeatureConfig,
    end_date: str,
    ts_codes: list[str],
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    redis_client: redis.Redis | None = None,
) -> ScanResult:
    """Sequential scanning (original implementation)."""
    signals = []
    skipped = 0
    errors = 0
    total = len(ts_codes)

    for i, ts_code in enumerate(ts_codes):
        try:
            df = get_daily_bar_from_db(ts_code, end_date=end_date)

            # Check minimum data requirement
            if len(df) < feature.min_days:
                skipped += 1
                continue

            # Run feature detection
            if feature.func(df):
                signals.append(ts_code)

        except (KeyError, ValueError, IndexError, TypeError) as e:
            _logger.debug("Error scanning %s for %s: %s", ts_code, feature.name, e)
            errors += 1

        # Progress callback
        if progress_callback:
            progress_callback(i + 1, total)

    # Cache results (always persist to SQLite, optionally to Redis)
    set_feature_cache(feature.name, end_date, signals, redis_client)

    return ScanResult(
        feature_name=feature.name,
        signals=signals,
        from_cache=False,
        scanned=total - skipped - errors,
        skipped=skipped,
        errors=errors,
    )


# pylint: disable=too-many-locals
def _scan_feature_parallel(
    feature: FeatureConfig,
    ts_codes: list[str],
    data_cache: dict[str, pd.DataFrame],
    *,
    max_workers: int = 6,
    progress_callback: Callable[[int, int], None] | None = None,
    redis_client: redis.Redis | None = None,
    end_date: str = "",
) -> ScanResult:
    """Parallel scanning using ProcessPoolExecutor."""
    # Prepare work items - serialize DataFrames to pickle (faster than JSON)
    work_items = []
    missing = 0

    for ts_code in ts_codes:
        if ts_code in data_cache:
            df = data_cache[ts_code]
            # Only serialize required columns to reduce pickle overhead (~30% smaller)
            cols_to_use = [c for c in _FEATURE_REQUIRED_COLS if c in df.columns]
            df_minimal = df[cols_to_use]
            work_items.append((ts_code, pickle.dumps(df_minimal), feature.name, feature.min_days))
        else:
            missing += 1

    if not work_items:
        return ScanResult(
            feature_name=feature.name,
            signals=[],
            from_cache=False,
            scanned=0,
            skipped=missing,
            errors=0,
        )

    # Parallel execution
    signals = []
    skipped = missing  # Start with symbols not in cache
    errors = 0
    completed = 0
    total = len(work_items)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_scan_symbol_worker, item): item[0] for item in work_items}

        for future in as_completed(futures):
            ts_code, has_signal, status = future.result()

            if status == "ok":
                if has_signal:
                    signals.append(ts_code)
            elif status == "skip":
                skipped += 1
            else:  # error
                errors += 1

            completed += 1
            if progress_callback:
                progress_callback(completed, total)

    # Cache results (always persist to SQLite, optionally to Redis)
    set_feature_cache(feature.name, end_date, signals, redis_client)

    return ScanResult(
        feature_name=feature.name,
        signals=signals,
        from_cache=False,
        scanned=total - (skipped - missing) - errors,
        skipped=skipped,
        errors=errors,
    )


def scan_all_features(
    end_date: str,
    use_cache: bool = True,
    feature_callback: Callable[[FeatureConfig], None] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[ScanResult]:
    """
    Scan all symbols for all features.

    Args:
        end_date: End date in YYYYMMDD format
        use_cache: Whether to use cached results
        feature_callback: Optional callback when starting a new feature
        progress_callback: Optional callback(current, total) for progress updates

    Returns:
        List of ScanResult for each feature
    """
    redis_client = get_redis_client()
    ts_codes = get_ts_codes()

    results = []
    for feature in FEATURES:
        if feature_callback:
            feature_callback(feature)

        result = scan_feature(
            feature=feature,
            end_date=end_date,
            ts_codes=ts_codes,
            use_cache=use_cache,
            redis_client=redis_client,
            progress_callback=progress_callback,
        )
        results.append(result)

    return results
