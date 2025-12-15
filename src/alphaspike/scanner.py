"""Feature scanner module."""

from dataclasses import dataclass
from typing import Callable

import pandas as pd
import redis

from src.alphaspike.cache import get_feature_cache, get_redis_client, set_feature_cache
from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.bbc import bbc
from src.feature.consolidation_breakout import consolidation_breakout
from src.feature.high_retracement import high_retracement
from src.feature.volume_stagnation import volume_stagnation
from src.feature.volume_upper_shadow import volume_upper_shadow


@dataclass
class FeatureConfig:
    """Configuration for a feature."""

    name: str  # Feature name (used as cache key and display)
    func: Callable[[pd.DataFrame], bool]  # Feature function
    min_days: int  # Minimum data days required


# Feature registry
FEATURES: list[FeatureConfig] = [
    FeatureConfig("bbc", bbc, 1000),
    FeatureConfig("volume_upper_shadow", volume_upper_shadow, 220),
    FeatureConfig("volume_stagnation", volume_stagnation, 550),
    FeatureConfig("high_retracement", high_retracement, 1500),
    FeatureConfig("consolidation_breakout", consolidation_breakout, 60),
]


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

    Returns:
        ScanResult with signals and statistics
    """
    # Try cache first
    if use_cache and redis_client:
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

    # Scan all symbols
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

        except Exception:
            errors += 1

        # Progress callback
        if progress_callback:
            progress_callback(i + 1, total)

    # Cache results
    if redis_client:
        set_feature_cache(feature.name, end_date, signals, redis_client)

    return ScanResult(
        feature_name=feature.name,
        signals=signals,
        from_cache=False,
        scanned=total - skipped - errors,
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
