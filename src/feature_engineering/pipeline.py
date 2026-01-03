"""Feature engineering pipeline for batch processing."""

from typing import Callable

from src.alphaspike.db import get_feature_results_by_name
from src.common.logging import get_logger
from src.datahub.daily_bar import batch_load_daily_bars
from src.datahub.symbol import get_ts_codes
from src.feature_engineering.db import (
    FeatureData,
    init_feature_data_db,
    save_feature_data_batch,
)
from src.feature_engineering.extractor import (
    create_feature_data_with_returns,
    extract_volume_upper_shadow_features,
)

_logger = get_logger(__name__)


def run_feature_engineering_full(  # pylint: disable=too-many-locals
    start_date: str | None = None,
    end_date: str | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> int:
    """
    Run feature engineering on ALL symbols for ALL trading days.

    This function:
    1. Loads all stock symbols
    2. For each symbol, scans all trading days
    3. Extracts feature values when signal is detected
    4. Calculates 1D/2D/3D returns
    5. Stores everything in feature_data table

    Args:
        start_date: Start date filter (YYYYMMDD), optional
        end_date: End date filter (YYYYMMDD), optional
        progress_callback: Optional callback(current, total, ts_code) for progress updates

    Returns:
        Number of records saved.
    """
    # Initialize database
    init_feature_data_db()

    # Get all stock symbols
    _logger.info("Loading all stock symbols...")
    ts_codes = get_ts_codes()
    _logger.info("Found %d symbols to process", len(ts_codes))

    # Batch load all daily bar data (no end_date filter here, we need full data for returns)
    _logger.info("Loading daily bar data for all symbols...")
    data_cache = batch_load_daily_bars(ts_codes)
    _logger.info("Loaded data for %d symbols", len(data_cache))

    # Process each symbol
    feature_data_list: list[FeatureData] = []
    total = len(ts_codes)
    batch_size = 1000  # Save in batches to avoid memory issues
    total_saved = 0

    for i, ts_code in enumerate(ts_codes):
        if progress_callback:
            progress_callback(i + 1, total, ts_code)

        if ts_code not in data_cache:
            continue

        df = data_cache[ts_code]

        if df.empty or len(df) < 220:
            continue

        # Get all trading dates for this symbol
        all_trade_dates = df["trade_date"].tolist()

        # Scan each date (need at least 220 days of history)
        for j in range(220, len(all_trade_dates)):
            signal_date = all_trade_dates[j]

            # Apply date filters on signal_date
            if start_date and signal_date < start_date:
                continue
            if end_date and signal_date > end_date:
                continue

            df_up_to_date = df.iloc[: j + 1].copy()

            # Extract features (includes signal check)
            features = extract_volume_upper_shadow_features(df_up_to_date, ts_code)

            if features is None:
                continue

            # Only save if it's a valid signal
            if not features.is_signal:
                continue

            # Create feature data with returns (use full df for return calculation)
            feature_data = create_feature_data_with_returns(features, df)
            feature_data_list.append(feature_data)

        # Save in batches
        if len(feature_data_list) >= batch_size:
            _logger.info("Saving batch of %d records...", len(feature_data_list))
            save_feature_data_batch(feature_data_list)
            total_saved += len(feature_data_list)
            feature_data_list = []

    # Save remaining records
    if feature_data_list:
        _logger.info("Saving final batch of %d records...", len(feature_data_list))
        save_feature_data_batch(feature_data_list)
        total_saved += len(feature_data_list)

    return total_saved


def run_feature_engineering(  # pylint: disable=too-many-locals
    feature_name: str = "volume_upper_shadow",
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> int:
    """
    Run feature engineering pipeline for signals already stored in feature_result.

    This function:
    1. Gets all signals from feature_result table
    2. Loads daily bar data for each signal's stock
    3. Extracts feature values at the signal date
    4. Calculates 1D/2D/3D returns
    5. Stores everything in feature_data table

    Args:
        feature_name: Feature name to process (default: volume_upper_shadow)
        progress_callback: Optional callback(current, total, ts_code) for progress updates

    Returns:
        Number of records saved.
    """
    # Initialize database
    init_feature_data_db()

    # Get all signals for this feature
    _logger.info("Loading signals for feature: %s", feature_name)
    results = get_feature_results_by_name(feature_name)

    if not results:
        _logger.warning("No signals found for feature: %s", feature_name)
        return 0

    # Collect all (ts_code, signal_date) pairs
    signals: list[tuple[str, str]] = []
    for scan_date, ts_codes in results:
        for ts_code in ts_codes:
            signals.append((ts_code, scan_date))

    _logger.info("Found %d signals to process", len(signals))

    # Get unique ts_codes for batch loading
    unique_ts_codes = list(set(ts_code for ts_code, _ in signals))
    _logger.info("Loading daily bar data for %d symbols...", len(unique_ts_codes))

    # Batch load all daily bar data
    data_cache = batch_load_daily_bars(unique_ts_codes)
    _logger.info("Loaded data for %d symbols", len(data_cache))

    # Process each signal
    feature_data_list: list[FeatureData] = []
    total = len(signals)

    for i, (ts_code, signal_date) in enumerate(signals):
        if progress_callback:
            progress_callback(i + 1, total, ts_code)

        if ts_code not in data_cache:
            _logger.debug("No data found for %s", ts_code)
            continue

        df = data_cache[ts_code]

        # Filter data up to signal date for feature extraction
        df_up_to_signal = df[df["trade_date"] <= signal_date].copy()

        if df_up_to_signal.empty:
            continue

        # Extract features
        features = extract_volume_upper_shadow_features(df_up_to_signal, ts_code)
        if features is None:
            continue

        # Verify the signal date matches
        if features.signal_date != signal_date:
            _logger.warning(
                "Signal date mismatch for %s: expected %s, got %s",
                ts_code,
                signal_date,
                features.signal_date,
            )
            continue

        # Create feature data with returns (use full df for return calculation)
        feature_data = create_feature_data_with_returns(features, df)
        feature_data_list.append(feature_data)

    # Save all feature data in batch
    _logger.info("Saving %d feature data records...", len(feature_data_list))
    save_feature_data_batch(feature_data_list)

    return len(feature_data_list)


def get_feature_engineering_stats(feature_name: str = "volume_upper_shadow") -> dict:
    """
    Get statistics about feature engineering data.

    Args:
        feature_name: Feature name to get stats for

    Returns:
        Dict with statistics.
    """
    from src.feature_engineering.db import get_feature_data_by_feature

    data = get_feature_data_by_feature(feature_name)

    if not data:
        return {
            "feature_name": feature_name,
            "total_records": 0,
            "records_with_returns": 0,
            "date_range": None,
        }

    total = len(data)
    with_returns = sum(1 for d in data if d.return_1d is not None)
    dates = [d.signal_date for d in data]

    return {
        "feature_name": feature_name,
        "total_records": total,
        "records_with_returns": with_returns,
        "date_range": (min(dates), max(dates)) if dates else None,
    }
