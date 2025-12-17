"""Shared utility functions for feature detection modules."""

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view


def calculate_price_quantile(close: pd.Series, window: int = 500) -> pd.Series:
    """
    Calculate price quantile based on rolling window (vectorized implementation).

    Returns the percentage of historical prices below the current price,
    providing a measure of where the current price sits relative to its history.

    This implementation uses numpy's sliding_window_view for ~50-100x faster
    computation compared to pandas rolling apply.

    Args:
        close: Close price series
        window: Lookback window for quantile calculation (default 500 days = ~2 years)

    Returns:
        pd.Series: Quantile value (0-1) for each day
            - High value (near 1.0): Price is high relative to history
            - Low value (near 0.0): Price is low relative to history
    """
    values = close.values.astype(np.float64)
    n = len(values)

    # Initialize result with NaN
    result = np.full(n, np.nan, dtype=np.float64)

    if n < window:
        return pd.Series(result, index=close.index)

    # Create sliding windows: shape (n - window + 1, window)
    # This is a view, not a copy, so memory efficient
    windows = sliding_window_view(values, window)

    # Get current values (last element of each window)
    current_values = windows[:, -1]

    # Count how many values in each window are strictly below current
    # Broadcasting: windows (m, window) < current_values (m, 1) -> (m, window)
    below_count = (windows < current_values[:, np.newaxis]).sum(axis=1)

    # Calculate quantile as proportion below current
    quantiles = below_count / window

    # Fill result starting from position (window - 1)
    result[window - 1 :] = quantiles

    return pd.Series(result, index=close.index)


def detect_consecutive_signals(signal_series: pd.Series, min_days: int) -> pd.Series:
    """
    Detect consecutive True values in a boolean series.

    Uses rolling sum to identify positions where at least min_days
    consecutive True values occur.

    Args:
        signal_series: Boolean Series indicating daily signal presence
        min_days: Minimum consecutive days required

    Returns:
        pd.Series: Boolean Series marking positions where consecutive requirement is met
    """
    signal_int = signal_series.astype(int)
    rolling_sum = signal_int.rolling(window=min_days, min_periods=min_days).sum()
    return rolling_sum >= min_days


def calculate_upper_shadow_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Calculate upper shadow ratio for each candle.

    Upper shadow ratio = (high - max(open, close)) / max(open, close) * 100

    Args:
        df: DataFrame with 'high', 'open', 'close' columns

    Returns:
        pd.Series: Upper shadow ratio (as percentage)
    """
    body_top = df[["open", "close"]].max(axis=1)
    upper_shadow = (df["high"] - body_top) / body_top * 100
    return upper_shadow
