"""High Retracement (冲高回落) feature detection module."""

import warnings

import pandas as pd
import talib

warnings.filterwarnings("ignore")


def _detect_consecutive_signals(signal_series: pd.Series, min_days: int) -> pd.Series:
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


def _calculate_price_quantile(close: pd.Series, window: int = 500) -> pd.Series:
    """
    Calculate price quantile based on rolling window.

    Args:
        close: Close price series
        window: Lookback window for quantile calculation (default 500 days = ~2 years)

    Returns:
        pd.Series: Quantile value (0-1) for each day
    """

    def quantile_rank(x):
        if len(x) < window:
            return float("nan")
        # Returns percentage of values BELOW current price
        # High price = high quantile (near 1.0), Low price = low quantile (near 0.0)
        return (x < x.iloc[-1]).mean()

    return close.rolling(window=window, min_periods=window).apply(quantile_rank, raw=False)


def _calculate_upper_shadow_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Calculate upper shadow ratio for each candle.

    Upper shadow = (high - max(open, close)) / max(open, close)

    Args:
        df: DataFrame with 'high', 'open', 'close' columns

    Returns:
        pd.Series: Upper shadow ratio (as percentage)
    """
    body_top = df[["open", "close"]].max(axis=1)
    upper_shadow = (df["high"] - body_top) / body_top * 100
    return upper_shadow


def high_retracement(df: pd.DataFrame, min_consecutive_days: int = 2) -> bool:
    """
    Feature: High Retracement (冲高回落)

    Detects candles with significant upper shadows during moderate volume increase
    at relatively low price levels, potentially indicating selling pressure at highs.

    Detection criteria:
        - Upper shadow: (high - max(open, close)) / max(open, close) > 2%
        - Moderate volume: vol_ma20 * 1.0 <= vol <= vol_ma20 * 1.5
        - Price quantile: Price below 55% quantile using a 6*250 (~6y) window
        - Consecutive: At least min_consecutive_days meeting all criteria
        - Signal window: Signal must occur within last 3 trading days

    Args:
        df: DataFrame with daily bar data containing 'high', 'open', 'close', 'vol' columns
        min_consecutive_days: Minimum consecutive days (2-5), default 2

    Returns:
        bool: True if high retracement signal detected in last 3 days, False otherwise.
    """
    df = df.dropna()

    # Need at least 500 days for price quantile calculation + buffer
    if len(df) < 6 * 250:
        return False

    # Validate min_consecutive_days parameter
    if not (2 <= min_consecutive_days <= 5):
        raise ValueError("min_consecutive_days must be between 2 and 5")

    tmp_df = df.copy()

    # Calculate 20-day volume moving average
    tmp_df["vol_ma20"] = talib.MA(tmp_df["vol"], timeperiod=20)

    # Condition 1: Upper shadow ratio > 2%
    tmp_df["upper_shadow"] = _calculate_upper_shadow_ratio(tmp_df)
    upper_shadow_condition = tmp_df["upper_shadow"] > 2

    # Condition 2: Moderate volume (1.0-1.5x of vol_ma20)
    moderate_volume = (tmp_df["vol"] >= tmp_df["vol_ma20"] * 1.0) & (tmp_df["vol"] <= tmp_df["vol_ma20"] * 1.5)

    # Condition 3: Price quantile < 55% (based on last 500 days)
    tmp_df["price_quantile"] = _calculate_price_quantile(tmp_df["close"], window=500)
    price_in_low_range = tmp_df["price_quantile"] < 0.55

    # Daily signal: all conditions met
    tmp_df["daily_signal"] = upper_shadow_condition & moderate_volume & price_in_low_range

    # Detect consecutive signals
    tmp_df["consecutive_signal"] = _detect_consecutive_signals(tmp_df["daily_signal"], min_consecutive_days)

    # Check if signal exists in last 3 trading days
    recent_signals = tmp_df["consecutive_signal"].tail(3)
    return recent_signals.any()
