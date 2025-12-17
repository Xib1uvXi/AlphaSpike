"""High Retracement (冲高回落) feature detection module."""

import warnings

import pandas as pd
import talib

from src.feature.utils import (
    calculate_price_quantile,
    calculate_upper_shadow_ratio,
    detect_consecutive_signals,
)

warnings.filterwarnings("ignore")


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
    tmp_df["upper_shadow"] = calculate_upper_shadow_ratio(tmp_df)
    upper_shadow_condition = tmp_df["upper_shadow"] > 2

    # Condition 2: Moderate volume (1.0-1.5x of vol_ma20)
    moderate_volume = (tmp_df["vol"] >= tmp_df["vol_ma20"] * 1.0) & (tmp_df["vol"] <= tmp_df["vol_ma20"] * 1.5)

    # Condition 3: Price quantile < 55% (based on last 500 days)
    tmp_df["price_quantile"] = calculate_price_quantile(tmp_df["close"], window=500)
    price_in_low_range = tmp_df["price_quantile"] < 0.55

    # Daily signal: all conditions met
    tmp_df["daily_signal"] = upper_shadow_condition & moderate_volume & price_in_low_range

    # Detect consecutive signals
    tmp_df["consecutive_signal"] = detect_consecutive_signals(tmp_df["daily_signal"], min_consecutive_days)

    # Check if signal exists in last 3 trading days
    recent_signals = tmp_df["consecutive_signal"].tail(3)
    return recent_signals.any()
