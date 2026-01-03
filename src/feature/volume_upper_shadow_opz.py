"""Volume Upper Shadow Optimized (放量上影线优化版) feature detection module.

This is an optimized version of volume_upper_shadow with tighter thresholds
based on statistical analysis of All Positive vs All Negative signals.

Key optimizations:
- Added pct_chg_max (1.5%): Filter out high-gain days which tend to be false signals
- Reduced price_quantile_max (0.45 -> 0.35): Lower price positions perform better
- Reduced vol_surge_max (2.0 -> 1.7): Moderate volume surge is more reliable

Expected improvement:
- All Positive ratio: 41% -> 87%+
- All Negative ratio: 25% -> ~0%
"""

import warnings

import pandas as pd
import talib

from src.common.config import VOLUME_UPPER_SHADOW_OPZ_CONFIG
from src.feature.utils import calculate_price_quantile, calculate_upper_shadow_ratio

warnings.filterwarnings("ignore")

# Local reference to config for cleaner code
_cfg = VOLUME_UPPER_SHADOW_OPZ_CONFIG


def volume_upper_shadow_opz(df: pd.DataFrame) -> bool:
    """
    Feature: Volume Upper Shadow Optimized (放量上影线优化版)

    Detects the last candle with significant upper shadow during volume surge
    at relatively low price levels, with optimized thresholds for higher accuracy.

    Detection criteria for the last candle:
        1. Upper shadow ratio > 2%
        2. Volume surge: 1.2x to 1.7x of previous day's 10-day MA volume (was 2.0x)
        3. Price quantile < 35% (was 45%, based on last 200 days)
        4. Close > MA5 (price above 5-day moving average)
        5. Close > MA10 (price above 10-day moving average)
        6. MA3 > MA5 (short-term trend above mid-term trend)
        7. No limit-up in last 3 days and cumulative gain < 15%
        8. Daily price change < 1.5% (new condition)

    Args:
        df: DataFrame with daily bar data containing OHLCV columns

    Returns:
        bool: True if signal detected on the last candle, False otherwise.
    """
    df = df.dropna()

    # Need at least 200 days for price quantile calculation + buffer
    if len(df) < 220:
        return False

    tmp_df = df.copy()

    # Calculate indicators
    tmp_df["ma3"] = talib.SMA(tmp_df["close"], timeperiod=3)
    tmp_df["ma5"] = talib.SMA(tmp_df["close"], timeperiod=5)
    tmp_df["ma10"] = talib.SMA(tmp_df["close"], timeperiod=10)
    tmp_df["vol_ma10"] = talib.SMA(tmp_df["vol"], timeperiod=10)
    tmp_df["upper_shadow"] = calculate_upper_shadow_ratio(tmp_df)
    tmp_df["price_quantile"] = calculate_price_quantile(tmp_df["close"], window=200)

    # Get last row for signal check
    last = tmp_df.iloc[-1]
    prev_vol_ma10 = tmp_df["vol_ma10"].iloc[-2]  # Previous day's 10-day MA volume
    vol_ratio = last["vol"] / prev_vol_ma10

    # Check recent price changes for condition 7
    last_3_pct = tmp_df["pct_chg"].tail(3)
    cumulative_gain = ((1 + last_3_pct / 100).prod() - 1) * 100

    # All conditions combined
    return (
        # Condition 1: Upper shadow ratio > threshold
        last["upper_shadow"] > _cfg.upper_shadow_ratio
        # Condition 2: Volume surge (optimized: max reduced from 2.0 to 1.7)
        and _cfg.vol_surge_min <= vol_ratio <= _cfg.vol_surge_max
        # Condition 3: Price quantile < threshold (optimized: 0.45 -> 0.35)
        and last["price_quantile"] < _cfg.price_quantile_max
        # Condition 4-6: MA structure (Close > MA5, Close > MA10, MA3 > MA5)
        and last["close"] > last["ma5"]
        and last["close"] > last["ma10"]
        and last["ma3"] > last["ma5"]
        # Condition 7: No limit-up in last 3 days and cumulative gain < threshold
        and (last_3_pct < _cfg.limit_up_threshold).all()
        and cumulative_gain < _cfg.cumulative_gain_max
        # Condition 8: Daily price change < threshold (NEW - key optimization)
        and last["pct_chg"] < _cfg.pct_chg_max
    )
