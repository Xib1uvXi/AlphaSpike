"""Volume Stagnation (放量滞涨) feature detection module."""

import warnings

import pandas as pd
import talib

from src.feature.utils import calculate_price_quantile, detect_consecutive_signals

warnings.filterwarnings("ignore")


def volume_stagnation(df: pd.DataFrame, min_consecutive_days: int = 3) -> bool:
    """
    Feature: Volume Stagnation (放量滞涨)

    Detects high volume days with limited price movement at relatively low price levels,
    potentially indicating accumulation phase or distribution by major players.

    Detection criteria:
        - Volume surge: vol > vol_ma10 * 1.5 (volume > 1.5x 10-day average)
        - Price stagnation: -3% < pct_chg < 3% (daily price change within range)
        - Price above MA10: close > close_ma10 (price above 10-day moving average)
        - MA3 > MA5: short-term trend above mid-term trend
        - Consecutive: At least min_consecutive_days meeting both criteria
        - Price quantile: Price at signal end must be in ~5-45% quantile (last 500 days)
        - Scan window: Signal must occur within last 3 trading days

    Args:
        df: DataFrame with daily bar data containing 'vol', 'pct_chg', 'close' columns
        min_consecutive_days: Minimum consecutive days (3-10), default 3

    Returns:
        bool: True if volume stagnation signal detected, False otherwise.
    """
    df = df.dropna()

    # Need at least ~550 days for price quantile calculation + buffer
    if len(df) < 550:
        return False

    # Validate min_consecutive_days parameter
    if not (3 <= min_consecutive_days <= 10):
        raise ValueError("min_consecutive_days must be between 3 and 10")

    tmp_df = df.copy()

    # Calculate 10-day volume moving average
    tmp_df["vol_ma10"] = talib.MA(tmp_df["vol"], timeperiod=10)

    # Calculate price moving averages
    tmp_df["close_ma3"] = talib.MA(tmp_df["close"], timeperiod=3)
    tmp_df["close_ma5"] = talib.MA(tmp_df["close"], timeperiod=5)
    tmp_df["close_ma10"] = talib.MA(tmp_df["close"], timeperiod=10)

    # Volume surge condition: vol > vol_ma10 * 1.5
    volume_surge = tmp_df["vol"] > tmp_df["vol_ma10"] * 1.5

    # Price stagnation condition: -3% < pct_chg < 3%
    price_stagnation = (tmp_df["pct_chg"] > -3) & (tmp_df["pct_chg"] < 3)

    # Price above MA10 condition: close > close_ma10
    price_above_ma10 = tmp_df["close"] > tmp_df["close_ma10"]

    # MA3 > MA5 condition: short-term trend above mid-term trend
    ma3_above_ma5 = tmp_df["close_ma3"] > tmp_df["close_ma5"]

    # Daily signal: all conditions met
    tmp_df["daily_signal"] = volume_surge & price_stagnation & price_above_ma10 & ma3_above_ma5

    # Detect consecutive signals
    tmp_df["consecutive_signal"] = detect_consecutive_signals(tmp_df["daily_signal"], min_consecutive_days)

    # Condition 1: Price quantile (25-45% based on last 500 days)
    tmp_df["price_quantile"] = calculate_price_quantile(tmp_df["close"], window=500)
    price_in_low_range = (tmp_df["price_quantile"] >= 0.05) & (tmp_df["price_quantile"] <= 0.45)

    # Condition 2: Cumulative gain during consecutive period < 10%
    # Calculate price change from min_consecutive_days ago to now
    # tmp_df["cumulative_gain"] = (
    #     tmp_df["close"] / tmp_df["close"].shift(min_consecutive_days) - 1
    # ) * 100
    # gain_limited = tmp_df["cumulative_gain"] < 15

    # Final signal: all conditions must be met at signal end point
    tmp_df["final_signal"] = tmp_df["consecutive_signal"] & price_in_low_range

    # Scan last 3 trading days for signals
    recent_signals = tmp_df["final_signal"].tail(3)
    return recent_signals.any()
