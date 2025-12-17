"""Volume Upper Shadow (放量上影线) feature detection module."""

import warnings

import pandas as pd
import talib

from src.feature.utils import calculate_price_quantile, calculate_upper_shadow_ratio

warnings.filterwarnings("ignore")


def volume_upper_shadow(df: pd.DataFrame) -> bool:
    """
    Feature: Volume Upper Shadow (放量上影线)

    Detects the last candle with significant upper shadow during volume surge
    at relatively low price levels, potentially indicating selling pressure.

    Detection criteria for the last candle:
        1. Upper shadow ratio > 2%
        2. Volume surge: 1.2x to 2x of previous day's 10-day MA volume
        3. Price quantile < 45% (based on last 200 days)
        4. Close > MA5 (price above 5-day moving average)
        5. Close > MA10 (price above 10-day moving average)
        6. MA3 > MA5 (short-term trend above mid-term trend)
        7. No limit-up in last 3 days and cumulative gain < 15%

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

    # Condition 1: Upper shadow ratio > 2%
    cond1 = last["upper_shadow"] > 2

    # Condition 2: Volume surge (1.2x to 2x of previous day's vol_ma10)
    cond2 = (last["vol"] >= prev_vol_ma10 * 1.2) and (last["vol"] <= prev_vol_ma10 * 2)

    # Condition 3: Price quantile < 45%
    cond3 = last["price_quantile"] < 0.45

    # Condition 4: Close > MA5
    cond4 = last["close"] > last["ma5"]

    # Condition 5: Close > MA10
    cond5 = last["close"] > last["ma10"]

    # Condition 6: MA3 > MA5
    cond6 = last["ma3"] > last["ma5"]

    # Condition 7: No limit-up in last 3 days and cumulative gain < 15%
    last_3_pct = tmp_df["pct_chg"].tail(3)
    no_limit_up = (last_3_pct < 9.8).all()  # A-share limit-up is ~10%
    cumulative_gain = ((1 + last_3_pct / 100).prod() - 1) * 100
    gain_limited = cumulative_gain < 15

    cond7 = no_limit_up and gain_limited

    return cond1 and cond2 and cond3 and cond4 and cond5 and cond6 and cond7
