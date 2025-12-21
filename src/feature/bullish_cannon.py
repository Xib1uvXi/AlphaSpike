"""Bullish Cannon (多方炮) feature detection module."""

import warnings

import pandas as pd
import talib

from src.common.config import BULLISH_CANNON_CONFIG

warnings.filterwarnings("ignore")

# Local reference to config for cleaner code
_cfg = BULLISH_CANNON_CONFIG


def _calculate_candle_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate candle metrics: body, range, upper/lower wick.

    Args:
        df: DataFrame with OHLC columns

    Returns:
        DataFrame with added metric columns
    """
    tmp = df.copy()

    # Body = |close - open|
    tmp["body"] = (tmp["close"] - tmp["open"]).abs()

    # Range = high - low
    tmp["range"] = tmp["high"] - tmp["low"]

    # Upper wick = high - max(open, close)
    tmp["upper_wick"] = tmp["high"] - tmp[["open", "close"]].max(axis=1)

    # Lower wick = min(open, close) - low
    tmp["lower_wick"] = tmp[["open", "close"]].min(axis=1) - tmp["low"]

    # Amplitude = (high - low) / prev_close
    tmp["amplitude"] = (tmp["high"] - tmp["low"]) / tmp["close"].shift(1)

    return tmp


def bullish_cannon(df: pd.DataFrame) -> bool:  # pylint: disable=too-many-locals,too-many-branches
    """
    Feature: Bullish Cannon (多方炮)

    Detects a pattern where a strong bullish candle (first cannon) is followed
    by 1-3 days of consolidation (cannon body), then another breakout candle
    (second cannon).

    Detection criteria:

    First Cannon (day0):
        - ret0 >= 7% (strong bullish day)
        - vol0 >= vol_ma5 * 1.8 (volume surge)
        - body0/range0 >= 0.40 (solid body, not doji)
        - upper_wick0/range0 <= 0.50 (limited upper shadow)
        - close0 > HHV(high, 20)[-1] (breaks 20-day high)

    Cannon Body (day1 to dayk, k=1..3):
        - mean(vol1..volk) <= vol0 * 0.8 (volume contraction)
        - max(amplitude1..k) <= 8% (limited volatility)
        - min(low1..k) >= open0 (holds above first cannon's open)

    Second Cannon (day(k+1)):
        - close > max(high1..k) (breaks body's high)
        - vol >= mean(vol1..k) * 1.0 (volume at least matches body)
        - (high - close) / range <= 0.25 (closes near high)

    Args:
        df: DataFrame with daily bar data containing OHLCV columns

    Returns:
        bool: True if signal detected on the last trading day, False otherwise.
    """
    df = df.dropna()

    # Need at least 30 days for 20-day HHV + pattern
    if len(df) < 30:
        return False

    tmp_df = _calculate_candle_metrics(df)

    # Calculate indicators
    tmp_df["vol_ma5"] = talib.SMA(tmp_df["vol"], timeperiod=5)
    tmp_df["hhv20"] = tmp_df["high"].rolling(window=20).max().shift(1)  # Previous 20-day high
    tmp_df["ret"] = tmp_df["pct_chg"] / 100  # Convert to decimal

    # Scan for patterns ending on the last day
    # The second cannon should be on the last day
    scan_end = len(tmp_df)
    scan_start = max(25, scan_end - 4)  # Look back enough for pattern

    for second_cannon_idx in range(scan_end - 1, scan_start - 1, -1):
        # Try different cannon body lengths (k=1,2,3)
        for k in range(1, 4):
            first_cannon_idx = second_cannon_idx - k - 1

            if first_cannon_idx < 20:  # Need 20 days for HHV
                continue

            # Get candle data
            day0 = tmp_df.iloc[first_cannon_idx]  # First cannon
            body_start = first_cannon_idx + 1
            body_end = second_cannon_idx  # exclusive
            second = tmp_df.iloc[second_cannon_idx]  # Second cannon

            if body_end <= body_start:
                continue

            body_df = tmp_df.iloc[body_start:body_end]

            # === First Cannon Conditions ===
            # ret0 >= threshold (default 7%)
            if day0["ret"] < _cfg.first_cannon_return:
                continue

            # vol0 >= vol_ma5 * ratio (default 1.8)
            if pd.isna(day0["vol_ma5"]) or day0["vol"] < day0["vol_ma5"] * _cfg.first_cannon_vol_ratio:
                continue

            # body0/range0 >= ratio (default 0.40)
            if day0["range"] == 0 or day0["body"] / day0["range"] < _cfg.first_cannon_body_ratio:
                continue

            # upper_wick0/range0 <= ratio (default 0.50)
            if day0["upper_wick"] / day0["range"] > _cfg.first_cannon_upper_wick_ratio:
                continue

            # close0 > HHV(high, 20)[-1] (breakthrough)
            if pd.isna(day0["hhv20"]) or day0["close"] <= day0["hhv20"]:
                continue

            # === Cannon Body Conditions ===
            # mean(vol1..volk) <= vol0 * ratio (default 0.8)
            body_vol_mean = body_df["vol"].mean()
            if body_vol_mean > day0["vol"] * _cfg.body_vol_contraction:
                continue

            # max(amplitude1..k) <= threshold (default 8%)
            if body_df["amplitude"].max() > _cfg.body_max_amplitude:
                continue

            # min(low1..k) >= open0 (holds above first cannon's open)
            if body_df["low"].min() < day0["open"]:
                continue

            # === Second Cannon Conditions ===
            # close1 > max(high1..k) (breaks body's high)
            body_high_max = body_df["high"].max()
            if second["close"] <= body_high_max:
                continue

            # vol1 >= mean(vol1..k) * ratio (default 1.0)
            if second["vol"] < body_vol_mean * _cfg.second_cannon_vol_ratio:
                continue

            # (high1 - close1) / range1 <= ratio (default 0.25, closes near high)
            if second["range"] == 0:
                continue
            upper_ratio = (second["high"] - second["close"]) / second["range"]
            if upper_ratio > _cfg.second_cannon_upper_ratio:
                continue

            # Check if second cannon is on the last trading day
            days_from_end = len(tmp_df) - 1 - second_cannon_idx
            if days_from_end == 0:
                return True

    return False
