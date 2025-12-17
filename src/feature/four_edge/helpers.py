"""Shared helper functions for Four-Edge feature detection."""

import pandas as pd

from src.common.config import FOUR_EDGE_CONFIG

_cfg = FOUR_EDGE_CONFIG

# Edge 3 thresholds
EDGE3_CLOSE_STRONG_RATIO = _cfg.close_strong_ratio
EDGE3_BULLISH_BODY_RATIO = _cfg.bullish_body_ratio


def calculate_amount_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Calculate Amount Ratio (AR) = Amount(T) / SMA(Amount, 5)(T).

    AR measures turnover surge relative to recent average.

    Args:
        df: DataFrame with 'amount' column (turnover value)

    Returns:
        Series of amount ratios
    """
    amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)
    amount_ma5 = amount.rolling(5).mean()
    # Avoid division by zero
    return amount / amount_ma5.replace(0, float("nan"))


def is_close_strong(df: pd.DataFrame) -> pd.Series:
    """
    Check if close is strong (in upper 70% of day's range).

    CloseStrong := Close >= High - 0.3 * (High - Low)

    Args:
        df: DataFrame with high, low, close columns

    Returns:
        Boolean Series indicating where close is strong
    """
    range_ = df["high"] - df["low"]
    threshold = df["high"] - EDGE3_CLOSE_STRONG_RATIO * range_
    return df["close"] >= threshold


def is_bullish_candle(df: pd.DataFrame) -> pd.Series:
    """
    Check for bullish candle pattern.

    BullishCandle conditions:
    1. Close > Open (bullish)
    2. Close >= High - 0.3 * (High - Low) (CloseStrong)
    3. RealBody / (High - Low) >= 0.5 (solid body)

    Args:
        df: DataFrame with OHLC data

    Returns:
        Boolean Series indicating where bullish candle is detected
    """
    # Condition 1: Close > Open
    cond_bullish = df["close"] > df["open"]

    # Condition 2: CloseStrong
    cond_close_strong = is_close_strong(df)

    # Condition 3: RealBody / Range >= 0.5
    real_body = (df["close"] - df["open"]).abs()
    range_ = df["high"] - df["low"]
    # Avoid division by zero for doji candles
    body_ratio = real_body / range_.replace(0, float("nan"))
    cond_body = body_ratio >= EDGE3_BULLISH_BODY_RATIO

    return cond_bullish & cond_close_strong & cond_body


def is_bullish_candle_simple(df: pd.DataFrame) -> pd.Series:
    """
    Check for simple bullish candle (Edge 4 version).

    BullishCandle conditions (simpler than Edge 3):
    1. Close > Open (bullish)
    2. Close >= High - 0.3 * (High - Low) (CloseStrong)

    Note: Does NOT require RealBody/Range >= 0.5 like Edge 3's BullishCandle.

    Args:
        df: DataFrame with OHLC data

    Returns:
        Boolean Series indicating where bullish candle is detected
    """
    # Condition 1: Close > Open
    cond_bullish = df["close"] > df["open"]

    # Condition 2: CloseStrong
    cond_close_strong = is_close_strong(df)

    return cond_bullish & cond_close_strong


def is_stop_drop(df: pd.DataFrame) -> pd.Series:
    """
    Check if price stopped dropping (LLV3 not making new low).

    StopDrop := LLV3 >= LLV3_prev

    Args:
        df: DataFrame with low column

    Returns:
        Boolean Series indicating where drop has stopped
    """
    llv3 = df["low"].rolling(3).min()
    return llv3 >= llv3.shift(1)
