"""Shared helper functions for Four-Edge feature detection."""

import pandas as pd
import talib

from src.common.config import FOUR_EDGE_CONFIG

_cfg = FOUR_EDGE_CONFIG


def precompute_indicators(df: pd.DataFrame) -> dict:  # pylint: disable=too-many-locals
    """
    Precompute all TA-Lib indicators needed by four_edge feature detection.

    This function computes indicators once to avoid redundant calculations
    across edge1, edge2, edge3, and edge4 functions.

    Precomputed indicators (with computation count savings):
    - atr14: ATR(14) - was computed 2x (edge1, edge2)
    - ma5: SMA(close, 5) - was computed 2x (edge2, edge3)
    - ma20: SMA(close, 20) - was computed 3x (edge2 type1/type2, edge3)
    - ma60: SMA(close, 60) - was computed 1x (edge2)
    - ma120: SMA(close, 120) - was computed 1x (edge2)
    - atr14_ma10: SMA(atr14, 10) - was computed 1x (edge2)
    - vol_ma3, vol_ma5, vol_ma10: volume MAs - were computed 1x each (edge2)
    - hhv20_prev: rolling 20-day high shifted - was computed 3x (edge2, edge3)
    - llv3: rolling 3-day low - was computed 2x (edge2, edge3)
    - llv5: rolling 5-day low - was computed 1x (edge2)
    - amount_ma3, amount_ma5, amount_ma10: amount MAs - were computed per-function

    Args:
        df: DataFrame with OHLCV + amount + pct_chg data

    Returns:
        Dict of precomputed indicator Series
    """
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["vol"] if "vol" in df.columns else df.get("volume", pd.Series(0, index=df.index))
    amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)

    # ATR and its MA
    atr14_values = talib.ATR(high.values, low.values, close.values, timeperiod=14)
    atr14 = pd.Series(atr14_values, index=df.index)
    atr14_ma10 = atr14.rolling(10).mean()

    # Moving averages for close
    ma5 = pd.Series(talib.SMA(close.values, timeperiod=5), index=df.index)
    ma20 = pd.Series(talib.SMA(close.values, timeperiod=20), index=df.index)
    ma60 = pd.Series(talib.SMA(close.values, timeperiod=60), index=df.index)
    ma120 = pd.Series(talib.SMA(close.values, timeperiod=120), index=df.index)

    # Volume moving averages
    vol_ma3 = vol.rolling(3).mean()
    vol_ma5 = vol.rolling(5).mean()
    vol_ma10 = vol.rolling(10).mean()

    # Amount moving averages
    amount_ma3 = amount.rolling(3).mean()
    amount_ma5 = amount.rolling(5).mean()
    amount_ma10 = amount.rolling(10).mean()

    # High/Low rolling values
    hhv20 = high.rolling(20).max()
    hhv20_prev = hhv20.shift(1)
    llv3 = low.rolling(3).min()
    llv5 = low.rolling(5).min()
    llv20 = low.rolling(20).min()

    return {
        "atr14": atr14,
        "atr14_ma10": atr14_ma10,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "ma120": ma120,
        "vol_ma3": vol_ma3,
        "vol_ma5": vol_ma5,
        "vol_ma10": vol_ma10,
        "amount_ma3": amount_ma3,
        "amount_ma5": amount_ma5,
        "amount_ma10": amount_ma10,
        "hhv20": hhv20,
        "hhv20_prev": hhv20_prev,
        "llv3": llv3,
        "llv5": llv5,
        "llv20": llv20,
    }


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
