"""Edge 1: ATR volatility condition for Four-Edge feature detection."""

import pandas as pd
import talib

from src.common.config import FOUR_EDGE_CONFIG

_cfg = FOUR_EDGE_CONFIG

# Edge 1 threshold
ATR_VOLATILITY_THRESHOLD = _cfg.atr_volatility_threshold


def calculate_atr_volatility(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate ATR volatility ratio = ATR(period) / Close.

    Args:
        df: DataFrame with high, low, close columns
        period: ATR period (default: 14)

    Returns:
        Series of ATR volatility ratios
    """
    atr = talib.ATR(
        df["high"].values,
        df["low"].values,
        df["close"].values,
        timeperiod=period,
    )
    return pd.Series(atr, index=df.index) / df["close"]


def check_edge1_atr_volatility(
    df: pd.DataFrame,
    threshold: float = ATR_VOLATILITY_THRESHOLD,
) -> pd.Series:
    """
    Edge 1: ATR volatility condition.

    Condition: ATR(14) / Close >= threshold

    Args:
        df: DataFrame with OHLCV data
        threshold: Minimum volatility ratio (default: 2.5%)

    Returns:
        Boolean Series indicating where condition is met
    """
    atr_volatility = calculate_atr_volatility(df, period=14)
    return atr_volatility >= threshold
