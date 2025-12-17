"""Edge 3: Entry signals for Four-Edge feature detection."""

import pandas as pd
import talib

from src.common.config import FOUR_EDGE_CONFIG

from .edge2 import (
    EDGE2_T3_RETEST_DAYS_MAX,
    EDGE2_T3_RETEST_DAYS_MIN,
    STRUCT_COMPRESS,
    STRUCT_PULLBACK,
    STRUCT_RETEST,
    get_edge2_struct_type,
)
from .helpers import (
    is_bullish_candle,
    is_close_strong,
    is_stop_drop,
)

_cfg = FOUR_EDGE_CONFIG

# Edge 3 thresholds
EDGE3_AR_COMPRESS = _cfg.ar_compress
EDGE3_AR_PULLBACK = _cfg.ar_pullback
EDGE3_AR_RETEST = _cfg.ar_retest
EDGE3_VOLUP_THRESHOLD = _cfg.volup_threshold
EDGE3_RETEST_SUPPORT_RATIO = _cfg.retest_support_ratio


def check_edge3_compress(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:
    """
    Edge 3 entry signal for COMPRESS structure.

    Conditions:
    - Close > HHV20_prev (breakout from compression)
    - AR >= 1.3 (turnover surge)
    - CloseStrong (close in upper 70% of range)

    Args:
        df: DataFrame with OHLCV + amount data
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where entry signal is detected
    """
    # Use precomputed indicators if available
    if indicators is not None:
        hhv20_prev = indicators["hhv20_prev"]
        amount_ma5 = indicators["amount_ma5"]
    else:
        hhv20_prev = df["high"].rolling(20).max().shift(1)
        amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)
        amount_ma5 = amount.rolling(5).mean()

    # Amount Ratio
    amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)
    ar = amount / amount_ma5.replace(0, float("nan"))

    # CloseStrong
    close_strong = is_close_strong(df)

    # All conditions must be met
    cond_breakout = df["close"] > hhv20_prev
    cond_ar = ar >= EDGE3_AR_COMPRESS

    return cond_breakout & cond_ar & close_strong


def check_edge3_pullback(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:
    """
    Edge 3 entry signal for PULLBACK structure.

    Two branches (OR logic):
    Branch 1: Close > MA20 AND AR >= 1.2
    Branch 2: StopDrop AND BullishCandle AND VolUp

    Args:
        df: DataFrame with OHLCV + amount data
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where entry signal is detected
    """
    # Use precomputed indicators if available
    if indicators is not None:
        ma20 = indicators["ma20"]
        amount_ma5 = indicators["amount_ma5"]
    else:
        ma20 = pd.Series(talib.SMA(df["close"].values, timeperiod=20), index=df.index)
        amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)
        amount_ma5 = amount.rolling(5).mean()

    # Amount Ratio
    amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)
    ar = amount / amount_ma5.replace(0, float("nan"))

    # Branch 1: Close > MA20 AND AR >= 1.2
    branch1 = (df["close"] > ma20) & (ar >= EDGE3_AR_PULLBACK)

    # Branch 2: StopDrop AND BullishCandle AND VolUp
    stop_drop = is_stop_drop(df)
    bullish_candle = is_bullish_candle(df)
    vol_up = ar >= EDGE3_VOLUP_THRESHOLD
    branch2 = stop_drop & bullish_candle & vol_up

    return branch1 | branch2


def check_edge3_retest(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:  # pylint: disable=too-many-locals
    """
    Edge 3 entry signal for RETEST structure.

    Conditions:
    - HoldBreakout: LLV3 >= BreakoutLevel * 0.99
                    AND SMA(Amount,3) < SMA(Amount,10)
                    AND (Close >= Open OR Close >= MA5 OR Close >= VWAP)
    - Close > High_prev (today's close > yesterday's high)
    - AR >= 1.3 (turnover surge)

    Note: BreakoutLevel is HHV20_prev from the breakout day (3-10 days ago).
    This function checks for any valid breakout in the lookback window.

    Args:
        df: DataFrame with OHLCV + amount data
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where entry signal is detected
    """
    close = df["close"]
    amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)

    # Use precomputed indicators if available
    if indicators is not None:
        hhv20_prev = indicators["hhv20_prev"]
        llv3 = indicators["llv3"]
        amount_ma3 = indicators["amount_ma3"]
        amount_ma5 = indicators["amount_ma5"]
        amount_ma10 = indicators["amount_ma10"]
        ma5 = indicators["ma5"]
    else:
        hhv20_prev = df["high"].rolling(20).max().shift(1)
        llv3 = df["low"].rolling(3).min()
        amount_ma3 = amount.rolling(3).mean()
        amount_ma5 = amount.rolling(5).mean()
        amount_ma10 = amount.rolling(10).mean()
        ma5 = pd.Series(talib.SMA(close.values, timeperiod=5), index=df.index)

    # Amount Ratio
    ar = amount / amount_ma5.replace(0, float("nan"))

    # Identify breakout days (close > HHV20_prev)
    # We look back 3-10 days for a valid breakout
    is_breakout = close > hhv20_prev

    # Check for valid retest signal
    signal = pd.Series(False, index=df.index)

    for k in range(EDGE2_T3_RETEST_DAYS_MIN, EDGE2_T3_RETEST_DAYS_MAX + 1):
        # Breakout occurred k days ago
        breakout_shifted = is_breakout.shift(k)
        breakout_k = breakout_shifted.where(breakout_shifted.notna(), False).astype(bool)

        # Breakout level (HHV20_prev at time of breakout)
        breakout_level = hhv20_prev.shift(k)

        # HoldBreakout conditions:
        # 1. LLV3 >= BreakoutLevel * 0.99
        cond_support = llv3 >= breakout_level * EDGE3_RETEST_SUPPORT_RATIO

        # 2. SMA(Amount,3) < SMA(Amount,10) (amount contraction)
        cond_amount_contract = amount_ma3 < amount_ma10

        # 3. Demand present: Close >= Open OR Close >= MA5
        # (VWAP not available in daily data, using Close >= Open OR Close >= MA5)
        cond_demand = (close >= df["open"]) | (close >= ma5)

        # HoldBreakout = all three conditions
        hold_breakout = cond_support & cond_amount_contract & cond_demand

        # Additional Edge 3 conditions:
        # Close > High_prev (today's close > yesterday's high)
        cond_close_above_prev_high = close > df["high"].shift(1)

        # AR >= 1.3
        cond_ar = ar >= EDGE3_AR_RETEST

        # Combine all conditions for this k
        signal_k = breakout_k & hold_breakout & cond_close_above_prev_high & cond_ar
        signal = signal | signal_k.where(signal_k.notna(), False).astype(bool)

    return signal


def check_edge3(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:
    """
    Edge 3: Entry signal based on Edge 2 structure type.

    Applies different conditions for each structure:
    - COMPRESS: breakout + AR >= 1.3 + CloseStrong
    - PULLBACK: (Close > MA20 AND AR >= 1.2) OR (StopDrop + BullishCandle + VolUp)
    - RETEST: HoldBreakout + Close > High_prev + AR >= 1.3

    Args:
        df: DataFrame with OHLCV + amount data
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where Edge 3 condition is met
    """
    # Get Edge 2 structure type for each day
    struct_type = get_edge2_struct_type(df, indicators)

    # Calculate Edge 3 conditions for each structure type
    compress_cond = check_edge3_compress(df, indicators)
    pullback_cond = check_edge3_pullback(df, indicators)
    retest_cond = check_edge3_retest(df, indicators)

    # Apply Edge 3 based on struct type
    edge3 = pd.Series(False, index=df.index)
    edge3 = edge3 | ((struct_type == STRUCT_COMPRESS) & compress_cond)
    edge3 = edge3 | ((struct_type == STRUCT_PULLBACK) & pullback_cond)
    edge3 = edge3 | ((struct_type == STRUCT_RETEST) & retest_cond)

    return edge3
