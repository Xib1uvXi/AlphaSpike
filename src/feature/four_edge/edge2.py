"""Edge 2: Structure patterns for Four-Edge feature detection."""

from typing import Optional

import pandas as pd
import talib

from src.common.config import FOUR_EDGE_CONFIG

_cfg = FOUR_EDGE_CONFIG

# Structure Tags (for Edge 3 filtering)
STRUCT_COMPRESS = "COMPRESS"  # Type 1: Compression -> Expansion
STRUCT_PULLBACK = "PULLBACK"  # Type 2: Trend Pullback
STRUCT_RETEST = "RETEST"  # Type 3: Breakout -> Retest

# Edge 2 Type 1 thresholds (Compression -> Expansion)
EDGE2_BOX_WIDTH_THRESHOLD = _cfg.box_width_threshold
EDGE2_MA20_SLOPE_THRESHOLD = _cfg.ma20_slope_threshold
EDGE2_CLOSE_TO_MA_THRESHOLD = _cfg.close_to_ma_threshold

# Edge 2 Type 2 thresholds (Trend Pullback)
EDGE2_T2_PULLBACK_RANGE = (_cfg.t2_pullback_range_low, _cfg.t2_pullback_range_high)
EDGE2_T2_SUPPORT_RATIO = _cfg.t2_support_ratio

# Edge 2 Type 3 thresholds (Breakout -> Retest)
EDGE2_T3_BREAKOUT_VOL_RATIO = _cfg.t3_breakout_vol_ratio
EDGE2_T3_RETEST_DAYS_MIN = _cfg.t3_retest_days_min
EDGE2_T3_RETEST_DAYS_MAX = _cfg.t3_retest_days_max
EDGE2_T3_RETEST_SUPPORT_RATIO = _cfg.t3_retest_support_ratio


def check_edge2_type1_compression(  # pylint: disable=too-many-locals
    df: pd.DataFrame, indicators: dict | None = None
) -> pd.Series:
    """
    Edge 2 Type 1: Compression -> Expansion pattern.

    Structure conditions (formed over 15-20 day windows):
    1. Box width: (HHV20 - LLV20) / Close <= 18%
    2. ATR convergence: ATR14 < SMA(ATR14, 10)
    3. MA20 slope: abs(MA20(T)/MA20(T-5) - 1) <= 0.8%
    4. Close to MA: abs(Close/MA20 - 1) <= 3%

    Args:
        df: DataFrame with OHLCV data
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where pattern is detected
    """
    # Use precomputed indicators if available
    if indicators is not None:
        ma20_series = indicators["ma20"]
        atr14_series = indicators["atr14"]
        atr14_ma10 = indicators["atr14_ma10"]
        high_20 = indicators["hhv20"]
        low_20 = indicators["llv20"]
    else:
        ma20 = talib.SMA(df["close"].values, timeperiod=20)
        ma20_series = pd.Series(ma20, index=df.index)
        atr14 = talib.ATR(
            df["high"].values,
            df["low"].values,
            df["close"].values,
            timeperiod=14,
        )
        atr14_series = pd.Series(atr14, index=df.index)
        atr14_ma10 = atr14_series.rolling(10).mean()
        high_20 = df["high"].rolling(20).max()
        low_20 = df["low"].rolling(20).min()

    # Condition 1: Box width <= 18%
    box_width = (high_20 - low_20) / df["close"]
    cond_box = box_width <= EDGE2_BOX_WIDTH_THRESHOLD

    # Condition 2: ATR convergence (current ATR < 10-day avg ATR)
    cond_atr = atr14_series < atr14_ma10

    # Condition 3: MA20 slope <= 0.8% (nearly flat)
    ma20_slope = (ma20_series / ma20_series.shift(5) - 1).abs()
    cond_slope = ma20_slope <= EDGE2_MA20_SLOPE_THRESHOLD

    # Condition 4: Close to MA <= 3%
    close_to_ma = (df["close"] / ma20_series - 1).abs()
    cond_close_ma = close_to_ma <= EDGE2_CLOSE_TO_MA_THRESHOLD

    # All conditions must be met
    return cond_box & cond_atr & cond_slope & cond_close_ma


def check_edge2_type2_trend_pullback(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:
    """
    Edge 2 Type 2: Trend Pullback pattern.

    Structure conditions:
    1. Trend: MA20 > MA60 (and optionally MA60 > MA120)
    2. Pullback distance: Close/MA20 in [0.97, 1.03] (within +/-3% of MA20)
    3. Volume contraction: SMA(Vol,3) < SMA(Vol,10) or Vol < SMA(Vol,5)
    4. Support not broken: LLV5 >= MA60 * 0.98

    Args:
        df: DataFrame with OHLCV data (requires 'vol' column)
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where pattern is detected
    """
    # Use precomputed indicators if available
    if indicators is not None:
        ma20 = indicators["ma20"]
        ma60 = indicators["ma60"]
        ma120 = indicators["ma120"]
        vol_ma3 = indicators["vol_ma3"]
        vol_ma5 = indicators["vol_ma5"]
        vol_ma10 = indicators["vol_ma10"]
        llv5 = indicators["llv5"]
    else:
        ma20 = pd.Series(talib.SMA(df["close"].values, timeperiod=20), index=df.index)
        ma60 = pd.Series(talib.SMA(df["close"].values, timeperiod=60), index=df.index)
        ma120 = pd.Series(talib.SMA(df["close"].values, timeperiod=120), index=df.index)
        vol = df["vol"] if "vol" in df.columns else df.get("volume", pd.Series(0, index=df.index))
        vol_ma3 = vol.rolling(3).mean()
        vol_ma5 = vol.rolling(5).mean()
        vol_ma10 = vol.rolling(10).mean()
        llv5 = df["low"].rolling(5).min()

    vol = df["vol"] if "vol" in df.columns else df.get("volume", pd.Series(0, index=df.index))

    # Condition 1: Trend (MA20 > MA60, optionally MA60 > MA120)
    cond_trend = (ma20 > ma60) & (ma60 > ma120)

    # Condition 2: Pullback distance (Close within +/-3% of MA20)
    close_to_ma20 = df["close"] / ma20
    cond_pullback = (close_to_ma20 >= EDGE2_T2_PULLBACK_RANGE[0]) & (close_to_ma20 <= EDGE2_T2_PULLBACK_RANGE[1])

    # Condition 3: Volume contraction (either of two conditions)
    cond_vol = (vol_ma3 < vol_ma10) | (vol < vol_ma5)

    # Condition 4: Support not broken (LLV5 >= MA60 * 0.98)
    cond_support = llv5 >= ma60 * EDGE2_T2_SUPPORT_RATIO

    # All conditions must be met
    return cond_trend & cond_pullback & cond_vol & cond_support


def check_edge2_type3_breakout_retest(  # pylint: disable=too-many-locals
    df: pd.DataFrame, indicators: dict | None = None
) -> pd.Series:
    """
    Edge 2 Type 3: Breakout -> Retest pattern.

    Structure conditions:
    1. Breakout occurred 3-10 days ago:
       - Close(T-k) > HHV20(T-k-1), k in [3, 10]
       - Vol(T-k) / SMA(Vol,5)(T-k) >= 1.5
    2. Retest confirmation (last 1-3 days):
       - LLV3 >= breakout_level * 0.99
       - SMA(Vol,3) < SMA(Vol,10) (volume contraction)
    3. Retest end signal:
       - Close(T) > Open(T) or Close(T) > MA5

    Args:
        df: DataFrame with OHLCV data (requires 'vol' column)
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series indicating where pattern is detected
    """
    close = df["close"]
    vol = df["vol"] if "vol" in df.columns else df.get("volume", pd.Series(0, index=df.index))

    # Use precomputed indicators if available
    if indicators is not None:
        hhv20_prev = indicators["hhv20_prev"]
        llv3 = indicators["llv3"]
        vol_ma3 = indicators["vol_ma3"]
        vol_ma5 = indicators["vol_ma5"]
        vol_ma10 = indicators["vol_ma10"]
        ma5 = indicators["ma5"]
    else:
        hhv20_prev = df["high"].rolling(20).max().shift(1)
        llv3 = df["low"].rolling(3).min()
        vol_ma3 = vol.rolling(3).mean()
        vol_ma5 = vol.rolling(5).mean()
        vol_ma10 = vol.rolling(10).mean()
        ma5 = pd.Series(talib.SMA(close.values, timeperiod=5), index=df.index)

    # Mark valid breakout days: Close > HHV20_prev AND Vol >= Vol_MA5 * 1.5
    is_breakout = (close > hhv20_prev) & (vol >= vol_ma5 * EDGE2_T3_BREAKOUT_VOL_RATIO)

    # Volume contraction during retest
    vol_contraction = vol_ma3 < vol_ma10

    # Retest end signal: Close > Open OR Close > MA5
    retest_end = (close > df["open"]) | (close > ma5)

    # Check for breakout 3-10 days ago with valid retest
    signal = pd.Series(False, index=df.index)

    for k in range(EDGE2_T3_RETEST_DAYS_MIN, EDGE2_T3_RETEST_DAYS_MAX + 1):
        # Breakout occurred k days ago (shift and fill NaN as False)
        breakout_shifted = is_breakout.shift(k)
        breakout_k = breakout_shifted.where(breakout_shifted.notna(), False).astype(bool)

        # Retest holds: LLV3 >= breakout_level * 0.99
        retest_holds = llv3 >= hhv20_prev.shift(k) * EDGE2_T3_RETEST_SUPPORT_RATIO

        # Combine conditions for this k and OR with existing signals
        signal_k = breakout_k & retest_holds & vol_contraction & retest_end
        signal = signal | signal_k.where(signal_k.notna(), False).astype(bool)

    return signal


def check_edge2(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:
    """
    Edge 2: Structure patterns for 5-day bullish outlook.

    Combines multiple pattern types with OR logic:
    - Type 1: Compression -> Expansion
    - Type 2: Trend Pullback
    - Type 3: Breakout -> Retest

    Args:
        df: DataFrame with OHLCV data
        indicators: Optional precomputed indicators dict

    Returns:
        Boolean Series (type1 OR type2 OR type3)
    """
    type1 = check_edge2_type1_compression(df, indicators)
    type2 = check_edge2_type2_trend_pullback(df, indicators)
    type3 = check_edge2_type3_breakout_retest(df, indicators)

    # Type 1 OR Type 2 OR Type 3
    return type1 | type2 | type3


def get_edge2_struct_type(df: pd.DataFrame, indicators: dict | None = None) -> pd.Series:
    """
    Get Edge 2 structure type tag for each day.

    Returns the structure pattern type that triggered Edge 2 signal.
    Priority order (first match wins): COMPRESS > PULLBACK > RETEST

    Tags:
    - STRUCT_COMPRESS: Type 1 - Compression -> Expansion (horizontal consolidation)
    - STRUCT_PULLBACK: Type 2 - Trend Pullback (pullback in uptrend)
    - STRUCT_RETEST: Type 3 - Breakout -> Retest (breakout confirmation)

    Args:
        df: DataFrame with OHLCV data
        indicators: Optional precomputed indicators dict

    Returns:
        Series with structure tag strings (None if no pattern matches)
    """
    type1 = check_edge2_type1_compression(df, indicators)
    type2 = check_edge2_type2_trend_pullback(df, indicators)
    type3 = check_edge2_type3_breakout_retest(df, indicators)

    # Initialize with None
    struct_type: pd.Series = pd.Series(None, index=df.index, dtype=object)

    # Apply tags in reverse priority order (later overwrites)
    # This way COMPRESS (highest priority) is applied last
    struct_type = struct_type.where(~type3, STRUCT_RETEST)
    struct_type = struct_type.where(~type2, STRUCT_PULLBACK)
    struct_type = struct_type.where(~type1, STRUCT_COMPRESS)

    return struct_type


def get_last_struct_type(df: pd.DataFrame, lookback: int = 3) -> Optional[str]:
    """
    Get the most recent Edge 2 structure type within lookback days.

    Useful for Edge 3/4 to determine which structure pattern triggered the signal.

    Args:
        df: DataFrame with OHLCV data
        lookback: Number of days to look back (default: 3)

    Returns:
        Structure tag string if found in last N days, None otherwise
    """
    struct_types = get_edge2_struct_type(df)
    recent = struct_types.tail(lookback)

    # Return the most recent non-None value
    valid = recent.dropna()
    if len(valid) > 0:
        return valid.iloc[-1]
    return None
