"""Four-Edge feature detection module."""

from typing import Optional

import pandas as pd
import talib

# Edge 1 thresholds
ATR_VOLATILITY_THRESHOLD = 0.025  # 2.5%

# Edge 2 Structure Tags (for Edge 3 filtering)
STRUCT_COMPRESS = "COMPRESS"  # Type 1: Compression -> Expansion
STRUCT_PULLBACK = "PULLBACK"  # Type 2: Trend Pullback
STRUCT_RETEST = "RETEST"  # Type 3: Breakout -> Retest

# Edge 2 Type 1 thresholds (Compression → Expansion)
EDGE2_BOX_WIDTH_THRESHOLD = 0.18  # 18%
EDGE2_MA20_SLOPE_THRESHOLD = 0.008  # 0.8%
EDGE2_CLOSE_TO_MA_THRESHOLD = 0.03  # 3%

# Edge 2 Type 2 thresholds (Trend Pullback)
EDGE2_T2_PULLBACK_RANGE = (0.97, 1.03)  # Close/MA20 within ±3%
EDGE2_T2_SUPPORT_RATIO = 0.98  # LLV5 >= MA60 * 0.98

# Edge 2 Type 3 thresholds (Breakout → Retest)
EDGE2_T3_BREAKOUT_VOL_RATIO = 1.5  # Vol / Vol_MA5 >= 1.5
EDGE2_T3_RETEST_DAYS_MIN = 3  # Minimum days since breakout
EDGE2_T3_RETEST_DAYS_MAX = 10  # Maximum days since breakout
EDGE2_T3_RETEST_SUPPORT_RATIO = 0.99  # LLV3 >= breakout_level * 0.99

# Edge 3 thresholds (Entry signals based on structure type)
EDGE3_AR_COMPRESS = 1.3  # AR threshold for COMPRESS
EDGE3_AR_PULLBACK = 1.2  # AR threshold for PULLBACK (close > MA20 branch)
EDGE3_AR_RETEST = 1.3  # AR threshold for RETEST
EDGE3_VOLUP_THRESHOLD = 1.3  # θ for VolUp (mild surge)
EDGE3_CLOSE_STRONG_RATIO = 0.3  # Close >= High - 0.3 * Range
EDGE3_BULLISH_BODY_RATIO = 0.5  # RealBody / Range >= 0.5
EDGE3_RETEST_SUPPORT_RATIO = 0.99  # LLV3 >= BreakoutLevel * 0.99

# Edge 4 thresholds (Overheated rejection filter)
EDGE4_CONSECUTIVE_BULLISH_DAYS = 4  # Consecutive bullish candle days
EDGE4_CUMULATIVE_RETURN_THRESHOLD = 15.0  # Sum of pct_chg >= 15%


def _calculate_atr_volatility(df: pd.DataFrame, period: int = 14) -> pd.Series:
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


def _check_edge1_atr_volatility(
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
    atr_volatility = _calculate_atr_volatility(df, period=14)
    return atr_volatility >= threshold


# === Edge 3 Helper Functions ===


def _calculate_amount_ratio(df: pd.DataFrame) -> pd.Series:
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


def _is_close_strong(df: pd.DataFrame) -> pd.Series:
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


def _is_bullish_candle(df: pd.DataFrame) -> pd.Series:
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
    cond_close_strong = _is_close_strong(df)

    # Condition 3: RealBody / Range >= 0.5
    real_body = (df["close"] - df["open"]).abs()
    range_ = df["high"] - df["low"]
    # Avoid division by zero for doji candles
    body_ratio = real_body / range_.replace(0, float("nan"))
    cond_body = body_ratio >= EDGE3_BULLISH_BODY_RATIO

    return cond_bullish & cond_close_strong & cond_body


def _is_stop_drop(df: pd.DataFrame) -> pd.Series:
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


# === Edge 2 Functions ===


def _check_edge2_type1_compression(df: pd.DataFrame) -> pd.Series:
    """
    Edge 2 Type 1: Compression → Expansion pattern.

    Structure conditions (formed over 15-20 day windows):
    1. Box width: (HHV20 - LLV20) / Close <= 18%
    2. ATR convergence: ATR14 < SMA(ATR14, 10)
    3. MA20 slope: abs(MA20(T)/MA20(T-5) - 1) <= 0.8%
    4. Close to MA: abs(Close/MA20 - 1) <= 3%

    Args:
        df: DataFrame with OHLCV data

    Returns:
        Boolean Series indicating where pattern is detected
    """
    # MA20
    ma20 = talib.SMA(df["close"].values, timeperiod=20)
    ma20_series = pd.Series(ma20, index=df.index)

    # ATR14 and its 10-day moving average
    atr14 = talib.ATR(
        df["high"].values,
        df["low"].values,
        df["close"].values,
        timeperiod=14,
    )
    atr14_series = pd.Series(atr14, index=df.index)
    atr14_ma10 = atr14_series.rolling(10).mean()

    # Rolling HHV20 and LLV20
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


def _check_edge2_type2_trend_pullback(df: pd.DataFrame) -> pd.Series:
    """
    Edge 2 Type 2: Trend Pullback pattern.

    Structure conditions:
    1. Trend: MA20 > MA60 (and optionally MA60 > MA120)
    2. Pullback distance: Close/MA20 in [0.97, 1.03] (within ±3% of MA20)
    3. Volume contraction: SMA(Vol,3) < SMA(Vol,10) or Vol < SMA(Vol,5)
    4. Support not broken: LLV5 >= MA60 * 0.98

    Args:
        df: DataFrame with OHLCV data (requires 'vol' column)

    Returns:
        Boolean Series indicating where pattern is detected
    """
    # Moving averages
    ma20 = pd.Series(talib.SMA(df["close"].values, timeperiod=20), index=df.index)
    ma60 = pd.Series(talib.SMA(df["close"].values, timeperiod=60), index=df.index)
    ma120 = pd.Series(talib.SMA(df["close"].values, timeperiod=120), index=df.index)

    # Volume moving averages
    vol = df["vol"] if "vol" in df.columns else df.get("volume", pd.Series(0, index=df.index))
    vol_ma3 = vol.rolling(3).mean()
    vol_ma5 = vol.rolling(5).mean()
    vol_ma10 = vol.rolling(10).mean()

    # LLV5 (lowest low of last 5 days)
    llv5 = df["low"].rolling(5).min()

    # Condition 1: Trend (MA20 > MA60, optionally MA60 > MA120)
    cond_trend = (ma20 > ma60) & (ma60 > ma120)

    # Condition 2: Pullback distance (Close within ±3% of MA20)
    close_to_ma20 = df["close"] / ma20
    cond_pullback = (close_to_ma20 >= EDGE2_T2_PULLBACK_RANGE[0]) & (close_to_ma20 <= EDGE2_T2_PULLBACK_RANGE[1])

    # Condition 3: Volume contraction (either of two conditions)
    cond_vol = (vol_ma3 < vol_ma10) | (vol < vol_ma5)

    # Condition 4: Support not broken (LLV5 >= MA60 * 0.98)
    cond_support = llv5 >= ma60 * EDGE2_T2_SUPPORT_RATIO

    # All conditions must be met
    return cond_trend & cond_pullback & cond_vol & cond_support


def _check_edge2_type3_breakout_retest(df: pd.DataFrame) -> pd.Series:  # pylint: disable=too-many-locals
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

    Returns:
        Boolean Series indicating where pattern is detected
    """
    close = df["close"]
    vol = df["vol"] if "vol" in df.columns else df.get("volume", pd.Series(0, index=df.index))

    # HHV20_prev: 20-day high as of previous day
    hhv20_prev = df["high"].rolling(20).max().shift(1)

    # LLV3 (lowest low of last 3 days)
    llv3 = df["low"].rolling(3).min()

    # Mark valid breakout days: Close > HHV20_prev AND Vol >= Vol_MA5 * 1.5
    is_breakout = (close > hhv20_prev) & (vol >= vol.rolling(5).mean() * EDGE2_T3_BREAKOUT_VOL_RATIO)

    # Volume contraction during retest
    vol_contraction = vol.rolling(3).mean() < vol.rolling(10).mean()

    # Retest end signal: Close > Open OR Close > MA5
    ma5 = pd.Series(talib.SMA(close.values, timeperiod=5), index=df.index)
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


def _check_edge2(df: pd.DataFrame) -> pd.Series:
    """
    Edge 2: Structure patterns for 5-day bullish outlook.

    Combines multiple pattern types with OR logic:
    - Type 1: Compression -> Expansion
    - Type 2: Trend Pullback
    - Type 3: Breakout -> Retest

    Args:
        df: DataFrame with OHLCV data

    Returns:
        Boolean Series (type1 OR type2 OR type3)
    """
    type1 = _check_edge2_type1_compression(df)
    type2 = _check_edge2_type2_trend_pullback(df)
    type3 = _check_edge2_type3_breakout_retest(df)

    # Type 1 OR Type 2 OR Type 3
    return type1 | type2 | type3


def get_edge2_struct_type(df: pd.DataFrame) -> pd.Series:
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

    Returns:
        Series with structure tag strings (None if no pattern matches)
    """
    type1 = _check_edge2_type1_compression(df)
    type2 = _check_edge2_type2_trend_pullback(df)
    type3 = _check_edge2_type3_breakout_retest(df)

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


# === Edge 3 Functions ===


def _check_edge3_compress(df: pd.DataFrame) -> pd.Series:
    """
    Edge 3 entry signal for COMPRESS structure.

    Conditions:
    - Close > HHV20_prev (breakout from compression)
    - AR >= 1.3 (turnover surge)
    - CloseStrong (close in upper 70% of range)

    Args:
        df: DataFrame with OHLCV + amount data

    Returns:
        Boolean Series indicating where entry signal is detected
    """
    # HHV20_prev: 20-day high as of previous day
    hhv20_prev = df["high"].rolling(20).max().shift(1)

    # Amount Ratio
    ar = _calculate_amount_ratio(df)

    # CloseStrong
    close_strong = _is_close_strong(df)

    # All conditions must be met
    cond_breakout = df["close"] > hhv20_prev
    cond_ar = ar >= EDGE3_AR_COMPRESS

    return cond_breakout & cond_ar & close_strong


def _check_edge3_pullback(df: pd.DataFrame) -> pd.Series:
    """
    Edge 3 entry signal for PULLBACK structure.

    Two branches (OR logic):
    Branch 1: Close > MA20 AND AR >= 1.2
    Branch 2: StopDrop AND BullishCandle AND VolUp

    Args:
        df: DataFrame with OHLCV + amount data

    Returns:
        Boolean Series indicating where entry signal is detected
    """
    # MA20
    ma20 = pd.Series(talib.SMA(df["close"].values, timeperiod=20), index=df.index)

    # Amount Ratio
    ar = _calculate_amount_ratio(df)

    # Branch 1: Close > MA20 AND AR >= 1.2
    branch1 = (df["close"] > ma20) & (ar >= EDGE3_AR_PULLBACK)

    # Branch 2: StopDrop AND BullishCandle AND VolUp
    stop_drop = _is_stop_drop(df)
    bullish_candle = _is_bullish_candle(df)
    vol_up = ar >= EDGE3_VOLUP_THRESHOLD
    branch2 = stop_drop & bullish_candle & vol_up

    return branch1 | branch2


def _check_edge3_retest(df: pd.DataFrame) -> pd.Series:  # pylint: disable=too-many-locals
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

    Returns:
        Boolean Series indicating where entry signal is detected
    """
    close = df["close"]
    amount = df["amount"] if "amount" in df.columns else pd.Series(0, index=df.index)

    # HHV20_prev: 20-day high as of previous day (breakout level reference)
    hhv20_prev = df["high"].rolling(20).max().shift(1)

    # LLV3 (lowest low of last 3 days)
    llv3 = df["low"].rolling(3).min()

    # Amount moving averages for contraction check
    amount_ma3 = amount.rolling(3).mean()
    amount_ma10 = amount.rolling(10).mean()

    # MA5 for demand signal
    ma5 = pd.Series(talib.SMA(close.values, timeperiod=5), index=df.index)

    # Amount Ratio
    ar = _calculate_amount_ratio(df)

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


def _check_edge3(df: pd.DataFrame) -> pd.Series:
    """
    Edge 3: Entry signal based on Edge 2 structure type.

    Applies different conditions for each structure:
    - COMPRESS: breakout + AR >= 1.3 + CloseStrong
    - PULLBACK: (Close > MA20 AND AR >= 1.2) OR (StopDrop + BullishCandle + VolUp)
    - RETEST: HoldBreakout + Close > High_prev + AR >= 1.3

    Args:
        df: DataFrame with OHLCV + amount data

    Returns:
        Boolean Series indicating where Edge 3 condition is met
    """
    # Get Edge 2 structure type for each day
    struct_type = get_edge2_struct_type(df)

    # Calculate Edge 3 conditions for each structure type
    compress_cond = _check_edge3_compress(df)
    pullback_cond = _check_edge3_pullback(df)
    retest_cond = _check_edge3_retest(df)

    # Apply Edge 3 based on struct type
    edge3 = pd.Series(False, index=df.index)
    edge3 = edge3 | ((struct_type == STRUCT_COMPRESS) & compress_cond)
    edge3 = edge3 | ((struct_type == STRUCT_PULLBACK) & pullback_cond)
    edge3 = edge3 | ((struct_type == STRUCT_RETEST) & retest_cond)

    return edge3


# === Edge 4 Functions ===


def _is_bullish_candle_simple(df: pd.DataFrame) -> pd.Series:
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
    cond_close_strong = _is_close_strong(df)

    return cond_bullish & cond_close_strong


def _check_edge4_overheated(df: pd.DataFrame) -> pd.Series:
    """
    Edge 4: Overheated rejection filter.

    Rejects signals when stock has risen too fast (overheated):
    - ConsecutiveBullishCandles >= 4 (last 4 days are all bullish)
    - Sum(pct_chg, 4) >= 15% (cumulative return >= 15%)

    If BOTH conditions are true → Reject (return False)
    Otherwise → Pass (return True)

    Args:
        df: DataFrame with OHLC + pct_chg data

    Returns:
        Boolean Series: True = pass (not overheated), False = reject (overheated)
    """
    # Simple bullish candle for Edge 4
    bullish = _is_bullish_candle_simple(df)

    # Check for 4 consecutive bullish candles
    # Rolling window of 4, all must be True (sum == 4)
    n_days = EDGE4_CONSECUTIVE_BULLISH_DAYS
    consecutive_bullish = bullish.rolling(n_days).sum() == n_days

    # Cumulative return over last 4 days
    pct_chg = df["pct_chg"] if "pct_chg" in df.columns else pd.Series(0, index=df.index)
    cumulative_return = pct_chg.rolling(n_days).sum()

    # Overheated condition: consecutive bullish AND high cumulative return
    overheated = consecutive_bullish & (cumulative_return >= EDGE4_CUMULATIVE_RETURN_THRESHOLD)

    # Edge 4 returns True when NOT overheated (pass filter)
    return ~overheated


def four_edge(df: pd.DataFrame) -> bool:
    """
    Four-Edge feature detection.

    Detect stocks with sufficient volatility and favorable structure for active trading.

    Conditions:
    - Edge 1: ATR(14) / Close >= 2.5%
    - Edge 2: Structure patterns (Type 1 OR Type 2 OR Type 3)
      - Type 1: Compression -> Expansion
      - Type 2: Trend Pullback
      - Type 3: Breakout -> Retest
    - Edge 3: Entry signals based on structure type
      - COMPRESS: Close > HHV20_prev AND AR >= 1.3 AND CloseStrong
      - PULLBACK: (Close > MA20 AND AR >= 1.2) OR (StopDrop + BullishCandle + VolUp)
      - RETEST: HoldBreakout AND Close > High_prev AND AR >= 1.3
    - Edge 4: Overheated rejection filter
      - Reject if: 4 consecutive bullish candles AND Sum(pct_chg, 4) >= 15%

    Args:
        df: DataFrame with daily bar data (OHLCV + amount + pct_chg)

    Returns:
        bool: True if signal detected in last 3 days
    """
    # Clean data
    df = df.dropna()

    # Minimum data validation (MA120 for Type 2 + buffer)
    min_days = 130
    if len(df) < min_days:
        return False

    # Create working copy
    tmp_df = df.copy()

    # Edge 1: ATR volatility
    edge1 = _check_edge1_atr_volatility(tmp_df)

    # Edge 2: Structure patterns
    edge2 = _check_edge2(tmp_df)

    # Edge 3: Entry signals based on structure type
    edge3 = _check_edge3(tmp_df)

    # Edge 4: Overheated rejection filter
    edge4 = _check_edge4_overheated(tmp_df)

    # Combine: Edge1 AND Edge2 AND Edge3 AND Edge4
    signal = edge1 & edge2 & edge3 & edge4

    # Check if signal exists in last 3 days
    return signal.tail(3).any()
