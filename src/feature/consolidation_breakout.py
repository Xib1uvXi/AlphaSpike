"""Consolidation Breakout (横盘突破) feature detection module."""

import warnings

import pandas as pd
import talib

warnings.filterwarnings("ignore")


def _calculate_bb_width(close: pd.Series, timeperiod: int = 20) -> pd.Series:
    """
    Calculate Bollinger Bands width as percentage.

    BB_Width = (Upper - Lower) / Middle * 100

    Args:
        close: Close price series
        timeperiod: Period for Bollinger Bands calculation

    Returns:
        pd.Series: BB width as percentage
    """
    upper, middle, lower = talib.BBANDS(close, timeperiod=timeperiod)
    bb_width = (upper - lower) / middle * 100
    return bb_width


def _calculate_bb_width_quantile(bb_width: pd.Series, window: int = 20) -> pd.Series:
    """
    Calculate BB width quantile over rolling window.

    Args:
        bb_width: BB width series
        window: Lookback window for quantile calculation

    Returns:
        pd.Series: Quantile value (0-1) for each day
    """

    def quantile_rank(x):
        if len(x) < window:
            return float("nan")
        return (x < x.iloc[-1]).mean()

    return bb_width.rolling(window=window, min_periods=window).apply(quantile_rank, raw=False)


def _check_ma20_flat(df: pd.DataFrame) -> pd.Series:
    """
    Check if MA20 is flat based on compound conditions.

    Conditions:
    - abs(MA20 − MA20[5]) / MA20 < 0.003 (stable over 5 days)
    - STD(MA20, 10) / MA20 < 0.002 (low variance)

    Args:
        df: DataFrame with ma20 column

    Returns:
        pd.Series: Boolean series indicating MA20 flat condition
    """
    ma20_stable = (df["ma20"] - df["ma20"].shift(5)).abs() / df["ma20"] < 0.003
    ma20_std = df["ma20"].rolling(window=10, min_periods=10).std()
    ma20_low_var = (ma20_std / df["ma20"]) < 0.002
    return ma20_stable & ma20_low_var


def _detect_consolidation(df: pd.DataFrame, min_days: int = 5) -> pd.Series:
    """
    Detect consolidation (横盘) periods.

    Consolidation conditions:
    1. ATR_14 / Close < 1.5% (low volatility)
    2. ADX_14 < 22 (no trend)
    3. BB_Width < 30th percentile of last 20 days
    4. MA20 is flat (multiple conditions)

    Args:
        df: DataFrame with OHLCV data and calculated indicators
        min_days: Minimum consecutive days for valid consolidation

    Returns:
        pd.Series: Boolean series indicating consolidation periods
    """
    # Combine all consolidation conditions
    daily_consolidation = (
        (df["atr14"] / df["close"] * 100 < 1.5)  # Low volatility
        & (df["adx14"] < 22)  # No trend
        & (df["bb_width_quantile"] < 0.30)  # BB width low
        & _check_ma20_flat(df)  # MA20 flat
    )

    # Detect consecutive consolidation days
    signal_int = daily_consolidation.astype(int)
    rolling_sum = signal_int.rolling(window=min_days, min_periods=min_days).sum()
    return rolling_sum >= min_days


def _detect_breakout(df: pd.DataFrame, lookback: int = 10) -> pd.Series:
    """
    Detect breakout signals.

    Breakout conditions:
    1. Close > HHV(High, lookback) - price breaks above consolidation range
    2. Volume > SMA(Volume, 20) * 1.5 - volume surge

    Args:
        df: DataFrame with OHLCV data and calculated indicators
        lookback: Lookback period for consolidation range (default 10)

    Returns:
        pd.Series: Boolean series indicating breakout signals
    """
    # Condition 1: Close > HHV(High, lookback)
    # We need the high of previous days (excluding today)
    hhv = df["high"].shift(1).rolling(window=lookback, min_periods=lookback).max()
    price_breakout = df["close"] > hhv

    # Condition 2: Volume > SMA(Volume, 20) * 1.5
    vol_sma20 = talib.SMA(df["vol"], timeperiod=20)
    volume_surge = df["vol"] > vol_sma20 * 1.5

    breakout_signal = price_breakout & volume_surge

    return breakout_signal


def consolidation_breakout(df: pd.DataFrame, min_consolidation_days: int = 3, lookback: int = 10) -> bool:
    """
    Feature: Consolidation Breakout (横盘突破)

    Detects breakout signals after a period of consolidation.

    Phase 1 - Consolidation Detection:
        - ATR_14 / Close < 1.5% (low volatility)
        - ADX_14 < 22 (no trend)
        - BB_Width < 30th percentile of last 20 days
        - MA20 is flat (compound condition)
        - Must persist for at least min_consolidation_days

    Phase 2 - Breakout Detection:
        - Close > HHV(High, lookback) from consolidation period
        - Volume > SMA(Volume, 20) * 1.5 (volume surge)

    Args:
        df: DataFrame with daily bar data containing OHLCV columns
        min_consolidation_days: Minimum days for valid consolidation (default 3)
        lookback: Lookback period for consolidation range (default 10)

    Returns:
        bool: True if breakout signal detected in last 3 days, False otherwise.
    """
    df = df.dropna()

    # Need sufficient data for indicators
    # 20 days for MA20/BB + 20 days for quantile window + buffer
    if len(df) < 60:
        return False

    # Validate parameters
    if not (3 <= min_consolidation_days <= 20):
        raise ValueError("min_consolidation_days must be between 3 and 20")

    if not (5 <= lookback <= 30):
        raise ValueError("lookback must be between 5 and 30")

    tmp_df = df.copy()

    # Calculate indicators
    tmp_df["atr14"] = talib.ATR(tmp_df["high"], tmp_df["low"], tmp_df["close"], timeperiod=14)
    tmp_df["adx14"] = talib.ADX(tmp_df["high"], tmp_df["low"], tmp_df["close"], timeperiod=14)
    tmp_df["ma20"] = talib.SMA(tmp_df["close"], timeperiod=20)

    # Calculate BB width and its quantile
    tmp_df["bb_width"] = _calculate_bb_width(tmp_df["close"], timeperiod=20)
    tmp_df["bb_width_quantile"] = _calculate_bb_width_quantile(tmp_df["bb_width"], window=20)

    # Detect consolidation periods
    tmp_df["consolidation"] = _detect_consolidation(tmp_df, min_consolidation_days)

    # Detect breakout signals
    tmp_df["breakout"] = _detect_breakout(tmp_df, lookback)

    # Final signal: breakout within 10 days after consolidation
    # Check if consolidation occurred in any of the previous 10 days
    consolidation_int = tmp_df["consolidation"].astype(int)
    tmp_df["consolidation_recent"] = consolidation_int.shift(1).rolling(window=10, min_periods=1).max().fillna(0) > 0
    tmp_df["final_signal"] = tmp_df["consolidation_recent"] & tmp_df["breakout"]

    # Check if signal exists in last 3 trading days
    recent_signals = tmp_df["final_signal"].tail(3)
    return recent_signals.any()
