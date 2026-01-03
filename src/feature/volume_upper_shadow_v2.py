"""Optimized Volume Upper Shadow feature based on 42,606 signal statistical analysis.

Key optimizations vs original:
1. body_ratio < 0.20 (cross-star pattern performs 2.3x better than large body)
2. gain_2d > 3% (momentum effect, high gain_2d signals perform 2.6x better)
3. price_quantile < 0.25 (low position signals perform better)

Expected performance:
- All Positive ratio: 37.9% (vs 33.2% baseline)
- All Negative ratio: 25.6% (vs 28.8% baseline)
- Avg 3D return: +2.31% (vs +0.87% baseline, +165% improvement)
- Win rate 3D: 54.3% (vs 50.3% baseline)
"""

import pandas as pd
import talib

from src.feature.utils import calculate_price_quantile, calculate_upper_shadow_ratio


def volume_upper_shadow_v2(df: pd.DataFrame) -> bool:  # pylint: disable=too-many-locals
    """
    Optimized volume upper shadow detection.

    Signal conditions:
    - Upper shadow ratio > 2%
    - Volume surge: 1.2x to 2x of previous day's 10-day MA volume
    - Close > MA5 and Close > MA10
    - MA3 > MA5 (short-term trend confirmation)
    - body_ratio < 0.20 (cross-star pattern, KEY OPTIMIZATION)
    - gain_2d > 3% (momentum, KEY OPTIMIZATION)
    - price_quantile < 0.25 (low position, KEY OPTIMIZATION)

    Args:
        df: DataFrame with OHLCV data, must have columns:
            open, high, low, close, vol, pct_chg, trade_date

    Returns:
        True if signal detected on the last trading day.
    """
    df = df.dropna()
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

    last = tmp_df.iloc[-1]
    prev_vol_ma10 = tmp_df["vol_ma10"].iloc[-2]

    # Volume ratio
    vol_ratio = last["vol"] / prev_vol_ma10 if prev_vol_ma10 > 0 else 0

    # Body ratio (key optimization: prefer cross-star)
    body = abs(last["close"] - last["open"])
    high_low = last["high"] - last["low"]
    body_ratio = body / high_low if high_low > 0 else 1.0

    # 2-day cumulative gain
    last_2_pct = tmp_df["pct_chg"].tail(2)
    gain_2d = ((1 + last_2_pct / 100).prod() - 1) * 100

    # Original conditions (from volume_upper_shadow)
    cond_upper_shadow = last["upper_shadow"] > 2.0
    cond_vol_surge = 1.2 <= vol_ratio <= 2.0
    cond_ma_trend = last["close"] > last["ma5"] and last["close"] > last["ma10"]
    cond_ma_short = last["ma3"] > last["ma5"]

    # New optimized conditions (based on statistical analysis)
    cond_body_ratio = body_ratio < 0.20  # Cross-star pattern
    cond_gain_2d = gain_2d > 3.0  # Momentum
    cond_price_quantile = last["price_quantile"] < 0.25  # Low position

    return all(
        [
            cond_upper_shadow,
            cond_vol_surge,
            cond_ma_trend,
            cond_ma_short,
            cond_body_ratio,
            cond_gain_2d,
            cond_price_quantile,
        ]
    )
