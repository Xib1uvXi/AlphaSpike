"""Four-Edge feature detection package.

This package provides the four-edge feature detection system:
- Edge 1: ATR volatility condition
- Edge 2: Structure patterns (Compression, Pullback, Retest)
- Edge 3: Entry signals based on structure type
- Edge 4: Overheated rejection filter
"""

import pandas as pd

from .edge1 import (
    ATR_VOLATILITY_THRESHOLD,
    calculate_atr_volatility as _calculate_atr_volatility,
    check_edge1_atr_volatility,
)
from .edge2 import (
    EDGE2_BOX_WIDTH_THRESHOLD,
    EDGE2_CLOSE_TO_MA_THRESHOLD,
    EDGE2_MA20_SLOPE_THRESHOLD,
    EDGE2_T2_PULLBACK_RANGE,
    EDGE2_T2_SUPPORT_RATIO,
    EDGE2_T3_BREAKOUT_VOL_RATIO,
    EDGE2_T3_RETEST_DAYS_MAX,
    EDGE2_T3_RETEST_DAYS_MIN,
    EDGE2_T3_RETEST_SUPPORT_RATIO,
    STRUCT_COMPRESS,
    STRUCT_PULLBACK,
    STRUCT_RETEST,
    check_edge2,
    check_edge2_type1_compression as _check_edge2_type1_compression,
    check_edge2_type2_trend_pullback as _check_edge2_type2_trend_pullback,
    check_edge2_type3_breakout_retest as _check_edge2_type3_breakout_retest,
    get_edge2_struct_type,
    get_last_struct_type,
)
from .edge3 import (
    EDGE3_AR_COMPRESS,
    EDGE3_AR_PULLBACK,
    EDGE3_AR_RETEST,
    EDGE3_RETEST_SUPPORT_RATIO,
    EDGE3_VOLUP_THRESHOLD,
    check_edge3,
    check_edge3_compress as _check_edge3_compress,
    check_edge3_pullback as _check_edge3_pullback,
    check_edge3_retest as _check_edge3_retest,
)
from .edge4 import (
    EDGE4_CONSECUTIVE_BULLISH_DAYS,
    EDGE4_CUMULATIVE_RETURN_THRESHOLD,
    check_edge4_overheated,
)
from .helpers import (
    EDGE3_BULLISH_BODY_RATIO,
    EDGE3_CLOSE_STRONG_RATIO,
    calculate_amount_ratio as _calculate_amount_ratio,
    is_bullish_candle as _is_bullish_candle,
    is_bullish_candle_simple as _is_bullish_candle_simple,
    is_close_strong as _is_close_strong,
    is_stop_drop as _is_stop_drop,
    precompute_indicators,
)

__all__ = [
    # Main function
    "four_edge",
    # Structure tags
    "STRUCT_COMPRESS",
    "STRUCT_PULLBACK",
    "STRUCT_RETEST",
    # Public functions
    "check_edge1_atr_volatility",
    "check_edge2",
    "check_edge3",
    "check_edge4_overheated",
    "get_edge2_struct_type",
    "get_last_struct_type",
    # Thresholds (for configuration reference and testing)
    "ATR_VOLATILITY_THRESHOLD",
    "EDGE2_BOX_WIDTH_THRESHOLD",
    "EDGE2_MA20_SLOPE_THRESHOLD",
    "EDGE2_CLOSE_TO_MA_THRESHOLD",
    "EDGE2_T2_PULLBACK_RANGE",
    "EDGE2_T2_SUPPORT_RATIO",
    "EDGE2_T3_BREAKOUT_VOL_RATIO",
    "EDGE2_T3_RETEST_DAYS_MIN",
    "EDGE2_T3_RETEST_DAYS_MAX",
    "EDGE2_T3_RETEST_SUPPORT_RATIO",
    "EDGE3_AR_COMPRESS",
    "EDGE3_AR_PULLBACK",
    "EDGE3_AR_RETEST",
    "EDGE3_VOLUP_THRESHOLD",
    "EDGE3_CLOSE_STRONG_RATIO",
    "EDGE3_BULLISH_BODY_RATIO",
    "EDGE3_RETEST_SUPPORT_RATIO",
    "EDGE4_CONSECUTIVE_BULLISH_DAYS",
    "EDGE4_CUMULATIVE_RETURN_THRESHOLD",
]

# Backward compatibility aliases for private functions (used by tests)
_check_edge1_atr_volatility = check_edge1_atr_volatility
_check_edge2 = check_edge2
_check_edge3 = check_edge3
_check_edge4_overheated = check_edge4_overheated


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

    # Precompute all indicators once (optimization: ~70% faster)
    indicators = precompute_indicators(tmp_df)

    # Edge 1: ATR volatility
    edge1 = check_edge1_atr_volatility(tmp_df, indicators=indicators)

    # Edge 2: Structure patterns
    edge2 = check_edge2(tmp_df, indicators=indicators)

    # Edge 3: Entry signals based on structure type
    edge3 = check_edge3(tmp_df, indicators=indicators)

    # Edge 4: Overheated rejection filter (doesn't need precomputed indicators)
    edge4 = check_edge4_overheated(tmp_df)

    # Combine: Edge1 AND Edge2 AND Edge3 AND Edge4
    signal = edge1 & edge2 & edge3 & edge4

    # Check if signal exists in last 3 days
    return signal.tail(3).any()
