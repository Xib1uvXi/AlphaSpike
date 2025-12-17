"""Edge 4: Overheated rejection filter for Four-Edge feature detection."""

import pandas as pd

from src.common.config import FOUR_EDGE_CONFIG

from .helpers import is_bullish_candle_simple

_cfg = FOUR_EDGE_CONFIG

# Edge 4 thresholds
EDGE4_CONSECUTIVE_BULLISH_DAYS = _cfg.consecutive_bullish_days
EDGE4_CUMULATIVE_RETURN_THRESHOLD = _cfg.cumulative_return_threshold


def check_edge4_overheated(df: pd.DataFrame) -> pd.Series:
    """
    Edge 4: Overheated rejection filter.

    Rejects signals when stock has risen too fast (overheated):
    - ConsecutiveBullishCandles >= 4 (last 4 days are all bullish)
    - Sum(pct_chg, 4) >= 15% (cumulative return >= 15%)

    If BOTH conditions are true -> Reject (return False)
    Otherwise -> Pass (return True)

    Args:
        df: DataFrame with OHLC + pct_chg data

    Returns:
        Boolean Series: True = pass (not overheated), False = reject (overheated)
    """
    # Simple bullish candle for Edge 4
    bullish = is_bullish_candle_simple(df)

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
