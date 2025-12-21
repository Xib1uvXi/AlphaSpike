"""Weak to Strong (弱转强) feature detection module."""

import warnings

import pandas as pd

warnings.filterwarnings("ignore")


def _get_limit_up_threshold(ts_code: str) -> float:
    """
    Get limit-up threshold based on stock code prefix.

    Args:
        ts_code: Stock code (e.g., '000001.SZ', '300001.SZ', '600001.SH')

    Returns:
        Limit-up threshold percentage (9.5 for main board, 19.2 for ChiNext)
    """
    code = ts_code.split(".")[0]
    # 30 prefix is ChiNext, 20% limit
    if code.startswith("30"):
        return 19.2
    # 00/60 prefix is main board, 10% limit
    return 9.5


def weak_to_strong(df: pd.DataFrame) -> bool:  # pylint: disable=too-many-return-statements
    """
    Feature: Weak to Strong (弱转强)

    Detects a pattern where:
    - T-2, T-1: Both days are limit-up (涨停)
    - T (end_date): Opens below previous close AND high stays below previous close

    Detection criteria:
    - T-2: pct_chg > threshold (9.5% for 00/60, 19.2% for 30)
    - T-1: pct_chg > threshold (same as above)
    - T: open < T-1 close (gap down open)
    - T: high < T-1 close (high does not recover previous close)

    Args:
        df: DataFrame with daily bar data containing OHLCV columns and ts_code

    Returns:
        bool: True if signal detected on the last trading day, False otherwise.
    """
    df = df.dropna()

    if len(df) < 3:
        return False

    # Get ts_code from DataFrame
    if "ts_code" not in df.columns or df["ts_code"].empty:
        return False

    ts_code = df["ts_code"].iloc[0]
    threshold = _get_limit_up_threshold(ts_code)

    # Get last 3 rows: T-2, T-1, T
    last3 = df.tail(3)
    if len(last3) < 3:
        return False

    t_minus_2 = last3.iloc[0]
    t_minus_1 = last3.iloc[1]
    t = last3.iloc[2]

    # T-2: limit up
    if t_minus_2["pct_chg"] <= threshold:
        return False

    # T-1: limit up
    if t_minus_1["pct_chg"] <= threshold:
        return False

    # T: open < T-1 close (gap down)
    if t["open"] >= t_minus_1["close"]:
        return False

    # T: high < T-1 close (does not recover)
    if t["high"] >= t_minus_1["close"]:
        return False

    return True
