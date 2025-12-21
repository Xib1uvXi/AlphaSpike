"""Common return calculation utilities for backtest and tracking."""

import pandas as pd


def calculate_period_returns(  # pylint: disable=too-many-locals
    df: pd.DataFrame,
    signal_date: str,
    holding_periods: list[int],
) -> dict | None:
    """
    Calculate returns for given holding periods after a signal date.

    Entry: Next trading day's open price after signal date
    Exit: Nth trading day's close price

    Args:
        df: Daily bar data with columns: ts_code, trade_date, open, close
        signal_date: Signal trigger date (YYYYMMDD)
        holding_periods: List of holding periods to calculate (e.g., [1, 2, 3] or [5])

    Returns:
        Dict with keys:
            - ts_code: Stock code
            - signal_date: Signal date
            - entry_date: Entry date (next trading day)
            - entry_price: Entry price (open price on entry date)
            - returns: Dict mapping period -> return percentage
            - max_return: Maximum return during longest holding period (based on close prices)
        Returns None if insufficient data.
    """
    if df.empty or "ts_code" not in df.columns:
        return None

    ts_code = df.iloc[0]["ts_code"]

    # Ensure df is sorted by trade_date
    df = df.sort_values("trade_date").reset_index(drop=True)

    # Find rows after signal_date (these are the future trading days)
    future_df = df[df["trade_date"] > signal_date].reset_index(drop=True)

    # Need at least 1 row for entry
    if len(future_df) < 1:
        return None

    # Entry is the first day after signal
    entry_row = future_df.iloc[0]
    entry_date = str(entry_row["trade_date"])
    entry_price = float(entry_row["open"])

    if entry_price <= 0:
        return None

    # Calculate returns for each holding period
    max_period = max(holding_periods)
    returns = {}

    for period in holding_periods:
        if len(future_df) >= period:
            exit_price = float(future_df.iloc[period - 1]["close"])
            period_return = (exit_price - entry_price) / entry_price * 100
            returns[period] = round(period_return, 2)
        else:
            returns[period] = None

    # Calculate max return during longest available holding period
    available_periods = min(max_period, len(future_df))
    max_return = None
    if available_periods > 0:
        holding_df = future_df.iloc[:available_periods]
        max_close = float(holding_df["close"].max())
        max_return = round((max_close - entry_price) / entry_price * 100, 2)

    return {
        "ts_code": ts_code,
        "signal_date": signal_date,
        "entry_date": entry_date,
        "entry_price": round(entry_price, 2),
        "returns": returns,
        "max_return": max_return,
    }
