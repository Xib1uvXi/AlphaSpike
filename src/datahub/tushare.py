"""Tushare API module with rate limiting support."""

import os
import time
from functools import wraps

import pandas as pd
import tushare as ts
from dotenv import load_dotenv

load_dotenv()

ts.set_token(os.getenv("TUSHARE_TOKEN"))

# Rate limiting: 45 requests per minute = 0.75 requests per second
# Use 1.4s interval for safety margin (60/45 = 1.33s)
_RATE_LIMIT_INTERVAL = 1.4  # seconds between requests
_last_request_time = 0.0


def rate_limit(func):
    """
    Decorator to enforce tushare API rate limit (45 requests/minute).

    Ensures at least 1.4s between consecutive API calls.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        global _last_request_time
        current_time = time.time()
        elapsed = current_time - _last_request_time

        if elapsed < _RATE_LIMIT_INTERVAL:
            time.sleep(_RATE_LIMIT_INTERVAL - elapsed)

        _last_request_time = time.time()
        return func(*args, **kwargs)

    return wrapper


@rate_limit
def get_daily_bar(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get daily bar data from tushare API.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        DataFrame with columns: ts_code, trade_date, open, high, low, close,
                               pre_close, change, pct_chg, vol, amount

    Raises:
        ValueError: If no data found for the given parameters.
    """
    df = ts.pro_bar(ts_code=ts_code, adj="qfq", start_date=start_date, end_date=end_date)

    # Drop any rows with missing fields; API can return sparse frames on partial trading days.
    if df is not None:
        df = df.dropna()

    if df is None or df.empty:
        raise ValueError(f"No data found for {ts_code} from {start_date} to {end_date}")

    return df
