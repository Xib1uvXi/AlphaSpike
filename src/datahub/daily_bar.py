"""Daily bar data storage and synchronization module."""

from datetime import datetime
from io import StringIO

import pandas as pd

from src.datahub.cache import get_daily_bar_cache, set_daily_bar_cache
from src.datahub.db import get_connection, init_db
from src.datahub.symbol import load_all_symbols
from src.datahub.trading_calendar import get_last_trading_day
from src.datahub.tushare import get_daily_bar


def _get_symbol_list_date(ts_code: str) -> str | None:
    """
    Get the list date for a stock symbol.

    Args:
        ts_code: Stock code in tushare format (e.g., '000001.SZ')

    Returns:
        List date in YYYYMMDD format, or None if not found.
    """
    # Convert ts_code to symbol code (e.g., '000001.SZ' -> '000001')
    code = ts_code.split(".")[0]

    symbols = load_all_symbols()
    symbol_row = symbols[symbols["code"] == code]

    if symbol_row.empty:
        return None

    # list_date format is like '1991-04-03' or '19910403', normalize to YYYYMMDD
    list_date = symbol_row.iloc[0]["list_date"]
    return list_date.replace("-", "")


def _get_latest_trade_date(ts_code: str) -> str | None:
    """
    Get the latest trade date for a stock from the database.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')

    Returns:
        Latest trade date in YYYYMMDD format, or None if no data exists.
    """
    with get_connection() as conn:
        cursor = conn.execute("SELECT MAX(trade_date) FROM daily_bar WHERE ts_code = ?", (ts_code,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None


def _get_next_date(date_str: str) -> str:
    """
    Get the next day's date string.

    Args:
        date_str: Date in YYYYMMDD format.

    Returns:
        Next day's date in YYYYMMDD format.
    """
    dt = datetime.strptime(date_str, "%Y%m%d")
    next_dt = dt + pd.Timedelta(days=1)
    return next_dt.strftime("%Y%m%d")


def _get_today() -> str:
    """Get today's date in YYYYMMDD format."""
    return datetime.now().strftime("%Y%m%d")


def _save_to_db(df: pd.DataFrame):
    """
    Save daily bar data to SQLite database.

    Args:
        df: DataFrame with daily bar data from tushare.
    """
    if df.empty:
        return

    with get_connection() as conn:
        # Use INSERT OR REPLACE to handle duplicates
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_bar
                (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["ts_code"],
                    row["trade_date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["pre_close"],
                    row["change"],
                    row["pct_chg"],
                    row["vol"],
                    row["amount"],
                ),
            )


def sync_daily_bar(ts_code: str, end_date: str | None = None) -> int:
    """
    Synchronize daily bar data for a single stock.

    Performs incremental update:
    - If no data exists, fetches from list date to today
    - If data exists, fetches from latest date + 1 to today

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        end_date: Optional end date in YYYYMMDD format. Defaults to latest trading day.

    Returns:
        Number of new records added.

    Raises:
        ValueError: If stock symbol not found or list date unavailable.
    """
    # Ensure database is initialized
    init_db()

    # Determine start date
    latest_date = _get_latest_trade_date(ts_code)

    if latest_date:
        # Incremental update: start from next day
        start_date = _get_next_date(latest_date)
    else:
        # First sync: start from list date
        start_date = _get_symbol_list_date(ts_code)
        if not start_date:
            raise ValueError(f"Cannot find list date for {ts_code}")

    final_end_date = end_date or get_last_trading_day()

    # Skip if start_date is after end_date (already up to date)
    if start_date > final_end_date:
        return 0

    # Fetch data from tushare
    try:
        df = get_daily_bar(ts_code, start_date, final_end_date)
    except ValueError:
        # No new data available
        return 0

    # Save to database
    _save_to_db(df)

    return len(df)


def batch_sync_daily_bar(ts_codes: list[str], progress_callback=None) -> dict[str, int]:
    """
    Synchronize daily bar data for multiple stocks.

    Rate limiting is handled by the tushare module (133ms between requests).

    Args:
        ts_codes: List of stock codes (e.g., ['000001.SZ', '600000.SH'])
        progress_callback: Optional callback function(ts_code, index, total) for progress tracking

    Returns:
        Dictionary mapping ts_code to number of new records added.
    """
    results = {}
    total = len(ts_codes)

    for i, ts_code in enumerate(ts_codes):
        if progress_callback:
            progress_callback(ts_code, i + 1, total)

        try:
            count = sync_daily_bar(ts_code)
            results[ts_code] = count
        except Exception as e:
            results[ts_code] = -1  # Indicate error

    return results


def get_daily_bar_from_db(
    ts_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Get daily bar data from SQLite database with Redis caching.

    Uses Redis cache with 2-hour TTL. Cache key includes today's date.
    Falls back to database query if Redis is unavailable.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format (optional)
        end_date: End date in YYYYMMDD format (optional)

    Returns:
        DataFrame with daily bar data.
    """
    # Try to get from cache
    try:
        cached = get_daily_bar_cache(ts_code, start_date, end_date)
        if cached:
            return pd.read_json(StringIO(cached))
    except Exception:
        pass  # Redis unavailable, fall through to database

    # Query from database
    query = "SELECT * FROM daily_bar WHERE ts_code = ?"
    params = [ts_code]

    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND trade_date <= ?"
        params.append(end_date)

    query += " ORDER BY trade_date"

    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=params)

    # Store in cache
    try:
        set_daily_bar_cache(ts_code, start_date, end_date, df.to_json())
    except Exception:
        pass  # Redis unavailable, continue without caching

    return df


def get_daily_bar_from_db_no_cache(
    ts_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Get daily bar data directly from SQLite database without Redis caching.

    Use this function when Redis caching is not desired (e.g., backtesting).

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format (optional)
        end_date: End date in YYYYMMDD format (optional)

    Returns:
        DataFrame with daily bar data.
    """
    query = "SELECT * FROM daily_bar WHERE ts_code = ?"
    params = [ts_code]

    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND trade_date <= ?"
        params.append(end_date)

    query += " ORDER BY trade_date"

    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=params)

    return df


def get_date_range(ts_code: str) -> tuple[str | None, str | None]:
    """
    Get the date range of data available for a stock.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')

    Returns:
        Tuple of (min_date, max_date) in YYYYMMDD format, or (None, None) if no data.
    """
    with get_connection() as conn:
        cursor = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_bar WHERE ts_code = ?", (ts_code,))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0], result[1]
        return None, None


def batch_load_daily_bars(
    ts_codes: list[str],
    end_date: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load daily bar data for multiple symbols in a single batch query.

    This is much faster than loading symbols one by one when you need
    data for many symbols (e.g., feature scanning).

    Args:
        ts_codes: List of stock codes to load
        end_date: Optional end date filter (YYYYMMDD format)

    Returns:
        Dict mapping ts_code to DataFrame with daily bar data.
    """
    # Build query - load all data at once
    query = "SELECT * FROM daily_bar"
    params = []

    if end_date:
        query += " WHERE trade_date <= ?"
        params.append(end_date)

    query += " ORDER BY ts_code, trade_date"

    # Execute single query
    with get_connection() as conn:
        all_data = pd.read_sql_query(query, conn, params=params if params else None)

    if all_data.empty:
        return {}

    # Convert ts_codes to set for O(1) lookup
    ts_codes_set = set(ts_codes)

    # Group by ts_code and filter
    data_cache = {}
    for ts_code, group in all_data.groupby("ts_code"):
        if ts_code in ts_codes_set:
            data_cache[ts_code] = group.reset_index(drop=True)

    return data_cache
