"""SQLite database module for managing daily bar data storage."""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Daily bar table schema
DAILY_BAR_TABLE = "daily_bar"
DAILY_BAR_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_bar (
    ts_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    pre_close REAL,
    change REAL,
    pct_chg REAL,
    vol REAL,
    amount REAL,
    PRIMARY KEY (ts_code, trade_date)
)
"""

# Indexes for faster queries
# Note: PRIMARY KEY (ts_code, trade_date) creates implicit compound index for ts_code-first queries
# This additional index optimizes date-range queries like "WHERE trade_date <= ?"
DAILY_BAR_INDEX_TRADE_DATE = """
CREATE INDEX IF NOT EXISTS idx_daily_bar_trade_date ON daily_bar (trade_date, ts_code)
"""


def get_db_path() -> Path:
    """
    Get the SQLite database path from environment variable.

    Returns:
        Path to the SQLite database file.

    Raises:
        ValueError: If SQLITE_PATH is not configured.
    """
    sqlite_path = os.getenv("SQLITE_PATH")
    if not sqlite_path:
        raise ValueError("SQLITE_PATH is not configured in .env file")
    return Path(sqlite_path)


@contextmanager
def get_connection():
    """
    Context manager for SQLite database connection.

    Yields:
        sqlite3.Connection: Database connection object.

    Example:
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM daily_bar")
    """
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """
    Initialize the database by creating tables and indexes.

    Creates the daily_bar table if it doesn't exist.
    """
    with get_connection() as conn:
        conn.execute(DAILY_BAR_SCHEMA)
        conn.execute(DAILY_BAR_INDEX_TRADE_DATE)


def drop_daily_bar_table():
    """
    Drop the daily_bar table. Use with caution.

    Returns:
        bool: True if table was dropped, False if it didn't exist.
    """
    with get_connection() as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (DAILY_BAR_TABLE,))
        if cursor.fetchone():
            conn.execute(f"DROP TABLE {DAILY_BAR_TABLE}")
            return True
        return False
