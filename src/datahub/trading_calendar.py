"""Trading calendar helpers for determining trading days."""

from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

_CALENDAR_DIR = Path(__file__).parent
_CALENDAR_PATTERN = "20*.csv"


def _to_date(value: str | date | datetime | None) -> date:
    """Normalize supported inputs to a date object."""
    if value is None:
        return date.today()

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    return pd.to_datetime(value).date()


@lru_cache(maxsize=1)
def _load_calendar() -> pd.DataFrame:
    """Load and cache all available calendar CSV files."""
    frames = []
    for csv_file in sorted(_CALENDAR_DIR.glob(_CALENDAR_PATTERN)):
        df = pd.read_csv(csv_file, dtype={"trade_date": str, "trade_status": int})
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        frames.append(df[["trade_date", "trade_status"]])

    if not frames:
        raise FileNotFoundError("No trading calendar files found")

    calendar = pd.concat(frames, ignore_index=True)
    calendar = calendar.drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
    return calendar.reset_index(drop=True)


def _last_weekday_before(ref_date: date) -> date:
    """Fallback to the most recent weekday (Mon-Fri)."""
    current = ref_date
    while current.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        current -= timedelta(days=1)
    return current


def is_trading_day(value: str | date | datetime | None = None) -> bool:
    """
    Check if the given date is a trading day according to the local calendar.

    Falls back to weekday logic if the date is outside known calendars.
    """
    target = _to_date(value)
    try:
        calendar = _load_calendar()
        match = calendar[calendar["trade_date"] == target]
        if not match.empty:
            return bool(match.iloc[0]["trade_status"])
    except FileNotFoundError:
        pass

    # Fallback: treat weekdays as trading days
    return target.weekday() < 5


def get_last_trading_day(value: str | date | datetime | None = None) -> str:
    """
    Get the most recent trading day on or before the given date.

    If the date is not in the calendar or falls in a future year without data,
    falls back to the nearest previous weekday.
    """
    target = _to_date(value)

    try:
        calendar = _load_calendar()
        before_or_equal = calendar[calendar["trade_date"] <= target]
        trading_days = before_or_equal[before_or_equal["trade_status"] == 1]

        if not trading_days.empty:
            last = trading_days.iloc[-1]["trade_date"]
            return last.strftime("%Y%m%d")
    except FileNotFoundError:
        # No calendar available; use weekday logic
        pass

    return _last_weekday_before(target).strftime("%Y%m%d")


def get_next_n_trading_days(value: str | date | datetime, n: int) -> list[str]:
    """
    Get the next N trading days after the given date (exclusive of the given date).

    Args:
        value: Reference date (YYYYMMDD string, date, or datetime)
        n: Number of trading days to retrieve

    Returns:
        List of trading dates in YYYYMMDD format.
        Returns fewer items if not enough future trading days in calendar.
    """
    if n <= 0:
        return []

    target = _to_date(value)
    result = []

    try:
        calendar = _load_calendar()
        # Get all trading days after the target date
        after_target = calendar[calendar["trade_date"] > target]
        trading_days = after_target[after_target["trade_status"] == 1]

        if not trading_days.empty:
            # Take the first n trading days
            for _, row in trading_days.head(n).iterrows():
                result.append(row["trade_date"].strftime("%Y%m%d"))
            return result
    except FileNotFoundError:
        pass

    # Fallback: use weekday logic
    current = target
    while len(result) < n:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Monday to Friday
            result.append(current.strftime("%Y%m%d"))

    return result
