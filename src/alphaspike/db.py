"""SQLite database module for feature scan results."""

import json

from src.datahub.db import get_connection

# Feature result table schema
FEATURE_RESULT_TABLE = "feature_result"
FEATURE_RESULT_SCHEMA = """
CREATE TABLE IF NOT EXISTS feature_result (
    feature_name TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    ts_codes TEXT NOT NULL,
    PRIMARY KEY (feature_name, scan_date)
)
"""


def init_feature_db() -> None:
    """Initialize the feature_result table if it doesn't exist."""
    with get_connection() as conn:
        conn.execute(FEATURE_RESULT_SCHEMA)


def get_feature_result(feature_name: str, scan_date: str) -> list[str] | None:
    """
    Get feature scan result from SQLite.

    Args:
        feature_name: Feature name (e.g., 'bbc')
        scan_date: Date in YYYYMMDD format

    Returns:
        List of ts_codes with signals, or None if not found.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT ts_codes FROM feature_result WHERE feature_name = ? AND scan_date = ?",
            (feature_name, scan_date),
        )
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return None


def save_feature_result(feature_name: str, scan_date: str, ts_codes: list[str]) -> None:
    """
    Save feature scan result to SQLite.

    Uses INSERT OR REPLACE to handle both inserts and updates.

    Args:
        feature_name: Feature name (e.g., 'bbc')
        scan_date: Date in YYYYMMDD format
        ts_codes: List of stock codes with signals
    """
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO feature_result (feature_name, scan_date, ts_codes) VALUES (?, ?, ?)",
            (feature_name, scan_date, json.dumps(ts_codes)),
        )


def delete_feature_result(feature_name: str, scan_date: str) -> bool:
    """
    Delete feature scan result from SQLite.

    Args:
        feature_name: Feature name
        scan_date: Date in YYYYMMDD format

    Returns:
        True if a row was deleted, False otherwise.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM feature_result WHERE feature_name = ? AND scan_date = ?",
            (feature_name, scan_date),
        )
        return cursor.rowcount > 0


def get_all_feature_results() -> list[tuple[str, str, list[str]]]:
    """
    Get all stored feature results from SQLite.

    Returns:
        List of (feature_name, scan_date, ts_codes) tuples.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT feature_name, scan_date, ts_codes FROM feature_result ORDER BY feature_name, scan_date"
        )
        results = []
        for row in cursor.fetchall():
            results.append((row[0], row[1], json.loads(row[2])))
        return results


def get_feature_results_by_name(feature_name: str) -> list[tuple[str, list[str]]]:
    """
    Get all stored results for a specific feature.

    Args:
        feature_name: Feature name (e.g., 'bbc')

    Returns:
        List of (scan_date, ts_codes) tuples.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT scan_date, ts_codes FROM feature_result WHERE feature_name = ? ORDER BY scan_date",
            (feature_name,),
        )
        results = []
        for row in cursor.fetchall():
            results.append((row[0], json.loads(row[1])))
        return results


def get_distinct_feature_names() -> list[str]:
    """
    Get list of distinct feature names with stored results.

    Returns:
        List of feature names.
    """
    with get_connection() as conn:
        cursor = conn.execute("SELECT DISTINCT feature_name FROM feature_result ORDER BY feature_name")
        return [row[0] for row in cursor.fetchall()]


def get_feature_results_by_date(scan_date: str) -> list[tuple[str, str, list[str]]]:
    """
    Get all feature results for a specific scan date.

    Args:
        scan_date: Date in YYYYMMDD format

    Returns:
        List of (feature_name, scan_date, ts_codes) tuples.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT feature_name, scan_date, ts_codes FROM feature_result WHERE scan_date = ? ORDER BY feature_name",
            (scan_date,),
        )
        results = []
        for row in cursor.fetchall():
            results.append((row[0], row[1], json.loads(row[2])))
        return results


def get_feature_result_by_name_and_date(feature_name: str, scan_date: str) -> list[tuple[str, list[str]]]:
    """
    Get feature result for a specific feature and date.

    Args:
        feature_name: Feature name (e.g., 'bbc')
        scan_date: Date in YYYYMMDD format

    Returns:
        List of (scan_date, ts_codes) tuples (at most one element).
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT scan_date, ts_codes FROM feature_result WHERE feature_name = ? AND scan_date = ?",
            (feature_name, scan_date),
        )
        results = []
        for row in cursor.fetchall():
            results.append((row[0], json.loads(row[1])))
        return results
