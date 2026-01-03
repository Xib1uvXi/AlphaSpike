"""SQLite database module for feature engineering data."""

from dataclasses import dataclass

from src.datahub.db import get_connection

# Feature data table schema
FEATURE_DATA_TABLE = "feature_data"
FEATURE_DATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS feature_data (
    ts_code TEXT NOT NULL,
    signal_date TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    upper_shadow REAL,
    vol_ratio REAL,
    price_quantile REAL,
    pct_chg REAL,
    body_ratio REAL,
    close_vs_ma20 REAL,
    prev_vol_ratio REAL,
    gain_2d REAL,
    return_1d REAL,
    return_2d REAL,
    return_3d REAL,
    PRIMARY KEY (ts_code, signal_date, feature_name)
)
"""

# Index for faster queries by feature and date
FEATURE_DATA_INDEX = """
CREATE INDEX IF NOT EXISTS idx_feature_data_feature_date
ON feature_data (feature_name, signal_date)
"""


@dataclass
class FeatureData:
    """Data class for a single feature engineering record."""

    ts_code: str
    signal_date: str
    feature_name: str
    upper_shadow: float | None
    vol_ratio: float | None
    price_quantile: float | None
    pct_chg: float | None
    body_ratio: float | None
    close_vs_ma20: float | None
    prev_vol_ratio: float | None
    gain_2d: float | None
    return_1d: float | None
    return_2d: float | None
    return_3d: float | None


def init_feature_data_db() -> None:
    """Initialize the feature_data table if it doesn't exist."""
    with get_connection() as conn:
        conn.execute(FEATURE_DATA_SCHEMA)
        conn.execute(FEATURE_DATA_INDEX)


def save_feature_data(data: FeatureData) -> None:
    """
    Save a single feature data record to SQLite.

    Uses INSERT OR REPLACE to handle both inserts and updates.

    Args:
        data: FeatureData object to save.
    """
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO feature_data
            (ts_code, signal_date, feature_name, upper_shadow, vol_ratio, price_quantile,
             pct_chg, body_ratio, close_vs_ma20, prev_vol_ratio, gain_2d,
             return_1d, return_2d, return_3d)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.ts_code,
                data.signal_date,
                data.feature_name,
                data.upper_shadow,
                data.vol_ratio,
                data.price_quantile,
                data.pct_chg,
                data.body_ratio,
                data.close_vs_ma20,
                data.prev_vol_ratio,
                data.gain_2d,
                data.return_1d,
                data.return_2d,
                data.return_3d,
            ),
        )


def save_feature_data_batch(data_list: list[FeatureData]) -> None:
    """
    Save multiple feature data records to SQLite in a single transaction.

    Args:
        data_list: List of FeatureData objects to save.
    """
    if not data_list:
        return

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO feature_data
            (ts_code, signal_date, feature_name, upper_shadow, vol_ratio, price_quantile,
             pct_chg, body_ratio, close_vs_ma20, prev_vol_ratio, gain_2d,
             return_1d, return_2d, return_3d)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    d.ts_code,
                    d.signal_date,
                    d.feature_name,
                    d.upper_shadow,
                    d.vol_ratio,
                    d.price_quantile,
                    d.pct_chg,
                    d.body_ratio,
                    d.close_vs_ma20,
                    d.prev_vol_ratio,
                    d.gain_2d,
                    d.return_1d,
                    d.return_2d,
                    d.return_3d,
                )
                for d in data_list
            ],
        )


def get_feature_data_by_feature(
    feature_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[FeatureData]:
    """
    Get all feature data records for a specific feature.

    Args:
        feature_name: Feature name (e.g., 'volume_upper_shadow')
        start_date: Start date in YYYYMMDD format (inclusive), optional
        end_date: End date in YYYYMMDD format (inclusive), optional

    Returns:
        List of FeatureData objects.
    """
    query = "SELECT * FROM feature_data WHERE feature_name = ?"
    params: list = [feature_name]

    if start_date:
        query += " AND signal_date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND signal_date <= ?"
        params.append(end_date)

    query += " ORDER BY signal_date, ts_code"

    with get_connection() as conn:
        cursor = conn.execute(query, params)
        results = []
        for row in cursor.fetchall():
            results.append(
                FeatureData(
                    ts_code=row[0],
                    signal_date=row[1],
                    feature_name=row[2],
                    upper_shadow=row[3],
                    vol_ratio=row[4],
                    price_quantile=row[5],
                    pct_chg=row[6],
                    body_ratio=row[7],
                    close_vs_ma20=row[8],
                    prev_vol_ratio=row[9],
                    gain_2d=row[10],
                    return_1d=row[11],
                    return_2d=row[12],
                    return_3d=row[13],
                )
            )
        return results


def get_feature_data_count(feature_name: str) -> int:
    """
    Get count of feature data records for a specific feature.

    Args:
        feature_name: Feature name

    Returns:
        Number of records.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM feature_data WHERE feature_name = ?",
            (feature_name,),
        )
        return cursor.fetchone()[0]


def delete_feature_data_by_feature(feature_name: str) -> int:
    """
    Delete all feature data records for a specific feature.

    Args:
        feature_name: Feature name

    Returns:
        Number of rows deleted.
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM feature_data WHERE feature_name = ?",
            (feature_name,),
        )
        return cursor.rowcount
