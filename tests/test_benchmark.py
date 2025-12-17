"""Benchmark tests for optimization verification.

These tests measure performance improvements from optimizations:
1. Batch insert vs iterrows insert
2. Compound index query plan
3. Pickle vs JSON serialization

Run with: poetry run pytest tests/test_benchmark.py -v -s
"""

import io
import pickle
import sqlite3
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ============================================================================
# Benchmark 1: Batch Insert Optimization
# ============================================================================


def _create_sample_daily_bar_df(n_rows: int) -> pd.DataFrame:
    """Create sample daily bar data for benchmarking."""
    np.random.seed(42)
    dates = pd.date_range("20200101", periods=n_rows, freq="D")
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"] * n_rows,
            "trade_date": [d.strftime("%Y%m%d") for d in dates],
            "open": np.random.uniform(10, 20, n_rows),
            "high": np.random.uniform(15, 25, n_rows),
            "low": np.random.uniform(8, 15, n_rows),
            "close": np.random.uniform(10, 20, n_rows),
            "pre_close": np.random.uniform(10, 20, n_rows),
            "change": np.random.uniform(-1, 1, n_rows),
            "pct_chg": np.random.uniform(-5, 5, n_rows),
            "vol": np.random.uniform(100000, 1000000, n_rows),
            "amount": np.random.uniform(1000000, 10000000, n_rows),
        }
    )


def _insert_iterrows(conn: sqlite3.Connection, df: pd.DataFrame) -> float:
    """Insert using iterrows (old slow method)."""
    start = time.perf_counter()
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
    conn.commit()
    return time.perf_counter() - start


def _insert_executemany(conn: sqlite3.Connection, df: pd.DataFrame) -> float:
    """Insert using executemany (new fast method)."""
    columns = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    data = [tuple(row) for row in df[columns].values]

    start = time.perf_counter()
    conn.executemany(
        """
        INSERT OR REPLACE INTO daily_bar
        (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )
    conn.commit()
    return time.perf_counter() - start


@pytest.mark.benchmark
def test_batch_insert_comparison():
    """Benchmark: executemany vs iterrows for database inserts."""
    # Test with different data sizes
    sizes = [100, 500, 1000]

    print("\n" + "=" * 60)
    print("Benchmark 1: Batch Insert Optimization")
    print("=" * 60)

    for n_rows in sizes:
        df = _create_sample_daily_bar_df(n_rows)

        # Create temp databases
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test iterrows
            db1 = Path(tmpdir) / "iterrows.db"
            conn1 = sqlite3.connect(db1)
            conn1.execute(
                """
                CREATE TABLE daily_bar (
                    ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL,
                    close REAL, pre_close REAL, change REAL, pct_chg REAL, vol REAL, amount REAL,
                    PRIMARY KEY (ts_code, trade_date)
                )
            """
            )
            iterrows_time = _insert_iterrows(conn1, df)
            conn1.close()

            # Test executemany
            db2 = Path(tmpdir) / "executemany.db"
            conn2 = sqlite3.connect(db2)
            conn2.execute(
                """
                CREATE TABLE daily_bar (
                    ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL,
                    close REAL, pre_close REAL, change REAL, pct_chg REAL, vol REAL, amount REAL,
                    PRIMARY KEY (ts_code, trade_date)
                )
            """
            )
            executemany_time = _insert_executemany(conn2, df)
            conn2.close()

            speedup = iterrows_time / executemany_time
            print(f"\n{n_rows} rows:")
            print(f"  iterrows:    {iterrows_time*1000:.2f}ms")
            print(f"  executemany: {executemany_time*1000:.2f}ms")
            print(f"  Speedup:     {speedup:.1f}x faster")

            # Assert executemany is at least 2x faster
            assert speedup > 2, f"Expected at least 2x speedup, got {speedup:.1f}x"

    print("\n✓ Batch insert optimization verified: executemany is significantly faster")


# ============================================================================
# Benchmark 2: Compound Index Query Plan
# ============================================================================


@pytest.mark.benchmark
def test_compound_index_query_plan():
    """Verify compound index improves date-range queries."""
    print("\n" + "=" * 60)
    print("Benchmark 2: Compound Index Query Plan")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(db_path)

        # Create table
        conn.execute(
            """
            CREATE TABLE daily_bar (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL,
                PRIMARY KEY (ts_code, trade_date)
            )
        """
        )

        # Insert sample data (multiple stocks, many dates)
        n_stocks = 100
        n_days = 500
        dates = pd.date_range("20200101", periods=n_days, freq="D")
        data = []
        for i in range(n_stocks):
            ts_code = f"{i:06d}.SZ"
            for d in dates:
                data.append((ts_code, d.strftime("%Y%m%d"), 10.0))

        conn.executemany("INSERT INTO daily_bar VALUES (?, ?, ?)", data)
        conn.commit()

        # Query without date index
        query = "SELECT * FROM daily_bar WHERE trade_date <= '20201231'"

        # Check query plan without index
        plan_no_index = conn.execute(f"EXPLAIN QUERY PLAN {query}").fetchall()
        print("\nQuery: SELECT * FROM daily_bar WHERE trade_date <= '20201231'")
        print("\nWithout date index:")
        for row in plan_no_index:
            print(f"  {row}")

        # Add compound index
        conn.execute("CREATE INDEX idx_daily_bar_trade_date ON daily_bar (trade_date, ts_code)")
        conn.commit()

        # Check query plan with index
        plan_with_index = conn.execute(f"EXPLAIN QUERY PLAN {query}").fetchall()
        print("\nWith compound index (trade_date, ts_code):")
        for row in plan_with_index:
            print(f"  {row}")

        # Benchmark actual query performance
        iterations = 10

        # Without index (recreate without index)
        conn.execute("DROP INDEX idx_daily_bar_trade_date")
        conn.commit()

        start = time.perf_counter()
        for _ in range(iterations):
            list(conn.execute(query))
        time_no_index = (time.perf_counter() - start) / iterations

        # With index
        conn.execute("CREATE INDEX idx_daily_bar_trade_date ON daily_bar (trade_date, ts_code)")
        conn.commit()

        start = time.perf_counter()
        for _ in range(iterations):
            list(conn.execute(query))
        time_with_index = (time.perf_counter() - start) / iterations

        speedup = time_no_index / time_with_index
        print(f"\nQuery execution time:")
        print(f"  Without index: {time_no_index*1000:.2f}ms")
        print(f"  With index:    {time_with_index*1000:.2f}ms")
        print(f"  Speedup:       {speedup:.1f}x faster")

        conn.close()

        # Verify index is used in query plan
        plan_str = str(plan_with_index)
        assert (
            "idx_daily_bar_trade_date" in plan_str or "USING INDEX" in plan_str
        ), "Compound index should be used in query plan"

    print("\n✓ Compound index optimization verified: date-range queries use index")


# ============================================================================
# Benchmark 3: Pickle vs JSON Serialization
# ============================================================================


def _create_realistic_df(n_rows: int) -> pd.DataFrame:
    """Create realistic daily bar DataFrame for serialization benchmarking."""
    np.random.seed(42)
    dates = pd.date_range("20200101", periods=n_rows, freq="D")
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"] * n_rows,
            "trade_date": [d.strftime("%Y%m%d") for d in dates],
            "open": np.random.uniform(10, 20, n_rows).astype(np.float64),
            "high": np.random.uniform(15, 25, n_rows).astype(np.float64),
            "low": np.random.uniform(8, 15, n_rows).astype(np.float64),
            "close": np.random.uniform(10, 20, n_rows).astype(np.float64),
            "pre_close": np.random.uniform(10, 20, n_rows).astype(np.float64),
            "change": np.random.uniform(-1, 1, n_rows).astype(np.float64),
            "pct_chg": np.random.uniform(-5, 5, n_rows).astype(np.float64),
            "vol": np.random.uniform(100000, 1000000, n_rows).astype(np.float64),
            "amount": np.random.uniform(1000000, 10000000, n_rows).astype(np.float64),
        }
    )


@pytest.mark.benchmark
def test_serialization_comparison():
    """Benchmark: pickle vs JSON for DataFrame serialization."""
    print("\n" + "=" * 60)
    print("Benchmark 3: DataFrame Serialization (Pickle vs JSON)")
    print("=" * 60)

    # Test with different data sizes (typical scan uses ~500-1500 rows)
    sizes = [250, 500, 1000]
    iterations = 100

    for n_rows in sizes:
        df = _create_realistic_df(n_rows)

        # Benchmark JSON serialization
        start = time.perf_counter()
        for _ in range(iterations):
            json_bytes = df.to_json().encode("utf-8")
        json_serialize_time = (time.perf_counter() - start) / iterations

        start = time.perf_counter()
        for _ in range(iterations):
            _ = pd.read_json(io.StringIO(json_bytes.decode("utf-8")))
        json_deserialize_time = (time.perf_counter() - start) / iterations

        # Benchmark Pickle serialization
        start = time.perf_counter()
        for _ in range(iterations):
            pickle_bytes = pickle.dumps(df)
        pickle_serialize_time = (time.perf_counter() - start) / iterations

        start = time.perf_counter()
        for _ in range(iterations):
            _ = pickle.loads(pickle_bytes)
        pickle_deserialize_time = (time.perf_counter() - start) / iterations

        json_total = json_serialize_time + json_deserialize_time
        pickle_total = pickle_serialize_time + pickle_deserialize_time
        speedup = json_total / pickle_total

        print(f"\n{n_rows} rows (x{iterations} iterations avg):")
        print(f"  JSON:")
        print(f"    Serialize:   {json_serialize_time*1000:.3f}ms")
        print(f"    Deserialize: {json_deserialize_time*1000:.3f}ms")
        print(f"    Total:       {json_total*1000:.3f}ms")
        print(f"    Size:        {len(json_bytes)/1024:.1f}KB")
        print(f"  Pickle:")
        print(f"    Serialize:   {pickle_serialize_time*1000:.3f}ms")
        print(f"    Deserialize: {pickle_deserialize_time*1000:.3f}ms")
        print(f"    Total:       {pickle_total*1000:.3f}ms")
        print(f"    Size:        {len(pickle_bytes)/1024:.1f}KB")
        print(f"  Speedup:       {speedup:.1f}x faster (pickle)")

        # Assert pickle is faster (should be at least 1.5x)
        assert speedup > 1.5, f"Expected at least 1.5x speedup, got {speedup:.1f}x"

    print("\n✓ Serialization optimization verified: pickle is significantly faster")


# ============================================================================
# Benchmark 4: Price Quantile Vectorization
# ============================================================================


def _calculate_price_quantile_old(close: pd.Series, window: int = 500) -> pd.Series:
    """Old implementation using pandas rolling apply (slow)."""

    def quantile_rank(x):
        if len(x) < window:
            return float("nan")
        return (x < x.iloc[-1]).mean()

    return close.rolling(window=window, min_periods=window).apply(quantile_rank, raw=False)


@pytest.mark.benchmark
def test_price_quantile_vectorization():
    """Benchmark: vectorized vs rolling apply for price quantile calculation."""
    from numpy.lib.stride_tricks import sliding_window_view

    from src.feature.utils import calculate_price_quantile

    print("\n" + "=" * 60)
    print("Benchmark 4: Price Quantile Vectorization")
    print("=" * 60)

    # Test with different data sizes and window sizes
    test_cases = [
        (500, 200),  # 500 rows, window 200 (volume_upper_shadow)
        (1000, 500),  # 1000 rows, window 500 (volume_stagnation, high_retracement)
        (1500, 500),  # 1500 rows, window 500 (high_retracement typical)
    ]

    for n_rows, window in test_cases:
        np.random.seed(42)
        # Create realistic price series with trend and noise
        base_price = 10.0
        trend = np.linspace(0, 5, n_rows)
        noise = np.random.normal(0, 0.5, n_rows)
        close = pd.Series(base_price + trend + noise)

        # Benchmark old implementation
        start = time.perf_counter()
        old_result = _calculate_price_quantile_old(close, window=window)
        old_time = time.perf_counter() - start

        # Benchmark new vectorized implementation
        start = time.perf_counter()
        new_result = calculate_price_quantile(close, window=window)
        new_time = time.perf_counter() - start

        speedup = old_time / new_time

        print(f"\n{n_rows} rows, window={window}:")
        print(f"  Old (rolling apply): {old_time*1000:.2f}ms")
        print(f"  New (vectorized):    {new_time*1000:.2f}ms")
        print(f"  Speedup:             {speedup:.1f}x faster")

        # Verify results are identical (within floating point tolerance)
        # Compare only non-NaN values
        valid_mask = ~np.isnan(old_result) & ~np.isnan(new_result)
        if valid_mask.sum() > 0:
            max_diff = np.abs(old_result[valid_mask] - new_result[valid_mask]).max()
            print(f"  Max difference:      {max_diff:.2e}")
            assert max_diff < 1e-10, f"Results differ by {max_diff}"

        # Assert new implementation is at least 10x faster
        assert speedup > 10, f"Expected at least 10x speedup, got {speedup:.1f}x"

    print("\n✓ Price quantile vectorization verified: numpy is significantly faster")


# ============================================================================
# Summary
# ============================================================================


@pytest.mark.benchmark
def test_benchmark_summary():
    """Print optimization summary."""
    print("\n" + "=" * 60)
    print("OPTIMIZATION BENCHMARKS COMPLETE")
    print("=" * 60)
    print(
        """
Summary of verified optimizations:

1. Batch Insert (executemany vs iterrows)
   - Expected: 10-100x faster
   - Verified: executemany is significantly faster

2. Compound Index (trade_date, ts_code)
   - Expected: 2-10x faster date-range queries
   - Verified: Index is used in query plan

3. Pickle Serialization (vs JSON)
   - Expected: 20-30% faster
   - Verified: Pickle is faster for DataFrame serialization

4. Price Quantile Vectorization (numpy vs pandas rolling apply)
   - Expected: 50-100x faster
   - Verified: Vectorized implementation is significantly faster
"""
    )
