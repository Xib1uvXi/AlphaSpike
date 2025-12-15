"""Tests for the database module."""

import os
import tempfile

import pytest

from src.datahub.db import (
    DAILY_BAR_TABLE,
    drop_daily_bar_table,
    get_connection,
    get_db_path,
    init_db,
)


@pytest.fixture
def temp_db(monkeypatch, tmp_path):
    """Create a temporary database for testing."""
    temp_path = tmp_path / "test.db"

    monkeypatch.setenv("SQLITE_PATH", str(temp_path))

    yield str(temp_path)

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


class TestGetDbPath:
    """Tests for get_db_path function."""

    def test_returns_path_when_configured(self, temp_db):
        """Should return path when SQLITE_PATH is configured."""
        path = get_db_path()
        assert str(path) == temp_db

    def test_raises_error_when_not_configured(self, monkeypatch):
        """Should raise ValueError when SQLITE_PATH is not configured."""
        monkeypatch.delenv("SQLITE_PATH", raising=False)
        monkeypatch.setenv("SQLITE_PATH", "")

        with pytest.raises(ValueError, match="SQLITE_PATH is not configured"):
            get_db_path()


class TestGetConnection:
    """Tests for get_connection context manager."""

    def test_creates_connection(self, temp_db):
        """Should create a working database connection."""
        with get_connection() as conn:
            cursor = conn.execute("SELECT 1")
            assert cursor.fetchone() == (1,)

    def test_creates_db_file(self, temp_db):
        """Should create the database file if it doesn't exist."""
        # temp_db doesn't exist yet (using tmp_path)
        assert not os.path.exists(temp_db)

        with get_connection() as conn:
            conn.execute("SELECT 1")

        assert os.path.exists(temp_db)

    def test_commits_on_success(self, temp_db):
        """Should commit changes on successful exit."""
        with get_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")

        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM test")
            assert cursor.fetchone() == (1,)

    def test_rollback_on_error(self, temp_db):
        """Should rollback changes on error."""
        with get_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")

        with pytest.raises(Exception):
            with get_connection() as conn:
                conn.execute("INSERT INTO test VALUES (1)")
                raise Exception("Test error")

        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM test")
            assert cursor.fetchone() is None


class TestInitDb:
    """Tests for init_db function."""

    def test_creates_daily_bar_table(self, temp_db):
        """Should create daily_bar table."""
        init_db()

        with get_connection() as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (DAILY_BAR_TABLE,))
            assert cursor.fetchone() is not None

    def test_creates_index(self, temp_db):
        """Should create index on ts_code."""
        init_db()

        with get_connection() as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_daily_bar_ts_code'")
            assert cursor.fetchone() is not None

    def test_idempotent(self, temp_db):
        """Should be safe to call multiple times."""
        init_db()
        init_db()  # Should not raise

        with get_connection() as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (DAILY_BAR_TABLE,))
            assert cursor.fetchone() is not None


class TestDropDailyBarTable:
    """Tests for drop_daily_bar_table function."""

    def test_drops_existing_table(self, temp_db):
        """Should drop the table when it exists."""
        init_db()
        result = drop_daily_bar_table()

        assert result is True

        with get_connection() as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (DAILY_BAR_TABLE,))
            assert cursor.fetchone() is None

    def test_returns_false_when_no_table(self, temp_db):
        """Should return False when table doesn't exist."""
        result = drop_daily_bar_table()
        assert result is False
