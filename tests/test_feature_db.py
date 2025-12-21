"""Tests for the feature database module."""

import pytest

from src.alphaspike.db import (
    FEATURE_RESULT_TABLE,
    delete_feature_result,
    get_feature_result,
    init_feature_db,
    save_feature_result,
)
from src.datahub.db import get_connection


@pytest.fixture
def temp_db(monkeypatch, tmp_path):
    """Create a temporary database for testing."""
    temp_path = tmp_path / "test.db"
    monkeypatch.setenv("SQLITE_PATH", str(temp_path))
    yield str(temp_path)
    if temp_path.exists():
        temp_path.unlink()


class TestInitFeatureDb:
    """Tests for init_feature_db function."""

    def test_creates_feature_result_table(self, temp_db):
        """Should create feature_result table."""
        init_feature_db()

        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (FEATURE_RESULT_TABLE,),
            )
            assert cursor.fetchone() is not None

    def test_idempotent(self, temp_db):
        """Should be safe to call multiple times."""
        init_feature_db()
        init_feature_db()  # Should not raise

        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (FEATURE_RESULT_TABLE,),
            )
            assert cursor.fetchone() is not None


class TestSaveAndGetFeatureResult:
    """Tests for save/get feature result functions."""

    def test_save_and_retrieve(self, temp_db):
        """Should save and retrieve feature results."""
        init_feature_db()
        ts_codes = ["000001.SZ", "600000.SH"]

        save_feature_result("bbc", "20251220", ts_codes)
        result = get_feature_result("bbc", "20251220")

        assert result == ts_codes

    def test_returns_none_when_not_found(self, temp_db):
        """Should return None when result doesn't exist."""
        init_feature_db()
        result = get_feature_result("bbc", "20251220")
        assert result is None

    def test_upsert_behavior(self, temp_db):
        """Should update existing result on second save."""
        init_feature_db()

        save_feature_result("bbc", "20251220", ["000001.SZ"])
        save_feature_result("bbc", "20251220", ["600000.SH"])

        result = get_feature_result("bbc", "20251220")
        assert result == ["600000.SH"]

    def test_empty_list(self, temp_db):
        """Should handle empty result list."""
        init_feature_db()

        save_feature_result("bbc", "20251220", [])
        result = get_feature_result("bbc", "20251220")

        assert result == []

    def test_different_features_same_date(self, temp_db):
        """Should store different features for same date separately."""
        init_feature_db()

        save_feature_result("bbc", "20251220", ["000001.SZ"])
        save_feature_result("bullish_cannon", "20251220", ["600000.SH"])

        assert get_feature_result("bbc", "20251220") == ["000001.SZ"]
        assert get_feature_result("bullish_cannon", "20251220") == ["600000.SH"]

    def test_same_feature_different_dates(self, temp_db):
        """Should store same feature for different dates separately."""
        init_feature_db()

        save_feature_result("bbc", "20251220", ["000001.SZ"])
        save_feature_result("bbc", "20251221", ["600000.SH"])

        assert get_feature_result("bbc", "20251220") == ["000001.SZ"]
        assert get_feature_result("bbc", "20251221") == ["600000.SH"]


class TestDeleteFeatureResult:
    """Tests for delete_feature_result function."""

    def test_delete_existing(self, temp_db):
        """Should delete and return True."""
        init_feature_db()
        save_feature_result("bbc", "20251220", ["000001.SZ"])

        deleted = delete_feature_result("bbc", "20251220")

        assert deleted is True
        assert get_feature_result("bbc", "20251220") is None

    def test_delete_nonexistent(self, temp_db):
        """Should return False when not found."""
        init_feature_db()
        deleted = delete_feature_result("bbc", "20251220")
        assert deleted is False
