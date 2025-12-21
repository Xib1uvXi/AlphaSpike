"""Tests for CLI feature filtering."""

from __future__ import annotations

import sys
from typing import Callable
from unittest.mock import Mock

import pytest

from src.alphaspike import cli
from src.feature.registry import FeatureConfig


class DummyProgress:
    """Minimal Progress replacement to avoid rich rendering in tests."""

    def __init__(self, *args, **kwargs):
        self._next_id = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def add_task(self, *args, **kwargs):
        task_id = self._next_id
        self._next_id += 1
        return task_id

    def update(self, *args, **kwargs):
        return None


def _make_features(names: list[str]) -> dict[str, FeatureConfig]:
    def _noop(_: object) -> bool:
        return False

    return {name: FeatureConfig(name, _noop, 1) for name in names}


def _setup_cli(monkeypatch: pytest.MonkeyPatch, feature_names: list[str]):
    feature_map = _make_features(feature_names)
    features = list(feature_map.values())

    scan_calls: list[str] = []

    def fake_scan_feature(*, feature: FeatureConfig, **kwargs):
        scan_calls.append(feature.name)
        return cli.ScanResult(
            feature_name=feature.name,
            signals=[],
            from_cache=True,
            scanned=0,
            skipped=0,
            errors=0,
        )

    mock_console = Mock()
    mock_console.print = Mock()

    monkeypatch.setattr(cli, "FEATURES", features)
    monkeypatch.setattr(cli, "get_all_feature_names", lambda: list(feature_map.keys()))
    monkeypatch.setattr(cli, "get_feature_by_name", lambda name: feature_map.get(name))
    monkeypatch.setattr(cli, "scan_feature", fake_scan_feature)
    monkeypatch.setattr(cli, "get_ts_codes", lambda: [])
    monkeypatch.setattr(cli, "batch_load_daily_bars", lambda ts_codes, end_date: {})
    monkeypatch.setattr(cli, "get_redis_client", lambda: None)
    monkeypatch.setattr(cli, "Console", lambda: mock_console)
    monkeypatch.setattr(cli, "create_progress_bar", lambda console: DummyProgress())

    return scan_calls, mock_console


def _run_cli(monkeypatch: pytest.MonkeyPatch, args: list[str], feature_names: list[str]):
    scan_calls, mock_console = _setup_cli(monkeypatch, feature_names)
    monkeypatch.setattr(sys, "argv", ["prog", *args])
    result = cli.main()
    return result, scan_calls, mock_console


def _printed_messages(mock_console: Mock) -> list[str]:
    messages = []
    for call in mock_console.print.call_args_list:
        if call.args:
            messages.append(str(call.args[0]))
    return messages


def test_parse_valid_single_feature(monkeypatch: pytest.MonkeyPatch):
    result, scan_calls, _ = _run_cli(
        monkeypatch,
        ["--end-date", "20240101", "--feature", "alpha"],
        ["alpha", "beta"],
    )

    assert result == 0
    assert scan_calls == ["alpha"]


def test_parse_valid_multiple_features(monkeypatch: pytest.MonkeyPatch):
    result, scan_calls, _ = _run_cli(
        monkeypatch,
        ["--end-date", "20240101", "--feature", "alpha,beta"],
        ["alpha", "beta"],
    )

    assert result == 0
    assert scan_calls == ["alpha", "beta"]


def test_parse_invalid_feature_warns(monkeypatch: pytest.MonkeyPatch):
    result, scan_calls, mock_console = _run_cli(
        monkeypatch,
        ["--end-date", "20240101", "--feature", "unknown,alpha"],
        ["alpha"],
    )

    assert result == 0
    assert scan_calls == ["alpha"]

    messages = _printed_messages(mock_console)
    assert any("Unknown feature 'unknown'" in msg for msg in messages)


def test_parse_mixed_valid_invalid(monkeypatch: pytest.MonkeyPatch):
    result, scan_calls, mock_console = _run_cli(
        monkeypatch,
        ["--end-date", "20240101", "--feature", "alpha,unknown,beta"],
        ["alpha", "beta"],
    )

    assert result == 0
    assert scan_calls == ["alpha", "beta"]

    messages = _printed_messages(mock_console)
    assert any("Unknown feature 'unknown'" in msg for msg in messages)


def test_parse_all_invalid_exits(monkeypatch: pytest.MonkeyPatch):
    scan_calls, mock_console = _setup_cli(monkeypatch, ["alpha"])
    monkeypatch.setattr(sys, "argv", ["prog", "--end-date", "20240101", "--feature", "unknown"])

    exit_calls: list[int] = []

    def fake_exit(code: int = 0):
        exit_calls.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(cli.sys, "exit", fake_exit)

    with pytest.raises(SystemExit) as exc:
        cli.sys.exit(cli.main())

    assert exc.value.code == 1
    assert exit_calls == [1]
    assert scan_calls == []

    messages = _printed_messages(mock_console)
    assert any("No valid features provided" in msg for msg in messages)


def test_no_feature_flag_uses_all(monkeypatch: pytest.MonkeyPatch):
    result, scan_calls, _ = _run_cli(
        monkeypatch,
        ["--end-date", "20240101"],
        ["alpha", "beta", "gamma"],
    )

    assert result == 0
    assert scan_calls == ["alpha", "beta", "gamma"]
