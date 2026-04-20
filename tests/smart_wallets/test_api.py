"""Tests for the DataAPIClient: caching, rate-limit thread safety."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from polybot.smart_wallets.api import DataAPIClient, weekly_run_epoch


def test_weekly_run_epoch_is_monday_00_utc():
    # Pick a known Wednesday (2026-04-15 12:34:56 UTC) and check it floors to
    # 2026-04-13 00:00:00 UTC.
    wed_noon = 1776342896  # Wed Apr 15 2026 12:34:56 UTC
    mon = weekly_run_epoch(wed_noon)
    tm = time.gmtime(mon)
    assert tm.tm_wday == 0
    assert (tm.tm_hour, tm.tm_min, tm.tm_sec) == (0, 0, 0)


def test_weekly_run_epoch_idempotent():
    ts = int(time.time())
    assert weekly_run_epoch(weekly_run_epoch(ts)) == weekly_run_epoch(ts)


def test_activity_cache_key_stable_within_week(tmp_path: Path, monkeypatch):
    client = DataAPIClient(cache_dir=tmp_path)
    calls: list[dict] = []

    def fake_get(path: str, params: dict | None = None):
        calls.append(params or {})
        return []  # empty page → pagination ends after one call

    monkeypatch.setattr(client, "_get", fake_get)

    # Two calls with different end_ts but within the same week should share
    # the cache file.
    week_mon = weekly_run_epoch(int(time.time()))
    end_a = week_mon + 86400 * 1  # Tuesday
    end_b = week_mon + 86400 * 3  # Thursday
    client.activity(user="0xabc", start=0, end=end_a)
    cache_files_a = set(p.name for p in tmp_path.glob("*.json"))
    client.activity(user="0xabc", start=0, end=end_b)
    cache_files_b = set(p.name for p in tmp_path.glob("*.json"))
    assert cache_files_a == cache_files_b
    # The HTTP path was hit only once for the paginated first call, and the
    # second invocation hit the cache — so calls should be len 1 (first empty page).
    assert len(calls) == 1


def test_rate_limit_serializes_across_threads(monkeypatch):
    client = DataAPIClient(cache_dir=Path("."))

    stamps: list[float] = []
    lock = threading.Lock()

    def fake_get(path, params=None):
        with lock:
            stamps.append(time.monotonic())
        return []

    monkeypatch.setattr(client, "_get", fake_get)
    monkeypatch.setattr(
        "polybot.smart_wallets.api._MIN_INTERVAL", 0.05
    )

    def worker():
        client._rate_limit()
        fake_get("/test")

    threads = [threading.Thread(target=worker) for _ in range(6)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0
    # 6 calls × 0.05s = 0.25s min if serialized.
    assert elapsed >= 0.20
