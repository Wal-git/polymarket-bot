"""Tests for strategies.smart_money._load_smart_wallets."""
from __future__ import annotations

import json
import time

import pytest


def test_load_smart_wallets_missing_file(tmp_path, monkeypatch):
    import strategies.smart_money as sm
    monkeypatch.setattr(sm, "_SMART_WALLETS_SIGNAL_JSON", tmp_path / "missing_signal.json")
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", tmp_path / "missing.json")
    assert sm._load_smart_wallets() == {}


def test_load_smart_wallets_valid_file(tmp_path, monkeypatch):
    import strategies.smart_money as sm

    fresh_ts = int(time.time()) - 3600  # 1h ago → fresh
    jfile = tmp_path / "smart_wallets_signal.json"
    jfile.write_text(json.dumps({
        "generated_at": "2026-04-19T00:00:00Z",
        "lookback_days": 90,
        "wallets": [
            {"proxy_wallet": "0xaaa", "score": 0.9, "signal_score": 0.9, "last_active_ts": fresh_ts},
            {"proxy_wallet": "0xbbb", "score": 0.8, "signal_score": 0.8, "last_active_ts": fresh_ts},
        ],
    }))
    monkeypatch.setattr(sm, "_SMART_WALLETS_SIGNAL_JSON", jfile)
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", tmp_path / "unused.json")

    wallets = sm._load_smart_wallets()
    assert wallets == {"0xaaa": 0.9, "0xbbb": 0.8}


def test_load_smart_wallets_drops_stale(tmp_path, monkeypatch):
    import strategies.smart_money as sm

    stale_ts = int(time.time()) - 30 * 86400
    jfile = tmp_path / "smart_wallets_signal.json"
    jfile.write_text(json.dumps({
        "wallets": [
            {"proxy_wallet": "0xold", "score": 0.9, "last_active_ts": stale_ts},
        ],
    }))
    monkeypatch.setattr(sm, "_SMART_WALLETS_SIGNAL_JSON", jfile)
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", tmp_path / "unused.json")

    assert sm._load_smart_wallets(stale_days=3) == {}


def test_load_smart_wallets_min_score_filter(tmp_path, monkeypatch):
    import strategies.smart_money as sm

    fresh_ts = int(time.time()) - 3600
    jfile = tmp_path / "smart_wallets_signal.json"
    jfile.write_text(json.dumps({
        "wallets": [
            {"proxy_wallet": "0xhi", "score": 0.9, "last_active_ts": fresh_ts},
            {"proxy_wallet": "0xlo", "score": 0.1, "last_active_ts": fresh_ts},
        ],
    }))
    monkeypatch.setattr(sm, "_SMART_WALLETS_SIGNAL_JSON", jfile)
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", tmp_path / "unused.json")

    result = sm._load_smart_wallets(min_score=0.5)
    assert "0xhi" in result
    assert "0xlo" not in result
