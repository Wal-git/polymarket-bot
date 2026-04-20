"""Tests for strategies.smart_money._load_smart_wallets."""
from __future__ import annotations

import json
import time

import pytest


def _patch_paths(monkeypatch, sm, signal=None, closer=None, fallback=None, tmp_path=None):
    """Redirect all three archetype paths. Pass None to use a non-existent path."""
    mp = tmp_path or (signal.parent if signal else closer.parent if closer else fallback.parent)
    monkeypatch.setattr(sm, "_SMART_WALLETS_SIGNAL_JSON", signal or mp / "missing_signal.json")
    monkeypatch.setattr(sm, "_SMART_WALLETS_CLOSER_JSON", closer or mp / "missing_closer.json")
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", fallback or mp / "missing.json")
    # _ARCHETYPE_PATHS is built at import time; redirect it too.
    monkeypatch.setattr(
        sm,
        "_ARCHETYPE_PATHS",
        [
            signal or mp / "missing_signal.json",
            closer or mp / "missing_closer.json",
            fallback or mp / "missing.json",
        ],
    )


def test_load_smart_wallets_missing_file(tmp_path, monkeypatch):
    import strategies.smart_money as sm
    _patch_paths(monkeypatch, sm, tmp_path=tmp_path)
    assert sm._load_smart_wallets() == {}


def test_load_smart_wallets_valid_file(tmp_path, monkeypatch):
    import strategies.smart_money as sm

    fresh_ts = int(time.time()) - 3600
    jfile = tmp_path / "smart_wallets_signal.json"
    jfile.write_text(json.dumps({
        "generated_at": "2026-04-19T00:00:00Z",
        "lookback_days": 90,
        "wallets": [
            {"proxy_wallet": "0xaaa", "score": 0.9, "signal_score": 0.9, "last_active_ts": fresh_ts},
            {"proxy_wallet": "0xbbb", "score": 0.8, "signal_score": 0.8, "last_active_ts": fresh_ts},
        ],
    }))
    _patch_paths(monkeypatch, sm, signal=jfile, tmp_path=tmp_path)

    wallets = sm._load_smart_wallets()
    assert wallets == {"0xaaa": 0.9, "0xbbb": 0.8}


def test_load_smart_wallets_unions_archetypes(tmp_path, monkeypatch):
    """Wallet present in closer but not signal should be included (max score)."""
    import strategies.smart_money as sm

    fresh_ts = int(time.time()) - 3600
    signal_file = tmp_path / "signal.json"
    signal_file.write_text(json.dumps({
        "wallets": [
            {"proxy_wallet": "0xaaa", "signal_score": 0.9, "last_active_ts": fresh_ts},
        ],
    }))
    closer_file = tmp_path / "closer.json"
    closer_file.write_text(json.dumps({
        "wallets": [
            {"proxy_wallet": "0xaaa", "score": 0.7, "last_active_ts": fresh_ts},  # lower → max kept
            {"proxy_wallet": "0xbbb", "score": 0.6, "last_active_ts": fresh_ts},  # only in closer
        ],
    }))
    _patch_paths(monkeypatch, sm, signal=signal_file, closer=closer_file, tmp_path=tmp_path)

    wallets = sm._load_smart_wallets()
    assert wallets["0xaaa"] == 0.9   # signal_score wins
    assert wallets["0xbbb"] == 0.6   # closer-only still included


def test_load_smart_wallets_drops_stale(tmp_path, monkeypatch):
    import strategies.smart_money as sm

    stale_ts = int(time.time()) - 30 * 86400
    jfile = tmp_path / "smart_wallets_signal.json"
    jfile.write_text(json.dumps({
        "wallets": [
            {"proxy_wallet": "0xold", "score": 0.9, "last_active_ts": stale_ts},
        ],
    }))
    _patch_paths(monkeypatch, sm, signal=jfile, tmp_path=tmp_path)

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
    _patch_paths(monkeypatch, sm, signal=jfile, tmp_path=tmp_path)

    result = sm._load_smart_wallets(min_score=0.5)
    assert "0xhi" in result
    assert "0xlo" not in result
