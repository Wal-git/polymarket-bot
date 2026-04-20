"""End-to-end pipeline test with a mocked DataAPIClient and Store."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from polybot.smart_wallets import pipeline


def _fake_activity_for(wallet: str, start: int, end: int, activity_type: str = "TRADE") -> list[dict]:
    if wallet == "0xelite":
        if activity_type == "TRADE":
            return [
                {
                    "conditionId": f"mkt-{i}",
                    "asset": f"tok-{i}-YES",
                    "usdcSize": 500.0,
                    "size": 1000.0,
                    "price": 0.5,
                    "side": "BUY",
                    "timestamp": end - 100 - i * 50,
                }
                for i in range(60)
            ]
        if activity_type == "REDEEM":
            return [
                {
                    "conditionId": f"mkt-{i}",
                    "asset": f"tok-{i}-YES",
                    "usdcSize": 1000.0,
                    "timestamp": end - 50,
                }
                for i in range(40)
            ]
    if wallet == "0xmid":
        if activity_type == "TRADE":
            return [
                {
                    "conditionId": f"mid-{i}",
                    "asset": f"mid-{i}-YES",
                    "usdcSize": 200.0,
                    "size": 400.0,
                    "price": 0.5,
                    "side": "BUY",
                    "timestamp": end - 200 - i * 50,
                }
                for i in range(5)
            ]
    return []


def _build_fake_client() -> MagicMock:
    client = MagicMock()
    client.activity.side_effect = _fake_activity_for
    client.positions.return_value = []
    client.value.return_value = {"value": 0.0}
    client.leaderboard.return_value = []
    client.close.return_value = None
    return client


@pytest.fixture
def relaxed_thresholds(monkeypatch):
    """Lower thresholds so the small fixtures pass."""
    mods = [
        "polybot.smart_wallets.pipeline",
        "polybot.smart_wallets.config",
    ]
    for mod in mods:
        monkeypatch.setattr(f"{mod}.MIN_TRADES_COUNT", 10, raising=False)
        monkeypatch.setattr(f"{mod}.MIN_RESOLVED_MARKETS", 5, raising=False)
        monkeypatch.setattr(f"{mod}.MIN_VOLUME_USD", 100.0, raising=False)
        monkeypatch.setattr(f"{mod}.MIN_REALIZED_PNL_USD", 100.0, raising=False)
        monkeypatch.setattr(f"{mod}.MIN_WIN_RATE", 0.5, raising=False)
        monkeypatch.setattr(f"{mod}.PCT_CUTOFF_SHARPE", None, raising=False)
        monkeypatch.setattr(f"{mod}.PCT_CUTOFF_EDGE", None, raising=False)
        monkeypatch.setattr(f"{mod}.PCT_CUTOFF_VOLUME", None, raising=False)
        monkeypatch.setattr(f"{mod}.MIN_SIGNIFICANCE_Z", -5.0, raising=False)
    # metrics module constants (captured at import time)
    monkeypatch.setattr("polybot.smart_wallets.metrics.MIN_TRADES_COUNT", 10, raising=False)
    monkeypatch.setattr("polybot.smart_wallets.metrics.MIN_RESOLVED_MARKETS", 5, raising=False)


@pytest.fixture
def redirect_paths(tmp_path: Path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "sw.db"
    json_path = data_dir / "sw.json"
    closer_json = data_dir / "sw_closer.json"
    signal_json = data_dir / "sw_signal.json"
    # store.Store uses these module-level defaults at call time.
    monkeypatch.setattr("polybot.smart_wallets.store.SMART_WALLETS_DB", db_path)
    monkeypatch.setattr("polybot.smart_wallets.store.SMART_WALLETS_JSON", json_path)
    monkeypatch.setattr("polybot.smart_wallets.store.SMART_WALLETS_CLOSER_JSON", closer_json)
    monkeypatch.setattr("polybot.smart_wallets.store.SMART_WALLETS_SIGNAL_JSON", signal_json)
    return data_dir, closer_json, signal_json


def test_pipeline_happy_path(relaxed_thresholds, redirect_paths, monkeypatch):
    data_dir, closer_json, signal_json = redirect_paths

    fake_client = _build_fake_client()
    monkeypatch.setattr(
        "polybot.smart_wallets.pipeline.DataAPIClient", lambda *a, **kw: fake_client
    )
    monkeypatch.setattr(
        "polybot.smart_wallets.seed.fetch_candidates",
        lambda client, **kw: [
            {
                "proxy_wallet": "0xelite",
                "username": "elite_trader",
                "leaderboard_pnl": 50_000.0,
                "leaderboard_volume": 500_000.0,
                "sources": ["lb:7d:pnl"],
            },
            {
                "proxy_wallet": "0xmid",
                "username": "mid_trader",
                "leaderboard_pnl": 500.0,
                "leaderboard_volume": 1000.0,
                "sources": ["lb:7d:pnl"],
            },
        ],
    )
    monkeypatch.setattr(
        "polybot.smart_wallets.pipeline.fetch_candidates",
        lambda client, **kw: [
            {
                "proxy_wallet": "0xelite",
                "username": "elite_trader",
                "leaderboard_pnl": 50_000.0,
                "leaderboard_volume": 500_000.0,
                "sources": ["lb:7d:pnl"],
            },
            {
                "proxy_wallet": "0xmid",
                "username": "mid_trader",
                "leaderboard_pnl": 500.0,
                "leaderboard_volume": 1000.0,
                "sources": ["lb:7d:pnl"],
            },
        ],
    )

    result = pipeline.run(top_k=10, dry_run=False)

    assert result["n_candidates"] == 2
    wallets = result["wallets"]
    assert wallets, "expected elite wallet to pass filters"
    assert wallets[0]["proxy_wallet"] == "0xelite"
    # 40 winning markets × ($1000 redeem − $500 buy) = $20,000
    assert wallets[0]["pnl_realized"] == pytest.approx(20_000.0, rel=1e-3)

    assert closer_json.exists()
    assert signal_json.exists()
    data = json.loads(closer_json.read_text())
    assert data["wallets"][0]["proxy_wallet"] == "0xelite"
    assert "signal_score" in data["wallets"][0]
