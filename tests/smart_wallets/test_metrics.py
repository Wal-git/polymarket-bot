"""Golden-file tests for metrics.py using a fixed fixture."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from polybot.smart_wallets.metrics import (
    avg_position_usd,
    compute_all_metrics,
    max_drawdown,
    realized_pnl,
    resolved_markets_count,
    score_wallets,
    trades_count,
    unrealized_pnl,
    volume,
    win_rate,
)

FIXTURE = json.loads((Path(__file__).parent / "fixture_activity.json").read_text())


def test_realized_pnl():
    # redeems: 4500 + 1200 = 5700; redeemable position cost: 3000 → net 2700
    assert abs(realized_pnl(FIXTURE) - 2700.0) < 1e-6


def test_unrealized_pnl():
    # cashPnl of non-redeemable position: -200
    assert abs(unrealized_pnl(FIXTURE) - (-200.0 + 1500.0)) < 1e-6


def test_win_rate():
    # mkt-1: redeem 4500 - cost 3000 = +1500 (win)
    # mkt-2: redeem 1200 - cost 0 = +1200 (win, not redeemable so no cost subtracted)
    # 2 markets, 2 wins → 1.0
    assert win_rate(FIXTURE) == pytest.approx(1.0, abs=1e-6)


def test_resolved_markets_count():
    assert resolved_markets_count(FIXTURE) == 2


def test_volume():
    # 1000 + 2000 + 3000 + 500 + 800 = 7300
    assert abs(volume(FIXTURE) - 7300.0) < 1e-6


def test_trades_count():
    assert trades_count(FIXTURE) == 5


def test_avg_position_usd():
    assert abs(avg_position_usd(FIXTURE) - 7300.0 / 5) < 1e-6


def test_max_drawdown():
    # Sorted by ts: BUY 1000, BUY 2000, SELL 3000, BUY 500, SELL 800
    # cum: -1000, -3000, 0, -500, 300
    # peak at 0 (after SELL 3000), drawdown from 0→-3000 = 3000
    assert max_drawdown(FIXTURE) == pytest.approx(3000.0, abs=1e-6)


def test_compute_all_metrics_keys():
    m = compute_all_metrics(FIXTURE)
    expected_keys = {
        "pnl_realized", "pnl_unrealized", "win_rate", "resolved_markets",
        "volume", "trades_count", "avg_position_usd", "max_drawdown",
        "last_active_ts", "recency",
    }
    assert expected_keys <= m.keys()


def test_score_wallets_zeroes_insufficient_trades():
    wallet = {**FIXTURE, **compute_all_metrics(FIXTURE)}
    wallet["trades_count"] = 5  # below MIN_TRADES_COUNT=50
    result = score_wallets([wallet])
    assert result[0]["score"] == 0.0


def test_score_wallets_valid_range():
    # Build two wallets that both pass the minimum bar
    base = {**FIXTURE, **compute_all_metrics(FIXTURE)}
    w1 = {**base, "trades_count": 60, "resolved_markets": 35}
    w2 = {**base, "trades_count": 80, "resolved_markets": 50, "pnl_realized": base["pnl_realized"] * 2}
    scored = score_wallets([w1, w2])
    for s in scored:
        assert 0.0 <= s["score"] <= 1.0


def test_score_wallets_preserves_all_fields():
    base = {**FIXTURE, **compute_all_metrics(FIXTURE)}
    base["trades_count"] = 60
    base["resolved_markets"] = 35
    result = score_wallets([base])
    assert "proxy_wallet" in result[0]
    assert "score" in result[0]


# ---------------------------------------------------------------------------
# json fallback loader
# ---------------------------------------------------------------------------

def test_load_smart_wallets_missing_file(tmp_path, monkeypatch):
    import polybot.smart_wallets.metrics  # noqa: F401 — just ensure importable
    # Patch the path to a non-existent file
    import strategies.smart_money as sm
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", tmp_path / "nonexistent.json")
    assert sm._load_smart_wallets() == []


def test_load_smart_wallets_valid_file(tmp_path, monkeypatch):
    import strategies.smart_money as sm

    jfile = tmp_path / "smart_wallets.json"
    jfile.write_text(json.dumps({
        "generated_at": "2026-04-19T00:00:00Z",
        "lookback_days": 60,
        "wallets": [
            {"proxy_wallet": "0xaaa", "score": 0.9},
            {"proxy_wallet": "0xbbb", "score": 0.8},
        ],
    }))
    monkeypatch.setattr(sm, "_SMART_WALLETS_JSON", jfile)
    wallets = sm._load_smart_wallets()
    assert wallets == ["0xaaa", "0xbbb"]
