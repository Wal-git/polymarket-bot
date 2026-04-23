"""Happy-path tests for the monthly leaderboard flow."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from polybot.client.goldsky import OrderFilledEvent
from polybot.smart_wallets import monthly


def _make_event(
    taker: str,
    maker: str,
    taker_asset_id: str,
    taker_amount: int,
    maker_amount: int,
    tx_hash: str,
    ts: int = 1_700_000_000,
) -> OrderFilledEvent:
    return OrderFilledEvent(
        timestamp=ts,
        maker=maker,
        maker_asset_id="0" if taker_asset_id != "0" else "token1",
        maker_amount_filled=Decimal(str(maker_amount)),
        taker=taker,
        taker_asset_id=taker_asset_id,
        taker_amount_filled=Decimal(str(taker_amount)),
        transaction_hash=tx_hash,
    )


ELITE = "0xelite000000000000000000000000000000000000"
MID = "0xmid0000000000000000000000000000000000000"


@pytest.fixture()
def mock_client():
    client = MagicMock()
    client.leaderboard.return_value = [
        {"proxyWallet": ELITE, "name": "elite", "pnl": 50_000.0, "volume": 200_000.0},
        {"proxyWallet": MID, "name": "mid", "pnl": 1_000.0, "volume": 10_000.0},
    ]
    client.positions.side_effect = lambda user: (
        [{"cashPnl": 100.0, "redeemable": False}] if user == ELITE else []
    )
    return client


@pytest.fixture()
def mock_gold():
    gold = MagicMock()
    usdc_unit = 10 ** 6

    # taker_in events: elite as taker buying (cash out), then selling (cash in)
    taker_events = [
        # elite BUYs: pays $10 000 USDC
        _make_event(ELITE, "0xmm1", "0", 10_000 * usdc_unit, 10_000_000, "tx1"),
        # elite SELLs: receives $30 000 USDC
        _make_event(ELITE, "0xmm2", "token1", 30_000_000, 30_000 * usdc_unit, "tx2"),
    ]
    # maker_in events: elite as maker receiving on a buy (cash in $100)
    maker_events = [
        _make_event("0xother", ELITE, "0", 100 * usdc_unit, 100_000, "tx3"),
    ]

    def parallel_side_effect(*args, extra_where="", **kwargs):
        if "taker_in" in extra_where:
            return taker_events
        return maker_events

    gold.fetch_events_parallel.side_effect = parallel_side_effect
    return gold


def test_run_ranks_elite_first(mock_client, mock_gold, tmp_path, monkeypatch):
    monkeypatch.setattr(monthly, "SMART_WALLETS_MONTHLY_JSON", tmp_path / "monthly.json")
    monkeypatch.setattr(monthly, "CACHE_DIR", tmp_path / ".cache")

    with (
        patch("polybot.smart_wallets.monthly.DataAPIClient", return_value=mock_client),
        patch("polybot.smart_wallets.monthly.GoldskyClient", return_value=mock_gold),
    ):
        result = monthly.run(top_n=2, lookback_days=30, dry_run=False)

    wallets = result["wallets"]
    assert result["n"] == 2

    elite_row = next(w for w in wallets if w["proxy_wallet"] == ELITE)
    mid_row = next(w for w in wallets if w["proxy_wallet"] == MID)

    # elite: -10_000 (buy) + 30_000 (sell) + 100 (maker in) = +20_100 realized
    #        + 100 unrealized from positions = 20_200 verified
    assert elite_row["realized_pnl_30d"] == pytest.approx(20_100.0, rel=1e-4)
    assert elite_row["unrealized_pnl"] == pytest.approx(100.0)
    assert elite_row["verified_pnl_30d"] == pytest.approx(20_200.0, rel=1e-4)

    # pnl_divergence = reported - verified
    assert elite_row["pnl_divergence"] == pytest.approx(50_000.0 - 20_200.0, rel=1e-4)

    # elite ranks first
    assert wallets[0]["proxy_wallet"] == ELITE

    # mid has no events → zero verified
    assert mid_row["verified_pnl_30d"] == pytest.approx(0.0)

    # output file written
    out = tmp_path / "monthly.json"
    assert out.exists()
    import json
    data = json.loads(out.read_text())
    assert data["lookback_days"] == 30
    assert len(data["wallets"]) == 2
