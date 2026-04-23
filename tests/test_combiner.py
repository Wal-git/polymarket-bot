import time
from unittest.mock import MagicMock

import pytest

from polybot.models.btc_market import (
    BtcPrices,
    Direction,
    ImbalanceReading,
    OrderBookSnapshot,
    OrderLevel,
    SlotInfo,
    TradeSignal,
)
from polybot.signals.combiner import should_trade


def _slot(price_to_beat: float = 95_000.0) -> SlotInfo:
    now = int(time.time() * 1000)
    return SlotInfo(
        slug="btc-updown-5m-test",
        start_ms=now - 60_000,
        end_ms=now + 240_000,
        price_to_beat=price_to_beat,
        up_token_id="up",
        down_token_id="down",
        condition_id="cond",
    )


def _prices(binance: float, coinbase: float) -> BtcPrices:
    return BtcPrices(binance=binance, coinbase=coinbase, chainlink=None, ts=time.time())


def _mock_book_ws(best_ask: float = 0.52, imbalance_ratio: float = 2.0, secs: float = 60.0):
    ws = MagicMock()
    ws.get_imbalance_history.return_value = [
        ImbalanceReading(ratio=imbalance_ratio, seconds_since_open=secs, ts=time.time())
    ]
    snapshot = OrderBookSnapshot(
        asset_id="up",
        bids=[OrderLevel(price=0.50, size=200)],
        asks=[OrderLevel(price=best_ask, size=100)],
    )
    ws.get_snapshot.return_value = snapshot
    ws.best_ask.return_value = best_ask
    return ws


_DEFAULT_CONFIG = {
    "signals": {
        "divergence": {"min_gap_usd": 50.0},
        "imbalance": {
            "buy_threshold": 1.8,
            "sell_threshold": 0.55,
            "detection_window_seconds": [30, 90],
            "depth_levels": 10,
        },
    },
    "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
}


class TestShouldTrade:
    def test_both_signals_agree_returns_signal(self):
        prices = _prices(95_100, 95_080)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP
        assert 0.0 < result.confidence <= 0.95
        assert result.size_usdc >= 10.0

    def test_no_divergence_returns_none(self):
        prices = _prices(95_020, 95_010)  # only $20 gap, below threshold
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_no_imbalance_returns_none(self):
        prices = _prices(95_100, 95_080)  # valid divergence UP
        ws = _mock_book_ws(imbalance_ratio=1.2, secs=60.0)  # below 1.8 threshold
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_signals_disagree_returns_none(self):
        prices = _prices(95_100, 95_080)  # divergence says UP
        ws = _mock_book_ws(imbalance_ratio=0.4, secs=60.0)  # imbalance says DOWN
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_down_direction(self):
        prices = _prices(94_900, 94_920)  # both below price_to_beat by >$50
        ws = _mock_book_ws(imbalance_ratio=0.4, secs=60.0, best_ask=0.52)
        ws.best_ask.return_value = 0.52
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is not None
        assert result.direction == Direction.DOWN

    def test_imbalance_outside_window_returns_none(self):
        prices = _prices(95_100, 95_080)
        ws = _mock_book_ws(imbalance_ratio=2.5, secs=15.0)  # too early (< 30s)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_confidence_capped_at_095(self):
        prices = _prices(200_000, 200_000)  # huge delta
        ws = _mock_book_ws(imbalance_ratio=10.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        if result:
            assert result.confidence <= 0.95
