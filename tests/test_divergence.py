import pytest

from polybot.models.btc_market import BtcPrices, Direction
from polybot.signals.divergence import detect_divergence


def _prices(binance: float, coinbase: float) -> BtcPrices:
    return BtcPrices(binance=binance, coinbase=coinbase, chainlink=None, ts=0.0)


class TestDetectDivergence:
    def test_both_above_gap_returns_up(self):
        prices = _prices(binance=95_100, coinbase=95_080)
        assert detect_divergence(prices, price_to_beat=95_000) == Direction.UP

    def test_both_below_gap_returns_down(self):
        prices = _prices(binance=94_900, coinbase=94_920)
        assert detect_divergence(prices, price_to_beat=95_000) == Direction.DOWN

    def test_only_binance_above_returns_none(self):
        prices = _prices(binance=95_100, coinbase=95_010)
        assert detect_divergence(prices, price_to_beat=95_000) is None

    def test_only_coinbase_above_returns_none(self):
        prices = _prices(binance=95_010, coinbase=95_100)
        assert detect_divergence(prices, price_to_beat=95_000) is None

    def test_exchanges_disagree_returns_none(self):
        prices = _prices(binance=95_100, coinbase=94_900)
        assert detect_divergence(prices, price_to_beat=95_000) is None

    def test_exact_gap_threshold(self):
        prices = _prices(binance=95_050, coinbase=95_050)
        assert detect_divergence(prices, price_to_beat=95_000, min_gap_usd=50.0) is None

    def test_just_over_threshold(self):
        prices = _prices(binance=95_050.01, coinbase=95_050.01)
        assert detect_divergence(prices, price_to_beat=95_000, min_gap_usd=50.0) == Direction.UP

    def test_custom_gap_threshold(self):
        prices = _prices(binance=95_030, coinbase=95_030)  # $30 gap
        assert detect_divergence(prices, price_to_beat=95_000, min_gap_usd=25.0) == Direction.UP
        assert detect_divergence(prices, price_to_beat=95_000, min_gap_usd=50.0) is None
