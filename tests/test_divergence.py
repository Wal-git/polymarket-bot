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


class TestNOfMAgreement:
    def test_3_of_5_up_no_dissent(self):
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_115, bitstamp=95_005, okx=95_010,
        )
        # 3 exchanges past 100 (binance/coinbase/kraken), no exchange < -100
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=3
        ) == Direction.UP

    def test_3_of_5_required_only_2_agree_returns_none(self):
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_005, bitstamp=95_000, okx=95_005,
        )
        # Only 2 past 100; needs 3
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=3
        ) is None

    def test_dissenting_exchange_blocks_signal(self):
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_115, bitstamp=94_880, okx=95_005,
        )
        # 3 past +100, but bitstamp is past -100 — dissent blocks
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=3
        ) is None

    def test_partial_data_3_of_3_available(self):
        # Only 3 exchanges responded, all agree → still fires with min_agreement=3
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_105,
            bitstamp=None, okx=None,
        )
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=3
        ) == Direction.UP

    def test_partial_data_insufficient_sources(self):
        # Only 2 exchanges responded, min_agreement=3 → no signal
        prices = BtcPrices(
            binance=95_120, coinbase=95_110,
            kraken=None, bitstamp=None, okx=None,
        )
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=3
        ) is None

    def test_no_exchanges_returns_none(self):
        prices = BtcPrices()
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=2
        ) is None

    def test_5_of_5_down(self):
        prices = BtcPrices(
            binance=94_850, coinbase=94_855, kraken=94_860, bitstamp=94_870, okx=94_865,
        )
        assert detect_divergence(
            prices, price_to_beat=95_000, min_gap_usd=100, min_agreement=3
        ) == Direction.DOWN
