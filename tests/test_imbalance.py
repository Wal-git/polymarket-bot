import pytest

from polybot.models.btc_market import Direction, ImbalanceReading, OrderBookSnapshot, OrderLevel
from polybot.signals.imbalance import calculate_imbalance, detect_smart_entry


def _snapshot(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        asset_id="test",
        bids=[OrderLevel(price=p, size=s) for p, s in bids],
        asks=[OrderLevel(price=p, size=s) for p, s in asks],
    )


def _reading(ratio: float, secs: float) -> ImbalanceReading:
    return ImbalanceReading(ratio=ratio, seconds_since_open=secs, ts=0.0)


class TestCalculateImbalance:
    def test_balanced_book(self):
        snap = _snapshot([(0.5, 100)], [(0.5, 100)])
        assert calculate_imbalance(snap) == 1.0

    def test_bullish_book(self):
        snap = _snapshot([(0.5, 200)], [(0.5, 100)])
        assert calculate_imbalance(snap) == 2.0

    def test_bearish_book(self):
        snap = _snapshot([(0.5, 50)], [(0.5, 100)])
        assert calculate_imbalance(snap) == 0.5

    def test_empty_ask_returns_inf(self):
        snap = _snapshot([(0.5, 100)], [])
        assert calculate_imbalance(snap) == float("inf")

    def test_empty_bid_returns_zero(self):
        snap = _snapshot([], [(0.5, 100)])
        assert calculate_imbalance(snap) == 0.0

    def test_depth_limit_applied(self):
        bids = [(float(i), 10.0) for i in range(20)]
        asks = [(float(i), 10.0) for i in range(20)]
        snap = _snapshot(bids, asks)
        result = calculate_imbalance(snap, depth=5)
        assert result == 1.0  # first 5 levels are equal

    def test_rounds_to_three_decimals(self):
        snap = _snapshot([(0.5, 1)], [(0.5, 3)])
        result = calculate_imbalance(snap)
        assert result == round(1 / 3, 3)


class TestDetectSmartEntry:
    def test_no_readings_returns_none(self):
        assert detect_smart_entry([]) is None

    def test_readings_outside_window_ignored(self):
        history = [_reading(2.5, 10.0), _reading(2.5, 100.0)]
        assert detect_smart_entry(history) is None

    def test_bullish_spike_in_window_returns_up(self):
        history = [_reading(2.0, 45.0), _reading(1.2, 60.0)]
        assert detect_smart_entry(history) == Direction.UP

    def test_bearish_dip_in_window_returns_down(self):
        history = [_reading(0.4, 50.0), _reading(1.0, 70.0)]
        assert detect_smart_entry(history) == Direction.DOWN

    def test_neutral_returns_none(self):
        history = [_reading(1.2, 45.0), _reading(0.9, 60.0)]
        assert detect_smart_entry(history) is None

    def test_exact_threshold_buy(self):
        history = [_reading(1.8, 30.0)]
        assert detect_smart_entry(history) == Direction.UP

    def test_exact_threshold_sell(self):
        history = [_reading(0.55, 90.0)]
        assert detect_smart_entry(history) == Direction.DOWN

    def test_custom_window(self):
        history = [_reading(2.0, 100.0), _reading(2.0, 150.0)]
        assert detect_smart_entry(history, window=(90.0, 160.0)) == Direction.UP
        assert detect_smart_entry(history, window=(30.0, 90.0)) is None
