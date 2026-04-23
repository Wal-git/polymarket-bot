import pytest

from polybot.execution.sizing import kelly_size


class TestKellySize:
    def test_returns_min_when_confidence_too_low(self):
        # confidence = 0.5, entry = 0.5 → b = 1, f* = (0.5 - 0.5) / 1 = 0 → min
        result = kelly_size(confidence=0.50, entry_price=0.50, bankroll=2000.0)
        assert result == 10.0

    def test_positive_edge_produces_nonzero_size(self):
        result = kelly_size(confidence=0.71, entry_price=0.50, bankroll=2000.0)
        assert result > 10.0

    def test_clamped_to_max(self):
        result = kelly_size(confidence=0.95, entry_price=0.10, bankroll=100_000.0)
        assert result == 200.0

    def test_clamped_to_min(self):
        result = kelly_size(confidence=0.51, entry_price=0.99, bankroll=2000.0)
        assert result == 10.0

    def test_invalid_entry_price_returns_min(self):
        assert kelly_size(0.9, 0.0, 2000.0) == 10.0
        assert kelly_size(0.9, 1.0, 2000.0) == 10.0

    def test_kelly_fraction_scales_size(self):
        # Use bankroll=100 so neither value hits the max cap
        full = kelly_size(confidence=0.75, entry_price=0.50, bankroll=100.0, kelly_fraction=1.0)
        quarter = kelly_size(confidence=0.75, entry_price=0.50, bankroll=100.0, kelly_fraction=0.25)
        assert abs(full - quarter * 4) < 0.01

    def test_larger_bankroll_scales_proportionally(self):
        s1 = kelly_size(0.71, 0.50, 1000.0)
        s2 = kelly_size(0.71, 0.50, 2000.0)
        if s1 < 200.0 and s2 < 200.0:
            assert abs(s2 / s1 - 2.0) < 0.01
