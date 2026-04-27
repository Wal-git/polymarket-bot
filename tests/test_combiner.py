import time
from unittest.mock import MagicMock

import pytest

from polybot.models.btc_market import (
    BtcPrices,
    ChainlinkRound,
    Direction,
    FuturesSnapshot,
    ImbalanceReading,
    MacroSnapshot,
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
        "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 200.0, "fast_pass_usd": 125.0},
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
    def test_divergence_fires_signal(self):
        prices = _prices(95_120, 95_110)  # both > $100 gap, under $200 max
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP
        assert 0.0 < result.confidence <= 0.95
        assert result.size_usdc >= 10.0

    def test_no_divergence_returns_none(self):
        prices = _prices(95_020, 95_010)  # only $20 gap, below $100 threshold
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_below_min_gap_returns_none(self):
        # Both exchanges past old 75 threshold but under new 100 — must reject
        prices = _prices(95_080, 95_080)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_divergence_fires_regardless_of_imbalance(self):
        # Imbalance is no longer a gate — low imbalance should still fire
        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=1.2, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP

    def test_divergence_fires_regardless_of_imbalance_window(self):
        # Imbalance window timing no longer blocks the trade
        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=2.5, secs=15.0)  # outside 30-90s window
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)

    def test_down_direction(self):
        prices = _prices(94_880, 94_890)  # both below price_to_beat by >$100, under $200 max
        ws = _mock_book_ws(imbalance_ratio=0.4, secs=60.0, best_ask=0.52)
        ws.best_ask.return_value = 0.52
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is not None
        assert result.direction == Direction.DOWN

    def test_fast_pass_one_exchange_large(self):
        # Only Binance > $125 fast-pass; Coinbase under min_gap but same direction
        prices = _prices(95_180, 95_030)  # binance +180, coinbase +30
        ws = _mock_book_ws(imbalance_ratio=1.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP

    def test_fast_pass_disabled_blocks_single_exchange_spike(self):
        # With fast_pass_enabled=false, a single $180 spike with no other agreement should NOT fire.
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 200.0,
                               "fast_pass_usd": 125.0, "fast_pass_enabled": False,
                               "min_agreement": 2},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        # binance +180 (over fast_pass), coinbase +20 (only 1 vote past min_gap=100)
        prices = _prices(95_180, 95_020)
        ws = _mock_book_ws(imbalance_ratio=1.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert result is None  # Would have fired with fast_pass on; should not now

    def test_fast_pass_disabled_still_fires_on_real_n_of_m(self):
        # 2-of-2 agreement past min_gap should still fire even with fast_pass off.
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 200.0,
                               "fast_pass_usd": 125.0, "fast_pass_enabled": False,
                               "min_agreement": 2},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        prices = _prices(95_120, 95_110)  # both past min_gap=100
        ws = _mock_book_ws(imbalance_ratio=1.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP

    def test_fast_pass_tolerates_small_opposite_noise(self):
        # Binance +180 (past fast_pass), coinbase -20 (small noise, under min_gap).
        # Under N-of-M, dissent only counts past min_gap. -$20 is noise → fires.
        prices = _prices(95_180, 94_980)
        ws = _mock_book_ws(imbalance_ratio=1.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP

    def test_fast_pass_blocked_when_real_dissent(self):
        # Binance +180 (fast_pass), coinbase -150 (real dissent past min_gap=100) — block
        prices = _prices(95_180, 94_850)
        ws = _mock_book_ws(imbalance_ratio=1.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_max_gap_rejects_over_extended_up(self):
        # Both exchanges past max_gap_usd — over-extended, must reject
        prices = _prices(95_250, 95_240)  # both +240 > 200 max
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_max_gap_rejects_over_extended_down(self):
        prices = _prices(94_750, 94_760)  # both -240/-250 < -200 max
        ws = _mock_book_ws(imbalance_ratio=0.4, secs=60.0, best_ask=0.52)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_max_gap_rejects_when_only_one_exchange_extreme(self):
        # Binance +250 (over max_gap), Coinbase +150 — still rejects
        prices = _prices(95_250, 95_150)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert result is None

    def test_max_gap_disabled_when_zero(self):
        # max_gap_usd=0 means disabled — large divergence should still fire
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0, "fast_pass_usd": 125.0},
                "imbalance": {
                    "buy_threshold": 1.8,
                    "sell_threshold": 0.55,
                    "detection_window_seconds": [30, 90],
                    "depth_levels": 10,
                },
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        prices = _prices(95_400, 95_400)  # both +400, far over default max
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert isinstance(result, TradeSignal)

    def test_n_of_m_3_of_5_fires(self, tmp_path, monkeypatch):
        # 5 exchanges, 3 past min_gap, no dissent → fires UP
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_115, bitstamp=95_005, okx=95_010,
        )
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0,
                               "fast_pass_usd": 1000.0, "min_agreement": 3},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert isinstance(result, TradeSignal)
        assert result.direction == Direction.UP

    def test_n_of_m_only_2_of_5_blocked(self, tmp_path, monkeypatch):
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_005, bitstamp=95_000, okx=95_005,
        )
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0,
                               "fast_pass_usd": 1000.0, "min_agreement": 3},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert result is None

    def test_partial_data_handled(self, tmp_path, monkeypatch):
        # Only 3 exchanges responded; min_agreement=3 → fires when all 3 agree
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")
        prices = BtcPrices(
            binance=95_120, coinbase=95_110, kraken=95_115,
            bitstamp=None, okx=None,
        )
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0,
                               "fast_pass_usd": 1000.0, "min_agreement": 3},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert isinstance(result, TradeSignal)

    def test_chainlink_passthrough_logged_but_does_not_gate(self, tmp_path, monkeypatch):
        # Chainlink data should reach evaluations but not affect the trade decision.
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evaluations.jsonl")

        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        cl = ChainlinkRound(answer=95_000.0, updated_at=int(time.time()) - 45, round_id=42)
        result = should_trade(
            prices, ws, _slot(95_000),
            bankroll=2000.0, config=_DEFAULT_CONFIG, chainlink=cl,
        )
        assert isinstance(result, TradeSignal)

        evals = [json.loads(l) for l in (tmp_path / "evaluations.jsonl").read_text().splitlines()]
        assert evals[-1]["chainlink_price"] == 95_000.0
        assert evals[-1]["chainlink_round_id"] == 42
        assert 30 <= evals[-1]["chainlink_lag_s"] <= 60
        assert evals[-1]["chainlink_vs_binance"] == 120.0
        assert evals[-1]["chainlink_vs_price_to_beat"] == 0.0

    def test_macro_passthrough_logged(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")

        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        m = MacroSnapshot(vix=18.5, dxy=104.2, es_price=5800.0, es_pct_change_1h=0.0035, ts=time.time())
        result = should_trade(
            prices, ws, _slot(95_000),
            bankroll=2000.0, config=_DEFAULT_CONFIG, macro=m,
        )
        assert isinstance(result, TradeSignal)

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        last = evals[-1]
        assert last["vix"] == 18.5
        assert last["dxy"] == 104.2
        assert last["es_price"] == 5800.0
        assert last["es_pct_change_1h"] == 0.0035

    def test_macro_partial_data_logged(self, tmp_path, monkeypatch):
        # Yahoo can return None for any field — make sure we still log what we have
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")

        prices = _prices(95_020, 95_010)  # below min_gap → no_divergence
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        m = MacroSnapshot(vix=18.5, dxy=None, es_price=None, es_pct_change_1h=None, ts=time.time())
        result = should_trade(
            prices, ws, _slot(95_000),
            bankroll=2000.0, config=_DEFAULT_CONFIG, macro=m,
        )
        assert result is None

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        last = evals[-1]
        assert last["vix"] == 18.5
        assert last["dxy"] is None
        assert last["es_price"] is None

    def test_futures_passthrough_logged(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")

        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        fut = FuturesSnapshot(
            mark_price=95_115.0,
            index_price=95_113.5,
            last_funding_rate=0.0001,
            next_funding_time_ms=int(time.time() * 1000) + 3_600_000,  # 1h from now
            ts=time.time(),
        )
        result = should_trade(
            prices, ws, _slot(95_000),
            bankroll=2000.0, config=_DEFAULT_CONFIG, futures=fut,
        )
        assert isinstance(result, TradeSignal)

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        last = evals[-1]
        assert last["futures_mark_price"] == 95_115.0
        assert last["futures_index_price"] == 95_113.5
        assert last["futures_mark_minus_index"] == 1.5
        # mark_minus_spot: spot mean = (95_120 + 95_110)/2 = 95_115; mark = 95_115 → 0
        assert last["futures_mark_minus_spot"] == 0.0
        assert last["futures_funding_rate"] == 0.0001
        assert 3500 <= last["futures_funding_until_next_s"] <= 3700

    def test_futures_none_emits_null_fields(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")

        prices = _prices(95_020, 95_010)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(
            prices, ws, _slot(95_000),
            bankroll=2000.0, config=_DEFAULT_CONFIG, futures=None,
        )
        assert result is None

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        last = evals[-1]
        assert last["futures_mark_price"] is None
        assert last["futures_funding_rate"] is None

    def test_chainlink_none_emits_null_fields(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evaluations.jsonl")

        prices = _prices(95_020, 95_010)  # below min_gap → no_divergence reject
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(
            prices, ws, _slot(95_000),
            bankroll=2000.0, config=_DEFAULT_CONFIG, chainlink=None,
        )
        assert result is None

        evals = [json.loads(l) for l in (tmp_path / "evaluations.jsonl").read_text().splitlines()]
        assert evals[-1]["chainlink_price"] is None
        assert evals[-1]["chainlink_lag_s"] is None

    def test_calibration_overrides_formula_when_enabled(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        from polybot.signals import calibration as calibration_mod
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")

        # Build a mini table that returns 0.83 for 100-150 delta bucket
        table_path = tmp_path / "table.json"
        table_path.write_text(json.dumps({
            "version": 1,
            "global": {"trials": 45, "wins": 34},
            "buckets": {
                "delta_x_entry_x_hour": {},
                "delta_x_entry": {},
                "delta": {"100-150": {"trials": 16, "wins": 14}},
            },
        }))
        calibration_mod.reset_cache()

        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0,
                               "fast_pass_usd": 1000.0, "min_agreement": 2},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
                "calibration": {
                    "enabled": True,
                    "table_path": str(table_path),
                    "min_n": 5,
                    "fallback_confidence": 0.5,
                },
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }

        prices = _prices(95_120, 95_110)  # both ≥100 → max_abs_delta = 120 → "100-150"
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert isinstance(result, TradeSignal)
        # Smoothed (14+1)/(16+2) = 15/18 ≈ 0.833 — formula would give 0.6 + 115/250 = 1.06 → cap 0.95
        assert result.confidence == pytest.approx(15 / 18, abs=0.005)

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        assert evals[-1]["confidence_source"] == "calibration:delta"

    def test_calibration_disabled_uses_formula(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")

        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=_DEFAULT_CONFIG)
        assert isinstance(result, TradeSignal)

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        assert evals[-1]["confidence_source"] == "formula"

    def test_calibration_enabled_but_no_table_falls_back_to_formula(self, tmp_path, monkeypatch):
        import json
        import polybot.monitoring.event_log as event_log
        from polybot.signals import calibration as calibration_mod
        monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evals.jsonl")
        calibration_mod.reset_cache()

        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0,
                               "fast_pass_usd": 1000.0, "min_agreement": 2},
                "imbalance": {"buy_threshold": 1.8, "sell_threshold": 0.55,
                              "detection_window_seconds": [30, 90], "depth_levels": 10},
                "calibration": {
                    "enabled": True,
                    "table_path": str(tmp_path / "missing.json"),
                },
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }

        prices = _prices(95_120, 95_110)
        ws = _mock_book_ws(imbalance_ratio=2.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        assert isinstance(result, TradeSignal)

        evals = [json.loads(l) for l in (tmp_path / "evals.jsonl").read_text().splitlines()]
        assert evals[-1]["confidence_source"] == "formula:no_table"

    def test_confidence_capped_at_095(self):
        # With max_gap disabled, confidence formula should still cap at 0.95
        cfg = {
            "signals": {
                "divergence": {"min_gap_usd": 100.0, "max_gap_usd": 0.0, "fast_pass_usd": 125.0},
                "imbalance": {
                    "buy_threshold": 1.8,
                    "sell_threshold": 0.55,
                    "detection_window_seconds": [30, 90],
                    "depth_levels": 10,
                },
            },
            "sizing": {"kelly_fraction": 0.25, "min_trade_usdc": 10, "max_trade_usdc": 200},
        }
        prices = _prices(200_000, 200_000)  # huge delta
        ws = _mock_book_ws(imbalance_ratio=10.0, secs=60.0)
        result = should_trade(prices, ws, _slot(95_000), bankroll=2000.0, config=cfg)
        if result:
            assert result.confidence <= 0.95
