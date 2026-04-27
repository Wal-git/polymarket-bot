import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polybot.engine.lifecycle import LifecycleState, MarketLifecycle
from polybot.models.asset import AssetSpec
from polybot.models.btc_market import SlotInfo


_TEST_ASSET = AssetSpec(
    name="BTC",
    slug_prefix="btc-updown-5m",
    slot_base_timestamp=1772568900,
    spot_urls={"binance": "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"},
)


def _make_slot(offset_secs: int = 0) -> SlotInfo:
    now_ms = int(time.time() * 1000)
    start_ms = now_ms + offset_secs * 1000
    return SlotInfo(
        slug="btc-updown-5m-test",
        start_ms=start_ms,
        end_ms=start_ms + 300_000,
        price_to_beat=95_000.0,
        up_token_id="up-token",
        down_token_id="down-token",
        condition_id="cond-1",
    )


def _make_lifecycle(slot: SlotInfo, dry_run: bool = True) -> MarketLifecycle:
    clob = MagicMock()
    clob.get_balance.return_value = __import__("decimal").Decimal("2000")
    tracker = MagicMock()
    tracker._positions = {}
    config = {
        "strategy": {
            "bankroll": {"source": "fixed", "fixed_usdc": 2000},
            "entry": {"window_seconds": [60, 180]},
            "exit": {"profit_target": 0.75, "stop_loss": 0.35, "hold_to_resolution_s_remaining": 60},
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
    }
    return MarketLifecycle(
        slot=slot, asset=_TEST_ASSET, clob=clob, tracker=tracker,
        dry_run=dry_run, config=config,
    )


class TestLifecycleState:
    def test_initial_state_is_init(self):
        lc = _make_lifecycle(_make_slot())
        assert lc.state == LifecycleState.INIT

    def test_remaining_secs_positive_for_future_slot(self):
        slot = _make_slot(offset_secs=0)
        lc = _make_lifecycle(slot)
        assert lc.remaining_secs > 0

    @pytest.mark.asyncio
    async def test_no_price_to_beat_resolves_immediately(self):
        slot = SlotInfo(
            slug="btc-updown-5m-0",
            start_ms=int(time.time() * 1000) - 70_000,
            end_ms=int(time.time() * 1000) + 230_000,
            price_to_beat=0.0,  # missing
            up_token_id="up",
            down_token_id="down",
            condition_id="cond",
        )
        lc = _make_lifecycle(slot)

        with patch.object(lc._book_ws, "subscribe"), \
             patch.object(lc._book_ws, "wait_ready", new_callable=AsyncMock), \
             patch.object(lc._book_ws, "destroy"):
            lc.start()
            await asyncio.wait_for(lc.wait(), timeout=2.0)

        assert lc.state == LifecycleState.RESOLVED

    @pytest.mark.asyncio
    async def test_window_skipped_when_past_deadline(self):
        """Slot that opened 200s ago is past the 180s entry window."""
        slot = SlotInfo(
            slug="btc-updown-5m-past",
            start_ms=int(time.time() * 1000) - 200_000,
            end_ms=int(time.time() * 1000) + 100_000,
            price_to_beat=95_000.0,
            up_token_id="up",
            down_token_id="down",
            condition_id="cond",
        )
        lc = _make_lifecycle(slot)

        with patch.object(lc._book_ws, "subscribe"), \
             patch.object(lc._book_ws, "wait_ready", new_callable=AsyncMock), \
             patch.object(lc._book_ws, "destroy"):
            lc.start()
            await asyncio.wait_for(lc.wait(), timeout=2.0)

        assert lc.state == LifecycleState.RESOLVED

    def test_shutdown_cancels_task(self):
        slot = _make_slot()
        lc = _make_lifecycle(slot)
        lc._task = MagicMock()
        lc._task.done.return_value = False
        lc.shutdown()
        lc._task.cancel.assert_called_once()
