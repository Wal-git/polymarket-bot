import asyncio
import time
from decimal import Decimal
from enum import Enum
from typing import Optional

import structlog
from web3 import Web3

from polybot.account.balance import get_usdc_balance, invalidate_cache
from polybot.client.clob import CLOBClient
from polybot.execution.entry import execute_entry
from polybot.execution.exit import monitor_position
from polybot.feeds.btc_price import fetch_btc_prices
from polybot.feeds.orderbook_ws import OrderBookWS
from polybot.models.btc_market import Direction, ExitReason, SlotInfo
from polybot.monitoring.tracker import PositionTracker
from polybot.signals.combiner import should_trade

logger = structlog.get_logger()


class LifecycleState(str, Enum):
    INIT = "INIT"
    WAITING_FOR_ENTRY = "WAITING_FOR_ENTRY"
    IN_POSITION = "IN_POSITION"
    STOPPING = "STOPPING"
    RESOLVED = "RESOLVED"


class MarketLifecycle:
    """Manages one 5-minute BTC market slot from open to resolution."""

    def __init__(
        self,
        slot: SlotInfo,
        clob: CLOBClient,
        tracker: PositionTracker,
        dry_run: bool,
        config: dict,
    ) -> None:
        self.slot = slot
        self._clob = clob
        self._tracker = tracker
        self._dry_run = dry_run
        self._config = config
        self._state = LifecycleState.INIT
        self._book_ws = OrderBookWS()
        self._task: Optional[asyncio.Task] = None
        self.pnl: Optional[float] = None

    @property
    def state(self) -> LifecycleState:
        return self._state

    @property
    def remaining_secs(self) -> float:
        return self.slot.end_ms / 1000 - time.time()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"lifecycle-{self.slot.slug}")

    async def wait(self) -> None:
        if self._task:
            await self._task

    def shutdown(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    async def _run(self) -> None:
        try:
            self._book_ws.subscribe(
                self.slot.up_token_id,
                self.slot.down_token_id,
                self.slot.start_ms / 1000,
            )
            try:
                await self._book_ws.wait_ready(timeout=15.0)
            except asyncio.TimeoutError:
                logger.warning("orderbook_not_ready", slug=self.slot.slug)

            if self.slot.price_to_beat == 0:
                logger.warning("no_price_to_beat", slug=self.slot.slug)
                self._state = LifecycleState.RESOLVED
                return

            self._state = LifecycleState.WAITING_FOR_ENTRY
            await self._evaluate_and_trade()
        except asyncio.CancelledError:
            self._state = LifecycleState.STOPPING
        except Exception as e:
            logger.error("lifecycle_error", slug=self.slot.slug, error=str(e))
            self._state = LifecycleState.RESOLVED
        finally:
            self._book_ws.destroy()

    async def _evaluate_and_trade(self) -> None:
        entry_cfg = self._config.get("strategy", {}).get("entry", {})
        window = entry_cfg.get("window_seconds", [60, 180])
        window_start, window_end = window[0], window[1]

        slot_start_sec = self.slot.start_ms / 1000
        elapsed = time.time() - slot_start_sec

        if elapsed < window_start:
            await asyncio.sleep(window_start - elapsed)

        elapsed = time.time() - slot_start_sec
        if elapsed > window_end or self.remaining_secs <= 0:
            logger.info("entry_window_skipped", slug=self.slot.slug, elapsed=round(elapsed))
            self._state = LifecycleState.RESOLVED
            return

        prices = await fetch_btc_prices()
        if prices is None:
            logger.warning("price_unavailable", slug=self.slot.slug)
            self._state = LifecycleState.RESOLVED
            return

        bankroll_cfg = self._config.get("strategy", {}).get("bankroll", {})
        if bankroll_cfg.get("source") == "wallet_balance":
            bankroll = float(get_usdc_balance(self._clob))
        else:
            bankroll = float(bankroll_cfg.get("fixed_usdc", 2000.0))

        signal = should_trade(
            prices=prices,
            book_ws=self._book_ws,
            slot=self.slot,
            bankroll=bankroll,
            config=self._config.get("strategy", {}),
        )

        if signal is None:
            logger.info("window_skipped_no_signal", slug=self.slot.slug)
            self._state = LifecycleState.RESOLVED
            return

        signal_ts = time.time()

        # Re-fetch live balance immediately before placing order
        invalidate_cache()
        live_balance = float(get_usdc_balance(self._clob))
        min_usdc = float(
            self._config.get("strategy", {}).get("sizing", {}).get("min_trade_usdc", 20.0)
        )
        if live_balance < min_usdc:
            logger.warning(
                "insufficient_balance",
                slug=self.slot.slug,
                balance=round(live_balance, 2),
                required=min_usdc,
            )
            from polybot.monitoring.event_log import emit_execution
            emit_execution(
                slug=self.slot.slug,
                status="blocked",
                block_reason="insufficient_balance",
                direction=signal.direction.value,
                confidence=signal.confidence,
                size_usdc=round(signal.size_usdc, 2),
                balance_at_block=round(live_balance, 2),
                required_usdc=min_usdc,
            )
            self._state = LifecycleState.RESOLVED
            return

        order_id = await execute_entry(
            signal=signal,
            slot=self.slot,
            orderbook_ws=self._book_ws,
            clob=self._clob,
            tracker=self._tracker,
            dry_run=self._dry_run,
            entry_window=(window_start, window_end),
            signal_ts=signal_ts,
        )

        if order_id is None and not self._dry_run:
            self._state = LifecycleState.RESOLVED
            return

        token_id = (
            self.slot.up_token_id
            if signal.direction == Direction.UP
            else self.slot.down_token_id
        )
        self._state = LifecycleState.IN_POSITION

        exit_cfg = self._config.get("strategy", {}).get("exit", {})
        result = await monitor_position(
            token_id=token_id,
            direction=signal.direction,
            slot=self.slot,
            orderbook_ws=self._book_ws,
            clob=self._clob,
            tracker=self._tracker,
            dry_run=self._dry_run,
            profit_target=float(exit_cfg.get("profit_target", 0.75)),
            stop_loss=float(exit_cfg.get("stop_loss", 0.35)),
            hold_to_resolution_secs=float(exit_cfg.get("hold_to_resolution_s_remaining", 60.0)),
        )

        self.pnl = result.pnl
        if result.reason != ExitReason.HOLD_TO_RESOLUTION:
            pnl_str = f"+${result.pnl:.2f}" if (result.pnl or 0) >= 0 else f"-${abs(result.pnl or 0):.2f}"
            logger.info(
                "trade_closed",
                slug=self.slot.slug,
                reason=result.reason.value,
                pnl=pnl_str,
            )
            from polybot.monitoring.event_log import emit_result
            emit_result(
                slug=self.slot.slug,
                exit_reason=result.reason.value,
                exit_price=result.exit_price,
                pnl=result.pnl,
                won=(result.pnl or 0) > 0,
                direction=signal.direction.value,
                confidence=signal.confidence,
                hold_duration_s=round(time.time() - signal_ts, 1),
            )
        else:
            # Market resolved — redeem winning CTF tokens on-chain then sync CLOB
            from polybot.execution.redeem import maybe_redeem
            from polybot.auth.wallet import get_private_key
            from polybot.monitoring.event_log import emit_result
            _, outcomes = maybe_redeem(get_private_key(), self._clob.client)
            self._clob.sync_balance_allowance()
            invalidate_cache()
            # Remove from tracker — position settled on-chain, no sell order needed
            self._tracker.close_position(token_id)
            self._tracker.save()
            # Record outcome for dashboard
            matched = False
            for outcome in outcomes:
                if outcome.get("slug") == self.slot.slug:
                    matched = True
                    self.pnl = outcome["pnl"]
                    emit_result(
                        slug=self.slot.slug,
                        won=outcome["won"],
                        pnl=outcome["pnl"],
                        shares=outcome["shares"],
                        entry_price=outcome["entry_price"],
                        direction=signal.direction.value,
                        exit_reason="HOLD_TO_RESOLUTION",
                        exit_price=1.0 if outcome["won"] else 0.0,
                        confidence=signal.confidence,
                        hold_duration_s=round(time.time() - signal_ts, 1),
                    )
                    logger.info(
                        "trade_resolved",
                        slug=self.slot.slug,
                        won=outcome["won"],
                        pnl=f"{'+' if outcome['pnl'] >= 0 else ''}{outcome['pnl']:.2f}",
                    )
                    break

            if not matched:
                # API may lag behind on-chain resolution — retry a few times before giving up.
                from polybot.execution.redeem import fetch_outcomes
                from polybot.auth.wallet import get_private_key as _gpk
                _pk = _gpk()
                _addr = Web3(Web3.HTTPProvider("https://1rpc.io/matic")).eth.account.from_key(_pk).address
                for _attempt in range(5):
                    fallback = fetch_outcomes(_addr, [self.slot.slug])
                    for outcome in fallback:
                        if outcome.get("slug") == self.slot.slug:
                            matched = True
                            self.pnl = outcome["pnl"]
                            emit_result(
                                slug=self.slot.slug,
                                won=outcome["won"],
                                pnl=outcome["pnl"],
                                shares=outcome["shares"],
                                entry_price=outcome["entry_price"],
                                direction=signal.direction.value,
                                exit_reason="HOLD_TO_RESOLUTION",
                                exit_price=1.0 if outcome["won"] else 0.0,
                                confidence=signal.confidence,
                                hold_duration_s=round(time.time() - signal_ts, 1),
                            )
                            logger.info(
                                "trade_resolved_fallback",
                                slug=self.slot.slug,
                                won=outcome["won"],
                                pnl=f"{'+' if outcome['pnl'] >= 0 else ''}{outcome['pnl']:.2f}",
                            )
                            break
                    if matched:
                        break
                    wait = 2 ** _attempt  # 1s, 2s, 4s, 8s, 16s
                    logger.warning(
                        "outcome_api_lag",
                        slug=self.slot.slug,
                        attempt=_attempt + 1,
                        retry_in=wait,
                    )
                    await asyncio.sleep(wait)

        self._state = LifecycleState.RESOLVED
