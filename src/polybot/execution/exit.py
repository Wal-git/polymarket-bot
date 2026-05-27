import asyncio
import time
from decimal import Decimal
from typing import Optional

import structlog

from polybot.client.clob import CLOBClient
from polybot.feeds.orderbook_ws import OrderBookWS
from polybot.models.btc_market import Direction, ExitReason, ExitResult, SlotInfo
from polybot.models.types import OrderRequest, OrderType, Side
from polybot.monitoring.tracker import PositionTracker

logger = structlog.get_logger()


async def _wait_for_entry_fill(
    token_id: str,
    expected_shares: Decimal,
    clob: CLOBClient,
    slot: SlotInfo,
    timeout_s: float = 30.0,
    poll_s: float = 1.0,
) -> bool:
    """Block until the proxy holds >= expected_shares of token_id, or timeout.

    The entry path places a LIMIT BUY and records a local fill optimistically.
    The exit loop must not run until the BUY has actually been matched on the
    CLOB — otherwise the first iteration sees a bid above profit_target,
    fires a SELL for shares the proxy doesn't yet hold, and the CLOB rejects
    with "balance: 0, order amount: X".
    """
    # Polymarket rounds CTF share balances to 2 decimal places, but the local
    # expected size is computed at 4 decimals (size_usdc / price). Without a
    # tolerance, an order for 33.3333 shares fills as 33.33 on-chain and the
    # check 33.33 >= 33.3333 fails forever → false entry_fill_timeout.
    fill_tolerance = Decimal("0.01")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        held = clob.get_conditional_balance(token_id)
        if held >= expected_shares - fill_tolerance:
            logger.info(
                "entry_fill_confirmed",
                slug=slot.slug,
                token_id=token_id,
                shares_held=str(held),
            )
            return True
        await asyncio.sleep(poll_s)
    logger.warning(
        "entry_fill_timeout",
        slug=slot.slug,
        token_id=token_id,
        expected=str(expected_shares),
        held=str(clob.get_conditional_balance(token_id)),
        timeout_s=timeout_s,
    )
    return False


async def monitor_position(
    token_id: str,
    direction: Direction,
    slot: SlotInfo,
    orderbook_ws: OrderBookWS,
    clob: CLOBClient,
    tracker: PositionTracker,
    dry_run: bool,
    stop_loss: float = 0.35,
    hold_to_resolution_secs: float = 60.0,
    order_id: Optional[str] = None,
) -> ExitResult:
    """Poll every 2 seconds. Exit only on stop-loss or hold-to-resolution.

    Profit target removed: entries are gated to min_entry_price >= 0.80 so a
    profit_target below that triggered instantly on every fill. Positions now
    ride to resolution (full $1.00 payout if correct) unless the bid collapses
    past stop_loss.
    """
    if not dry_run:
        pos = tracker._positions.get(token_id)
        expected = pos.shares if pos is not None else Decimal("0")
        filled = await _wait_for_entry_fill(token_id, expected, clob, slot)
        if not filled:
            held = clob.get_conditional_balance(token_id)
            if held <= 0 and order_id:
                order_info = clob.get_order(order_id)
                size_matched = Decimal(order_info.get("size_matched", "0")) if order_info else Decimal("0")
                if size_matched <= 0:
                    logger.warning(
                        "order_unfilled_cleanup",
                        slug=slot.slug,
                        token_id=token_id,
                        order_id=order_id,
                    )
                    clob.cancel_order(order_id)
                    tracker.close_position(token_id)
                    tracker.save()
                    return ExitResult(reason=ExitReason.UNFILLED, pnl=None)
            if held <= 0:
                logger.warning(
                    "order_unfilled_cleanup_no_order_id",
                    slug=slot.slug,
                    token_id=token_id,
                )
                tracker.close_position(token_id)
                tracker.save()
                return ExitResult(reason=ExitReason.UNFILLED, pnl=None)

    while True:
        time_remaining = slot.end_ms / 1000 - time.time()
        current_bid = orderbook_ws.best_bid(direction)

        if current_bid is not None and current_bid <= stop_loss:
            pnl, exit_price = await _sell(token_id, current_bid, slot, clob, tracker, dry_run)
            logger.info("exit_stop_loss", slug=slot.slug, bid=current_bid, pnl=pnl)
            return ExitResult(reason=ExitReason.STOP_LOSS, pnl=pnl, exit_price=exit_price)

        if time_remaining < hold_to_resolution_secs:
            logger.info(
                "exit_hold_to_resolution",
                slug=slot.slug,
                remaining_s=round(time_remaining, 1),
            )
            return ExitResult(reason=ExitReason.HOLD_TO_RESOLUTION, pnl=None)

        await orderbook_ws.wait_bid_change(timeout=2.0)


async def _sell(
    token_id: str,
    bid_price: float,
    slot: SlotInfo,
    clob: CLOBClient,
    tracker: PositionTracker,
    dry_run: bool,
) -> tuple[Optional[float], float]:
    pos = tracker._positions.get(token_id)
    if pos is None:
        return None, bid_price

    # Cap sell size to what's actually held on-chain. Without this, a tracker
    # entry created before the BUY filled (or after a partial fill) causes the
    # CLOB to reject the SELL with "balance: 0, order amount: X".
    sell_size = pos.shares
    if not dry_run:
        held = clob.get_conditional_balance(token_id)
        if held <= 0:
            logger.warning(
                "sell_skipped_no_shares",
                slug=slot.slug, token_id=token_id, tracker_shares=str(pos.shares),
            )
            return None, bid_price
        if held < pos.shares:
            sell_size = held

    limit_price = Decimal(str(round(bid_price, 2)))
    sell_order = OrderRequest(
        token_id=token_id,
        side=Side.SELL,
        order_type=OrderType.LIMIT,
        size=sell_size,
        limit_price=limit_price,
    )
    clob.place_order(sell_order, dry_run=dry_run)
    pnl = float(sell_size * (limit_price - pos.avg_entry_price))
    tracker.record_fill(
        token_id=token_id,
        side=Side.SELL,
        size=sell_size,
        price=limit_price,
        market_question=slot.slug,
    )
    return round(pnl, 4), float(limit_price)
