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


async def monitor_position(
    token_id: str,
    direction: Direction,
    slot: SlotInfo,
    orderbook_ws: OrderBookWS,
    clob: CLOBClient,
    tracker: PositionTracker,
    dry_run: bool,
    profit_target: float = 0.75,
    stop_loss: float = 0.35,
    hold_to_resolution_secs: float = 60.0,
) -> ExitResult:
    """Poll every 2 seconds. Exit on 75c profit, 35c stop, or time threshold."""
    while True:
        time_remaining = slot.end_ms / 1000 - time.time()
        current_bid = orderbook_ws.best_bid(direction)

        if current_bid is not None:
            if current_bid >= profit_target:
                pnl, exit_price = await _sell(token_id, current_bid, slot, clob, tracker, dry_run)
                logger.info("exit_profit", slug=slot.slug, bid=current_bid, pnl=pnl)
                return ExitResult(reason=ExitReason.PROFIT_TARGET, pnl=pnl, exit_price=exit_price)

            if current_bid <= stop_loss:
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

        await asyncio.sleep(2)


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

    limit_price = Decimal(str(round(bid_price, 2)))
    sell_order = OrderRequest(
        token_id=token_id,
        side=Side.SELL,
        order_type=OrderType.LIMIT,
        size=pos.shares,
        limit_price=limit_price,
    )
    clob.place_order(sell_order, dry_run=dry_run)
    pnl = float(pos.shares * (limit_price - pos.avg_entry_price))
    tracker.record_fill(
        token_id=token_id,
        side=Side.SELL,
        size=pos.shares,
        price=limit_price,
        market_question=slot.slug,
    )
    return round(pnl, 4), float(limit_price)
