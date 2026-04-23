import asyncio
import time
from decimal import Decimal
from typing import Optional

import structlog

from polybot.client.clob import CLOBClient
from polybot.feeds.orderbook_ws import OrderBookWS
from polybot.models.btc_market import Direction, SlotInfo, TradeSignal
from polybot.models.types import OrderRequest, OrderType, Side
from polybot.monitoring.tracker import PositionTracker

logger = structlog.get_logger()


async def execute_entry(
    signal: TradeSignal,
    slot: SlotInfo,
    orderbook_ws: OrderBookWS,
    clob: CLOBClient,
    tracker: PositionTracker,
    dry_run: bool,
    entry_window: tuple[int, int] = (60, 180),
) -> Optional[str]:
    """Place a limit BUY order within the 60-180s entry window.

    Returns the order_id (or a dry-run placeholder) on success, None on skip.
    """
    window_start, window_end = entry_window
    slot_start_sec = slot.start_ms / 1000

    elapsed = time.time() - slot_start_sec
    if elapsed > window_end:
        logger.info("entry_window_expired", slug=slot.slug, elapsed=round(elapsed))
        return None

    if elapsed < window_start:
        wait = window_start - elapsed
        logger.info("waiting_for_entry_window", slug=slot.slug, wait_s=round(wait, 1))
        await asyncio.sleep(wait)

    elapsed = time.time() - slot_start_sec
    if elapsed > window_end:
        logger.info("entry_window_missed_after_wait", slug=slot.slug)
        return None

    best_ask = orderbook_ws.best_ask(signal.direction)
    if best_ask is None:
        logger.warning("no_ask_price", slug=slot.slug, direction=signal.direction.value)
        return None

    token_id = slot.up_token_id if signal.direction == Direction.UP else slot.down_token_id
    price = Decimal(str(round(best_ask, 2)))
    size = Decimal(str(round(signal.size_usdc / float(price), 4)))

    order = OrderRequest(
        token_id=token_id,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        size=size,
        limit_price=price,
    )

    order_id = clob.place_order(order, dry_run=dry_run)
    tracker.record_fill(
        token_id=token_id,
        side=Side.BUY,
        size=size,
        price=price,
        market_question=slot.slug,
        outcome_label=signal.direction.value,
    )
    logger.info(
        "entry_placed",
        slug=slot.slug,
        direction=signal.direction.value,
        price=str(price),
        size=str(size),
        confidence=signal.confidence,
        dry_run=dry_run,
    )
    return order_id or "dry-run"
