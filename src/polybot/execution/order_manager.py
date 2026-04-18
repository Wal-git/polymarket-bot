from decimal import Decimal

import structlog

from polybot.client.clob import CLOBClient
from polybot.models.types import Side, SignalSet
from polybot.monitoring.tracker import PositionTracker

logger = structlog.get_logger()


class OrderManager:
    def __init__(self, clob: CLOBClient, tracker: PositionTracker, dry_run: bool = True):
        self._clob = clob
        self._tracker = tracker
        self._dry_run = dry_run

    def execute_signals(self, signals: list[SignalSet]) -> list[str]:
        order_ids: list[str] = []
        for signal in signals:
            logger.info(
                "executing_signal",
                market=signal.market_condition_id,
                orders=len(signal.orders),
                rationale=signal.rationale,
                confidence=signal.confidence,
            )
            for order in signal.orders:
                try:
                    order_id = self._clob.place_order(order, dry_run=self._dry_run)
                    if order_id:
                        order_ids.append(order_id)
                        self._tracker.record_fill(
                            token_id=order.token_id,
                            side=order.side,
                            size=order.size,
                            price=order.limit_price or Decimal("0"),
                            market_question=signal.market_condition_id,
                        )
                except Exception as e:
                    logger.error("order_failed", token_id=order.token_id, error=str(e))

        return order_ids

    def close_position(self, token_id: str):
        pos = self._tracker.close_position(token_id)
        if not pos:
            return
        try:
            from polybot.models.types import OrderRequest, OrderType

            sell_order = OrderRequest(
                token_id=token_id,
                side=Side.SELL,
                order_type=OrderType.MARKET_FOK,
                size=pos.shares,
                limit_price=pos.current_price if pos.current_price else None,
            )
            self._clob.place_order(sell_order, dry_run=self._dry_run)
            logger.info("position_closed", token_id=token_id, shares=str(pos.shares))
        except Exception as e:
            logger.error("close_position_failed", token_id=token_id, error=str(e))
