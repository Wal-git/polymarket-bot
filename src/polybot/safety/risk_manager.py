from decimal import Decimal

import structlog

from polybot.models.types import OrderRequest, Position, Side, SignalSet

logger = structlog.get_logger()


class RiskManager:
    def __init__(
        self,
        max_total_exposure_pct: float = 0.20,
        max_position_usdc: float = 200,
        max_single_bet_usdc: float = 50,
        stop_loss_pct: float = 0.30,
    ):
        self._max_exposure_pct = Decimal(str(max_total_exposure_pct))
        self._max_position = Decimal(str(max_position_usdc))
        self._max_bet = Decimal(str(max_single_bet_usdc))
        self._stop_loss_pct = Decimal(str(stop_loss_pct))

    def validate_signals(
        self,
        signals: list[SignalSet],
        positions: list[Position],
        balance: Decimal,
    ) -> tuple[list[SignalSet], dict[str, str]]:
        """Validate signals against risk caps.

        Returns ``(approved, rejection_reasons)`` where ``rejection_reasons`` maps
        ``market_condition_id`` -> human-readable reason for any signal that ended
        up with no valid orders. Approved signals may have orders downsized.
        """
        current_exposure = sum(p.shares * p.avg_entry_price for p in positions)
        max_exposure = balance * self._max_exposure_pct
        remaining = max_exposure - current_exposure

        approved: list[SignalSet] = []
        rejections: dict[str, str] = {}
        for signal in signals:
            valid_orders: list[OrderRequest] = []
            reasons: list[str] = []
            for order in signal.orders:
                # SELL orders reduce exposure — skip buy-side caps entirely.
                if order.side == Side.SELL:
                    valid_orders.append(order)
                    continue

                if order.size > self._max_bet:
                    logger.warning("order_capped", original=str(order.size), cap=str(self._max_bet))
                    order = order.model_copy(update={"size": self._max_bet})

                token_exposure = sum(
                    p.shares * p.avg_entry_price
                    for p in positions
                    if p.token_id == order.token_id
                )
                if token_exposure + order.size > self._max_position:
                    allowed = self._max_position - token_exposure
                    if allowed <= 0:
                        logger.warning("position_limit_hit", token_id=order.token_id)
                        reasons.append("position cap")
                        continue
                    order = order.model_copy(update={"size": allowed})

                if order.size > remaining:
                    if remaining <= 0:
                        logger.warning("exposure_limit_hit")
                        reasons.append("exposure cap")
                        continue
                    order = order.model_copy(update={"size": remaining})

                remaining -= order.size
                valid_orders.append(order)

            if valid_orders:
                approved.append(signal.model_copy(update={"orders": valid_orders}))
            else:
                rejections[signal.market_condition_id] = "; ".join(reasons) or "no valid orders"

        return approved, rejections

    def check_stop_losses(self, positions: list[Position]) -> list[str]:
        tokens_to_close: list[str] = []
        for pos in positions:
            if pos.avg_entry_price == 0:
                continue
            loss_pct = (pos.avg_entry_price - pos.current_price) / pos.avg_entry_price
            if loss_pct >= self._stop_loss_pct:
                logger.warning(
                    "stop_loss_triggered",
                    token_id=pos.token_id,
                    loss_pct=f"{loss_pct:.2%}",
                )
                tokens_to_close.append(pos.token_id)
        return tokens_to_close
