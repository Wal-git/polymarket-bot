import json
from decimal import Decimal
from pathlib import Path

import structlog

from polybot.models.types import Position, Side, TradeRecord

logger = structlog.get_logger()


class PositionTracker:
    def __init__(self, state_file: str = "./data/state.json"):
        self._state_file = Path(state_file)
        self._positions: dict[str, Position] = {}
        self._trades: list[TradeRecord] = []
        self._load()

    def _load(self):
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text())
            for p in data.get("positions", []):
                pos = Position(**p)
                self._positions[pos.token_id] = pos
            self._trades = [TradeRecord(**t) for t in data.get("trades", [])]
            logger.info("state_loaded", positions=len(self._positions))
        except Exception as e:
            logger.warning("state_load_failed", error=str(e))

    def save(self):
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "positions": [p.model_dump(mode="json") for p in self._positions.values()],
            "trades": [t.model_dump(mode="json") for t in self._trades[-500:]],
        }
        self._state_file.write_text(json.dumps(data, indent=2, default=str))

    @property
    def positions(self) -> list[Position]:
        return list(self._positions.values())

    def record_fill(
        self,
        token_id: str,
        side: Side,
        size: Decimal,
        price: Decimal,
        market_question: str,
        outcome_label: str = "",
        timestamp: str = "",
        confidence: float | None = None,
    ):
        from datetime import datetime, timezone

        ts = timestamp or datetime.now(timezone.utc).isoformat()

        self._trades.append(
            TradeRecord(
                timestamp=ts,
                token_id=token_id,
                side=side,
                size=size,
                price=price,
                market_question=market_question,
            )
        )

        if side == Side.BUY:
            if token_id in self._positions:
                pos = self._positions[token_id]
                total_cost = pos.shares * pos.avg_entry_price + size * price
                total_shares = pos.shares + size
                self._positions[token_id] = pos.model_copy(
                    update={
                        "shares": total_shares,
                        "avg_entry_price": total_cost / total_shares if total_shares else Decimal("0"),
                    }
                )
            else:
                self._positions[token_id] = Position(
                    token_id=token_id,
                    market_question=market_question,
                    outcome_label=outcome_label,
                    shares=size,
                    avg_entry_price=price,
                    confidence=confidence,
                )
        elif side == Side.SELL:
            if token_id in self._positions:
                pos = self._positions[token_id]
                remaining = pos.shares - size
                realized = size * (price - pos.avg_entry_price)
                if remaining <= 0:
                    del self._positions[token_id]
                else:
                    self._positions[token_id] = pos.model_copy(
                        update={
                            "shares": remaining,
                            "realized_pnl": pos.realized_pnl + realized,
                        }
                    )

        self.save()

    def update_prices(self, price_map: dict[str, Decimal]):
        for token_id, price in price_map.items():
            if token_id in self._positions:
                pos = self._positions[token_id]
                unrealized = pos.shares * (price - pos.avg_entry_price)
                self._positions[token_id] = pos.model_copy(
                    update={"current_price": price, "unrealized_pnl": unrealized}
                )
        self.save()

    def close_position(self, token_id: str) -> Position | None:
        return self._positions.pop(token_id, None)

    def total_pnl(self) -> Decimal:
        realized = sum((p.realized_pnl for p in self._positions.values()), Decimal("0"))
        unrealized = sum((p.unrealized_pnl for p in self._positions.values()), Decimal("0"))
        return realized + unrealized
