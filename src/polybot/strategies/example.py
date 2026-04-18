from decimal import Decimal

from polybot.models.types import OrderRequest, OrderType, Side, SignalSet
from polybot.strategies.base import BaseStrategy, StrategyContext


class ExampleStrategy(BaseStrategy):
    NAME = "example"

    def evaluate(self, ctx: StrategyContext) -> SignalSet:
        min_edge = Decimal(str(ctx.config.get("min_edge", "0.05")))

        for outcome in ctx.market.outcomes:
            if outcome.best_ask and outcome.best_ask < Decimal("0.5") - min_edge:
                return SignalSet(
                    market_condition_id=ctx.market.condition_id,
                    orders=[
                        OrderRequest(
                            token_id=outcome.token_id,
                            side=Side.BUY,
                            order_type=OrderType.LIMIT,
                            size=Decimal("10"),
                            limit_price=outcome.best_ask,
                        )
                    ],
                    rationale=f"Edge on {outcome.label}: ask {outcome.best_ask} < 0.50 - {min_edge}",
                    confidence=0.6,
                )

        return SignalSet(
            market_condition_id=ctx.market.condition_id,
            rationale="No edge found",
        )
