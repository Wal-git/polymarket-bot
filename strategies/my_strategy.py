"""Template for user-defined strategies. Copy and modify this file."""

from decimal import Decimal

from polybot.models.types import OrderRequest, OrderType, Side, SignalSet
from polybot.strategies.base import BaseStrategy, StrategyContext


class MyStrategy(BaseStrategy):
    NAME = "my_strategy"

    def evaluate(self, ctx: StrategyContext) -> SignalSet:
        # Access your custom config params via ctx.config
        # Example: threshold = Decimal(str(ctx.config.get("threshold", "0.10")))

        # Inspect the market
        for outcome in ctx.market.outcomes:
            pass  # Add your logic here

        return SignalSet(
            market_condition_id=ctx.market.condition_id,
            rationale="No signal",
        )
