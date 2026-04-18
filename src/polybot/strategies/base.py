from abc import ABC, abstractmethod
from decimal import Decimal

from pydantic import BaseModel

from polybot.models.types import Market, Position, SignalSet


class StrategyContext(BaseModel):
    market: Market
    open_positions: list[Position]
    portfolio_balance: Decimal
    historical_prices: list[dict]
    config: dict


class BaseStrategy(ABC):
    NAME: str = ""

    @abstractmethod
    def evaluate(self, ctx: StrategyContext) -> SignalSet:
        ...

    def filter_market(self, market: Market) -> bool:
        return market.active
