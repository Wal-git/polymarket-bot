from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET_FOK = "MARKET_FOK"
    MARKET_FAK = "MARKET_FAK"


class MarketOutcome(BaseModel):
    token_id: str
    label: str
    price: Decimal
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None


class Market(BaseModel):
    condition_id: str
    question: str
    active: bool
    outcomes: list[MarketOutcome]
    volume_24h: Optional[Decimal] = None
    volume_total: Optional[Decimal] = None
    end_date_iso: Optional[str] = None
    market_slug: Optional[str] = None
    ticker: Optional[str] = None
    neg_risk: bool = False


class OrderRequest(BaseModel):
    token_id: str
    side: Side
    order_type: OrderType = OrderType.LIMIT
    size: Decimal
    limit_price: Optional[Decimal] = None


class SignalSet(BaseModel):
    market_condition_id: str
    orders: list[OrderRequest] = Field(default_factory=list)
    rationale: str = ""
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)


class Position(BaseModel):
    token_id: str
    market_question: str
    outcome_label: str
    shares: Decimal
    avg_entry_price: Decimal
    current_price: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    confidence: Optional[float] = None


class TradeRecord(BaseModel):
    timestamp: str
    token_id: str
    side: Side
    size: Decimal
    price: Decimal
    market_question: str
