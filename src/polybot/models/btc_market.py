from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


class ExitReason(str, Enum):
    PROFIT_TARGET = "PROFIT_TARGET"
    STOP_LOSS = "STOP_LOSS"
    HOLD_TO_RESOLUTION = "HOLD_TO_RESOLUTION"
    TIME_EXPIRED = "TIME_EXPIRED"


@dataclass(frozen=True)
class BtcPrices:
    binance: float
    coinbase: float
    chainlink: Optional[float]
    ts: float  # unix timestamp


@dataclass(frozen=True)
class OrderLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    asset_id: str
    bids: list[OrderLevel] = field(default_factory=list)  # sorted desc by price
    asks: list[OrderLevel] = field(default_factory=list)  # sorted asc by price


@dataclass(frozen=True)
class ImbalanceReading:
    ratio: float
    seconds_since_open: float
    ts: float


@dataclass(frozen=True)
class TradeSignal:
    direction: Direction
    confidence: float  # 0.0–0.95
    size_usdc: float


@dataclass(frozen=True)
class SlotInfo:
    slug: str
    start_ms: int
    end_ms: int
    price_to_beat: float
    up_token_id: str
    down_token_id: str
    condition_id: str


@dataclass(frozen=True)
class ExitResult:
    reason: ExitReason
    pnl: Optional[float]  # None when HOLD_TO_RESOLUTION (pending resolution)
