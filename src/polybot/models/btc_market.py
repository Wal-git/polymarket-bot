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


_EXCHANGE_NAMES = ("binance", "coinbase", "kraken", "bitstamp", "okx")


@dataclass(frozen=True)
class BtcPrices:
    """Spot BTC prices from multiple exchanges. Any field may be None when
    that source failed; callers should use ``exchange_prices()`` to iterate
    only the available ones."""
    binance: Optional[float] = None
    coinbase: Optional[float] = None
    kraken: Optional[float] = None
    bitstamp: Optional[float] = None
    okx: Optional[float] = None
    chainlink: Optional[float] = None
    ts: float = 0.0

    def exchange_prices(self) -> dict[str, float]:
        """Return mapping of exchange→price, excluding nulls and chainlink."""
        return {
            name: getattr(self, name)
            for name in _EXCHANGE_NAMES
            if getattr(self, name) is not None
        }


@dataclass(frozen=True)
class ChainlinkRound:
    """A single Chainlink aggregator round."""
    answer: float        # price in USD (already scaled from raw int by decimals)
    updated_at: int      # unix seconds — when this round was published on-chain
    round_id: int


@dataclass(frozen=True)
class FuturesSnapshot:
    """Binance Futures premium index snapshot (BTCUSDT perp)."""
    mark_price: float
    index_price: float
    last_funding_rate: float    # decimal, e.g. 0.0001 = 0.01% per 8h funding interval
    next_funding_time_ms: int   # unix ms — when next funding hits
    ts: float                   # unix seconds — when we fetched it


@dataclass(frozen=True)
class MacroSnapshot:
    """Macro market context: VIX, DXY, S&P futures.

    Any field may be None when that source failed or the market is closed.
    Used for analysis only — never gated on at signal time.
    """
    vix: Optional[float]                # CBOE volatility index
    dxy: Optional[float]                # US dollar index
    es_price: Optional[float]           # E-mini S&P 500 futures spot
    es_pct_change_1h: Optional[float]   # 1-hour % change in ES, signed decimal (0.005 = +0.5%)
    ts: float                           # unix seconds — when fetched


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
    exit_price: Optional[float] = None
