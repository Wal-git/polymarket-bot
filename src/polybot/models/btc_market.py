"""Back-compat shim — all types now live in ``polybot.models.market``.

Kept so existing imports (``from polybot.models.btc_market import ...``) and
the test suite keep working during the multi-asset refactor. Will be removed
once callers are migrated.
"""
from polybot.models.market import (
    ChainlinkRound,
    Direction,
    ExitReason,
    ExitResult,
    FuturesSnapshot,
    ImbalanceReading,
    MacroSnapshot,
    OrderBookSnapshot,
    OrderLevel,
    SlotInfo,
    SpotPrices,
    TradeSignal,
)

# Pre-rename alias — `BtcPrices` is now `SpotPrices`.
BtcPrices = SpotPrices

__all__ = [
    "BtcPrices",
    "ChainlinkRound",
    "Direction",
    "ExitReason",
    "ExitResult",
    "FuturesSnapshot",
    "ImbalanceReading",
    "MacroSnapshot",
    "OrderBookSnapshot",
    "OrderLevel",
    "SlotInfo",
    "SpotPrices",
    "TradeSignal",
]
