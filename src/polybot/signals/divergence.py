from typing import Optional

from polybot.models.btc_market import BtcPrices, Direction


def detect_divergence(
    prices: BtcPrices,
    price_to_beat: float,
    min_gap_usd: float = 50.0,
    min_agreement: int = 2,
) -> Optional[Direction]:
    """Return direction when at least ``min_agreement`` exchanges exceed the gap
    threshold in the same direction, with no exchange exceeding it in the other.

    The "Price to Beat" is set by the resolution oracle at market open. When live
    exchange prices have moved significantly past it, a tradeable gap exists
    before the oracle catches up.

    With 2 exchanges and ``min_agreement=2``, this matches the original
    "both Binance AND Coinbase" semantics.
    """
    available = prices.exchange_prices()
    if not available:
        return None

    deltas = {name: price - price_to_beat for name, price in available.items()}

    up_votes = sum(1 for d in deltas.values() if d > min_gap_usd)
    down_votes = sum(1 for d in deltas.values() if d < -min_gap_usd)

    if up_votes >= min_agreement and down_votes == 0:
        return Direction.UP
    if down_votes >= min_agreement and up_votes == 0:
        return Direction.DOWN
    return None
