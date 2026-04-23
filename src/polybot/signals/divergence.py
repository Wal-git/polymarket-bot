from typing import Optional

from polybot.models.btc_market import BtcPrices, Direction


def detect_divergence(
    prices: BtcPrices,
    price_to_beat: float,
    min_gap_usd: float = 50.0,
) -> Optional[Direction]:
    """Return direction when Binance AND Coinbase both exceed the gap threshold.

    The "Price to Beat" is set by Chainlink at market open. When live exchange
    prices have moved significantly past it while Chainlink lags, a tradeable
    gap exists before settlement catches up.
    """
    binance_delta = prices.binance - price_to_beat
    coinbase_delta = prices.coinbase - price_to_beat

    if binance_delta > min_gap_usd and coinbase_delta > min_gap_usd:
        return Direction.UP
    if binance_delta < -min_gap_usd and coinbase_delta < -min_gap_usd:
        return Direction.DOWN
    return None
