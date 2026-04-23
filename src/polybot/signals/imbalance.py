from typing import Optional

from polybot.models.btc_market import Direction, ImbalanceReading, OrderBookSnapshot


def calculate_imbalance(snapshot: OrderBookSnapshot, depth: int = 10) -> float:
    """Top-N bid depth / top-N ask depth. Returns inf when ask side is empty."""
    bid_depth = sum(level.size for level in snapshot.bids[:depth])
    ask_depth = sum(level.size for level in snapshot.asks[:depth])
    if ask_depth == 0:
        return float("inf")
    return round(bid_depth / ask_depth, 3)


def detect_smart_entry(
    history: list[ImbalanceReading],
    threshold_buy: float = 1.8,
    threshold_sell: float = 0.55,
    window: tuple[float, float] = (30.0, 90.0),
) -> Optional[Direction]:
    """Detect directional imbalance in the 30-90s window (smart-money entry period).

    Returns UP when buyers dominate (ratio >= threshold_buy),
    DOWN when sellers dominate (ratio <= threshold_sell), None otherwise.
    """
    window_readings = [
        r for r in history
        if window[0] <= r.seconds_since_open <= window[1]
    ]
    if not window_readings:
        return None

    max_ratio = max(r.ratio for r in window_readings)
    min_ratio = min(r.ratio for r in window_readings)

    if max_ratio >= threshold_buy:
        return Direction.UP
    if min_ratio <= threshold_sell:
        return Direction.DOWN
    return None
