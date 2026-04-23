def kelly_size(
    confidence: float,
    entry_price: float,
    bankroll: float,
    kelly_fraction: float = 0.25,
    min_usdc: float = 10.0,
    max_usdc: float = 200.0,
) -> float:
    """Quarter-Kelly position sizing for binary outcome bets.

    b = decimal odds (profit per $1 risked)
    f* = (p*b - q) / b  (Kelly fraction)
    size = bankroll * f* * kelly_fraction (safety-capped)
    """
    if entry_price <= 0 or entry_price >= 1:
        return min_usdc
    b = (1.0 - entry_price) / entry_price
    p = confidence
    q = 1.0 - p
    f_star = (p * b - q) / b
    if f_star <= 0:
        return min_usdc
    raw_size = bankroll * f_star * kelly_fraction
    return max(min_usdc, min(max_usdc, raw_size))
