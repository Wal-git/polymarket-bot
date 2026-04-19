"""Pure metric functions. Each takes enriched wallet data and returns a scalar."""
from __future__ import annotations

import time
from collections import defaultdict

from polybot.smart_wallets.config import (
    MAX_DAYS_INACTIVE,
    MIN_RESOLVED_MARKETS,
    MIN_TRADES_COUNT,
    SCORE_WEIGHTS,
)


# ---------------------------------------------------------------------------
# individual metrics
# ---------------------------------------------------------------------------

def realized_pnl(wallet: dict) -> float:
    """Sum of net USDC received from resolved markets (REDEEM events)."""
    total = 0.0
    for ev in wallet.get("raw_redeems", []):
        total += _usdc(ev.get("usdcSize") or ev.get("amount") or 0)
    # Subtract cost basis from positions marked redeemable
    for pos in wallet.get("raw_positions", []):
        if pos.get("redeemable"):
            total -= _usdc(pos.get("initialValue") or pos.get("buyCost") or 0)
    return total


def unrealized_pnl(wallet: dict) -> float:
    total = 0.0
    for pos in wallet.get("raw_positions", []):
        total += _usdc(pos.get("cashPnl") or pos.get("pnl") or 0)
    return total


def win_rate(wallet: dict) -> float:
    """Fraction of resolved markets where net PnL > 0."""
    pnl_by_market: dict[str, float] = defaultdict(float)
    for ev in wallet.get("raw_redeems", []):
        mid = ev.get("conditionId") or ev.get("market") or ev.get("marketId") or ""
        pnl_by_market[mid] += _usdc(ev.get("usdcSize") or ev.get("amount") or 0)
    for pos in wallet.get("raw_positions", []):
        if pos.get("redeemable"):
            mid = pos.get("conditionId") or pos.get("market") or ""
            pnl_by_market[mid] -= _usdc(pos.get("initialValue") or pos.get("buyCost") or 0)
    if not pnl_by_market:
        return 0.0
    wins = sum(1 for v in pnl_by_market.values() if v > 0)
    return wins / len(pnl_by_market)


def resolved_markets_count(wallet: dict) -> int:
    seen: set[str] = set()
    for ev in wallet.get("raw_redeems", []):
        mid = ev.get("conditionId") or ev.get("market") or ev.get("marketId") or ""
        if mid:
            seen.add(mid)
    return len(seen)


def volume(wallet: dict) -> float:
    total = 0.0
    for ev in wallet.get("raw_trades", []):
        total += _usdc(ev.get("usdcSize") or ev.get("amount") or 0)
    return total


def trades_count(wallet: dict) -> int:
    return len(wallet.get("raw_trades", []))


def avg_position_usd(wallet: dict) -> float:
    trades = wallet.get("raw_trades", [])
    if not trades:
        return 0.0
    sizes = [_usdc(ev.get("usdcSize") or ev.get("amount") or 0) for ev in trades]
    return sum(sizes) / len(sizes)


def max_drawdown(wallet: dict) -> float:
    """Running-peak drawdown on cumulative PnL time series from trade activity."""
    events = sorted(wallet.get("raw_trades", []), key=lambda e: int(e.get("timestamp") or 0))
    cum = 0.0
    peak = 0.0
    worst = 0.0
    for ev in events:
        size = _usdc(ev.get("usdcSize") or ev.get("amount") or 0)
        side = (ev.get("side") or ev.get("type") or "").upper()
        # BUY = outflow (cost), SELL/REDEEM = inflow (gain)
        cum += size if side in ("SELL", "REDEEM") else -size
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > worst:
            worst = dd
    return worst


def last_active_ts(wallet: dict) -> int:
    ts = 0
    for ev in wallet.get("raw_trades", []) + wallet.get("raw_redeems", []):
        t = int(ev.get("timestamp") or 0)
        if t > ts:
            ts = t
    return ts


def recency_score(wallet: dict) -> float:
    """1.0 if active within RECENCY_DAYS, decays linearly to 0 at 2× that window."""
    from polybot.smart_wallets.config import RECENCY_DAYS

    lat = last_active_ts(wallet)
    if lat == 0:
        return 0.0
    days_ago = (time.time() - lat) / 86400
    if days_ago <= RECENCY_DAYS:
        return 1.0
    if days_ago >= RECENCY_DAYS * 2:
        return 0.0
    return 1.0 - (days_ago - RECENCY_DAYS) / RECENCY_DAYS


# ---------------------------------------------------------------------------
# composite score
# ---------------------------------------------------------------------------

def compute_all_metrics(wallet: dict) -> dict:
    return {
        "pnl_realized": realized_pnl(wallet),
        "pnl_unrealized": unrealized_pnl(wallet),
        "win_rate": win_rate(wallet),
        "resolved_markets": resolved_markets_count(wallet),
        "volume": volume(wallet),
        "trades_count": trades_count(wallet),
        "avg_position_usd": avg_position_usd(wallet),
        "max_drawdown": max_drawdown(wallet),
        "last_active_ts": last_active_ts(wallet),
        "recency": recency_score(wallet),
    }


def score_wallets(wallets_with_metrics: list[dict]) -> list[dict]:
    """Add a normalised composite `score` field to each wallet dict."""
    keys = ["pnl_realized", "win_rate", "volume", "resolved_markets", "recency"]

    # build per-key min/max across the candidate pool
    ranges: dict[str, tuple[float, float]] = {}
    for key in keys:
        vals = [w[key] for w in wallets_with_metrics]
        lo, hi = min(vals, default=0.0), max(vals, default=1.0)
        ranges[key] = (lo, hi if hi > lo else lo + 1e-9)

    scored = []
    for w in wallets_with_metrics:
        # zero out wallets that don't meet minimum statistical weight
        if w["trades_count"] < MIN_TRADES_COUNT or w["resolved_markets"] < MIN_RESOLVED_MARKETS:
            scored.append({**w, "score": 0.0})
            continue

        total = 0.0
        for key, weight in SCORE_WEIGHTS.items():
            lo, hi = ranges[key]
            norm = (w[key] - lo) / (hi - lo)
            total += weight * norm
        scored.append({**w, "score": round(total, 6)})

    return scored


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _usdc(val) -> float:
    try:
        f = float(val)
        # Data API returns micro-USDC (1e6) for some endpoints; detect by magnitude
        return f / 1_000_000 if f > 1_000_000 else f
    except (TypeError, ValueError):
        return 0.0
