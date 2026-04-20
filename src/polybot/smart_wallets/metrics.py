"""Ledger-based wallet metrics.

All metrics derive from a single per-wallet, per-market cash-flow ledger built
from the Data API /activity (TRADE + REDEEM) and /positions endpoints. The
ledger is the only source of truth — no inlined magnitude heuristics, no
double-counting across buy+sell sides.
"""
from __future__ import annotations

import math
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from polybot.smart_wallets.config import (
    MIN_RESOLVED_MARKETS,
    MIN_TRADES_COUNT,
    RECENCY_DAYS,
    SCORE_WEIGHTS,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# ledger construction
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MarketLedgerEntry:
    """Net cash flow for one (wallet, conditionId) pair over the lookback."""

    condition_id: str
    buy_cost: float
    sell_proceeds: float
    redeem_proceeds: float
    buy_trades: int
    sell_trades: int
    redeems: int
    first_ts: int
    last_ts: int
    is_resolved: bool
    winning_token_ids: frozenset[str]
    buy_shares_by_token: Mapping[str, float]
    buy_usd_by_token: Mapping[str, float]

    @property
    def net_pnl(self) -> float:
        return self.sell_proceeds + self.redeem_proceeds - self.buy_cost


def _market_id(ev: Mapping[str, Any]) -> str:
    return str(
        ev.get("conditionId")
        or ev.get("market")
        or ev.get("marketId")
        or ev.get("condition_id")
        or ""
    )


def _token_id(ev: Mapping[str, Any]) -> str:
    return str(ev.get("asset") or ev.get("tokenId") or ev.get("token_id") or "")


def _ts(ev: Mapping[str, Any]) -> int:
    try:
        return int(ev.get("timestamp") or 0)
    except (TypeError, ValueError):
        return 0


def _side(ev: Mapping[str, Any]) -> str:
    return str(ev.get("side") or ev.get("type") or "").upper()


def build_ledger(wallet: Mapping[str, Any]) -> dict[str, MarketLedgerEntry]:
    """Compile per-market cash-flow ledger from raw activity + positions."""
    trades: list[Mapping[str, Any]] = list(wallet.get("raw_trades") or [])
    redeems: list[Mapping[str, Any]] = list(wallet.get("raw_redeems") or [])
    positions: list[Mapping[str, Any]] = list(wallet.get("raw_positions") or [])

    # Winning token_ids come from two sources:
    #  (a) token_id of any REDEEM event (you only redeem winners)
    #  (b) redeemable positions (asset = winning token)
    winners_by_market: dict[str, set[str]] = {}
    resolved_markets: set[str] = set()
    for ev in redeems:
        mid = _market_id(ev)
        if not mid:
            continue
        resolved_markets.add(mid)
        tok = _token_id(ev)
        if tok:
            winners_by_market.setdefault(mid, set()).add(tok)
    for pos in positions:
        if not pos.get("redeemable"):
            continue
        mid = _market_id(pos)
        if not mid:
            continue
        resolved_markets.add(mid)
        tok = _token_id(pos)
        if tok:
            winners_by_market.setdefault(mid, set()).add(tok)

    buys: dict[str, float] = {}
    sells: dict[str, float] = {}
    redeem_proceeds: dict[str, float] = {}
    buy_counts: dict[str, int] = {}
    sell_counts: dict[str, int] = {}
    redeem_counts: dict[str, int] = {}
    first_ts: dict[str, int] = {}
    last_ts: dict[str, int] = {}
    buy_shares_by_token: dict[str, dict[str, float]] = {}
    buy_usd_by_token: dict[str, dict[str, float]] = {}

    def _touch(mid: str, ts: int) -> None:
        if ts <= 0:
            return
        if mid not in first_ts or ts < first_ts[mid]:
            first_ts[mid] = ts
        if mid not in last_ts or ts > last_ts[mid]:
            last_ts[mid] = ts

    for ev in trades:
        mid = _market_id(ev)
        if not mid:
            continue
        usd = _safe_float(ev.get("usdcSize") or ev.get("amount"))
        shares = _safe_float(ev.get("size") or ev.get("shares"))
        tok = _token_id(ev)
        side = _side(ev)
        ts = _ts(ev)
        _touch(mid, ts)
        if side == "BUY":
            buys[mid] = buys.get(mid, 0.0) + usd
            buy_counts[mid] = buy_counts.get(mid, 0) + 1
            if tok:
                buy_shares_by_token.setdefault(mid, {})
                buy_usd_by_token.setdefault(mid, {})
                buy_shares_by_token[mid][tok] = buy_shares_by_token[mid].get(tok, 0.0) + shares
                buy_usd_by_token[mid][tok] = buy_usd_by_token[mid].get(tok, 0.0) + usd
        elif side in ("SELL", "REDEEM"):
            # Some Data API responses file REDEEMs under TRADE+type=REDEEM; treat
            # those as redeem proceeds, not as sells.
            if side == "REDEEM":
                redeem_proceeds[mid] = redeem_proceeds.get(mid, 0.0) + usd
                redeem_counts[mid] = redeem_counts.get(mid, 0) + 1
                resolved_markets.add(mid)
                if tok:
                    winners_by_market.setdefault(mid, set()).add(tok)
            else:
                sells[mid] = sells.get(mid, 0.0) + usd
                sell_counts[mid] = sell_counts.get(mid, 0) + 1

    for ev in redeems:
        mid = _market_id(ev)
        if not mid:
            continue
        usd = _safe_float(ev.get("usdcSize") or ev.get("amount") or ev.get("payout"))
        redeem_proceeds[mid] = redeem_proceeds.get(mid, 0.0) + usd
        redeem_counts[mid] = redeem_counts.get(mid, 0) + 1
        _touch(mid, _ts(ev))

    all_mids = (
        set(buys) | set(sells) | set(redeem_proceeds) | resolved_markets
    )
    ledger: dict[str, MarketLedgerEntry] = {}
    for mid in all_mids:
        ledger[mid] = MarketLedgerEntry(
            condition_id=mid,
            buy_cost=buys.get(mid, 0.0),
            sell_proceeds=sells.get(mid, 0.0),
            redeem_proceeds=redeem_proceeds.get(mid, 0.0),
            buy_trades=buy_counts.get(mid, 0),
            sell_trades=sell_counts.get(mid, 0),
            redeems=redeem_counts.get(mid, 0),
            first_ts=first_ts.get(mid, 0),
            last_ts=last_ts.get(mid, 0),
            is_resolved=mid in resolved_markets,
            winning_token_ids=frozenset(winners_by_market.get(mid, set())),
            buy_shares_by_token=dict(buy_shares_by_token.get(mid, {})),
            buy_usd_by_token=dict(buy_usd_by_token.get(mid, {})),
        )
    return ledger


def _ensure_ledger(wallet: dict[str, Any]) -> dict[str, MarketLedgerEntry]:
    """Cache ledger on the wallet dict to avoid rebuilding per metric call."""
    cached = wallet.get("_ledger")
    if cached is not None:
        return cached
    ledger = build_ledger(wallet)
    wallet["_ledger"] = ledger
    return ledger


# ---------------------------------------------------------------------------
# resolved-market PnL metrics
# ---------------------------------------------------------------------------

def realized_pnl(wallet: dict[str, Any]) -> float:
    """Net USDC from fully-resolved markets (proceeds − cost basis)."""
    ledger = _ensure_ledger(wallet)
    return sum(e.net_pnl for e in ledger.values() if e.is_resolved)


def unrealized_pnl(wallet: dict[str, Any]) -> float:
    """cashPnl reported by /positions for still-open positions."""
    total = 0.0
    for pos in wallet.get("raw_positions") or []:
        if pos.get("redeemable"):
            continue
        total += _safe_float(pos.get("cashPnl") or pos.get("pnl"))
    return total


def win_rate(wallet: dict[str, Any]) -> float:
    """Fraction of resolved markets where net cash-flow > 0."""
    ledger = _ensure_ledger(wallet)
    resolved = [e for e in ledger.values() if e.is_resolved]
    if not resolved:
        return 0.0
    wins = sum(1 for e in resolved if e.net_pnl > 0)
    return wins / len(resolved)


def resolved_markets_count(wallet: dict[str, Any]) -> int:
    return sum(1 for e in _ensure_ledger(wallet).values() if e.is_resolved)


# ---------------------------------------------------------------------------
# volume / activity metrics
# ---------------------------------------------------------------------------

def volume(wallet: dict[str, Any]) -> float:
    """Buy-side notional, USDC. Single-side to avoid round-trip double-count."""
    ledger = _ensure_ledger(wallet)
    return sum(e.buy_cost for e in ledger.values())


def trades_count(wallet: dict[str, Any]) -> int:
    return len(wallet.get("raw_trades") or [])


def avg_position_usd(wallet: dict[str, Any]) -> float:
    trades = wallet.get("raw_trades") or []
    if not trades:
        return 0.0
    sizes = [_safe_float(ev.get("usdcSize") or ev.get("amount")) for ev in trades]
    sizes = [s for s in sizes if s > 0]
    if not sizes:
        return 0.0
    return sum(sizes) / len(sizes)


def last_active_ts(wallet: dict[str, Any]) -> int:
    ts = 0
    for ev in (wallet.get("raw_trades") or []) + (wallet.get("raw_redeems") or []):
        t = _ts(ev)
        if t > ts:
            ts = t
    return ts


def recency_score(wallet: dict[str, Any]) -> float:
    """1.0 if active within RECENCY_DAYS, linear decay to 0 at 2x window."""
    lat = last_active_ts(wallet)
    if lat == 0:
        return 0.0
    days_ago = (time.time() - lat) / 86400.0
    if days_ago <= RECENCY_DAYS:
        return 1.0
    if days_ago >= RECENCY_DAYS * 2:
        return 0.0
    return 1.0 - (days_ago - RECENCY_DAYS) / RECENCY_DAYS


# ---------------------------------------------------------------------------
# risk / skill-adjusted metrics
# ---------------------------------------------------------------------------

def equity_curve_drawdown(wallet: dict[str, Any]) -> float:
    """Max peak-to-trough drawdown of cumulative *realized* PnL over time.

    Each resolved market contributes its net_pnl at last_ts. Drawdown is the
    largest decline from any running peak in that time series.
    """
    ledger = _ensure_ledger(wallet)
    events = sorted(
        (e for e in ledger.values() if e.is_resolved and e.last_ts > 0),
        key=lambda e: e.last_ts,
    )
    peak = 0.0
    cum = 0.0
    worst = 0.0
    for e in events:
        cum += e.net_pnl
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > worst:
            worst = dd
    return worst


def sharpe_like(wallet: dict[str, Any]) -> float:
    """Mean per-resolved-market PnL / stdev. Dimensionless skill proxy."""
    ledger = _ensure_ledger(wallet)
    pnls = [e.net_pnl for e in ledger.values() if e.is_resolved]
    if len(pnls) < 2:
        return 0.0
    mean = sum(pnls) / len(pnls)
    var = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
    std = math.sqrt(var)
    if std <= 0:
        return 0.0
    return mean / std


def edge(wallet: dict[str, Any]) -> float:
    """Dollars of realized PnL per dollar of buy-cost on resolved markets."""
    ledger = _ensure_ledger(wallet)
    cost = sum(e.buy_cost for e in ledger.values() if e.is_resolved)
    pnl = sum(e.net_pnl for e in ledger.values() if e.is_resolved)
    if cost <= 0:
        return 0.0
    return pnl / cost


def binomial_significance(wallet: dict[str, Any]) -> float:
    """One-sided z-score of (wins / resolved) vs. 50% null. ≥ 1.64 ≈ p<0.05."""
    ledger = _ensure_ledger(wallet)
    resolved = [e for e in ledger.values() if e.is_resolved]
    n = len(resolved)
    if n < 2:
        return 0.0
    wins = sum(1 for e in resolved if e.net_pnl > 0)
    p_hat = wins / n
    se = math.sqrt(0.25 / n)  # std err under H0: p=0.5
    return (p_hat - 0.5) / se


def early_signal_score(wallet: dict[str, Any]) -> float:
    """Per-share edge on BUYs resolved in the lookback window.

    For each resolved market:
        winner_shares * (1 - avg_fill_price) - loser_shares * avg_fill_price
    Normalized by total buy_cost on resolved markets.
    """
    ledger = _ensure_ledger(wallet)
    total_edge_usd = 0.0
    total_cost = 0.0
    for e in ledger.values():
        if not e.is_resolved or not e.buy_shares_by_token:
            continue
        winners = e.winning_token_ids
        for tok, shares in e.buy_shares_by_token.items():
            usd = e.buy_usd_by_token.get(tok, 0.0)
            if shares <= 0 or usd <= 0:
                continue
            avg_price = usd / shares
            if tok in winners:
                total_edge_usd += shares * (1.0 - avg_price)
            else:
                total_edge_usd -= shares * avg_price
            total_cost += usd
    if total_cost <= 0:
        return 0.0
    return total_edge_usd / total_cost


# ---------------------------------------------------------------------------
# composite
# ---------------------------------------------------------------------------

def compute_all_metrics(wallet: dict[str, Any]) -> dict[str, float]:
    return {
        "pnl_realized": realized_pnl(wallet),
        "pnl_unrealized": unrealized_pnl(wallet),
        "win_rate": win_rate(wallet),
        "resolved_markets": resolved_markets_count(wallet),
        "volume": volume(wallet),
        "trades_count": trades_count(wallet),
        "avg_position_usd": avg_position_usd(wallet),
        "max_drawdown": equity_curve_drawdown(wallet),
        "last_active_ts": last_active_ts(wallet),
        "recency": recency_score(wallet),
        "sharpe": sharpe_like(wallet),
        "edge": edge(wallet),
        "significance_z": binomial_significance(wallet),
        "early_signal": early_signal_score(wallet),
    }


def _percentile_ranks(values: Iterable[float]) -> list[float]:
    """Return (n,) list of percentile ranks in [0,1], ties averaged."""
    vals = list(values)
    n = len(vals)
    if n == 0:
        return []
    # rank each value by (count of strictly smaller + 0.5 * count of equal) / n
    sorted_vals = sorted(vals)
    out = []
    for v in vals:
        # lower = count strictly less
        lo = _bisect_left(sorted_vals, v)
        hi = _bisect_right(sorted_vals, v)
        rank = (lo + hi) / 2.0
        out.append(rank / n)
    return out


def _bisect_left(arr: list[float], x: float) -> int:
    lo, hi = 0, len(arr)
    while lo < hi:
        mid = (lo + hi) // 2
        if arr[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _bisect_right(arr: list[float], x: float) -> int:
    lo, hi = 0, len(arr)
    while lo < hi:
        mid = (lo + hi) // 2
        if arr[mid] <= x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def score_wallets(
    wallets: list[dict[str, Any]],
    weights: Mapping[str, float] = SCORE_WEIGHTS,
    score_key: str = "score",
) -> list[dict[str, Any]]:
    """Add percentile-rank weighted composite to each wallet dict.

    Wallets below MIN_TRADES_COUNT or MIN_RESOLVED_MARKETS receive 0.
    """
    if not wallets:
        return []

    eligible_idx = [
        i for i, w in enumerate(wallets)
        if w.get("trades_count", 0) >= MIN_TRADES_COUNT
        and w.get("resolved_markets", 0) >= MIN_RESOLVED_MARKETS
    ]

    ranks_per_key: dict[str, list[float]] = {}
    for key in weights:
        vals = [wallets[i].get(key, 0.0) for i in eligible_idx]
        ranks = _percentile_ranks(vals)
        ranks_per_key[key] = ranks

    rank_lookup: dict[int, dict[str, float]] = {}
    for pos, i in enumerate(eligible_idx):
        rank_lookup[i] = {k: ranks_per_key[k][pos] for k in weights}

    weight_sum = sum(weights.values()) or 1.0

    scored: list[dict[str, Any]] = []
    for i, w in enumerate(wallets):
        if i not in rank_lookup:
            scored.append({**w, score_key: 0.0})
            continue
        total = 0.0
        for key, weight in weights.items():
            total += weight * rank_lookup[i][key]
        scored.append({**w, score_key: round(total / weight_sum, 6)})
    return scored


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> float:
    """Coerce to float. Data API returns USDC as a regular decimal — no magic."""
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0
