"""Pull candidate wallets from multiple sources.

Seeding exclusively from ``/leaderboard`` suffers survivorship bias: wallets
that blew up yesterday no longer appear. We union leaderboard results with
Goldsky on-chain top-volume wallets over a longer window to surface steady
performers and recently-departed losers alike.
"""
from __future__ import annotations

import time
from collections import Counter

import structlog

from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.config import (
    CACHE_DIR,
    GOLDSKY_CHUNK_DAYS,
    GOLDSKY_FETCH_WORKERS,
    GOLDSKY_SEED_DAYS,
    GOLDSKY_SEED_TOP_N,
    LEADERBOARD_LIMIT,
    MONTHLY_TOP_N,
)

logger = structlog.get_logger()

_PERIODS = ["7d", "30d", "all"]
_ORDERS = ["pnl", "volume"]


def _normalize_leaderboard_row(row: dict, source: str = "") -> dict | None:
    """Extract and normalize fields from a single leaderboard API row."""
    wallet = (row.get("proxyWallet") or row.get("proxy_wallet") or "").lower()
    if not wallet:
        return None
    return {
        "proxy_wallet": wallet,
        "username": row.get("name") or row.get("username") or "",
        "leaderboard_pnl": _to_float(row.get("pnl") or row.get("pnlPerShare")),
        "leaderboard_volume": _to_float(row.get("volume")),
        "sources": {source} if source else set(),
    }


def fetch_monthly_pnl_leaders(
    client: DataAPIClient,
    limit: int = MONTHLY_TOP_N,
) -> list[dict]:
    """Fetch the 30d-by-PnL leaderboard and return normalized wallet rows."""
    rows = client.leaderboard(period="30d", order="pnl", limit=limit)
    if not isinstance(rows, list):
        logger.warning("monthly_leaderboard_bad_response")
        return []
    result = []
    for row in rows:
        normalized = _normalize_leaderboard_row(row, source="lb:30d:pnl")
        if normalized:
            normalized["sources"] = sorted(normalized["sources"])
            result.append(normalized)
    logger.info("monthly_leaderboard_fetched", count=len(result))
    return result


def fetch_candidates(
    client: DataAPIClient,
    limit: int = LEADERBOARD_LIMIT,
    include_goldsky: bool = True,
    goldsky_days: int = GOLDSKY_SEED_DAYS,
    goldsky_top_n: int = GOLDSKY_SEED_TOP_N,
) -> list[dict]:
    """Return deduplicated wallet dicts from leaderboard + Goldsky volume tops."""
    seen: dict[str, dict] = {}

    # --- leaderboard seed ---
    for period in _PERIODS:
        for order in _ORDERS:
            rows = client.leaderboard(period=period, order=order, limit=limit)
            if not isinstance(rows, list):
                logger.warning("leaderboard_bad_response", period=period, order=order)
                continue
            for row in rows:
                normalized = _normalize_leaderboard_row(row, source=f"lb:{period}:{order}")
                if not normalized:
                    continue
                wallet = normalized["proxy_wallet"]
                entry = seen.setdefault(wallet, {**normalized, "sources": set()})
                entry["username"] = entry["username"] or normalized["username"]
                entry["leaderboard_pnl"] = max(entry["leaderboard_pnl"], normalized["leaderboard_pnl"])
                entry["leaderboard_volume"] = max(entry["leaderboard_volume"], normalized["leaderboard_volume"])
                entry["sources"].add(f"lb:{period}:{order}")
            logger.info("leaderboard_fetched", period=period, order=order, seen=len(seen))

    # --- Goldsky volume seed ---
    if include_goldsky:
        try:
            gs_wallets = _goldsky_top_volume_wallets(
                days=goldsky_days, top_n=goldsky_top_n
            )
            for wallet, vol in gs_wallets:
                entry = seen.setdefault(
                    wallet,
                    {
                        "proxy_wallet": wallet,
                        "username": "",
                        "leaderboard_pnl": 0.0,
                        "leaderboard_volume": 0.0,
                        "sources": set(),
                    },
                )
                entry["sources"].add("goldsky")
                entry["leaderboard_volume"] = max(entry["leaderboard_volume"], vol)
            logger.info("goldsky_seed_fetched", wallets=len(gs_wallets))
        except Exception as exc:
            logger.warning("goldsky_seed_failed", error=str(exc))

    # Convert sources set → list for JSON-friendliness downstream.
    candidates = []
    for c in seen.values():
        c["sources"] = sorted(c["sources"])
        candidates.append(c)
    logger.info("candidates_total", count=len(candidates))
    return candidates


def _goldsky_top_volume_wallets(days: int, top_n: int) -> list[tuple[str, float]]:
    """Return top-N wallets by USD-notional buy volume over the last ``days``."""
    from polybot.client.goldsky import GoldskyClient

    since_ts = int(time.time()) - days * 86400
    client = GoldskyClient()
    try:
        events = client.fetch_events_parallel(
            since_ts=since_ts,
            chunk_days=GOLDSKY_CHUNK_DAYS,
            workers=GOLDSKY_FETCH_WORKERS,
            cache_dir=CACHE_DIR / "goldsky",
        )
    finally:
        client.close()

    buy_usd: Counter[str] = Counter()
    for ev in events:
        # The taker that paid USDC is the buyer; that wallet is the interesting one.
        if ev.taker_direction == "BUY":
            buyer = ev.taker.lower()
        else:
            buyer = ev.maker.lower()
        buy_usd[buyer] += float(ev.usd_amount)

    # Trim to top_n by volume.
    top = buy_usd.most_common(top_n)
    return [(w, v) for w, v in top if w]


def _to_float(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
