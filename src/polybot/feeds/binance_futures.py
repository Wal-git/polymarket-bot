"""Binance Futures premiumIndex reader.

Captures mark price, index price, and funding rate for a perp symbol.
Funding rate flips often precede short-term reversals (longs paid up →
unwind risk). Mark/spot divergence at slot boundary can also predict
near-term direction.

URL is parameterized so the same reader serves BTCUSDT, ETHUSDT, etc.
The cache is keyed by URL so concurrent BTC/ETH readers don't clobber
each other.
"""
from __future__ import annotations

import time
from typing import Optional

import aiohttp
import structlog

from polybot.models.market import FuturesSnapshot

logger = structlog.get_logger()

DEFAULT_URL = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
_TIMEOUT = aiohttp.ClientTimeout(total=3)
_CACHE_TTL_S = 5.0

_cache: dict[str, tuple[float, FuturesSnapshot]] = {}


async def fetch_futures_snapshot(
    url: str = DEFAULT_URL,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[FuturesSnapshot]:
    """Fetch latest mark/index/funding for one symbol. Cached per-URL for 5s.
    Returns None on failure.
    """
    now = time.time()
    cached = _cache.get(url)
    if cached and now - cached[0] < _CACHE_TTL_S:
        return cached[1]

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        async with session.get(url, timeout=_TIMEOUT) as resp:
            data = await resp.json(content_type=None)
        snap = FuturesSnapshot(
            mark_price=float(data["markPrice"]),
            index_price=float(data["indexPrice"]),
            last_funding_rate=float(data["lastFundingRate"]),
            next_funding_time_ms=int(data["nextFundingTime"]),
            ts=now,
        )
        _cache[url] = (now, snap)
        return snap
    except Exception as e:
        logger.warning("binance_futures_fetch_failed", error=str(e), url=url)
        return None
    finally:
        if own_session:
            await session.close()


def reset_cache() -> None:
    """Test hook — clears the snapshot cache."""
    _cache.clear()
