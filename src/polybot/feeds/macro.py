"""Macro context feed — VIX, DXY, S&P futures.

BTC has well-documented correlation with risk-on/off macros. At 5-minute
cadence the signal-to-noise is low, but logging it costs nothing and a
4-week review will tell us whether to use it as a feature or delete this
module.

Data source: Yahoo Finance chart API (unofficial, free, undocumented).
Failures are silent — we always return a snapshot, with None where a source
errored. The user-agent header is required to avoid 401s.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp
import structlog

from polybot.models.btc_market import MacroSnapshot

logger = structlog.get_logger()

_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
_VIX_SYMBOL = "^VIX"
_DXY_SYMBOL = "DX-Y.NYB"
_ES_SYMBOL = "ES=F"
_TIMEOUT = aiohttp.ClientTimeout(total=5)
_CACHE_TTL_S = 60.0  # macros don't change fast; 1-min cache is plenty

# Yahoo unofficial API blocks default Python user agents
_UA = "Mozilla/5.0 (compatible; polybot/1.0)"

_cache: tuple[float, MacroSnapshot] | None = None


async def _fetch_yahoo_chart(
    session: aiohttp.ClientSession, symbol: str, *, interval: str = "5m", range_: str = "1d",
) -> Optional[dict]:
    """Return Yahoo's chart payload or None on any failure."""
    url = f"{_BASE}/{symbol}?interval={interval}&range={range_}"
    try:
        async with session.get(url, timeout=_TIMEOUT, headers={"User-Agent": _UA}) as resp:
            data = await resp.json(content_type=None)
        results = data.get("chart", {}).get("result")
        if not results:
            return None
        return results[0]
    except Exception as e:
        logger.debug("yahoo_chart_failed", symbol=symbol, error=str(e))
        return None


def _latest_close(payload: Optional[dict]) -> Optional[float]:
    """Pull the most recent non-null close from a Yahoo chart payload."""
    if not payload:
        return None
    meta = payload.get("meta") or {}
    rmp = meta.get("regularMarketPrice")
    if rmp is not None:
        return float(rmp)
    closes = (payload.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    for c in reversed(closes):
        if c is not None:
            return float(c)
    return None


def _pct_change_1h(payload: Optional[dict]) -> Optional[float]:
    """Compute (latest_close / close_1h_ago) - 1 from a Yahoo payload.

    Uses the timestamp array to find the bar closest to 1 hour ago. Returns
    None when there isn't enough history (e.g. market just opened, weekend).
    """
    if not payload:
        return None
    timestamps = payload.get("timestamp") or []
    closes = (payload.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    if len(timestamps) < 2 or len(closes) != len(timestamps):
        return None

    latest_close = None
    latest_ts = None
    for t, c in zip(reversed(timestamps), reversed(closes)):
        if c is not None:
            latest_close = float(c)
            latest_ts = t
            break
    if latest_close is None:
        return None

    target_ts = latest_ts - 3600
    best_ts = None
    best_close = None
    for t, c in zip(timestamps, closes):
        if c is None:
            continue
        if t <= target_ts and (best_ts is None or t > best_ts):
            best_ts = t
            best_close = float(c)
    if best_close is None or best_close == 0:
        return None
    return round(latest_close / best_close - 1.0, 5)


async def fetch_macro_snapshot(
    session: Optional[aiohttp.ClientSession] = None,
) -> MacroSnapshot:
    """Fetch VIX, DXY, ES in parallel. Always returns a snapshot (Nones on failure)."""
    global _cache
    now = time.time()
    if _cache and now - _cache[0] < _CACHE_TTL_S:
        return _cache[1]

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        vix_payload, dxy_payload, es_payload = await asyncio.gather(
            _fetch_yahoo_chart(session, _VIX_SYMBOL),
            _fetch_yahoo_chart(session, _DXY_SYMBOL),
            _fetch_yahoo_chart(session, _ES_SYMBOL),
            return_exceptions=False,
        )
    finally:
        if own_session:
            await session.close()

    snap = MacroSnapshot(
        vix=_latest_close(vix_payload),
        dxy=_latest_close(dxy_payload),
        es_price=_latest_close(es_payload),
        es_pct_change_1h=_pct_change_1h(es_payload),
        ts=now,
    )
    _cache = (now, snap)
    return snap


def reset_cache() -> None:
    global _cache
    _cache = None
