"""Asset-agnostic spot price reader.

Fetches spot prices for a single asset from up to 5 exchanges in parallel.
The exchanges are fixed (binance, coinbase, kraken, bitstamp, okx) — what
varies per-asset is the URL for each exchange. URLs come from the
``AssetSpec.spot_urls`` mapping.

Each exchange's response shape is exchange-specific, so the parser stays
hardcoded per exchange. Only the URL changes between assets.
"""
from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Optional

import aiohttp
import structlog

from polybot.models.market import SpotPrices

logger = structlog.get_logger()

_TIMEOUT = aiohttp.ClientTimeout(total=3)


async def _fetch_binance(session: aiohttp.ClientSession, url: str) -> float:
    async with session.get(url, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["price"])


async def _fetch_coinbase(session: aiohttp.ClientSession, url: str) -> float:
    async with session.get(url, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["data"]["amount"])


async def _fetch_kraken(session: aiohttp.ClientSession, url: str) -> float:
    async with session.get(url, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        result = data.get("result", {})
        if not result:
            raise ValueError(f"kraken empty result: {data}")
        pair_data = next(iter(result.values()))
        return float(pair_data["c"][0])


async def _fetch_bitstamp(session: aiohttp.ClientSession, url: str) -> float:
    async with session.get(url, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["last"])


async def _fetch_okx(session: aiohttp.ClientSession, url: str) -> float:
    async with session.get(url, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        items = data.get("data", [])
        if not items:
            raise ValueError(f"okx empty data: {data}")
        return float(items[0]["last"])


_FETCHERS: dict[str, Callable[[aiohttp.ClientSession, str], Awaitable[float]]] = {
    "binance": _fetch_binance,
    "coinbase": _fetch_coinbase,
    "kraken": _fetch_kraken,
    "bitstamp": _fetch_bitstamp,
    "okx": _fetch_okx,
}


async def fetch_spot_prices(
    spot_urls: dict[str, str],
    min_sources: int = 2,
    asset_name: str = "",
) -> Optional[SpotPrices]:
    """Fetch spot prices from the configured exchanges in parallel.

    Returns None only if fewer than ``min_sources`` exchanges responded
    successfully. Individual exchange failures are logged but do not abort.

    ``spot_urls`` is a mapping from exchange name (e.g. "binance") to that
    exchange's URL for the asset. Any exchange not present in the mapping
    is skipped. ``asset_name`` is used only for logging.
    """
    if not spot_urls:
        logger.error("spot_price_no_urls_configured", asset=asset_name)
        return None

    fetchers = [
        (name, _FETCHERS[name], spot_urls[name])
        for name in _FETCHERS
        if name in spot_urls
    ]

    try:
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *(fetcher(session, url) for _, fetcher, url in fetchers),
                return_exceptions=True,
            )

        prices: dict[str, Optional[float]] = {}
        failures: dict[str, str] = {}
        for (name, _, _), result in zip(fetchers, results):
            if isinstance(result, Exception):
                prices[name] = None
                failures[name] = str(result)
            else:
                prices[name] = float(result)

        if failures:
            logger.warning(
                "spot_price_partial",
                asset=asset_name,
                failures=failures,
                ok=list(p for p, v in prices.items() if v is not None),
            )

        ok_count = sum(1 for v in prices.values() if v is not None)
        if ok_count < min_sources:
            logger.error(
                "spot_price_insufficient_sources",
                asset=asset_name,
                ok=ok_count,
                required=min_sources,
            )
            return None

        return SpotPrices(
            binance=prices.get("binance"),
            coinbase=prices.get("coinbase"),
            kraken=prices.get("kraken"),
            bitstamp=prices.get("bitstamp"),
            okx=prices.get("okx"),
            chainlink=None,
            ts=time.time(),
        )
    except Exception as e:
        logger.error("spot_price_fetch_failed", asset=asset_name, error=str(e))
        return None
