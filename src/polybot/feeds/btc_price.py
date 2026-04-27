import asyncio
import time
from typing import Optional

import aiohttp
import structlog

from polybot.models.btc_market import BtcPrices

logger = structlog.get_logger()

_BINANCE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
_COINBASE_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
_KRAKEN_URL = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
_BITSTAMP_URL = "https://www.bitstamp.net/api/v2/ticker/btcusd/"
_OKX_URL = "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"
_TIMEOUT = aiohttp.ClientTimeout(total=3)


async def _fetch_binance(session: aiohttp.ClientSession) -> float:
    async with session.get(_BINANCE_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["price"])


async def _fetch_coinbase(session: aiohttp.ClientSession) -> float:
    async with session.get(_COINBASE_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["data"]["amount"])


async def _fetch_kraken(session: aiohttp.ClientSession) -> float:
    async with session.get(_KRAKEN_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        # Kraken returns {"result": {"XXBTZUSD": {"c": ["price", "vol"], ...}}}
        result = data.get("result", {})
        if not result:
            raise ValueError(f"kraken empty result: {data}")
        pair_data = next(iter(result.values()))
        return float(pair_data["c"][0])


async def _fetch_bitstamp(session: aiohttp.ClientSession) -> float:
    async with session.get(_BITSTAMP_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["last"])


async def _fetch_okx(session: aiohttp.ClientSession) -> float:
    async with session.get(_OKX_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        # OKX returns {"data": [{"last": "...", ...}]}
        items = data.get("data", [])
        if not items:
            raise ValueError(f"okx empty data: {data}")
        return float(items[0]["last"])


_FETCHERS = {
    "binance": _fetch_binance,
    "coinbase": _fetch_coinbase,
    "kraken": _fetch_kraken,
    "bitstamp": _fetch_bitstamp,
    "okx": _fetch_okx,
}


async def fetch_btc_prices(min_sources: int = 2) -> Optional[BtcPrices]:
    """Fetch BTC spot from all configured exchanges in parallel.

    Returns None only if fewer than ``min_sources`` exchanges responded successfully.
    Individual exchange failures are logged but do not abort.
    """
    try:
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *(fetcher(session) for fetcher in _FETCHERS.values()),
                return_exceptions=True,
            )

        prices: dict[str, Optional[float]] = {}
        failures: dict[str, str] = {}
        for name, result in zip(_FETCHERS.keys(), results):
            if isinstance(result, Exception):
                prices[name] = None
                failures[name] = str(result)
            else:
                prices[name] = float(result)

        if failures:
            logger.warning("btc_price_partial", failures=failures, ok=list(p for p, v in prices.items() if v is not None))

        ok_count = sum(1 for v in prices.values() if v is not None)
        if ok_count < min_sources:
            logger.error("btc_price_insufficient_sources", ok=ok_count, required=min_sources)
            return None

        return BtcPrices(
            binance=prices.get("binance"),
            coinbase=prices.get("coinbase"),
            kraken=prices.get("kraken"),
            bitstamp=prices.get("bitstamp"),
            okx=prices.get("okx"),
            chainlink=None,
            ts=time.time(),
        )
    except Exception as e:
        logger.error("btc_price_fetch_failed", error=str(e))
        return None
