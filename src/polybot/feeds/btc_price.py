import asyncio
import time
from typing import Optional

import aiohttp
import structlog

from polybot.models.btc_market import BtcPrices

logger = structlog.get_logger()

_BINANCE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
_COINBASE_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
_TIMEOUT = aiohttp.ClientTimeout(total=3)


async def _fetch_binance(session: aiohttp.ClientSession) -> float:
    async with session.get(_BINANCE_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["price"])


async def _fetch_coinbase(session: aiohttp.ClientSession) -> float:
    async with session.get(_COINBASE_URL, timeout=_TIMEOUT) as resp:
        data = await resp.json(content_type=None)
        return float(data["data"]["amount"])


async def fetch_btc_prices() -> Optional[BtcPrices]:
    try:
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                _fetch_binance(session),
                _fetch_coinbase(session),
                return_exceptions=True,
            )

        binance = results[0] if not isinstance(results[0], Exception) else None
        coinbase = results[1] if not isinstance(results[1], Exception) else None

        if binance is None or coinbase is None:
            logger.warning(
                "btc_price_partial",
                binance_ok=binance is not None,
                coinbase_ok=coinbase is not None,
                binance_err=str(results[0]) if isinstance(results[0], Exception) else None,
                coinbase_err=str(results[1]) if isinstance(results[1], Exception) else None,
            )
            return None

        return BtcPrices(binance=binance, coinbase=coinbase, chainlink=None, ts=time.time())
    except Exception as e:
        logger.error("btc_price_fetch_failed", error=str(e))
        return None
