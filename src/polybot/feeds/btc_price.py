"""Back-compat shim — generic reader lives in ``polybot.feeds.spot_price``.

Kept so existing imports (``from polybot.feeds.btc_price import fetch_btc_prices``)
keep working during the multi-asset refactor. New code should call
``fetch_spot_prices(asset.spot_urls, ...)`` directly with an ``AssetSpec``.
"""
from __future__ import annotations

from typing import Optional

from polybot.feeds.spot_price import fetch_spot_prices
from polybot.models.market import SpotPrices

BTC_SPOT_URLS = {
    "binance": "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
    "coinbase": "https://api.coinbase.com/v2/prices/BTC-USD/spot",
    "kraken": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
    "bitstamp": "https://www.bitstamp.net/api/v2/ticker/btcusd/",
    "okx": "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT",
}


async def fetch_btc_prices(min_sources: int = 2) -> Optional[SpotPrices]:
    """Deprecated — use ``fetch_spot_prices(asset.spot_urls)`` with an AssetSpec."""
    return await fetch_spot_prices(BTC_SPOT_URLS, min_sources=min_sources, asset_name="BTC")
