import json
import time
from typing import Optional

import httpx
import structlog

from polybot.feeds.spot_price import fetch_spot_prices
from polybot.models.asset import AssetSpec
from polybot.models.market import SlotInfo

logger = structlog.get_logger()

_GAMMA_BASE = "https://gamma-api.polymarket.com"


def get_slot_ts(asset: AssetSpec, offset: int = 0) -> tuple[int, int]:
    """Return (start_ms, end_ms) for the current slot, or ±offset slots,
    on the asset's slot grid.
    """
    interval = asset.slot_interval_s
    base = asset.slot_base_timestamp
    now_sec = int(time.time())
    slot_sec = base + ((now_sec - base) // interval) * interval + offset * interval
    return slot_sec * 1000, (slot_sec + interval) * 1000


def get_slug(asset: AssetSpec, offset: int = 0) -> str:
    start_ms, _ = get_slot_ts(asset, offset)
    return f"{asset.slug_prefix}-{start_ms // 1000}"


def slot_from_slug(slug: str, interval_s: int = 300) -> tuple[int, int]:
    """Parse the trailing unix-second timestamp from a slug. Asset-agnostic."""
    ts = int(slug.split("-")[-1])
    return ts * 1000, (ts + interval_s) * 1000


async def fetch_slot_details(slug: str, asset: AssetSpec) -> Optional[SlotInfo]:
    """Query Gamma API for event/market data matching the slug. The asset is
    needed for the spot-price fallback when ``startPrice`` isn't exposed.
    """
    url = f"{_GAMMA_BASE}/events?slug={slug}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            events = resp.json()

        if not events:
            logger.warning("slot_not_found", slug=slug)
            return None

        event = events[0]
        markets = event.get("markets", [])
        if not markets:
            logger.warning("slot_no_markets", slug=slug)
            return None
        market = markets[0]

        raw_ids = market.get("clobTokenIds", "[]")
        token_ids: list[str] = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        if len(token_ids) < 2:
            logger.warning("slot_missing_token_ids", slug=slug)
            return None

        # price_to_beat is the open price set by Chainlink at slot start.
        # The Gamma API rarely exposes it — fall back to live spot mid-price
        # captured at slot discovery time, which closely tracks Chainlink.
        price_to_beat = float(
            market.get("startPrice")
            or market.get("openPrice")
            or event.get("startPrice")
            or 0
        )
        if price_to_beat == 0:
            prices = await fetch_spot_prices(asset.spot_urls, asset_name=asset.name)
            if prices is not None:
                avail = prices.exchange_prices()
                if avail:
                    price_to_beat = round(sum(avail.values()) / len(avail), 2)
                    logger.info(
                        "price_to_beat_live_fallback",
                        slug=slug, price=price_to_beat,
                        sources=list(avail.keys()), asset=asset.name,
                    )

        start_ms, end_ms = slot_from_slug(slug, asset.slot_interval_s)
        return SlotInfo(
            slug=slug,
            start_ms=start_ms,
            end_ms=end_ms,
            price_to_beat=price_to_beat,
            up_token_id=token_ids[0],
            down_token_id=token_ids[1],
            condition_id=market.get("conditionId", ""),
        )
    except Exception as e:
        logger.error("fetch_slot_failed", slug=slug, error=str(e))
        return None
