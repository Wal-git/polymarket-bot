import json
import time
from typing import Optional

import httpx
import structlog

from polybot.feeds.btc_price import fetch_btc_prices
from polybot.models.btc_market import SlotInfo

logger = structlog.get_logger()

_BASE_TIMESTAMP = 1772568900
_INTERVAL_5M = 300  # seconds
_GAMMA_BASE = "https://gamma-api.polymarket.com"
_SLUG_PREFIX = "btc-updown-5m"


def get_slot_ts(offset: int = 0) -> tuple[int, int]:
    """Return (start_ms, end_ms) for the current 5-min slot, or ±offset slots."""
    now_sec = int(time.time())
    slot_sec = (
        _BASE_TIMESTAMP
        + ((now_sec - _BASE_TIMESTAMP) // _INTERVAL_5M) * _INTERVAL_5M
        + offset * _INTERVAL_5M
    )
    return slot_sec * 1000, (slot_sec + _INTERVAL_5M) * 1000


def get_slug(offset: int = 0) -> str:
    start_ms, _ = get_slot_ts(offset)
    return f"{_SLUG_PREFIX}-{start_ms // 1000}"


def slot_from_slug(slug: str) -> tuple[int, int]:
    ts = int(slug.split("-")[-1])
    return ts * 1000, (ts + _INTERVAL_5M) * 1000


async def fetch_slot_details(slug: str) -> Optional[SlotInfo]:
    """Query Gamma API for event/market data matching the slug."""
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
        # The Gamma API rarely exposes it — fall back to live BTC mid-price
        # captured at slot discovery time, which closely tracks Chainlink.
        price_to_beat = float(
            market.get("startPrice")
            or market.get("openPrice")
            or event.get("startPrice")
            or 0
        )
        if price_to_beat == 0:
            prices = await fetch_btc_prices()
            if prices is not None:
                avail = prices.exchange_prices()
                if avail:
                    price_to_beat = round(sum(avail.values()) / len(avail), 2)
                    logger.info("price_to_beat_live_fallback", slug=slug,
                                price=price_to_beat, sources=list(avail.keys()))

        start_ms, end_ms = slot_from_slug(slug)
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
