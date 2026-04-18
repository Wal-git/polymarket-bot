from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
import structlog

from polybot.models.types import Market, MarketOutcome

logger = structlog.get_logger()

GAMMA_API_BASE = "https://gamma-api.polymarket.com"


def _safe_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


class GammaClient:
    def __init__(self, min_volume: Decimal = Decimal("1000"), max_markets: int = 20):
        self._min_volume = min_volume
        self._max_markets = max_markets
        self._http = httpx.Client(base_url=GAMMA_API_BASE, timeout=30)

    def fetch_active_markets(self) -> list[Market]:
        markets: list[Market] = []
        offset = 0
        limit = 100

        while len(markets) < self._max_markets:
            resp = self._http.get(
                "/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break

            for raw in batch:
                market = self._parse_market(raw)
                if market and self._passes_filter(market):
                    markets.append(market)
                    if len(markets) >= self._max_markets:
                        break

            offset += limit

        logger.info("fetched_markets", count=len(markets))
        return markets

    def fetch_market(self, condition_id: str) -> Optional[Market]:
        resp = self._http.get(f"/markets/{condition_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return self._parse_market(resp.json())

    def _parse_market(self, raw: dict) -> Optional[Market]:
        try:
            tokens = raw.get("clobTokenIds", "")
            outcomes_raw = raw.get("outcomes", "")
            if isinstance(tokens, str):
                tokens = [t.strip() for t in tokens.split(",") if t.strip()]
            if isinstance(outcomes_raw, str):
                outcomes_raw = [o.strip() for o in outcomes_raw.split(",") if o.strip()]

            prices = raw.get("outcomePrices", "")
            if isinstance(prices, str):
                prices = [p.strip() for p in prices.split(",") if p.strip()]

            if not tokens or len(tokens) != len(outcomes_raw):
                return None

            outcomes = []
            for i, token_id in enumerate(tokens):
                price = _safe_decimal(prices[i]) if i < len(prices) else Decimal("0")
                outcomes.append(
                    MarketOutcome(
                        token_id=token_id,
                        label=outcomes_raw[i] if i < len(outcomes_raw) else f"Outcome {i}",
                        price=price,
                    )
                )

            volume_raw = raw.get("volume24hr")
            volume_24h = _safe_decimal(volume_raw) if volume_raw not in (None, "") else None

            return Market(
                condition_id=raw.get("conditionId", raw.get("condition_id", "")),
                question=raw.get("question", ""),
                active=raw.get("active", True),
                outcomes=outcomes,
                volume_24h=volume_24h,
                end_date_iso=raw.get("endDate"),
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("parse_market_failed", error=str(e))
            return None

    def _passes_filter(self, market: Market) -> bool:
        if market.volume_24h is not None and market.volume_24h < self._min_volume:
            return False
        return True

    def close(self):
        self._http.close()
