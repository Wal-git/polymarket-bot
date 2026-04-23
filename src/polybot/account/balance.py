import time
from decimal import Decimal
from typing import Optional

import structlog

from polybot.client.clob import CLOBClient

logger = structlog.get_logger()

_cache: Optional[tuple[float, Decimal]] = None
_CACHE_TTL = 30.0


def get_usdc_balance(clob: CLOBClient) -> Decimal:
    """Return USDC collateral balance, cached for 30s."""
    global _cache
    now = time.time()
    if _cache is not None and now - _cache[0] < _CACHE_TTL:
        return _cache[1]
    balance = clob.get_balance()
    _cache = (now, balance)
    logger.info("balance_refreshed", usdc=str(balance))
    return balance


def invalidate_cache() -> None:
    global _cache
    _cache = None
