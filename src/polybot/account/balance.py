import json
import time
from decimal import Decimal
from pathlib import Path
from typing import Optional

import structlog

from polybot.client.clob import CLOBClient

logger = structlog.get_logger()

_cache: Optional[tuple[float, Decimal]] = None
_CACHE_TTL = 30.0
_BALANCE_FILE = Path("./data/balance.json")


def get_usdc_balance(clob: CLOBClient) -> Decimal:
    """Return USDC collateral balance, cached for 30s."""
    global _cache
    now = time.time()
    if _cache is not None and now - _cache[0] < _CACHE_TTL:
        return _cache[1]
    balance = clob.get_balance()
    _cache = (now, balance)
    logger.info("balance_refreshed", usdc=str(balance))
    _persist_balance(balance)
    return balance


def _persist_balance(balance: Decimal) -> None:
    try:
        _BALANCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _BALANCE_FILE.write_text(
            json.dumps({"balance": str(balance), "ts": time.time()}),
            encoding="utf-8",
        )
    except OSError:
        pass


def invalidate_cache() -> None:
    global _cache
    _cache = None
