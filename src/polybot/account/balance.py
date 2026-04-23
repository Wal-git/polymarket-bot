import json
import time
from decimal import Decimal
from pathlib import Path
from typing import Optional

import httpx
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
    portfolio = _fetch_portfolio_value(clob.client.get_address())
    _cache = (now, balance)
    logger.info("balance_refreshed", usdc=str(balance))
    _persist_balance(balance, portfolio)
    return balance


def _fetch_portfolio_value(address: str) -> Optional[float]:
    try:
        r = httpx.get(
            f"https://data-api.polymarket.com/value?user={address}",
            timeout=5,
        )
        data = r.json()
        if data and isinstance(data, list):
            return float(data[0].get("value", 0))
    except Exception:
        pass
    return None


def _persist_balance(balance: Decimal, portfolio: Optional[float] = None) -> None:
    try:
        _BALANCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload: dict = {"balance": str(balance), "ts": time.time()}
        if portfolio is not None:
            payload["portfolio_value"] = portfolio
            payload["total_value"] = float(balance) + portfolio
        _BALANCE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        pass


def invalidate_cache() -> None:
    global _cache
    _cache = None
