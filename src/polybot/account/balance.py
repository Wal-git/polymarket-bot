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

# pUSD — Polymarket's own stablecoin. The V2 exchange debits this directly for buys.
# USDC.e lands in the EOA after CTF redemption but is NOT automatically usable for
# new trades — it must be converted to pUSD via the Polymarket website deposit flow.
_PUSD_ADDRESS   = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
_USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_TOKEN_ABI = [{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]


def _on_chain_pusd(address: str) -> Decimal:
    """Read pUSD on-chain — the only token the V2 exchange actually debits."""
    try:
        from web3 import Web3
        from requests.adapters import HTTPAdapter
        session = __import__("requests").Session()
        session.mount("https://", HTTPAdapter())
        w3 = Web3(Web3.HTTPProvider("https://polygon.drpc.org", request_kwargs={"timeout": 8}, session=session))
        tok = w3.eth.contract(address=w3.to_checksum_address(_PUSD_ADDRESS), abi=_TOKEN_ABI)
        raw = tok.functions.balanceOf(w3.to_checksum_address(address)).call()
        usdc_e = w3.eth.contract(address=w3.to_checksum_address(_USDC_E_ADDRESS), abi=_TOKEN_ABI)
        usdc_e_raw = usdc_e.functions.balanceOf(w3.to_checksum_address(address)).call()
        if usdc_e_raw > 0:
            logger.info("usdc_e_pending_conversion", usdc_e=str(Decimal(usdc_e_raw) / Decimal("1e6")))
        return Decimal(raw) / Decimal("1e6")
    except Exception as e:
        logger.warning("on_chain_balance_failed", error=str(e))
        return Decimal("0")


def get_usdc_balance(clob: CLOBClient) -> Decimal:
    """Return tradeable pUSD balance, cached for 30s.

    Only pUSD is directly debited by the V2 CLOB exchange for buy orders.
    USDC.e sitting in the EOA after redemption is logged as 'usdc_e_pending_conversion'
    but not counted in the bankroll until converted via the Polymarket deposit flow.
    """
    global _cache
    now = time.time()
    if _cache is not None and now - _cache[0] < _CACHE_TTL:
        return _cache[1]
    clob_balance = clob.get_balance()
    address = clob.client.get_address()
    on_chain = _on_chain_pusd(address)
    # Use on-chain value when it's fresher/larger (CLOB balance can lag after redemption sync)
    balance = max(clob_balance, on_chain)
    portfolio = _fetch_portfolio_value(address)
    _cache = (now, balance)
    logger.info("balance_refreshed", usdc=str(balance), clob=str(clob_balance), on_chain_pusd=str(on_chain))
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
