"""Chainlink aggregator reader on Polygon mainnet.

The strategy thesis is "Chainlink lags exchange prices." This module reads the
latest round directly from the on-chain aggregator so we can quantify that lag
instead of inferring it.

Default address is the public Chainlink Polygon BTC/USD aggregator proxy. Pass
a different ``address`` to read other feeds (e.g. ETH/USD). The round cache
is keyed by aggregator address so concurrent BTC/ETH readers don't clobber.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import structlog
from web3 import Web3

from polybot.models.market import ChainlinkRound

logger = structlog.get_logger()

# Polygon mainnet BTC/USD aggregator proxy. EACAggregatorProxy → underlying.
DEFAULT_BTC_USD_AGGREGATOR = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
DEFAULT_RPC_URL = "https://1rpc.io/matic"

_AGGREGATOR_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Cache the web3 client + decimals between calls so we don't re-instantiate
# every poll. Decimals never change on a deployed aggregator.
_w3: Optional[Web3] = None
_decimals_cache: dict[str, int] = {}
_round_cache: dict[str, tuple[float, ChainlinkRound]] = {}  # address -> (fetched_at, round)
_CACHE_TTL_S = 5.0


def _get_web3(rpc_url: str) -> Web3:
    global _w3
    if _w3 is None or _w3.provider.endpoint_uri != rpc_url:
        _w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 5}))
    return _w3


def _get_decimals(w3: Web3, address: str) -> int:
    if address not in _decimals_cache:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(address), abi=_AGGREGATOR_ABI
        )
        _decimals_cache[address] = int(contract.functions.decimals().call())
    return _decimals_cache[address]


def fetch_chainlink_round_sync(
    rpc_url: str = DEFAULT_RPC_URL,
    address: str = DEFAULT_BTC_USD_AGGREGATOR,
) -> Optional[ChainlinkRound]:
    """Synchronous fetch — call from threads or via ``asyncio.to_thread``."""
    now = time.time()
    cached = _round_cache.get(address)
    if cached and now - cached[0] < _CACHE_TTL_S:
        return cached[1]

    try:
        w3 = _get_web3(rpc_url)
        decimals = _get_decimals(w3, address)
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(address), abi=_AGGREGATOR_ABI
        )
        round_id, answer, _started_at, updated_at, _answered_in = (
            contract.functions.latestRoundData().call()
        )
        price = float(answer) / (10 ** decimals)
        round_obj = ChainlinkRound(
            answer=price, updated_at=int(updated_at), round_id=int(round_id)
        )
        _round_cache[address] = (now, round_obj)
        return round_obj
    except Exception as e:
        logger.warning("chainlink_fetch_failed", error=str(e), address=address)
        return None


async def fetch_chainlink_round(
    rpc_url: str = DEFAULT_RPC_URL,
    address: str = DEFAULT_BTC_USD_AGGREGATOR,
) -> Optional[ChainlinkRound]:
    """Async wrapper — runs the sync RPC call in a thread so we don't block the loop."""
    return await asyncio.to_thread(fetch_chainlink_round_sync, rpc_url, address)


def reset_cache() -> None:
    """Test hook — clears the round cache."""
    global _w3
    _round_cache.clear()
    _decimals_cache.clear()
    _w3 = None
