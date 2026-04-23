"""Auto-redeem resolved Polymarket positions and sync CLOB balance.

Calls redeemPositions() on the Gnosis CTF contract for each winning position,
then triggers update_balance_allowance so the CLOB sees the recovered USDC.
"""
from __future__ import annotations

import time
from typing import Optional

import httpx
import structlog
from web3 import Web3

logger = structlog.get_logger()

_CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_POLYGON_RPC = "https://1rpc.io/matic"
_DATA_API = "https://data-api.polymarket.com"

_CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


def _fetch_redeemable(address: str) -> list[dict]:
    try:
        r = httpx.get(
            f"{_DATA_API}/positions?user={address}&sizeThreshold=0",
            timeout=10,
        )
        return [p for p in r.json() if p.get("redeemable") and p.get("curPrice", 0) == 1]
    except Exception as e:
        logger.warning("redeem_fetch_failed", error=str(e))
        return []


def redeem_resolved_positions(private_key: str, clob_client) -> int:
    """Redeem all winning positions on-chain, then sync CLOB. Returns count redeemed."""
    w3 = Web3(Web3.HTTPProvider(_POLYGON_RPC))
    account = w3.eth.account.from_key(private_key)
    address = account.address

    collateral = clob_client.get_collateral_address()
    ctf = w3.eth.contract(address=_CTF_ADDRESS, abi=_CTF_ABI)

    positions = _fetch_redeemable(address)
    if not positions:
        logger.info("no_redeemable_positions")
        return 0

    redeemed = 0
    for pos in positions:
        condition_id = pos["conditionId"]
        outcome_index = pos.get("outcomeIndex", 0)
        # For binary markets: outcome 0 → indexSet 1 (bit 0), outcome 1 → indexSet 2 (bit 1)
        index_set = 1 << outcome_index

        try:
            nonce = w3.eth.get_transaction_count(address)
            gas_price = w3.eth.gas_price

            tx = ctf.functions.redeemPositions(
                w3.to_checksum_address(collateral),
                b"\x00" * 32,  # parentCollectionId = bytes32(0)
                bytes.fromhex(condition_id[2:]),
                [index_set],
            ).build_transaction({
                "from": address,
                "nonce": nonce,
                "gas": 150_000,
                "gasPrice": int(gas_price * 1.1),
                "chainId": 137,  # Polygon
            })

            signed = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status == 1:
                logger.info(
                    "position_redeemed",
                    condition_id=condition_id,
                    size=pos.get("size"),
                    tx=tx_hash.hex(),
                )
                redeemed += 1
            else:
                logger.warning("redeem_tx_failed", condition_id=condition_id, tx=tx_hash.hex())

            time.sleep(2)  # avoid nonce collisions between transactions

        except Exception as e:
            logger.error("redeem_error", condition_id=condition_id, error=str(e))

    if redeemed > 0:
        # Sync CLOB so it sees the recovered USDC
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            clob_client.update_balance_allowance(params=params)
            logger.info("clob_balance_synced_after_redeem", redeemed=redeemed)
        except Exception as e:
            logger.warning("clob_sync_failed", error=str(e))

    return redeemed


def maybe_redeem(private_key: str, clob_client) -> Optional[int]:
    """Non-blocking wrapper — swallows errors so it never breaks lifecycle."""
    try:
        return redeem_resolved_positions(private_key, clob_client)
    except Exception as e:
        logger.warning("redeem_skipped", error=str(e))
        return None
