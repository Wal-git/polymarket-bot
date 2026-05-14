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

_CTF_ADDRESS    = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_ADAPTER        = "0xAdA100Db00Ca00073811820692005400218FcE1f"  # CtfCollateralAdapter (official)
_PUSD_ADDRESS   = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
_USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_POLYGON_RPC = "https://polygon.drpc.org"
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
    },
    {
        "inputs": [{"name": "owner", "type": "address"}, {"name": "id", "type": "uint256"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

_ADAPTER_ABI = [
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
    },
]

_BAL_ABI = [{"inputs":[{"name":"a","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]



def _fetch_redeemable(address: str) -> list[dict]:
    try:
        r = httpx.get(
            f"{_DATA_API}/positions?user={address}&sizeThreshold=0",
            timeout=10,
        )
        return [p for p in r.json() if p.get("redeemable")]
    except Exception as e:
        logger.warning("redeem_fetch_failed", error=str(e))
        return []


def fetch_outcomes(address: str, slugs: list[str]) -> list[dict]:
    """Return win/loss outcome records for a list of slugs."""
    try:
        r = httpx.get(
            f"{_DATA_API}/positions?user={address}&sizeThreshold=0",
            timeout=10,
        )
        positions = r.json()
        slug_set = set(slugs)
        results = []
        for p in positions:
            if p.get("eventSlug") in slug_set:
                won = p.get("curPrice", 0) == 1
                pnl = round(float(p.get("cashPnl", 0)), 4)
                results.append({
                    "slug": p["eventSlug"],
                    "won": won,
                    "pnl": pnl,
                    "shares": float(p.get("size", 0)),
                    "entry_price": float(p.get("avgPrice", 0)),
                    "payout": round(float(p.get("size", 0)) if won else 0.0, 4),
                })
        return results
    except Exception as e:
        logger.warning("fetch_outcomes_failed", error=str(e))
        return []


def _pusd_balance(w3: Web3, address: str) -> int:
    tok = w3.eth.contract(address=w3.to_checksum_address(_PUSD_ADDRESS), abi=_BAL_ABI)
    return tok.functions.balanceOf(w3.to_checksum_address(address)).call()


def _ensure_adapter_approved(w3: Web3, private_key: str, address: str, ctf) -> bool:
    """Grant the adapter setApprovalForAll on the CTF contract (one-time, idempotent)."""
    try:
        if ctf.functions.isApprovedForAll(
            w3.to_checksum_address(address),
            w3.to_checksum_address(_ADAPTER),
        ).call():
            return True
        nonce = w3.eth.get_transaction_count(address)
        tx = ctf.functions.setApprovalForAll(
            w3.to_checksum_address(_ADAPTER), True
        ).build_transaction({
            "from": address, "nonce": nonce,
            "gas": 60_000, "gasPrice": int(w3.eth.gas_price * 1.1), "chainId": 137,
        })
        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt.status == 1:
            logger.info("adapter_approved", adapter=_ADAPTER, tx=tx_hash.hex())
            return True
        logger.warning("adapter_approval_failed", tx=tx_hash.hex())
        return False
    except Exception as e:
        logger.warning("adapter_approval_error", error=str(e))
        return False


def _try_redeem_via_adapter(w3: Web3, private_key: str, address: str,
                             condition_id: str, outcome_index: int) -> bool:
    """Redeem via the USDC.e→pUSD adapter.

    The adapter pulls CTF tokens using setApprovalForAll, calls redeemPositions
    on the CTF (receiving USDC.e), wraps to pUSD, and sends pUSD to the caller.
    Returns True if pUSD balance increased.
    """
    pusd_before = _pusd_balance(w3, address)
    try:
        adapter = w3.eth.contract(
            address=w3.to_checksum_address(_ADAPTER), abi=_ADAPTER_ABI
        )
        index_set = 1 << outcome_index
        nonce = w3.eth.get_transaction_count(address)
        tx = adapter.functions.redeemPositions(
            w3.to_checksum_address(_USDC_E_ADDRESS),
            b"\x00" * 32,
            bytes.fromhex(condition_id[2:]),
            [index_set],
        ).build_transaction({
            "from": address, "nonce": nonce,
            "gas": 300_000, "gasPrice": int(w3.eth.gas_price * 1.1), "chainId": 137,
        })
        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt.status != 1:
            logger.warning("adapter_redeem_tx_failed", condition_id=condition_id, tx=tx_hash.hex())
            return False
        pusd_after = _pusd_balance(w3, address)
        gained = (pusd_after - pusd_before) / 1e6
        if gained > 0:
            logger.info("position_redeemed_via_adapter", condition_id=condition_id,
                        pusd_gained=gained, tx=tx_hash.hex())
            return True
        logger.warning("adapter_no_pusd_gained", condition_id=condition_id, tx=tx_hash.hex())
        return False
    except Exception as e:
        logger.warning("adapter_redeem_error", condition_id=condition_id, error=str(e))
        return False


def redeem_resolved_positions(private_key: str, clob_client) -> tuple[int, list[dict]]:
    """Redeem winning positions on-chain, sync CLOB. Returns (count, outcome_list).

    Strategy (in priority order for each winning position):
    1. Adapter (0xADa100874d...) redeemPositions with USDC.e collateral — adapter
       pulls CTF tokens via setApprovalForAll, redeems for USDC.e, wraps to pUSD,
       and sends pUSD directly to the caller. No manual conversion needed.
    2. Direct redeemPositions() fallback on the CTF — gives USDC.e which sits in
       the EOA and requires a manual Polymarket deposit to become tradeable pUSD.
    """
    w3 = Web3(Web3.HTTPProvider(_POLYGON_RPC))
    account = w3.eth.account.from_key(private_key)
    address = account.address

    ctf = w3.eth.contract(address=_CTF_ADDRESS, abi=_CTF_ABI)

    # Ensure adapter is approved once per session (no-op if already set)
    _ensure_adapter_approved(w3, private_key, address, ctf)

    all_positions = _fetch_redeemable(address)
    if not all_positions:
        logger.info("no_redeemable_positions")
        return 0, []

    outcomes = []
    for pos in all_positions:
        won = pos.get("curPrice", 0) == 1
        outcomes.append({
            "slug": pos.get("eventSlug", ""),
            "won": won,
            "pnl": round(float(pos.get("cashPnl", 0)), 4),
            "shares": float(pos.get("size", 0)),
            "entry_price": float(pos.get("avgPrice", 0)),
        })

    winning_positions = [p for p in all_positions if p.get("curPrice", 0) == 1]
    losing_positions  = [p for p in all_positions if p.get("curPrice", 0) == 0]
    redeemed = 0

    # Clear losing positions first — burns $0-value CTF tokens so they stop
    # appearing as "unredeemed" in the portfolio. No payout expected.
    for pos in losing_positions:
        condition_id = pos.get("conditionId")
        outcome_index = pos.get("outcomeIndex", 0)
        index_set = 1 << outcome_index
        asset_id = pos.get("asset")
        if not asset_id or not condition_id:
            continue
        on_chain_bal = ctf.functions.balanceOf(
            w3.to_checksum_address(address), int(asset_id)
        ).call()
        if on_chain_bal == 0:
            continue  # already cleared
        try:
            nonce = w3.eth.get_transaction_count(address)
            tx = ctf.functions.redeemPositions(
                w3.to_checksum_address(_USDC_E_ADDRESS),
                b"\x00" * 32,
                bytes.fromhex(condition_id[2:]),
                [index_set],
            ).build_transaction({
                "from": address, "nonce": nonce,
                "gas": 100_000, "gasPrice": int(w3.eth.gas_price * 1.1), "chainId": 137,
            })
            signed = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt.status == 1:
                logger.info("losing_position_cleared", condition_id=condition_id,
                            slug=pos.get("eventSlug"), tx=tx_hash.hex())
            else:
                logger.warning("losing_clear_failed", condition_id=condition_id, tx=tx_hash.hex())
        except Exception as e:
            logger.warning("losing_clear_error", condition_id=condition_id, error=str(e))
        time.sleep(2)

    for pos in winning_positions:
        condition_id = pos["conditionId"]
        outcome_index = pos.get("outcomeIndex", 0)
        index_set = 1 << outcome_index
        asset_id = pos.get("asset")

        # Skip if no on-chain tokens (nothing to redeem)
        if asset_id:
            on_chain_bal = ctf.functions.balanceOf(
                w3.to_checksum_address(address), int(asset_id)
            ).call()
            if on_chain_bal == 0:
                continue

        # --- Path 1: adapter.redeemPositions(USDC.e) → pUSD ---
        if _try_redeem_via_adapter(w3, private_key, address, condition_id, outcome_index):
            redeemed += 1
            time.sleep(2)
            continue

        # --- Path 2: direct CTF redeemPositions() fallback (USDC.e) ---
        for attempt in range(4):
            delay = 2 ** attempt
            try:
                nonce = w3.eth.get_transaction_count(address)
                tx = ctf.functions.redeemPositions(
                    w3.to_checksum_address(_USDC_E_ADDRESS),
                    b"\x00" * 32,
                    bytes.fromhex(condition_id[2:]),
                    [index_set],
                ).build_transaction({
                    "from": address, "nonce": nonce,
                    "gas": 150_000, "gasPrice": int(w3.eth.gas_price * 1.1), "chainId": 137,
                })
                signed = w3.eth.account.sign_transaction(tx, private_key)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                if receipt.status == 1:
                    logger.info("position_redeemed_usdc_e", condition_id=condition_id,
                                size=pos.get("size"), tx=tx_hash.hex())
                    redeemed += 1
                else:
                    logger.warning("redeem_tx_failed", condition_id=condition_id, tx=tx_hash.hex())
                break
            except Exception as e:
                if attempt < 3:
                    logger.warning("redeem_retry", condition_id=condition_id,
                                   attempt=attempt + 1, delay=delay, error=str(e))
                    time.sleep(delay)
                else:
                    logger.error("redeem_error", condition_id=condition_id, error=str(e))

        time.sleep(2)

    if redeemed > 0:
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            clob_client.update_balance_allowance(params=params)
            logger.info("clob_balance_synced_after_redeem", redeemed=redeemed)
        except Exception as e:
            logger.warning("clob_sync_failed", error=str(e))

    return redeemed, outcomes


def maybe_redeem(private_key: str, clob_client) -> tuple[int, list[dict]]:
    """Non-blocking wrapper — swallows errors so it never breaks lifecycle."""
    try:
        return redeem_resolved_positions(private_key, clob_client)
    except Exception as e:
        logger.warning("redeem_skipped", error=str(e))
        return 0, []


_GAMMA_BASE = "https://gamma-api.polymarket.com"


def _outcome_from_prices(prices) -> Optional[str]:
    """Map gamma-api outcomePrices to UP/DOWN. None when unresolved/pushed."""
    import json as _json
    if isinstance(prices, str):
        try:
            prices = _json.loads(prices)
        except _json.JSONDecodeError:
            return None
    if not isinstance(prices, list) or len(prices) != 2:
        return None
    try:
        a, b = float(prices[0]), float(prices[1])
    except (TypeError, ValueError):
        return None
    if a == 1 and b == 0:
        return "UP"
    if a == 0 and b == 1:
        return "DOWN"
    return None


def reconcile_resolved_positions(tracker, grace_secs: int = 300) -> int:
    """Clean up state for positions whose slot has already resolved.

    The on-chain redeem flow only processes ``redeemable=true`` positions, which
    excludes losses (nothing to claim on-chain when shares = $0). Without a
    second cleanup path, lost positions accumulate in ``state.json`` forever
    after any bot restart that interrupted ``lifecycle.close_position``.

    For each tracked position whose slot end + ``grace_secs`` is in the past,
    query gamma-api for resolution. If resolved, ``emit_result`` (dedup'd by
    slug) and ``close_position``. Returns the number of positions cleaned.
    """
    from polybot.monitoring.event_log import emit_result
    from datetime import datetime, timezone

    now_ts = datetime.now(timezone.utc).timestamp()
    candidates = []
    for pos in tracker.positions:
        slug = pos.market_question
        try:
            slot_ts = int(slug.split("-")[-1])
        except (ValueError, AttributeError):
            continue
        if not slug.startswith(("btc-updown-5m-", "eth-updown-5m-")):
            continue
        # 5-min slots: open at slot_ts, close at slot_ts + 300
        slot_close_ts = slot_ts + 300
        if slot_close_ts + grace_secs > now_ts:
            continue  # too soon, let the slot finish + propagate
        candidates.append((slug, pos))

    if not candidates:
        return 0

    # Batch query gamma-api: it accepts repeated &slug= params (verified in
    # scripts/backfill_slot_history.py). 100 per batch is safe.
    cleaned = 0
    BATCH = 100
    for i in range(0, len(candidates), BATCH):
        batch = candidates[i : i + BATCH]
        slugs = [s for s, _ in batch]
        try:
            resp = httpx.get(
                f"{_GAMMA_BASE}/markets",
                params=[("slug", s) for s in slugs] + [("closed", "true"), ("limit", str(len(slugs) * 2))],
                timeout=15,
            )
            resp.raise_for_status()
            markets = {m.get("slug"): m for m in resp.json() if m.get("slug")}
        except Exception as e:
            logger.warning("reconcile_fetch_failed", error=str(e), batch_size=len(batch))
            continue

        for slug, pos in batch:
            m = markets.get(slug)
            if m is None:
                continue  # not yet in API as closed; try next cycle
            outcome = _outcome_from_prices(m.get("outcomePrices"))
            if outcome is None:
                continue  # unresolved or pushed; try next cycle
            won = (pos.outcome_label.upper() == outcome)
            shares = float(pos.shares)
            entry = float(pos.avg_entry_price)
            pnl = round(shares * (1.0 - entry), 4) if won else round(-shares * entry, 4)
            emit_result(
                slug=slug,
                won=won,
                pnl=pnl,
                shares=shares,
                entry_price=entry,
                exit_reason="HOLD_TO_RESOLUTION",
                exit_price=1.0 if won else 0.0,
                confidence=pos.confidence,
            )
            tracker.close_position(pos.token_id)
            cleaned += 1
            logger.info(
                "stale_position_reconciled",
                slug=slug,
                won=won,
                pnl=pnl,
                age_h=round((now_ts - (int(slug.split('-')[-1]) + 300)) / 3600, 1),
            )

    if cleaned:
        tracker.save()
    return cleaned
