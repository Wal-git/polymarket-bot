"""Swap all EOA USDC.e -> pUSD via LI.FI/SushiSwap, depositing directly to the Safe.

Two on-chain txs:
  1. Approve LI.FI diamond (`0x1231DEB6...`) to spend the full USDC.e balance
  2. Call the swap; pUSD delivered to the Safe (per `toAddress` in the quote)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

load_dotenv(Path(".env"))

EOA    = "0x7d128C4e9199130fCb0e375476d544a107ab33c2"
SAFE   = "0x05586e36b88f12d0a5287eecadba6f6a5f55e439"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
PUSD   = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
RPC    = "https://1rpc.io/matic"

ERC20_ABI = [
    {"inputs": [{"name": "a", "type": "address"}], "name": "balanceOf",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "s", "type": "address"}, {"name": "o", "type": "address"}],
     "name": "allowance",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve",
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
]


def get_quote(amount: int) -> dict:
    r = httpx.get("https://li.quest/v1/quote", params={
        "fromChain": "POL",
        "toChain": "POL",
        "fromToken": USDC_E,
        "toToken": PUSD,
        "fromAmount": str(amount),
        "fromAddress": EOA,
        "toAddress":   SAFE,
        "slippage": "0.01",
    }, timeout=30)
    r.raise_for_status()
    return r.json()


def main(dry_run: bool) -> int:
    w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 20}))
    if not w3.is_connected():
        print("RPC not connected"); return 1
    key = os.environ.get("POLYGON_PRIVATE_KEY")
    acct = Account.from_key(key)
    assert acct.address.lower() == EOA.lower()

    usdce = w3.eth.contract(address=w3.to_checksum_address(USDC_E), abi=ERC20_ABI)
    pusd  = w3.eth.contract(address=w3.to_checksum_address(PUSD),   abi=ERC20_ABI)

    eoa_usdce_before = usdce.functions.balanceOf(w3.to_checksum_address(EOA)).call()
    safe_pusd_before = pusd.functions.balanceOf(w3.to_checksum_address(SAFE)).call()
    print(f"EOA  USDC.e: {eoa_usdce_before / 1e6:.6f}")
    print(f"Safe pUSD  : {safe_pusd_before / 1e6:.6f}")
    if eoa_usdce_before == 0:
        print("Nothing to swap."); return 0

    quote = get_quote(eoa_usdce_before)
    spender = quote["estimate"]["approvalAddress"]
    to_amount     = int(quote["estimate"]["toAmount"])
    to_amount_min = int(quote["estimate"]["toAmountMin"])
    swap_to       = quote["transactionRequest"]["to"]
    swap_data     = quote["transactionRequest"]["data"]
    print(f"\nQuote: {quote.get('tool')}")
    print(f"  in:    {eoa_usdce_before / 1e6} USDC.e")
    print(f"  out:   {to_amount / 1e6} pUSD (min {to_amount_min / 1e6})")
    print(f"  spender: {spender}")
    print(f"  swap target: {swap_to}")

    nonce = w3.eth.get_transaction_count(acct.address)
    gas_price = int(w3.eth.gas_price * 1.15)

    # --- Tx 1: approve --------------------------------------------------
    cur_allowance = usdce.functions.allowance(
        w3.to_checksum_address(EOA), w3.to_checksum_address(spender)
    ).call()
    need_approve = cur_allowance < eoa_usdce_before
    if need_approve:
        approve_tx = usdce.functions.approve(
            w3.to_checksum_address(spender), eoa_usdce_before
        ).build_transaction({
            "from": acct.address, "nonce": nonce,
            "gasPrice": gas_price, "chainId": 137,
        })
        approve_tx["gas"] = int(w3.eth.estimate_gas(approve_tx) * 1.2)
        print(f"\nTx 1 (approve): gas={approve_tx['gas']}")
    else:
        print(f"\nApproval already sufficient ({cur_allowance / 1e6} USDC.e). Skipping.")

    # --- Tx 2: swap -----------------------------------------------------
    swap_tx = {
        "from": acct.address,
        "to": w3.to_checksum_address(swap_to),
        "value": int(quote["transactionRequest"].get("value", "0x0"), 16),
        "data": swap_data,
        "nonce": nonce + (1 if need_approve else 0),
        "gasPrice": gas_price,
        "chainId": 137,
    }
    # gas estimate will fail without approval; use the LI.FI hint
    gas_hint = int(quote["transactionRequest"].get("gasLimit", "0x80000"), 16)
    swap_tx["gas"] = int(gas_hint * 1.15)
    print(f"Tx 2 (swap):    gas={swap_tx['gas']} (LI.FI hint)")

    total_matic = ((approve_tx['gas'] if need_approve else 0) + swap_tx['gas']) * gas_price / 1e18
    print(f"\nTotal max gas cost: {total_matic:.6f} MATIC")

    if dry_run:
        print("\nDRY RUN — pass --send to execute")
        return 0

    if need_approve:
        signed = acct.sign_transaction(approve_tx)
        h1 = w3.eth.send_raw_transaction(signed.raw_transaction)
        print(f"\nApprove submitted: 0x{h1.hex()}")
        r1 = w3.eth.wait_for_transaction_receipt(h1, timeout=180)
        if r1.status != 1:
            print("Approve FAILED"); return 1
        print(f"  approved  block: {r1.blockNumber}")

    signed = acct.sign_transaction(swap_tx)
    h2 = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"Swap submitted:    0x{h2.hex()}")
    r2 = w3.eth.wait_for_transaction_receipt(h2, timeout=240)
    print(f"  status: {'SUCCESS' if r2.status == 1 else 'FAILED'}  block: {r2.blockNumber}")
    if r2.status != 1:
        return 1

    eoa_usdce_after = usdce.functions.balanceOf(w3.to_checksum_address(EOA)).call()
    safe_pusd_after = pusd.functions.balanceOf(w3.to_checksum_address(SAFE)).call()
    gained = (safe_pusd_after - safe_pusd_before) / 1e6
    print(f"\nEOA  USDC.e after: {eoa_usdce_after / 1e6:.6f}")
    print(f"Safe pUSD  after:  {safe_pusd_after / 1e6:.6f}  (gained {gained})")
    return 0


if __name__ == "__main__":
    sys.exit(main(dry_run="--send" not in sys.argv))
