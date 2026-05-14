"""One-shot: approve USDC for Polymarket NegRisk contracts.

Run from the polymarket-bot project root:
    .venv/bin/python scripts/fix_negrisk_allowance.py
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

USDC = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
NEGRISK_EXCHANGE = Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a")
NEGRISK_ADAPTER  = Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296")
MAX_UINT256 = (1 << 256) - 1

ERC20_ABI = [{
    "name": "approve", "type": "function", "stateMutability": "nonpayable",
    "inputs":  [{"name": "spender", "type": "address"},
                {"name": "amount",  "type": "uint256"}],
    "outputs": [{"name": "", "type": "bool"}],
}, {
    "name": "allowance", "type": "function", "stateMutability": "view",
    "inputs":  [{"name": "owner",   "type": "address"},
                {"name": "spender", "type": "address"}],
    "outputs": [{"name": "", "type": "uint256"}],
}]


def main() -> int:
    load_dotenv()
    pk = os.getenv("POLYGON_PRIVATE_KEY")
    if not pk:
        print("POLYGON_PRIVATE_KEY missing from environment / .env", file=sys.stderr)
        return 1

    rpc = os.getenv("POLYGON_RPC_URL", "https://polygon.drpc.org")
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        print(f"RPC unreachable: {rpc}", file=sys.stderr)
        return 1

    acct = Account.from_key(pk)
    print(f"signer:  {acct.address}")
    print(f"network: chainId={w3.eth.chain_id}, latest block={w3.eth.block_number}")
    print(f"balance: {w3.from_wei(w3.eth.get_balance(acct.address), 'ether'):.4f} POL/MATIC\n")

    usdc = w3.eth.contract(address=USDC, abi=ERC20_ABI)

    targets = [
        ("NegRisk CTF Exchange", NEGRISK_EXCHANGE),
        ("NegRisk Adapter",       NEGRISK_ADAPTER),
    ]

    nonce = w3.eth.get_transaction_count(acct.address)
    for name, spender in targets:
        current = usdc.functions.allowance(acct.address, spender).call()
        print(f"[{name}] current allowance: {current}")
        if current >= MAX_UINT256 // 2:
            print("  already effectively unlimited — skipping\n")
            continue

        tx = usdc.functions.approve(spender, MAX_UINT256).build_transaction({
            "chainId": w3.eth.chain_id,
            "from":    acct.address,
            "nonce":   nonce,
            "gas":     80_000,
            "maxFeePerGas":         w3.to_wei(80, "gwei"),
            "maxPriorityFeePerGas": w3.to_wei(35, "gwei"),
        })
        signed = acct.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        print(f"  sent: {tx_hash.hex()}  (waiting for receipt...)")
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        ok = receipt.status == 1
        print(f"  block={receipt.blockNumber}  status={'OK' if ok else 'REVERTED'}\n")
        if not ok:
            return 2
        nonce += 1

    print("Done. Restart the bot (or wait <30s for its 30s balance cache to expire)")
    print("and check that `balance_refreshed` logs go back to nonzero.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
