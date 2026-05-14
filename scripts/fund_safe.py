"""Transfer all EOA pUSD into the registered funder Safe.

Safe `0x05586e36...` is a Gnosis Safe v1.3.0 owned by the EOA (1-of-1).
Sending pUSD to it is a plain ERC20 transfer — no Safe-signing dance needed
(that's only required to move funds *out* of the Safe).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

load_dotenv(Path(".env"))

EOA  = "0x7d128C4e9199130fCb0e375476d544a107ab33c2"
SAFE = "0x05586e36b88f12d0a5287eecadba6f6a5f55e439"
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
RPC  = "https://1rpc.io/matic"

ERC20_ABI = [
    {"inputs": [{"name": "a", "type": "address"}], "name": "balanceOf",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "transfer",
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
]


def main(dry_run: bool) -> int:
    w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 20}))
    if not w3.is_connected():
        print("RPC not connected"); return 1

    key = os.environ.get("POLYGON_PRIVATE_KEY")
    if not key:
        print("POLYGON_PRIVATE_KEY missing"); return 1
    acct = Account.from_key(key)
    if acct.address.lower() != EOA.lower():
        print(f"Key mismatch: expected {EOA}, got {acct.address}"); return 1

    pusd = w3.eth.contract(address=w3.to_checksum_address(PUSD), abi=ERC20_ABI)
    eoa_pusd  = pusd.functions.balanceOf(w3.to_checksum_address(EOA)).call()
    safe_pusd = pusd.functions.balanceOf(w3.to_checksum_address(SAFE)).call()
    print(f"EOA  pUSD: {eoa_pusd / 1e6:.6f}")
    print(f"Safe pUSD: {safe_pusd / 1e6:.6f}")

    if eoa_pusd == 0:
        print("Nothing to move."); return 0

    nonce = w3.eth.get_transaction_count(acct.address)
    gas_price = int(w3.eth.gas_price * 1.15)
    tx = pusd.functions.transfer(w3.to_checksum_address(SAFE), eoa_pusd).build_transaction({
        "from": acct.address,
        "nonce": nonce,
        "gasPrice": gas_price,
        "chainId": 137,
    })
    tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.2)
    print(f"\nPlanned tx:")
    print(f"  transfer({SAFE}, {eoa_pusd / 1e6:.6f} pUSD)")
    print(f"  gas: {tx['gas']}  gasPrice: {gas_price / 1e9:.2f} gwei  cost: {tx['gas'] * gas_price / 1e18:.6f} MATIC")

    if dry_run:
        print("\nDRY RUN — pass --send to execute")
        return 0

    signed = acct.sign_transaction(tx)
    txh = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"\nSubmitted: 0x{txh.hex()}")
    rcpt = w3.eth.wait_for_transaction_receipt(txh, timeout=180)
    print(f"Status: {'SUCCESS' if rcpt.status == 1 else 'FAILED'}  block: {rcpt.blockNumber}")
    eoa_after  = pusd.functions.balanceOf(w3.to_checksum_address(EOA)).call() / 1e6
    safe_after = pusd.functions.balanceOf(w3.to_checksum_address(SAFE)).call() / 1e6
    print(f"After — EOA pUSD: {eoa_after}  Safe pUSD: {safe_after}")
    return 0 if rcpt.status == 1 else 1


if __name__ == "__main__":
    sys.exit(main(dry_run="--send" not in sys.argv))
