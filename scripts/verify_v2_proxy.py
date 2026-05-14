"""Verify the bot's V2 CLOB wiring end-to-end.

Checks (in order):
  1. EOA derived from private key matches expectation
  2. L1 auth works (signs an EIP-712 auth message)
  3. L2 auth works (HMAC over API creds)
  4. Funder Safe's balance + allowance are visible to the CLOB
  5. There are no leftover V1 pre-migration orders
  6. The SDK can locally sign a V2 order for the configured funder

If all six pass, the bot's V2 wiring is sound. The only remaining failure mode
is the API rejecting a *posted* order due to proxy registration — that has to
be tested by actually placing one (`scripts/place_test_order.py`).
"""
from __future__ import annotations

import sys
from decimal import Decimal

from polybot.auth.wallet import get_clob_creds, get_private_key, load_env
from polybot.client.clob import DEPOSIT_WALLET

EXPECTED_EOA = "0x7d128C4e9199130fCb0e375476d544a107ab33c2"


def main() -> int:
    load_env()
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import (
        ApiCreds,
        AssetType,
        BalanceAllowanceParams,
        OrderArgsV2,
    )
    from py_clob_client_v2.constants import POLYGON
    from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

    creds = get_clob_creds()
    pk = get_private_key()

    client = ClobClient(
        "https://clob.polymarket.com",
        key=pk,
        chain_id=POLYGON,
        creds=ApiCreds(
            api_key=creds["api_key"],
            api_secret=creds["api_secret"],
            api_passphrase=creds["passphrase"],
        ),
        signature_type=SignatureTypeV2.POLY_1271,
        funder=DEPOSIT_WALLET,
    )

    ok = True

    addr = client.get_address()
    eoa_match = addr.lower() == EXPECTED_EOA.lower()
    print(f"[{'OK' if eoa_match else 'FAIL'}] EOA = {addr}  (expected {EXPECTED_EOA})")
    ok &= eoa_match

    try:
        client.assert_level_1_auth()
        print("[OK]   L1 auth (EIP-712 signing)")
    except Exception as e:
        print(f"[FAIL] L1 auth: {e}")
        ok = False

    try:
        client.assert_level_2_auth()
        print("[OK]   L2 auth (HMAC creds)")
    except Exception as e:
        print(f"[FAIL] L2 auth: {e}")
        ok = False

    try:
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        bal = client.get_balance_allowance(params=params)
        b = Decimal(str(bal.get("balance", 0))) / Decimal("1e6")
        a = Decimal(str(bal.get("allowance", 0))) / Decimal("1e6")
        print(f"[OK]   funder {DEPOSIT_WALLET} → balance ${b} pUSD, allowance ${a}")
    except Exception as e:
        print(f"[FAIL] get_balance_allowance for funder: {e}")
        ok = False

    try:
        pre = client.get_pre_migration_orders()
        n = len(pre) if pre else 0
        print(f"[{'OK' if n == 0 else 'WARN'}] pre-migration V1 orders: {n}")
    except Exception as e:
        print(f"[WARN] pre-migration check: {e}")

    try:
        markets = client.get_sampling_simplified_markets()
        sample_token = None
        for m in (markets.get("data") if isinstance(markets, dict) else markets) or []:
            for tok in m.get("tokens", []):
                if tok.get("token_id"):
                    sample_token = tok["token_id"]
                    break
            if sample_token:
                break
        if not sample_token:
            print("[WARN] couldn't find a sample token to test signing")
        else:
            signed = client.create_order(
                OrderArgsV2(
                    token_id=str(sample_token),
                    price=0.01,
                    size=5,
                    side="BUY",
                )
            )
            sig = getattr(signed, "signature", None) or (signed.get("signature") if isinstance(signed, dict) else None)
            print(f"[OK]   locally signed a V2 order (sig prefix: {str(sig)[:18]}...)")
    except Exception as e:
        print(f"[FAIL] local order signing: {e}")
        ok = False

    print()
    print("All wiring checks passed." if ok else "One or more checks failed — see above.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
