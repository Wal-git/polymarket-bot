"""End-to-end V2 CLOB live test: place a tiny resting bid, verify, cancel.

Order is intentionally well below the spread (1¢ in a market where the best bid
is much higher) so it won't fill. Used to confirm the V2 SDK + POLY_GNOSIS_SAFE
+ funder Safe path actually submits and cancels orders against the live CLOB.
"""
from __future__ import annotations

import os
import sys
import time
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(".env"))

sys.path.insert(0, "src")
from polybot.client.clob import CLOBClient
from polybot.models.types import OrderRequest, Side

# Liquid binary market — "Yes" token; current price ~16.5¢
TOKEN_ID = "78433024518676680431174478322854148606578065650008220678402966840627347604025"
TEST_PRICE = Decimal("0.01")
TEST_SIZE  = Decimal("5")


def main(dry_run: bool) -> int:
    c = CLOBClient(); c.connect()

    book = c.client.get_order_book(TOKEN_ID)
    bids = book.get("bids", []) if isinstance(book, dict) else (getattr(book, "bids", []) or [])
    asks = book.get("asks", []) if isinstance(book, dict) else (getattr(book, "asks", []) or [])
    best_bid = bids[-1]["price"] if bids else "?"
    best_ask = asks[0]["price"]  if asks else "?"
    print(f"Market state: best_bid={best_bid}  best_ask={best_ask}")
    print(f"Test order:   BUY {TEST_SIZE} @ ${TEST_PRICE} (max risk ${TEST_PRICE * TEST_SIZE})")

    bal = c.get_balance()
    print(f"Safe pUSD:    {bal}")
    if bal < TEST_PRICE * TEST_SIZE:
        print("Insufficient balance"); return 1

    order = OrderRequest(
        token_id=TOKEN_ID,
        side=Side.BUY,
        size=TEST_SIZE,
        limit_price=TEST_PRICE,
    )
    if dry_run:
        print("\nDRY RUN — pass --send to execute")
        return 0

    print("\n→ Placing order...")
    order_id = c.place_order(order, dry_run=False)
    if not order_id or order_id == "unknown":
        print(f"  No order id returned (got {order_id!r}) — order may have been rejected")
        return 1
    print(f"  order_id: {order_id}")

    time.sleep(2)
    print("→ Verifying via get_order...")
    try:
        info = c.client.get_order(order_id)
        print(f"  status: {info.get('status') if isinstance(info, dict) else info}")
    except Exception as e:
        print(f"  get_order error: {e}")

    print("→ Canceling...")
    try:
        resp = c.client.cancel_orders([order_id])
        print(f"  cancel resp: {resp}")
    except Exception as e:
        print(f"  cancel failed: {e}")
        return 1

    time.sleep(2)
    try:
        info = c.client.get_order(order_id)
        print(f"  status after cancel: {info.get('status') if isinstance(info, dict) else info}")
    except Exception as e:
        print(f"  get_order error after cancel: {e}")

    bal_after = c.get_balance()
    print(f"\nSafe pUSD after: {bal_after}")
    return 0


if __name__ == "__main__":
    sys.exit(main(dry_run="--send" not in sys.argv))
