#!/usr/bin/env python3
"""One-off ad hoc bet: buy a single outcome token at a limit price.

Validates the live ask before paying (aborts if it moved up past --max-price),
dry-runs by default, and prints the fill. Real order requires --execute.

Example:
  python scripts/place_adhoc_bet.py --token <id> --usdc 75 --price 0.27 --execute
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

from polybot.client.clob import CLOBClient  # noqa: E402
from polybot.models.types import OrderRequest, OrderType, Side  # noqa: E402


def live_best_ask(token: str) -> float | None:
    """Best ask from the public CLOB REST book (the authed client wrapper
    returns a dict the bot's get_order_book mis-parses, so go straight to REST)."""
    url = f"https://clob.polymarket.com/book?token_id={token}"
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        book = json.loads(r.read())
    asks = [float(a["price"]) for a in book.get("asks", [])]
    return min(asks) if asks else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True)
    ap.add_argument("--usdc", type=float, required=True, help="dollars to spend")
    ap.add_argument("--price", type=float, required=True, help="limit price per share")
    ap.add_argument("--max-price", type=float, default=None,
                    help="abort if live best ask exceeds this (default: --price)")
    ap.add_argument("--execute", action="store_true", help="place the real order")
    args = ap.parse_args()
    max_price = args.max_price if args.max_price is not None else args.price

    clob = CLOBClient()
    clob.connect()

    ask = live_best_ask(args.token)
    print(f"live best ask: {ask}")
    if ask is None:
        print("ABORT: no asks on the book"); return
    if ask > max_price:
        print(f"ABORT: best ask {ask} > max_price {max_price} — not overpaying"); return

    tick = clob.get_tick_size(args.token)
    shares = (Decimal(str(args.usdc)) / Decimal(str(args.price))).quantize(Decimal("0.01"))
    cost = (shares * Decimal(str(args.price))).quantize(Decimal("0.0001"))
    print(f"tick={tick}  price={args.price}  shares={shares}  est_cost=${cost}")

    order = OrderRequest(
        token_id=args.token,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        size=shares,
        limit_price=Decimal(str(args.price)),
    )

    if not args.execute:
        print("DRY RUN — pass --execute to place for real")
        clob.place_order(order, dry_run=True)
        return

    held_before = clob.get_conditional_balance(args.token)
    print(f"shares held before: {held_before}")
    order_id = clob.place_order(order, dry_run=False)
    print(f"order_id: {order_id}")
    if order_id:
        info = clob.get_order(order_id)
        if info:
            print(f"status={info.get('status')} "
                  f"size_matched={info.get('size_matched')} "
                  f"price={info.get('price')}")
        held_after = clob.get_conditional_balance(args.token)
        print(f"shares held after: {held_after}  (filled ~{held_after - held_before})")


if __name__ == "__main__":
    main()
