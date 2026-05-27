"""Place a bet on any Polymarket market by slug.

Usage:
    python quick_bet.py <slug> <yes|no> <usd> [--send]

Examples:
    python quick_bet.py will-btc-hit-100k-2026 yes 50
    python quick_bet.py will-btc-hit-100k-2026 no 100 --send

Dry-run by default. Pass --send to execute.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from decimal import Decimal
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(".env"))

sys.path.insert(0, "src")
from polybot.client.clob import CLOBClient
from polybot.models.types import OrderRequest, Side

GAMMA_URL = "https://gamma-api.polymarket.com/markets"


def fetch_market(slug: str) -> dict | None:
    r = requests.get(GAMMA_URL, params={"slug": slug, "limit": 1}, timeout=10)
    r.raise_for_status()
    markets = r.json()
    if not markets:
        print(f"No market found for slug '{slug}'.")
        return None
    return markets[0]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("slug", help="Market slug (last part of the Polymarket URL)")
    p.add_argument("side", choices=["yes", "no"], help="Outcome to buy")
    p.add_argument("usd", help="USD to spend (number or 'max')")
    p.add_argument("--send", action="store_true", help="Actually place the order")
    args = p.parse_args()

    market = fetch_market(args.slug)
    if not market:
        return 1

    outcomes = json.loads(market.get("outcomes", "[]"))
    token_ids = json.loads(market.get("clobTokenIds", "[]"))
    prices = json.loads(market.get("outcomePrices", "[]"))

    if len(outcomes) < 2:
        print(f"Market has unexpected outcomes: {outcomes}")
        return 1

    idx = 0 if args.side == "yes" else 1
    outcome = outcomes[idx]
    token_id = token_ids[idx]
    quoted_price = Decimal(str(prices[idx]))

    print(f"Market:  {market.get('question', '').strip()}")
    print(f"Side:    BUY {outcome} ({args.side.upper()})")
    print(f"Quoted:  ${quoted_price}")

    c = CLOBClient()
    c.connect()

    book = c.client.get_order_book(token_id)
    asks_raw = book.get("asks", []) if isinstance(book, dict) else (getattr(book, "asks", []) or [])
    asks = sorted(
        [{"price": str(a.get("price") if isinstance(a, dict) else a.price),
          "size":  str(a.get("size")  if isinstance(a, dict) else a.size)} for a in asks_raw],
        key=lambda x: float(x["price"]),
    )
    if not asks:
        print("No asks in the order book — can't buy this side right now.")
        return 1

    best_ask = Decimal(asks[0]["price"])
    bal = c.get_balance()
    print(f"Ask:     ${best_ask}  (top size {asks[0]['size']})")
    print(f"Balance: ${bal}")

    if args.usd.lower() == "max":
        budget = bal - Decimal("0.50")
    else:
        budget = Decimal(args.usd)
    if budget > bal:
        print(f"Requested ${budget} > balance ${bal}.")
        return 1
    if budget <= 0:
        print("Budget is non-positive.")
        return 1

    target_size = (budget / best_ask).quantize(Decimal("1"), rounding="ROUND_DOWN")
    if target_size < 1:
        print(f"Order size too small ({target_size} shares).")
        return 1

    # Walk the book to find the limit price that covers our size
    cum = Decimal("0")
    limit_price = best_ask
    for a in asks:
        cum += Decimal(str(a["size"]))
        limit_price = Decimal(str(a["price"]))
        if cum >= target_size:
            break

    notional = target_size * limit_price
    payout = target_size * Decimal("1")
    potential_pnl = payout - notional

    print(f"\nOrder:   BUY {target_size} {outcome} @ ${limit_price}")
    print(f"Cost:    ${notional:.2f}")
    print(f"Payout:  ${payout:.2f} if {outcome}")
    print(f"Profit:  ${potential_pnl:.2f} if {outcome}")

    if not args.send:
        print("\nDRY RUN — pass --send to execute")
        return 0

    order = OrderRequest(
        token_id=token_id,
        side=Side.BUY,
        size=target_size,
        limit_price=limit_price,
    )
    print("\n-> Placing order...")
    order_id = c.place_order(order, dry_run=False)
    if not order_id or order_id == "unknown":
        print("Order rejected.")
        return 1
    print(f"   Order ID: {order_id}")

    time.sleep(3)
    info = c.get_order(order_id)
    if info:
        print(f"   Status: {info.get('status')}  Matched: {info.get('size_matched')}/{info.get('original_size')}")
    else:
        print("   Could not fetch order status.")

    bal_after = c.get_balance()
    shares = c.get_conditional_balance(token_id)
    print(f"\nBalance: ${bal_after}  |  {outcome} shares held: {shares}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
