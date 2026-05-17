"""Place a CLOB bet on any Polymarket market via the bot's trading wallet.

Usage:
    python place_bet.py <query> [--outcome <label>] [--usd <amount|max>] [--send]

Examples:
    python place_bet.py "Rousey Carano"
    python place_bet.py "Rousey Carano" --outcome Rousey --usd 10 --send
    python place_bet.py "Bitcoin 150k" --outcome No --usd max --send

Dry-run by default. Re-fetches the order book at send-time and sweeps asks to
guarantee a fill at the chosen USD size.
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

SEARCH_URL = "https://gamma-api.polymarket.com/public-search"


def search_market(query: str) -> dict | None:
    """Return the single best-matching active market, or None if ambiguous/missing."""
    r = requests.get(SEARCH_URL, params={"q": query, "limit_per_type": 10}, timeout=10)
    r.raise_for_status()
    events = r.json().get("events", [])
    candidates = []
    for ev in events:
        for m in ev.get("markets", []):
            if m.get("active") and not m.get("closed") and m.get("acceptingOrders", True):
                candidates.append(m)
    if not candidates:
        print(f"No active markets match '{query}'.")
        return None
    if len(candidates) > 1:
        print(f"Multiple active markets match '{query}'. Narrow your query:")
        for m in candidates[:10]:
            prices = json.loads(m.get("outcomePrices", "[]"))
            outs = json.loads(m.get("outcomes", "[]"))
            pretty = "  ".join(f"{o}@{p}" for o, p in zip(outs, prices))
            print(f"  - {m['question'].strip()}  [{pretty}]")
        return None
    return candidates[0]


def pick_outcome(market: dict, label: str | None) -> tuple[str, str, Decimal]:
    """Return (outcome_label, token_id, current_price) for the chosen side."""
    outcomes = json.loads(market["outcomes"])
    token_ids = json.loads(market["clobTokenIds"])
    prices = json.loads(market["outcomePrices"])
    if label is None:
        print(f"Market: {market['question'].strip()}")
        print("Outcomes:")
        for o, t, p in zip(outcomes, token_ids, prices):
            print(f"  - {o} @ ${p}  (token {t})")
        sys.exit(0)
    matches = [i for i, o in enumerate(outcomes) if label.lower() in o.lower()]
    if len(matches) != 1:
        print(f"Outcome '{label}' did not match exactly one of {outcomes}.")
        sys.exit(2)
    i = matches[0]
    return outcomes[i], token_ids[i], Decimal(str(prices[i]))


def sweep_for_size(asks: list[dict], target_size: Decimal) -> Decimal:
    """Walk the ask side until cumulative size >= target. Return the marginal price."""
    cum = Decimal("0")
    last = Decimal(str(asks[0]["price"]))
    for a in asks:
        cum += Decimal(str(a["size"]))
        last = Decimal(str(a["price"]))
        if cum >= target_size:
            return last
    return last  # not enough liquidity — order will partially rest at top price


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("query", help="Keyword search for the market")
    p.add_argument("--outcome", help="Outcome label to buy (e.g. 'Rousey', 'Yes', 'No')")
    p.add_argument("--usd", default=None,
                   help="USD to spend. 'max' uses full proxy balance (with $0.50 buffer). "
                        "If omitted, dry-runs at $5 so you can see the order shape.")
    p.add_argument("--send", action="store_true", help="Actually place the order")
    args = p.parse_args()

    market = search_market(args.query)
    if not market:
        return 1

    outcome, token_id, current_price = pick_outcome(market, args.outcome)
    print(f"Market: {market['question'].strip()}")
    print(f"Side:   BUY {outcome}  (token {token_id})")
    print(f"Quoted: ${current_price}  (last gamma price)")

    c = CLOBClient(); c.connect()
    book = c.client.get_order_book(token_id)
    asks_raw = book.get("asks", []) if isinstance(book, dict) else (getattr(book, "asks", []) or [])
    asks = sorted(
        [{"price": str(a.get("price") if isinstance(a, dict) else a.price),
          "size":  str(a.get("size")  if isinstance(a, dict) else a.size)} for a in asks_raw],
        key=lambda x: float(x["price"]),
    )
    if not asks:
        print("Order book has no asks — can't market-buy this side right now.")
        return 1
    best_ask = Decimal(asks[0]["price"])
    bal = c.get_balance()
    print(f"Best ask: ${best_ask}  (top size {asks[0]['size']})")
    print(f"Proxy balance: ${bal} pUSD")

    # Determine budget
    if args.usd is None:
        budget = Decimal("5")
    elif args.usd.lower() == "max":
        budget = bal - Decimal("0.50")
    else:
        budget = Decimal(args.usd)
    if budget > bal:
        print(f"Requested ${budget} > balance ${bal}. Lower --usd or fund the proxy.")
        return 1
    if budget <= 0:
        print("Budget is non-positive.")
        return 1

    target_size = (budget / best_ask).quantize(Decimal("1"), rounding="ROUND_DOWN")
    if target_size < 5:
        print(f"Computed size {target_size} is below Polymarket's typical $5 minimum.")
        return 1
    limit_price = sweep_for_size(asks, target_size)
    notional = target_size * limit_price
    print(f"\nOrder: BUY {target_size} {outcome} @ limit ${limit_price}  =  max ${notional}")

    if not args.send:
        print("\nDRY RUN — pass --send to execute")
        return 0

    order = OrderRequest(
        token_id=token_id,
        side=Side.BUY,
        size=target_size,
        limit_price=limit_price,
    )
    print("\n-> placing order...")
    order_id = c.place_order(order, dry_run=False)
    if not order_id or order_id == "unknown":
        print("Order rejected or no id returned.")
        return 1
    print(f"   order_id: {order_id}")

    time.sleep(3)
    try:
        info = c.client.get_order(order_id)
        if isinstance(info, dict):
            print(f"   status: {info.get('status')}  size_matched: {info.get('size_matched')}")
    except Exception as e:
        print(f"   get_order error: {e}")

    bal_after = c.get_balance()
    print(f"\nProxy pUSD after: {bal_after}")
    try:
        shares = c.get_conditional_balance(token_id)
        print(f"{outcome} shares held: {shares}")
    except Exception as e:
        print(f"shares fetch error: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
