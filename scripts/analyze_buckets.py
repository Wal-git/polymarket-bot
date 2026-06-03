"""Realized P&L analysis: per (asset, entry-price) bucket + stop-loss vs hold
decomposition + recent daily P&L. Joins data/executions.jsonl (entry fill price)
with data/results.jsonl (realized pnl / exit reason).

Run: .venv/bin/python scripts/analyze_buckets.py [--days N]
Used by the weekly scheduled review to check whether disabling the stop-loss
(2026-06-03) actually improved net P&L and whether low-entry buckets bleed.
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics
from datetime import datetime, timedelta, timezone


def load(path: str) -> list[dict]:
    out = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except FileNotFoundError:
        pass
    return out


def bucket(p: float | None) -> str:
    if p is None:
        return "??"
    if p < 0.60:
        return "0.50-0.60"
    if p < 0.70:
        return "0.60-0.70"
    if p < 0.80:
        return "0.70-0.80"
    if p < 0.85:
        return "0.80-0.85"
    if p < 0.90:
        return "0.85-0.90"
    return "0.90+"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0,
                    help="only include results from the last N days (0 = all)")
    args = ap.parse_args()

    results = load("data/results.jsonl")
    execs = load("data/executions.jsonl")

    if args.days > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
        results = [r for r in results if r.get("ts", "") >= cutoff]

    entry = {e["slug"]: e.get("fill_price")
             for e in execs if e.get("status") == "filled"}

    print(f"=== realized P&L  (n={len(results)} resolved trades"
          + (f", last {args.days}d" if args.days else ", all time") + ") ===\n")

    # Daily
    daily = collections.defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0.0})
    for r in results:
        d = r.get("ts", "")[:10]
        daily[d]["n"] += 1
        daily[d]["pnl"] += r.get("pnl", 0)
        if r.get("won"):
            daily[d]["w"] += 1
    print("Date         trades  wins      P&L")
    for d in sorted(daily)[-14:]:
        x = daily[d]
        print(f"{d}   {x['n']:>5}  {x['w']:>4}   {x['pnl']:>8.2f}")

    # Stop vs hold
    stops = [r["pnl"] for r in results if r.get("exit_reason") == "STOP_LOSS"]
    holds = [r["pnl"] for r in results if r.get("exit_reason") != "STOP_LOSS"]
    print("\n--- exit-type decomposition ---")
    if stops:
        print(f"STOP_LOSS: n={len(stops):>4}  total={sum(stops):>9.1f}  avg={statistics.mean(stops):>7.2f}")
    else:
        print("STOP_LOSS: none (stop disabled 2026-06-03 — expected to stay 0)")
    if holds:
        print(f"HOLD     : n={len(holds):>4}  total={sum(holds):>9.1f}  avg={statistics.mean(holds):>7.2f}")

    # Buckets
    agg = collections.defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0.0})
    for r in results:
        ep = entry.get(r["slug"])
        if ep is None:
            continue
        k = (r.get("asset"), bucket(ep))
        a = agg[k]
        a["n"] += 1
        a["pnl"] += r.get("pnl", 0)
        if r.get("won"):
            a["w"] += 1
    print(f"\n--- realized P&L by asset x entry-price bucket ---")
    print(f"{'asset/bucket':22}{'n':>4}{'WR':>6}{'P&L':>9}{'$/trade':>9}")
    for k in sorted(agg, key=lambda x: (str(x[0]), str(x[1]))):
        a = agg[k]
        wr = a["w"] / a["n"] * 100 if a["n"] else 0
        flag = "  <-- LOSER" if a["pnl"] < 0 and a["n"] >= 5 else ""
        print(f"{str(k[0]) + ' ' + str(k[1]):22}{a['n']:>4}{wr:>5.0f}%{a['pnl']:>9.1f}{a['pnl'] / a['n']:>9.2f}{flag}")

    total = sum(r.get("pnl", 0) for r in results)
    print(f"\nTOTAL realized P&L: {total:+.2f}")


if __name__ == "__main__":
    main()
