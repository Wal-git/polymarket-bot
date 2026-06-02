#!/usr/bin/env python3
"""Backtest the time-gated stop-loss (strategy.exit.stop_window_s).

For every historical STOP_LOSS trade we reconstruct the slot's true resolution
from internal price data (the next 5-min slot's reference price == this slot's
settlement price), then compare:

  * realized P&L under the *current* policy (every stop fires), vs
  * P&L under a *gated* policy where a stop only fires if it triggered within
    `window` seconds of entry; otherwise the position is held to resolution.

Resolution reconstruction is validated against known HOLD_TO_RESOLUTION wins.

Usage:  python scripts/backtest_stop_window.py [--data DIR]
"""
from __future__ import annotations

import argparse
import json
import os
from glob import glob


def load_beats(data_dir: str) -> dict[str, float]:
    """slug -> price_to_beat (first-seen == slot-open reference price)."""
    beat: dict[str, float] = {}
    path = os.path.join(data_dir, "evaluations.jsonl")
    with open(path) as fh:
        for line in fh:
            if "updown" not in line:
                continue
            d = json.loads(line)
            s = d.get("slug")
            if s and s not in beat and d.get("price_to_beat"):
                beat[s] = d["price_to_beat"]
    return beat


def resolution(slug: str, beat: dict[str, float]):
    """Return (resolved_direction, fractional_margin) or (None, None)."""
    asset = slug.split("-")[0]
    slot = int(slug.rsplit("-", 1)[1])
    succ = f"{asset}-updown-5m-{slot + 300}"
    if slug not in beat or succ not in beat:
        return None, None
    b, fin = beat[slug], beat[succ]
    return ("UP" if fin > b else "DOWN"), (fin - b) / b


def load_entries(data_dir: str) -> dict[str, dict]:
    exe: dict[str, dict] = {}
    for path in glob(os.path.join(data_dir, "executions.jsonl*")):
        with open(path) as fh:
            for line in fh:
                if "updown" not in line:
                    continue
                d = json.loads(line)
                if d.get("status") == "filled":
                    exe[d["slug"]] = d  # later files (newer) win; fine for entry size
    return exe


def load_stops(data_dir: str) -> list[dict]:
    seen, stops = set(), []
    for path in glob(os.path.join(data_dir, "results.jsonl*")):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                if d.get("exit_reason") != "STOP_LOSS":
                    continue
                key = (d["slug"], d["ts"])
                if key in seen:
                    continue
                seen.add(key)
                stops.append(d)
    stops.sort(key=lambda d: d["ts"])
    return stops


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    args = ap.parse_args()
    data_dir = os.path.abspath(args.data)

    beat = load_beats(data_dir)
    exe = load_entries(data_dir)
    stops = load_stops(data_dir)

    # Validate reconstruction against known HOLD_TO_RESOLUTION wins.
    checked = ok = 0
    for path in glob(os.path.join(data_dir, "results.jsonl")):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                if d.get("exit_reason") != "HOLD_TO_RESOLUTION" or not d.get("won"):
                    continue
                e = exe.get(d["slug"])
                rr, _ = resolution(d["slug"], beat)
                if not e or not rr:
                    continue
                checked += 1
                ok += int(rr == e["direction"])
    print(f"resolution validation on known wins: {ok}/{checked} match\n")

    rows = []
    for d in stops:
        e = exe.get(d["slug"])
        rr, marg = resolution(d["slug"], beat)
        if not e or not rr:
            continue
        won = rr == e["direction"]
        hold_pnl = (e["size_shares"] - e["size_usdc"]) if won else -e["size_usdc"]
        rows.append({
            "slug": d["slug"],
            "hs": d.get("hold_duration_s", 0.0),
            "sl_pnl": d["pnl"],          # realized if the stop fires
            "hold_pnl": hold_pnl,         # counterfactual if held to resolution
        })

    current = sum(r["sl_pnl"] for r in rows)
    hold_all = sum(r["hold_pnl"] for r in rows)
    print(f"resolved stops: {len(rows)}")
    print(f"current policy (every stop fires):     {current:+9.2f}")
    print(f"never stop (hold everything):          {hold_all:+9.2f}\n")

    # A stop fires under gate `w` iff the code's predicate holds:
    #   stop_window_s <= 0  (gate disabled) OR  held_s <= stop_window_s
    def fires(hs: float, w: float) -> bool:
        return w <= 0 or hs <= w

    print(f"{'window_s':>9} {'stops_fire':>10} {'realized_pnl':>13} {'vs_current':>11}")
    for w in [30, 45, 60, 75, 90, 120]:
        kept = [r for r in rows if fires(r["hs"], w)]
        total = sum(r["sl_pnl"] if fires(r["hs"], w) else r["hold_pnl"] for r in rows)
        print(f"{w:>9} {len(kept):>10} {total:>13.2f} {total - current:>+11.2f}")


if __name__ == "__main__":
    main()
