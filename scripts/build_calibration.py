#!/usr/bin/env python3
"""Build the empirical win-rate calibration table.

The runtime confidence (``signals/calibration.lookup_entry_win_rate``) is the
historical *resolution* win rate for the trade's (asset, entry-price) bucket.
This script builds that table by, for each fired signal:

  * taking the fired direction from ``data/evaluations.jsonl``,
  * reconstructing the slot's true resolution from internal price data — the
    next 5-min slot's ``price_to_beat`` is this slot's settlement price (the
    same reconstruction validated 160/160 against known HOLD_TO_RESOLUTION wins),
  * recording a win when fired direction == resolution.

We deliberately use the *resolution* outcome, NOT ``results.jsonl``'s ``won``
flag: ``won`` is false for every STOP_LOSS exit even when the slot ultimately
resolved in our favour, which would bias confidence downward. The stop-loss is
a separate risk layer; confidence answers "would this have resolved in our
favour", which is the right ``p`` for Kelly.

Aggregated at nested levels (most → least specific):

    asset × entry  →  entry  →  asset  →  global

The legacy delta×entry×hour buckets are still emitted for diagnostics but are
no longer consulted at runtime.

Bucket boundaries match ``signals/calibration.py``. Output: ``data/calibration_table.json``.

Run: ``python -m scripts.build_calibration`` (or ``python scripts/build_calibration.py``).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

# Allow running as a script from project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from polybot.signals.calibration import bucket_delta, bucket_entry  # noqa: E402


DATA_DIR = ROOT / "data"
EVALS_PATH = DATA_DIR / "evaluations.jsonl"
RESULTS_PATH = DATA_DIR / "results.jsonl"
OUT_PATH = DATA_DIR / "calibration_table.json"

SLOT_SECONDS = 300  # 5-min markets; successor slot opens at slot_ts + 300


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def _build_beat_map(evals: list[dict]) -> dict[str, float]:
    """slug -> price_to_beat (first-seen == the slot-open reference price)."""
    beat: dict[str, float] = {}
    for e in evals:
        s = e.get("slug")
        if s and s not in beat and e.get("price_to_beat"):
            beat[s] = float(e["price_to_beat"])
    return beat


def _resolution(slug: str, beat: dict[str, float]) -> str | None:
    """UP/DOWN the slot actually settled to, or None if unknown.

    Settlement price == the next consecutive slot's price_to_beat (same source).
    """
    try:
        asset, slot_ts = slug.split("-updown-5m-")
        succ = f"{asset}-updown-5m-{int(slot_ts) + SLOT_SECONDS}"
    except (ValueError, KeyError):
        return None
    if slug not in beat or succ not in beat:
        return None
    return "UP" if beat[succ] > beat[slug] else "DOWN"


def _max_abs_delta(eval_row: dict) -> float | None:
    """Compute max(|delta|) across whatever exchange columns the eval recorded."""
    candidates = []
    for name in ("binance_delta", "coinbase_delta", "kraken_delta", "bitstamp_delta", "okx_delta"):
        v = eval_row.get(name)
        if v is not None:
            candidates.append(abs(float(v)))
    # Older evals stored max_abs_delta directly when fired
    if not candidates and eval_row.get("max_abs_delta") is not None:
        return float(eval_row["max_abs_delta"])
    return max(candidates) if candidates else None


def build_table() -> dict:
    evals = _load_jsonl(EVALS_PATH)
    results = _load_jsonl(RESULTS_PATH)
    beat = _build_beat_map(evals)

    # Keep only the LAST approved eval per slug (the one that became the trade)
    approved: dict[str, dict] = {}
    for e in evals:
        if e.get("reject_reason") is None and e.get("slug") and e.get("direction"):
            approved[e["slug"]] = e

    # Dedupe results by slug (defensive — bug may have been fixed but earlier
    # data still has dupes). Used only for the executed entry price.
    res_by_slug: dict[str, dict] = {}
    for r in results:
        if r.get("slug"):
            res_by_slug[r["slug"]] = r

    skipped_no_resolution = 0
    rows = []
    for slug, e in approved.items():
        resolution = _resolution(slug, beat)
        if resolution is None:
            skipped_no_resolution += 1
            continue
        r = res_by_slug.get(slug)
        # Prefer the executed entry price, fall back to best_ask logged in the eval
        entry = (r.get("entry_price") if r else None) or e.get("best_ask")
        max_d = _max_abs_delta(e)
        asset = e.get("asset") or slug.split("-")[0].upper()
        if entry is None:
            continue
        won = e["direction"] == resolution
        ts = (r.get("ts") if r else None) or e.get("ts")
        try:
            hour = datetime.fromisoformat(ts.replace("Z", "+00:00")).hour
        except (ValueError, AttributeError):
            hour = None
        rows.append({
            "slug": slug,
            "asset": str(asset),
            "max_abs_delta": float(max_d) if max_d is not None else None,
            "entry_price": float(entry),
            "hour_utc": hour,
            "won": won,
        })

    # Aggregate. Primary groups are entry-price keyed (used at runtime); the
    # delta groups are legacy diagnostics.
    asset_x_entry: dict[str, dict] = {}
    entry_only: dict[str, dict] = {}
    asset_only: dict[str, dict] = {}
    delta_x_entry_x_hour: dict[str, dict] = {}
    delta_x_entry: dict[str, dict] = {}
    delta_only: dict[str, dict] = {}

    def _bump(bucket: dict, key: str, won: bool) -> None:
        cell = bucket.setdefault(key, {"trials": 0, "wins": 0})
        cell["trials"] += 1
        if won:
            cell["wins"] += 1

    total_trials = 0
    total_wins = 0
    for row in rows:
        eb = bucket_entry(row["entry_price"])
        won = row["won"]
        _bump(asset_x_entry, f"{row['asset']}_{eb}", won)
        _bump(entry_only, eb, won)
        _bump(asset_only, row["asset"], won)
        if row["max_abs_delta"] is not None:
            db = bucket_delta(row["max_abs_delta"])
            _bump(delta_x_entry, f"{db}_{eb}", won)
            _bump(delta_only, db, won)
            if row["hour_utc"] is not None:
                _bump(delta_x_entry_x_hour, f"{db}_{eb}_{row['hour_utc']}", won)
        total_trials += 1
        if won:
            total_wins += 1

    return {
        "version": 2,
        "built_at": datetime.now().astimezone().isoformat(),
        "outcome": "resolution",
        "trade_count": total_trials,
        "skipped_no_resolution": skipped_no_resolution,
        "global": {"trials": total_trials, "wins": total_wins},
        "buckets": {
            "asset_x_entry": asset_x_entry,
            "entry": entry_only,
            "asset": asset_only,
            "delta_x_entry_x_hour": delta_x_entry_x_hour,
            "delta_x_entry": delta_x_entry,
            "delta": delta_only,
        },
    }


def main() -> None:
    table = build_table()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(table, indent=2))
    print(f"Wrote {OUT_PATH}")
    print(f"  outcome: {table['outcome']}  trade_count: {table['trade_count']}"
          f"  (skipped no-resolution: {table['skipped_no_resolution']})")
    g = table["global"]
    print(f"  global wr: {g['wins']}/{g['trials']} = {g['wins'] / g['trials']:.3f}"
          if g["trials"] else "  global wr: n/a")
    print("  asset × entry buckets:")
    for k, c in sorted(table["buckets"]["asset_x_entry"].items()):
        print(f"    {k:18} {c['wins']:>3}/{c['trials']:<3} "
              f"raw={c['wins'] / c['trials']:.3f} smoothed={(c['wins'] + 1) / (c['trials'] + 2):.3f}")


if __name__ == "__main__":
    main()
