#!/usr/bin/env python3
"""Build the empirical win-rate calibration table.

Joins ``data/evaluations.jsonl`` (the features at signal time) with
``data/results.jsonl`` (the realized outcome) on slug, then aggregates wins
and trials per bucket at three nested levels:

    delta × entry × hour   (most specific)
    delta × entry
    delta                  (least specific)

Plus a global cell for last-resort fallback.

Bucket boundaries match ``signals/calibration.py``. Output is JSON written to
``data/calibration_table.json``.

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


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


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

    # Keep only the LAST approved eval per slug (the one that became the trade)
    approved: dict[str, dict] = {}
    for e in evals:
        if e.get("reject_reason") is None and e.get("slug"):
            approved[e["slug"]] = e

    # Dedupe results by slug (defensive — bug may have been fixed but earlier
    # data still has dupes)
    res_by_slug: dict[str, dict] = {}
    for r in results:
        if r.get("slug"):
            res_by_slug[r["slug"]] = r

    # Join
    rows = []
    for slug, e in approved.items():
        r = res_by_slug.get(slug)
        if r is None:
            continue
        max_d = _max_abs_delta(e)
        # Prefer the executed entry price, fall back to best_ask logged in the eval
        entry = r.get("entry_price") or e.get("best_ask")
        if max_d is None or entry is None:
            continue
        ts = r.get("ts") or e.get("ts")
        try:
            hour = datetime.fromisoformat(ts.replace("Z", "+00:00")).hour
        except (ValueError, AttributeError):
            continue
        rows.append({
            "slug": slug,
            "max_abs_delta": float(max_d),
            "entry_price": float(entry),
            "hour_utc": int(hour),
            "won": bool(r.get("won")),
        })

    # Aggregate
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
        db = bucket_delta(row["max_abs_delta"])
        eb = bucket_entry(row["entry_price"])
        h = row["hour_utc"]
        won = row["won"]
        _bump(delta_x_entry_x_hour, f"{db}_{eb}_{h}", won)
        _bump(delta_x_entry, f"{db}_{eb}", won)
        _bump(delta_only, db, won)
        total_trials += 1
        if won:
            total_wins += 1

    return {
        "version": 1,
        "built_at": datetime.now().astimezone().isoformat(),
        "trade_count": total_trials,
        "global": {"trials": total_trials, "wins": total_wins},
        "buckets": {
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
    print(f"  trade_count: {table['trade_count']}")
    print(f"  global wr: {table['global']['wins']}/{table['global']['trials']}")
    print(f"  delta buckets: {len(table['buckets']['delta'])}")
    print(f"  delta×entry buckets: {len(table['buckets']['delta_x_entry'])}")
    print(f"  delta×entry×hour buckets: {len(table['buckets']['delta_x_entry_x_hour'])}")


if __name__ == "__main__":
    main()
