#!/usr/bin/env python3
"""Backfill per-slot outcomes from Polymarket gamma-api into ``data/slot_history.jsonl``.

For every unique slug seen in ``data/evaluations.jsonl``, fetch the resolved
outcome from gamma-api and join with: the last eval row (decision-time
features), the count of evaluations, and the trade result (if any).

Output is one JSONL row per slug with the schema:

    {
      "slug": "btc-updown-5m-1776954600",
      "asset": "BTC",
      "slot_open_iso": "2026-04-23T14:30:00+00:00",
      "slot_close_iso": "2026-04-23T14:35:00Z",
      "outcome": "UP" | "DOWN" | null,
      "resolved": true,
      "outcome_prices": ["1", "0"],
      "n_evaluations": 3,
      "last_eval": { ... full eval row ... },
      "was_traded": true,
      "trade_result": { ... results.jsonl row, or null ... }
    }

Resumable: rows already present in slot_history.jsonl are skipped, so reruns
only fetch new slugs. Markets that aren't resolved yet are skipped (no row
written) so they get picked up next run.

Run: ``python scripts/backfill_slot_history.py``
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable

import httpx

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
EVALS_PATH = DATA_DIR / "evaluations.jsonl"
RESULTS_PATH = DATA_DIR / "results.jsonl"
OUT_PATH = DATA_DIR / "slot_history.jsonl"

GAMMA_BASE = "https://gamma-api.polymarket.com"
BATCH_SIZE = 100  # gamma-api accepts repeated ?slug= params; 100 is comfortable


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def _slot_unix(slug: str) -> int | None:
    tail = slug.rsplit("-", 1)[-1]
    try:
        return int(tail)
    except ValueError:
        return None


def _outcome_from_prices(prices: list) -> str | None:
    """Map outcomePrices to UP/DOWN/None.

    A resolved binary market shows ["1","0"] (Up) or ["0","1"] (Down).
    Anything else (e.g. ["0.5","0.5"]) means unresolved or pushed — return None.
    """
    if not isinstance(prices, list) or len(prices) != 2:
        return None
    try:
        a, b = float(prices[0]), float(prices[1])
    except (TypeError, ValueError):
        return None
    if a == 1 and b == 0:
        return "UP"
    if a == 0 and b == 1:
        return "DOWN"
    return None


def _existing_slugs(path: Path) -> set[str]:
    seen: set[str] = set()
    if not path.exists():
        return seen
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        slug = rec.get("slug")
        if slug:
            seen.add(slug)
    return seen


def _chunks(items: list, n: int) -> Iterable[list]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _fetch_batch(client: httpx.Client, slugs: list[str]) -> dict[str, dict]:
    """Return {slug: market_dict} for the resolved markets in this batch."""
    params = [("slug", s) for s in slugs] + [
        ("closed", "true"),
        ("limit", str(len(slugs) * 2)),
    ]
    resp = client.get("/markets", params=params)
    resp.raise_for_status()
    out: dict[str, dict] = {}
    for m in resp.json():
        s = m.get("slug")
        if s:
            out[s] = m
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N new slugs (debug)",
    )
    ap.add_argument(
        "--rebuild",
        action="store_true",
        help="Ignore existing slot_history.jsonl and rewrite from scratch",
    )
    args = ap.parse_args()

    evals = _load_jsonl(EVALS_PATH)
    results = _load_jsonl(RESULTS_PATH)

    # Group evals by slug, keep last eval and a count.
    eval_count: dict[str, int] = {}
    last_eval: dict[str, dict] = {}
    for e in evals:
        slug = e.get("slug")
        if not slug or _slot_unix(slug) is None:
            continue
        eval_count[slug] = eval_count.get(slug, 0) + 1
        # Evaluations file is append-only and already in time order; last write wins.
        last_eval[slug] = e

    # Index trade results by slug.
    res_by_slug: dict[str, dict] = {r["slug"]: r for r in results if r.get("slug")}

    if args.rebuild and OUT_PATH.exists():
        OUT_PATH.unlink()
    already = _existing_slugs(OUT_PATH)

    todo = sorted(s for s in last_eval if s not in already)
    if args.limit:
        todo = todo[: args.limit]

    print(
        f"evaluations: {len(evals)} rows / {len(last_eval)} unique slugs",
        f"already in slot_history: {len(already)}",
        f"to fetch: {len(todo)}",
        sep="\n",
    )
    if not todo:
        print("Nothing to do.")
        return 0

    written = 0
    skipped_unresolved = 0
    skipped_missing = 0
    started = time.time()

    with httpx.Client(base_url=GAMMA_BASE, timeout=30) as client, OUT_PATH.open(
        "a", encoding="utf-8"
    ) as out_f:
        for chunk in _chunks(todo, BATCH_SIZE):
            try:
                markets = _fetch_batch(client, chunk)
            except httpx.HTTPError as exc:
                print(f"batch fetch failed ({exc}); retrying once after 2s", file=sys.stderr)
                time.sleep(2)
                markets = _fetch_batch(client, chunk)

            for slug in chunk:
                m = markets.get(slug)
                if m is None:
                    skipped_missing += 1
                    continue
                prices_raw = m.get("outcomePrices")
                if isinstance(prices_raw, str):
                    try:
                        prices = json.loads(prices_raw)
                    except json.JSONDecodeError:
                        prices = []
                else:
                    prices = prices_raw or []
                outcome = _outcome_from_prices(prices)
                resolved = bool(m.get("closed")) and outcome is not None
                if not resolved:
                    # Not yet resolved — skip so a future run picks it up.
                    skipped_unresolved += 1
                    continue

                e = last_eval[slug]
                slot_ts = _slot_unix(slug)
                row = {
                    "slug": slug,
                    "asset": e.get("asset")
                    or ("BTC" if slug.startswith("btc-") else "ETH" if slug.startswith("eth-") else None),
                    "slot_open_unix": slot_ts,
                    "slot_open_iso": datetime.fromtimestamp(slot_ts, timezone.utc).isoformat()
                    if slot_ts is not None
                    else None,
                    "slot_close_iso": m.get("endDate"),
                    "outcome": outcome,
                    "resolved": True,
                    "outcome_prices": prices,
                    "n_evaluations": eval_count[slug],
                    "last_eval": e,
                    "was_traded": slug in res_by_slug,
                    "trade_result": res_by_slug.get(slug),
                }
                out_f.write(json.dumps(row) + "\n")
                written += 1

            # Be nice to gamma-api between batches.
            time.sleep(0.25)

    elapsed = time.time() - started
    print(
        f"done in {elapsed:.1f}s: wrote {written} rows, "
        f"skipped {skipped_unresolved} unresolved, {skipped_missing} not found"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
