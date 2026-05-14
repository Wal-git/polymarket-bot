"""One-shot recovery for trades stuck on the Positions tab as "⏳ Pending".

A trade renders Pending when its slug exists in ``state.trades`` but has no row
in ``data/results.jsonl``. This happens when lifecycle hits HOLD_TO_RESOLUTION,
the gamma-api outcome hasn't published within the retry budget, and the position
gets evicted before the reconciler can pick it up.

This script finds every such orphaned slug, looks up its resolved outcome via
gamma-api, and writes the missing result row using the same shape as
``emit_result``. Safe to re-run — emit_result dedupes by slug.

Usage:
    cd /root/polymarket-bot
    python scripts/recover_orphaned_results.py            # dry-run, lists work
    python scripts/recover_orphaned_results.py --apply    # actually writes
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from polybot.execution.redeem import _outcome_from_prices  # noqa: E402
from polybot.monitoring.event_log import emit_result  # noqa: E402

STATE_FILE = REPO_ROOT / "data" / "state.json"
RESULTS_FILE = REPO_ROOT / "data" / "results.jsonl"
EXECUTIONS_FILE = REPO_ROOT / "data" / "executions.jsonl"
GAMMA_BASE = "https://gamma-api.polymarket.com"
BATCH = 100


def load_orphaned_slugs() -> set[str]:
    state = json.loads(STATE_FILE.read_text())
    trade_slugs = {t.get("market_question", "") for t in state.get("trades", [])}
    trade_slugs.discard("")

    resolved: set[str] = set()
    if RESULTS_FILE.exists():
        for line in RESULTS_FILE.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "slug" in rec:
                resolved.add(rec["slug"])

    return trade_slugs - resolved


def load_executions_by_slug() -> dict[str, dict]:
    """Return the most-recent filled execution per slug.

    A slug can have multiple BUY records if the position was scaled in, but
    for these 5-min up/down markets each slug is a single entry — the latest
    filled record reflects final size + direction.
    """
    out: dict[str, dict] = {}
    if not EXECUTIONS_FILE.exists():
        return out
    for line in EXECUTIONS_FILE.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("status") != "filled":
            continue
        slug = rec.get("slug")
        if slug:
            out[slug] = rec  # later writes overwrite earlier
    return out


def fetch_outcomes(slugs: list[str]) -> dict[str, str | None]:
    """Return {slug: 'UP'|'DOWN'|None}. None means still unresolved at gamma-api."""
    result: dict[str, str | None] = {}
    for i in range(0, len(slugs), BATCH):
        batch = slugs[i : i + BATCH]
        params = [("slug", s) for s in batch] + [("closed", "true"), ("limit", str(len(batch) * 2))]
        resp = httpx.get(f"{GAMMA_BASE}/markets", params=params, timeout=15)
        resp.raise_for_status()
        markets = {m.get("slug"): m for m in resp.json() if m.get("slug")}
        for s in batch:
            m = markets.get(s)
            if m is None:
                result[s] = None
                continue
            result[s] = _outcome_from_prices(m.get("outcomePrices"))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="actually write results (default: dry-run)")
    args = parser.parse_args()

    orphans = sorted(load_orphaned_slugs())
    if not orphans:
        print("Nothing to recover — all trades have result rows.")
        return 0

    executions = load_executions_by_slug()
    missing_exec = [s for s in orphans if s not in executions]
    if missing_exec:
        print(f"WARN: {len(missing_exec)} orphan slugs have no execution record — skipping:")
        for s in missing_exec:
            print(f"  {s}")
    have_exec = [s for s in orphans if s in executions]

    print(f"Querying gamma-api for {len(have_exec)} slug outcomes...")
    outcomes = fetch_outcomes(have_exec)

    pending = [s for s, o in outcomes.items() if o is None]
    resolvable = [(s, o) for s, o in outcomes.items() if o is not None]
    if pending:
        print(f"INFO: {len(pending)} slugs not yet resolved at gamma-api — will retry on next run:")
        for s in pending:
            print(f"  {s}")

    print(f"\n{len(resolvable)} slugs ready to write:")
    written = 0
    skipped_wash = 0
    total_pnl = 0.0
    for slug, outcome in resolvable:
        ex = executions[slug]
        direction = ex.get("direction", "").upper()
        shares = float(ex.get("size_shares") or 0)
        entry = float(ex.get("fill_price") or 0)
        confidence = ex.get("confidence")
        asset = ex.get("asset") or ("ETH" if slug.startswith("eth-") else "BTC")

        won = direction == outcome
        pnl = round(shares * (1.0 - entry), 4) if won else round(-shares * entry, 4)
        total_pnl += pnl
        flag = "WIN" if won else "LOSS"
        print(f"  {slug}  dir={direction} outcome={outcome} {flag} pnl={pnl:+.2f}")

        if args.apply:
            emit_result(
                slug=slug,
                asset=asset,
                won=won,
                pnl=pnl,
                shares=shares,
                entry_price=entry,
                direction=direction,
                exit_reason="HOLD_TO_RESOLUTION",
                exit_price=1.0 if won else 0.0,
                confidence=confidence,
                recovered=True,
            )
            written += 1

    print(f"\nTotal P&L from recovery: {total_pnl:+.2f}")
    if args.apply:
        print(f"Wrote {written} rows to {RESULTS_FILE}")
    else:
        print("DRY RUN — re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
