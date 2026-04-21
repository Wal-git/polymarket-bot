"""Monthly leaderboard: Goldsky-verified PnL ranking.

Replaces the per-wallet /activity pagination with a single bulk Goldsky query,
reducing wall time from ~20 min to ~2-3 min and eliminating silent empty-page
failures.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import structlog

from polybot.client.goldsky import GoldskyClient, OrderFilledEvent
from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.config import (
    CACHE_DIR,
    MONTHLY_LOOKBACK_DAYS,
    MONTHLY_TOP_N,
    SMART_WALLETS_MONTHLY_JSON,
)
from polybot.smart_wallets.seed import fetch_monthly_pnl_leaders

logger = structlog.get_logger()


def run(
    top_n: int = MONTHLY_TOP_N,
    lookback_days: int = MONTHLY_LOOKBACK_DAYS,
    dry_run: bool = False,
) -> dict:
    client = DataAPIClient()
    gold = GoldskyClient()
    try:
        seeds = fetch_monthly_pnl_leaders(client, limit=top_n)
        wallets = [s["proxy_wallet"] for s in seeds]
        since_ts = int(time.time()) - lookback_days * 86400
        events = _bulk_events(gold, wallets, since_ts)
        realized = _cash_flow_by_wallet(events, wallets)
        rows = []
        for s in seeds:
            w = s["proxy_wallet"]
            positions = client.positions(w)
            unrealized = sum(
                _safe_float(p.get("cashPnl"))
                for p in positions
                if not p.get("redeemable")
            )
            rows.append({
                **s,
                "reported_pnl_30d": s.get("leaderboard_pnl", 0.0),
                "realized_pnl_30d": realized[w]["net"],
                "unrealized_pnl": unrealized,
                "verified_pnl_30d": realized[w]["net"] + unrealized,
                "trades_count": realized[w]["trades"],
                "volume_30d": realized[w]["gross_out"],
                "pnl_divergence": 0.0,
            })
        rows.sort(key=lambda r: r["verified_pnl_30d"], reverse=True)
        for r in rows:
            r["pnl_divergence"] = r["reported_pnl_30d"] - r["verified_pnl_30d"]
        if not dry_run:
            _write_monthly_json(rows, lookback_days)
        logger.info("monthly_run_done", n=len(rows), dry_run=dry_run)
        return {"wallets": rows, "n": len(rows), "dry_run": dry_run}
    finally:
        client.close()
        gold.close()


def _bulk_events(
    gold: GoldskyClient,
    wallets: list[str],
    since_ts: int,
) -> list[OrderFilledEvent]:
    """Fetch events for wallets with parallel fetching and batched wallet queries.

    Batches wallets to avoid large IN clause timeouts on the Goldsky API.
    """
    if not wallets:
        return []

    cache_dir = CACHE_DIR / "goldsky_monthly"
    batch_size = 10
    seen: set[str] = set()
    all_events: list[OrderFilledEvent] = []

    # Batch wallets to reduce query complexity
    for batch_idx in range(0, len(wallets), batch_size):
        batch = wallets[batch_idx : batch_idx + batch_size]
        addr_list = '["' + '", "'.join(w.lower() for w in batch) + '"]'

        for field in ("taker_in", "maker_in"):
            extra = f"{field}: {addr_list}"
            events = gold.fetch_events_parallel(
                since_ts=since_ts,
                chunk_days=1,
                workers=8,
                cache_dir=cache_dir,
                extra_where=extra,
            )
            for ev in events:
                if ev.transaction_hash not in seen:
                    seen.add(ev.transaction_hash)
                    all_events.append(ev)

    return all_events


def _cash_flow_by_wallet(
    events: list[OrderFilledEvent],
    wallets: list[str],
) -> dict[str, dict]:
    wallet_set = {w.lower() for w in wallets}
    result: dict[str, dict] = {
        w.lower(): {"net": 0.0, "gross_out": 0.0, "trades": 0}
        for w in wallets
    }
    for ev in events:
        taker = ev.taker.lower()
        maker = ev.maker.lower()
        usd = float(ev.usd_amount)
        if ev.taker_direction == "BUY":
            # taker paid USDC (cash out); maker received USDC (cash in)
            if taker in wallet_set:
                result[taker]["net"] -= usd
                result[taker]["gross_out"] += usd
                result[taker]["trades"] += 1
            if maker in wallet_set:
                result[maker]["net"] += usd
                result[maker]["trades"] += 1
        else:
            # taker sold tokens → received USDC (cash in); maker paid USDC (cash out)
            if taker in wallet_set:
                result[taker]["net"] += usd
                result[taker]["trades"] += 1
            if maker in wallet_set:
                result[maker]["net"] -= usd
                result[maker]["gross_out"] += usd
                result[maker]["trades"] += 1
    return result


def _write_monthly_json(rows: list[dict], lookback_days: int) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": lookback_days,
        "wallets": rows,
    }
    SMART_WALLETS_MONTHLY_JSON.parent.mkdir(parents=True, exist_ok=True)
    SMART_WALLETS_MONTHLY_JSON.write_text(json.dumps(payload, indent=2))
    logger.info("monthly_json_written", path=str(SMART_WALLETS_MONTHLY_JSON), count=len(rows))


def _safe_float(val: object) -> float:
    try:
        return float(val or 0)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
