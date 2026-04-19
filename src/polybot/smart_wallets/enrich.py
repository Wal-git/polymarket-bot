"""Per-wallet enrichment: fetch activity, positions, and value from the Data API."""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import structlog

from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.config import ENRICH_WORKERS, LOOKBACK_DAYS

logger = structlog.get_logger()


def enrich_candidates(
    candidates: list[dict],
    client: DataAPIClient,
    lookback_days: int = LOOKBACK_DAYS,
    max_workers: int = ENRICH_WORKERS,
) -> list[dict]:
    """Enrich each candidate with raw activity + positions. Returns enriched list."""
    now = int(time.time())
    start_ts = now - lookback_days * 86400

    enriched: list[dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_enrich_one, c, client, start_ts, now): c
            for c in candidates
        }
        for i, future in enumerate(as_completed(futures), 1):
            candidate = futures[future]
            wallet = candidate["proxy_wallet"]
            try:
                result = future.result()
                enriched.append(result)
            except Exception as exc:
                logger.warning("enrich_failed", wallet=wallet, error=str(exc))
            if i % 50 == 0:
                logger.info("enrich_progress", done=i, total=len(candidates))

    logger.info("enriched_total", count=len(enriched))
    return enriched


def _enrich_one(candidate: dict, client: DataAPIClient, start_ts: int, end_ts: int) -> dict:
    wallet = candidate["proxy_wallet"]

    trades = client.activity(wallet, start=start_ts, end=end_ts, activity_type="TRADE")
    redeems = client.activity(wallet, start=start_ts, end=end_ts, activity_type="REDEEM")
    positions = client.positions(wallet)
    value_resp = client.value(wallet)

    return {
        **candidate,
        "raw_trades": trades,
        "raw_redeems": redeems,
        "raw_positions": positions,
        "raw_value": value_resp,
        "enrich_start_ts": start_ts,
        "enrich_end_ts": end_ts,
    }
