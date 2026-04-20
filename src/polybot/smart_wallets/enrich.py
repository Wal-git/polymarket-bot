"""Per-wallet enrichment: fetch activity, positions, and value from the Data API.

Two concurrency modes:
  * thread-pool (default) — uses the shared, thread-safe DataAPIClient.
  * async (opt-in)        — httpx.AsyncClient + asyncio.Semaphore.

The async variant is faster for large candidate pools because it avoids the
per-thread overhead and shares a single HTTP connection pool, but requires
an event loop. Pipeline picks thread-pool by default.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import structlog

from polybot.smart_wallets.api import DataAPIClient, weekly_run_epoch
from polybot.smart_wallets.config import (
    DATA_API_BASE,
    ENRICH_WORKERS,
    LOOKBACK_DAYS,
    MAX_RPS,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# thread-pool path
# ---------------------------------------------------------------------------

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
                enriched.append(future.result())
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


# ---------------------------------------------------------------------------
# async path (opt-in)
# ---------------------------------------------------------------------------

async def enrich_candidates_async(
    candidates: list[dict],
    lookback_days: int = LOOKBACK_DAYS,
    concurrency: int = ENRICH_WORKERS,
    base_url: str = DATA_API_BASE,
    rate_limit_rps: float = MAX_RPS,
) -> list[dict]:
    """Async variant of ``enrich_candidates`` using httpx + asyncio.Semaphore."""
    now = int(time.time())
    start_ts = now - lookback_days * 86400
    sem = asyncio.Semaphore(concurrency)
    min_interval = 1.0 / max(rate_limit_rps, 0.1)
    last_ts = [0.0]
    rate_lock = asyncio.Lock()

    async def _rate_limit() -> None:
        async with rate_lock:
            wait = min_interval - (time.monotonic() - last_ts[0])
            if wait > 0:
                await asyncio.sleep(wait)
            last_ts[0] = time.monotonic()

    async def _get(client: httpx.AsyncClient, path: str, params: dict) -> Any:
        await _rate_limit()
        try:
            resp = await client.get(path, params=params, timeout=30.0)
            if resp.status_code == 429:
                await asyncio.sleep(5)
                resp = await client.get(path, params=params, timeout=30.0)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            logger.warning("async_data_api_error", path=path, error=str(exc))
            return []

    async def _activity(client: httpx.AsyncClient, user: str, activity_type: str) -> list[dict]:
        cache_epoch = weekly_run_epoch(now)
        results: list[dict] = []
        offset = 0
        limit = 500
        while True:
            page = await _get(
                client,
                "/activity",
                {
                    "user": user,
                    "start": start_ts,
                    "end": now,
                    "type": activity_type,
                    "limit": limit,
                    "offset": offset,
                },
            )
            # cache_epoch kept in scope for parity with sync path; cache is
            # not persisted here — async mode prioritises speed, not disk reuse.
            del cache_epoch
            if not page:
                break
            results.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return results

    async def _enrich(client: httpx.AsyncClient, candidate: dict) -> dict:
        wallet = candidate["proxy_wallet"]
        async with sem:
            trades = await _activity(client, wallet, "TRADE")
            redeems = await _activity(client, wallet, "REDEEM")
            positions = await _get(
                client, "/positions", {"user": wallet, "sortBy": "CASHPNL", "limit": 500}
            )
            value_resp = await _get(client, "/value", {"user": wallet})
        if isinstance(value_resp, list):
            value_resp = value_resp[0] if value_resp else {}
        return {
            **candidate,
            "raw_trades": trades or [],
            "raw_redeems": redeems or [],
            "raw_positions": positions or [],
            "raw_value": value_resp or {},
            "enrich_start_ts": start_ts,
            "enrich_end_ts": now,
        }

    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        tasks = [_enrich(client, c) for c in candidates]
        results: list[dict] = []
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            try:
                results.append(await coro)
            except Exception as exc:
                logger.warning("async_enrich_failed", error=str(exc))
            if i % 50 == 0:
                logger.info("async_enrich_progress", done=i, total=len(candidates))
    logger.info("async_enriched_total", count=len(results))
    return results


def enrich_candidates_via_asyncio(
    candidates: list[dict],
    lookback_days: int = LOOKBACK_DAYS,
    concurrency: int = ENRICH_WORKERS,
) -> list[dict]:
    """Sync entrypoint that runs the async enrichment."""
    return asyncio.run(
        enrich_candidates_async(
            candidates, lookback_days=lookback_days, concurrency=concurrency
        )
    )
