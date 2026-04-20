"""Goldsky subgraph client for recent Polymarket order-filled events.

Adapted from warproxxx/poly_data — queries the orderbook subgraph for
filled trades in a time window. Used by smart-money and activity-based
strategies to see what the market is *doing*, not just its current snapshot.
"""
from __future__ import annotations

import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests
import structlog

logger = structlog.get_logger()

GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
)

PLATFORM_WALLETS: frozenset[str] = frozenset(
    {
        "0xc5d563a36ae78145c45a50134d48a1215220f80a",
        "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    }
)


@dataclass(frozen=True)
class OrderFilledEvent:
    timestamp: int
    maker: str
    maker_asset_id: str
    maker_amount_filled: Decimal
    taker: str
    taker_asset_id: str
    taker_amount_filled: Decimal
    transaction_hash: str

    @property
    def non_usdc_asset_id(self) -> str:
        return self.maker_asset_id if self.maker_asset_id != "0" else self.taker_asset_id

    @property
    def taker_direction(self) -> str:
        return "BUY" if self.taker_asset_id == "0" else "SELL"

    @property
    def usd_amount(self) -> Decimal:
        raw = self.taker_amount_filled if self.taker_asset_id == "0" else self.maker_amount_filled
        return raw / Decimal(10**6)

    @property
    def token_amount(self) -> Decimal:
        raw = self.taker_amount_filled if self.taker_asset_id != "0" else self.maker_amount_filled
        return raw / Decimal(10**6)

    @property
    def price(self) -> Decimal:
        if self.token_amount == 0:
            return Decimal("0")
        return self.usd_amount / self.token_amount


class GoldskyClient:
    def __init__(self, url: str = GOLDSKY_URL, batch_size: int = 1000, max_retries: int = 5):
        self._url = url
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._session = requests.Session()
        # Rolling cache: stores events within the last lookback window
        self._event_cache: list[OrderFilledEvent] = []
        self._last_fetch_ts: int | None = None

    def recent_events(self, lookback_minutes: int = 30) -> list[OrderFilledEvent]:
        now = int(time.time())
        window_start = now - lookback_minutes * 60

        if self._last_fetch_ts is None:
            # Cold start: fetch full window
            new_events = self.fetch_events_since(since_ts=window_start, until_ts=now)
            self._event_cache = new_events
        else:
            # Incremental: only fetch events since last fetch
            new_events = self.fetch_events_since(since_ts=self._last_fetch_ts, until_ts=now)
            self._event_cache.extend(new_events)
            # Drop events outside the rolling window
            self._event_cache = [e for e in self._event_cache if e.timestamp >= window_start]

        self._last_fetch_ts = now
        return list(self._event_cache)

    def fetch_events_parallel(
        self,
        since_ts: int,
        until_ts: int | None = None,
        chunk_days: int = 1,
        workers: int = 8,
        cache_dir: Path | None = None,
    ) -> list[OrderFilledEvent]:
        """Fetch events over a large window using parallel chunks with disk caching.

        Splits [since_ts, until_ts] into chunk_days-sized slices, loads any
        already-cached slices from disk, and fetches the rest in parallel.
        Completed slices (ending >1h ago) are written to cache for future runs.
        """
        until_ts = until_ts or int(time.time())
        chunk_secs = int(chunk_days * 86400)
        now = int(time.time())
        cache_cutoff = now - 3600  # chunks ending before this are safe to cache

        # Snap since_ts to the nearest chunk grid so boundaries are stable across runs.
        since_ts = (since_ts // chunk_secs) * chunk_secs

        # Build chunk boundaries
        chunks: list[tuple[int, int]] = []
        t = since_ts
        while t < until_ts:
            chunks.append((t, min(t + chunk_secs, until_ts)))
            t += chunk_secs

        results: list[list[OrderFilledEvent]] = [[] for _ in chunks]
        to_fetch: list[tuple[int, int, int]] = []  # (index, since, until)

        for i, (cs, cu) in enumerate(chunks):
            cached = self._load_cache(cs, cu, cache_dir)
            if cached is not None:
                results[i] = cached
                logger.debug("goldsky_chunk_cached", since=cs, until=cu)
            else:
                to_fetch.append((i, cs, cu))

        if to_fetch:
            logger.info(
                "goldsky_parallel_start",
                chunks_total=len(chunks),
                chunks_to_fetch=len(to_fetch),
                workers=workers,
            )

            def _fetch_chunk(cs: int, cu: int) -> list[OrderFilledEvent]:
                # Each worker gets its own client/session to avoid thread-safety issues.
                client = GoldskyClient(url=self._url, batch_size=self._batch_size)
                try:
                    return client.fetch_events_since(cs, cu)
                finally:
                    client.close()

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_fetch_chunk, cs, cu): (i, cs, cu)
                    for i, cs, cu in to_fetch
                }
                for future in as_completed(futures):
                    i, cs, cu = futures[future]
                    try:
                        events = future.result()
                    except Exception as exc:
                        logger.warning("goldsky_chunk_failed", since=cs, until=cu, error=str(exc))
                        events = []
                    results[i] = events
                    if cache_dir and cu < cache_cutoff:
                        self._save_cache(cs, cu, events, cache_dir)

        # Merge, sort, deduplicate
        seen: set[str] = set()
        deduped: list[OrderFilledEvent] = []
        for event in sorted(
            (e for chunk in results for e in chunk),
            key=lambda e: (e.timestamp, e.transaction_hash),
        ):
            if event.transaction_hash not in seen:
                seen.add(event.transaction_hash)
                deduped.append(event)

        logger.info(
            "goldsky_parallel_done",
            count=len(deduped),
            chunks=len(chunks),
            fetched=len(to_fetch),
            cached=len(chunks) - len(to_fetch),
        )
        return deduped

    def fetch_events_since(
        self,
        since_ts: int,
        until_ts: int | None = None,
        exclude_platform_wallets: bool = True,
    ) -> list[OrderFilledEvent]:
        """Fetch all orderFilledEvents in (since_ts, until_ts]. Uses sticky-cursor
        pagination so that events sharing a timestamp are never dropped.
        """
        until_ts = until_ts if until_ts is not None else int(time.time())
        events: list[OrderFilledEvent] = []
        last_ts = since_ts
        last_id: str | None = None
        sticky_ts: int | None = None

        while True:
            batch = self._query_batch(last_ts, last_id, sticky_ts, until_ts)
            if not batch:
                if sticky_ts is not None:
                    last_ts = sticky_ts
                    sticky_ts = None
                    last_id = None
                    continue
                break

            for raw in batch:
                event = self._parse_event(raw)
                if exclude_platform_wallets and (
                    event.maker in PLATFORM_WALLETS or event.taker in PLATFORM_WALLETS
                ):
                    continue
                events.append(event)

            first_ts = int(batch[0]["timestamp"])
            batch_last_ts = int(batch[-1]["timestamp"])

            if len(batch) >= self._batch_size:
                sticky_ts = batch_last_ts
                last_id = batch[-1]["id"]
                if first_ts != batch_last_ts:
                    logger.debug(
                        "goldsky_sticky_boundary",
                        first_ts=first_ts,
                        last_ts=batch_last_ts,
                    )
            else:
                if sticky_ts is not None:
                    last_ts = sticky_ts
                    sticky_ts = None
                    last_id = None
                else:
                    last_ts = batch_last_ts
                if batch_last_ts >= until_ts:
                    break

        logger.info(
            "goldsky_fetched",
            count=len(events),
            since=datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat(),
        )
        return events

    def _query_batch(
        self,
        last_ts: int,
        last_id: str | None,
        sticky_ts: int | None,
        until_ts: int,
    ) -> list[dict]:
        if sticky_ts is not None:
            where = f'timestamp: "{sticky_ts}", id_gt: "{last_id}"'
        else:
            where = f'timestamp_gt: "{last_ts}", timestamp_lte: "{until_ts}"'

        query = (
            "{ orderFilledEvents("
            f"orderBy: timestamp, orderDirection: asc, first: {self._batch_size}, "
            f"where: {{{where}}}"
            ") { id timestamp maker makerAmountFilled makerAssetId "
            "taker takerAmountFilled takerAssetId transactionHash } }"
        )

        for attempt in range(self._max_retries):
            try:
                resp = self._session.post(self._url, json={"query": query}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if "errors" in data:
                    raise RuntimeError(data["errors"])
                return data.get("data", {}).get("orderFilledEvents", []) or []
            except (requests.RequestException, RuntimeError) as exc:
                wait = min(2**attempt, 30)
                logger.warning("goldsky_retry", attempt=attempt + 1, error=str(exc), wait=wait)
                time.sleep(wait)
        return []

    # --- disk cache helpers ---

    def _cache_path(self, since_ts: int, until_ts: int, cache_dir: Path) -> Path:
        return cache_dir / f"goldsky_{since_ts}_{until_ts}.pkl"

    def _load_cache(
        self, since_ts: int, until_ts: int, cache_dir: Path | None
    ) -> list[OrderFilledEvent] | None:
        if cache_dir is None:
            return None
        path = self._cache_path(since_ts, until_ts, cache_dir)
        if not path.exists():
            return None
        try:
            with path.open("rb") as fh:
                return pickle.load(fh)
        except Exception as exc:
            logger.warning("goldsky_cache_load_error", path=str(path), error=str(exc))
            return None

    def _save_cache(
        self,
        since_ts: int,
        until_ts: int,
        events: list[OrderFilledEvent],
        cache_dir: Path,
    ) -> None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._cache_path(since_ts, until_ts, cache_dir)
        try:
            with path.open("wb") as fh:
                pickle.dump(events, fh)
            logger.debug("goldsky_cache_saved", path=str(path), count=len(events))
        except Exception as exc:
            logger.warning("goldsky_cache_save_error", path=str(path), error=str(exc))

    @staticmethod
    def _parse_event(raw: dict) -> OrderFilledEvent:
        return OrderFilledEvent(
            timestamp=int(raw["timestamp"]),
            maker=raw["maker"],
            maker_asset_id=str(raw["makerAssetId"]),
            maker_amount_filled=Decimal(str(raw["makerAmountFilled"])),
            taker=raw["taker"],
            taker_asset_id=str(raw["takerAssetId"]),
            taker_amount_filled=Decimal(str(raw["takerAmountFilled"])),
            transaction_hash=raw["transactionHash"],
        )

    def close(self) -> None:
        self._session.close()
