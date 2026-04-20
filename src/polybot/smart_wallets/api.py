"""Thin Polymarket Data API client with retry, rate-limiting, and disk cache."""
from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any

import requests
import structlog
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from polybot.smart_wallets.config import CACHE_DIR, DATA_API_BASE, MAX_RPS

logger = structlog.get_logger()

_MIN_INTERVAL = 1.0 / MAX_RPS


def weekly_run_epoch(now: int | None = None) -> int:
    """Quantize a timestamp to the most recent Monday 00:00 UTC.

    Used to make cache keys reusable across all runs within the same week.
    """
    now = int(now if now is not None else time.time())
    # Monday 00:00 UTC boundary. time.gmtime().tm_wday: Monday=0.
    tm = time.gmtime(now)
    seconds_since_monday = (
        tm.tm_wday * 86400 + tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec
    )
    return now - seconds_since_monday


class DataAPIClient:
    def __init__(
        self,
        base_url: str = DATA_API_BASE,
        cache_dir: Path = CACHE_DIR,
        cache_ttl_seconds: int = 7 * 86400,
    ):
        self._base = base_url.rstrip("/")
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_ttl = cache_ttl_seconds
        self._last_req_ts: float = 0.0
        self._rate_lock = threading.Lock()
        self._cache_lock = threading.Lock()

        retry = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session = requests.Session()
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    # ------------------------------------------------------------------
    # public helpers
    # ------------------------------------------------------------------

    def leaderboard(self, period: str, order: str = "pnl", limit: int = 500) -> list[dict]:
        return self._get("/leaderboard", params={"period": period, "order": order, "limit": limit}) or []

    def activity(
        self,
        user: str,
        start: int,
        end: int,
        activity_type: str = "TRADE",
    ) -> list[dict]:
        """Paginate /activity for a wallet over a time range. Cached on disk.

        The cache key quantizes ``end`` to the current weekly-run epoch so that
        multiple runs in the same week can reuse it. The underlying HTTP call
        still uses the original ``end``.
        """
        results: list[dict] = []
        offset = 0
        limit = 500
        cache_epoch = weekly_run_epoch(end)
        while True:
            page = self._get_cached(
                "/activity",
                params={
                    "user": user,
                    "start": start,
                    "end": end,
                    "type": activity_type,
                    "limit": limit,
                    "offset": offset,
                },
                cache_key=(
                    f"activity_{user}_{start}_{cache_epoch}_{activity_type}_{offset}"
                ),
            )
            if not page:
                break
            results.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return results

    def positions(self, user: str) -> list[dict]:
        resp = self._get("/positions", params={"user": user, "sortBy": "CASHPNL", "limit": 500})
        return resp or []

    def value(self, user: str) -> dict:
        resp = self._get("/value", params={"user": user})
        if isinstance(resp, list):
            return resp[0] if resp else {}
        return resp or {}

    # ------------------------------------------------------------------
    # internal
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> Any:
        self._rate_limit()
        url = self._base + path
        try:
            resp = self._session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                time.sleep(5)
                self._rate_limit()
                resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning("data_api_error", path=path, error=str(exc))
            return []

    def _get_cached(self, path: str, params: dict, cache_key: str) -> Any:
        cache_file = self._cache_dir / f"{_slug(cache_key)}.json"
        with self._cache_lock:
            if cache_file.exists():
                age = time.time() - cache_file.stat().st_mtime
                if age < self._cache_ttl:
                    try:
                        return json.loads(cache_file.read_text())
                    except json.JSONDecodeError:
                        pass
        result = self._get(path, params)
        try:
            with self._cache_lock:
                cache_file.write_text(json.dumps(result))
        except OSError:
            pass
        return result

    def _rate_limit(self) -> None:
        """Block the caller until at least _MIN_INTERVAL since the last request."""
        with self._rate_lock:
            now = time.monotonic()
            wait = _MIN_INTERVAL - (now - self._last_req_ts)
            if wait > 0:
                time.sleep(wait)
                self._last_req_ts = time.monotonic()
            else:
                self._last_req_ts = now

    def close(self) -> None:
        self._session.close()


def _slug(key: str) -> str:
    return hashlib.sha1(key.encode()).hexdigest()[:16] + "_" + key[:40].replace("/", "_")
