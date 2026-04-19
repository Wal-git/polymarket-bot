"""Thin Polymarket Data API client with retry, rate-limiting, and disk cache."""
from __future__ import annotations

import hashlib
import json
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


class DataAPIClient:
    def __init__(self, base_url: str = DATA_API_BASE, cache_dir: Path = CACHE_DIR):
        self._base = base_url.rstrip("/")
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_req_ts: float = 0.0

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
        return self._get("/leaderboard", params={"period": period, "order": order, "limit": limit})

    def activity(
        self,
        user: str,
        start: int,
        end: int,
        activity_type: str = "TRADE",
    ) -> list[dict]:
        """Paginate /activity for a wallet over a time range. Cached on disk."""
        results: list[dict] = []
        offset = 0
        limit = 500
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
                cache_key=f"activity_{user}_{start}_{end}_{activity_type}_{offset}",
            )
            if not page:
                break
            results.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return results

    def positions(self, user: str) -> list[dict]:
        return self._get("/positions", params={"user": user, "sortBy": "CASHPNL", "limit": 500})

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
                resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning("data_api_error", path=path, error=str(exc))
            return []

    def _get_cached(self, path: str, params: dict, cache_key: str) -> Any:
        cache_file = self._cache_dir / f"{_slug(cache_key)}.json"
        if cache_file.exists():
            try:
                return json.loads(cache_file.read_text())
            except json.JSONDecodeError:
                pass
        result = self._get(path, params)
        try:
            cache_file.write_text(json.dumps(result))
        except OSError:
            pass
        return result

    def _rate_limit(self) -> None:
        now = time.monotonic()
        wait = _MIN_INTERVAL - (now - self._last_req_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_req_ts = time.monotonic()

    def close(self) -> None:
        self._session.close()


def _slug(key: str) -> str:
    return hashlib.sha1(key.encode()).hexdigest()[:16] + "_" + key[:40].replace("/", "_")
