"""Pull candidate wallets from the Polymarket leaderboard across periods."""
from __future__ import annotations

import structlog

from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.config import LEADERBOARD_LIMIT

logger = structlog.get_logger()

_PERIODS = ["7d", "30d", "all"]
_ORDERS = ["pnl", "volume"]


def fetch_candidates(client: DataAPIClient, limit: int = LEADERBOARD_LIMIT) -> list[dict]:
    """Return deduplicated wallet dicts from leaderboard (all periods × sort orders)."""
    seen: dict[str, dict] = {}  # proxy_wallet -> raw row

    for period in _PERIODS:
        for order in _ORDERS:
            rows = client.leaderboard(period=period, order=order, limit=limit)
            if not isinstance(rows, list):
                logger.warning("leaderboard_bad_response", period=period, order=order)
                continue
            for row in rows:
                wallet = (row.get("proxyWallet") or row.get("proxy_wallet") or "").lower()
                if not wallet or wallet in seen:
                    continue
                seen[wallet] = {
                    "proxy_wallet": wallet,
                    "username": row.get("name") or row.get("username") or "",
                    "leaderboard_pnl": _to_float(row.get("pnl") or row.get("pnlPerShare")),
                    "leaderboard_volume": _to_float(row.get("volume")),
                }

            logger.info(
                "leaderboard_fetched",
                period=period,
                order=order,
                new=len(seen),
            )

    candidates = list(seen.values())
    logger.info("candidates_total", count=len(candidates))
    return candidates


def _to_float(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
