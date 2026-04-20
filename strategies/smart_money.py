"""Follow-the-whales strategy.

Watches a configurable list of elite Polymarket wallets (via the Goldsky
subgraph). When the weighted sum of tracked-wallet confluence on an outcome
crosses a threshold inside the lookback window, enters a capped limit order
on that outcome.

Wallets and their per-wallet scores are loaded from the signal-archetype
JSON produced by the smart-wallet pipeline. A stale wallet (last_active_ts
too old) is excluded at load time.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

import structlog

from polybot.client.goldsky import GoldskyClient, OrderFilledEvent
from polybot.models.types import OrderRequest, OrderType, Side, SignalSet
from polybot.strategies.base import BaseStrategy, StrategyContext

logger = structlog.get_logger()

_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
_SMART_WALLETS_SIGNAL_JSON = _DATA_DIR / "smart_wallets_signal.json"
_SMART_WALLETS_JSON = _DATA_DIR / "smart_wallets.json"

_DEFAULT_STALE_DAYS = 3
_DEFAULT_MIN_SCORE = 0.0


def _load_smart_wallets(
    min_score: float = _DEFAULT_MIN_SCORE,
    stale_days: int = _DEFAULT_STALE_DAYS,
) -> dict[str, float]:
    """Return {proxy_wallet: score} from signal JSON (falls back to closer JSON).

    Drops wallets with stale last_active_ts or score below threshold.
    """
    path = _SMART_WALLETS_SIGNAL_JSON if _SMART_WALLETS_SIGNAL_JSON.exists() else _SMART_WALLETS_JSON
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}

    cutoff = time.time() - stale_days * 86400
    wallets: dict[str, float] = {}
    for w in data.get("wallets", []):
        addr = str(w.get("proxy_wallet", "")).lower()
        if not addr:
            continue
        score = float(w.get("score") or w.get("signal_score") or 0.0)
        last_active = int(w.get("last_active_ts") or 0)
        if score < min_score:
            continue
        if last_active and last_active < cutoff:
            continue
        wallets[addr] = score
    logger.info(
        "smart_wallets_loaded",
        count=len(wallets),
        path=str(path),
        min_score=min_score,
    )
    return wallets


class SmartMoneyStrategy(BaseStrategy):
    NAME = "smart_money"

    _goldsky: GoldskyClient | None = None
    _cycle_events: list[OrderFilledEvent] | None = None

    def _client(self) -> GoldskyClient:
        if self._goldsky is None:
            self._goldsky = GoldskyClient()
        return self._goldsky

    def _fetch_once_per_cycle(self, lookback_minutes: int) -> list[OrderFilledEvent]:
        if self._cycle_events is None:
            self._cycle_events = self._client().recent_events(
                lookback_minutes=lookback_minutes
            )
        return self._cycle_events

    def reset_cycle_cache(self) -> None:
        self._cycle_events = None

    def evaluate(self, ctx: StrategyContext) -> SignalSet:
        cfg = ctx.config
        min_score = float(cfg.get("min_wallet_score", _DEFAULT_MIN_SCORE))
        stale_days = int(cfg.get("stale_days", _DEFAULT_STALE_DAYS))

        dynamic = _load_smart_wallets(min_score=min_score, stale_days=stale_days)
        if dynamic:
            wallet_scores = dynamic
        else:
            # YAML fallback: treat all at score 1.0.
            wallet_scores = {str(w).lower(): 1.0 for w in cfg.get("wallets", [])}

        if not wallet_scores:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale="No tracked wallets configured",
            )

        lookback_minutes = int(cfg.get("lookback_minutes", 60))
        min_confluence_weight = Decimal(str(cfg.get("min_confluence_weight", "1.5")))
        min_wallet_buys = int(cfg.get("min_wallet_buys", 2))
        max_price = Decimal(str(cfg.get("max_entry_price", "0.85")))
        size_usdc = Decimal(str(cfg.get("size_usdc", "10")))

        events = self._fetch_once_per_cycle(lookback_minutes)
        if not events:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale="No recent activity",
            )

        token_ids = {o.token_id for o in ctx.market.outcomes}

        buyers_by_token: dict[str, set[str]] = {t: set() for t in token_ids}
        for ev in events:
            asset = ev.non_usdc_asset_id
            if asset not in token_ids:
                continue
            if ev.taker.lower() in wallet_scores and ev.taker_direction == "BUY":
                buyers_by_token[asset].add(ev.taker.lower())
            elif ev.maker.lower() in wallet_scores and ev.taker_direction == "SELL":
                # If taker sold, maker bought.
                buyers_by_token[asset].add(ev.maker.lower())

        for outcome in ctx.market.outcomes:
            buyers = buyers_by_token.get(outcome.token_id, set())
            wallet_count = len(buyers)
            total_weight = sum(wallet_scores[w] for w in buyers)

            if wallet_count < min_wallet_buys:
                continue
            if Decimal(str(total_weight)) < min_confluence_weight:
                continue

            entry_price = outcome.best_ask or outcome.price
            if entry_price is None or entry_price <= 0 or entry_price > max_price:
                logger.info(
                    "smart_money_skip_price",
                    token_id=outcome.token_id,
                    entry_price=str(entry_price),
                    max_price=str(max_price),
                )
                continue

            size_shares = (size_usdc / entry_price).quantize(Decimal("0.01"))
            if size_shares <= 0:
                continue

            confidence = min(0.95, 0.3 + 0.15 * float(total_weight))
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                orders=[
                    OrderRequest(
                        token_id=outcome.token_id,
                        side=Side.BUY,
                        order_type=OrderType.LIMIT,
                        size=size_shares,
                        limit_price=entry_price,
                    )
                ],
                rationale=(
                    f"{wallet_count} tracked wallet(s) (weight={float(total_weight):.2f}) "
                    f"bought {outcome.label} in last {lookback_minutes}m; "
                    f"entering at {entry_price}"
                ),
                confidence=confidence,
            )

        return SignalSet(
            market_condition_id=ctx.market.condition_id,
            rationale="No wallet confluence",
        )
