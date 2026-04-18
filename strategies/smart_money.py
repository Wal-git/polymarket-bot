"""Follow-the-whales strategy.

Watches a configurable list of elite Polymarket wallets (via the Goldsky
subgraph). When at least `min_wallet_buys` distinct tracked wallets have
BOUGHT into the same outcome inside the lookback window, we enter a small
position on that outcome at a capped limit price.

Inspired by the trader-watchlist from warproxxx/poly_data:
    domah, 50pence, fhantom, car, theo4 — consistently profitable makers.

Config (config/default.yaml):
    strategies:
      - name: smart_money
        module: strategies.smart_money
        enabled: true
        config:
          wallets:
            - "0x9d84ce0306f8551e02efef1680475fc0f1dc1344"  # domah
            - "0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3"  # 50pence
            - "0x6356fb47642a028bc09df92023c35a21a0b41885"  # fhantom
            - "0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b"  # car
            - "0x56687bf447db6ffa42ffe2204a05edaa20f55839"  # theo4
          lookback_minutes: 60
          min_wallet_buys: 2
          max_entry_price: 0.85
          size_usdc: 10
"""
from __future__ import annotations

from decimal import Decimal

import structlog

from polybot.client.goldsky import GoldskyClient, OrderFilledEvent
from polybot.models.types import OrderRequest, OrderType, Side, SignalSet
from polybot.strategies.base import BaseStrategy, StrategyContext

logger = structlog.get_logger()


class SmartMoneyStrategy(BaseStrategy):
    NAME = "smart_money"

    _goldsky: GoldskyClient | None = None
    _cycle_events: list[OrderFilledEvent] | None = None

    def _client(self) -> GoldskyClient:
        if self._goldsky is None:
            self._goldsky = GoldskyClient()
        return self._goldsky

    def _fetch_once_per_cycle(self, lookback_minutes: int) -> list[OrderFilledEvent]:
        # StrategyContext is rebuilt per (strategy, market), but we only want
        # to hit Goldsky once per overall cycle.
        if self._cycle_events is None:
            self._cycle_events = self._client().recent_events(
                lookback_minutes=lookback_minutes
            )
        return self._cycle_events

    def reset_cycle_cache(self) -> None:
        # Clear per-cycle snapshot; GoldskyClient keeps its rolling cache.
        self._cycle_events = None

    def evaluate(self, ctx: StrategyContext) -> SignalSet:
        cfg = ctx.config
        wallets = {w.lower() for w in cfg.get("wallets", [])}
        if not wallets:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale="No tracked wallets configured",
            )

        lookback_minutes = int(cfg.get("lookback_minutes", 60))
        min_buys = int(cfg.get("min_wallet_buys", 2))
        max_price = Decimal(str(cfg.get("max_entry_price", "0.85")))
        size_usdc = Decimal(str(cfg.get("size_usdc", "10")))

        events = self._fetch_once_per_cycle(lookback_minutes)
        if not events:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale="No recent activity",
            )

        token_ids = {o.token_id for o in ctx.market.outcomes}

        # Count distinct tracked wallets that BOUGHT each outcome token.
        buys_by_token: dict[str, set[str]] = {t: set() for t in token_ids}
        for ev in events:
            asset = ev.non_usdc_asset_id
            if asset not in token_ids:
                continue
            # A tracked wallet buying shows up as that wallet being the *taker*
            # paying USDC OR the *maker* receiving USDC — poly_data's README
            # confirms maker trades reflect the user's perspective w/ price.
            if ev.taker.lower() in wallets and ev.taker_direction == "BUY":
                buys_by_token[asset].add(ev.taker.lower())
            elif ev.maker.lower() in wallets and ev.taker_direction == "SELL":
                # maker_direction is opposite of taker_direction — if taker sold,
                # maker bought.
                buys_by_token[asset].add(ev.maker.lower())

        for outcome in ctx.market.outcomes:
            wallet_count = len(buys_by_token.get(outcome.token_id, set()))
            if wallet_count < min_buys:
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
                    f"{wallet_count} tracked wallet(s) bought {outcome.label} "
                    f"in last {lookback_minutes}m; entering at {entry_price}"
                ),
                confidence=min(0.9, 0.4 + 0.15 * wallet_count),
            )

        return SignalSet(
            market_condition_id=ctx.market.condition_id,
            rationale="No wallet confluence",
        )
