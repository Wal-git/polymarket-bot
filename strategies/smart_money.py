"""Follow-the-whales strategy.

Watches elite Polymarket wallets identified by the smart-wallet pipeline.
When the weighted sum of tracked-wallet confluence on an outcome crosses a
threshold inside the lookback window, enters a limit order on that outcome.

Improvements over v1:
- Wallet list loaded ONCE per cycle (mtime-invalidated), not per market.
- Goldsky query filtered to whale addresses — 50-200x less data.
- Event index built once per cycle; per-market lookup is O(1).
- Signal + closer archetypes unioned (max score per wallet).
- SELL detection: when a triggering whale exits, the bot closes its position.
- Confluence-proportional position sizing.
- Per-market cooldown to prevent duplicate orders.
- Slippage guard: entry must be within N% of whale's avg fill price.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

import structlog

from polybot.client.goldsky import GoldskyClient, OrderFilledEvent
from polybot.models.types import OrderRequest, OrderType, Position, Side, SignalSet
from polybot.strategies.base import BaseStrategy, StrategyContext

logger = structlog.get_logger()

_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
_SMART_WALLETS_SIGNAL_JSON = _DATA_DIR / "smart_wallets_signal.json"
_SMART_WALLETS_CLOSER_JSON = _DATA_DIR / "smart_wallets_closer.json"
_SMART_WALLETS_JSON = _DATA_DIR / "smart_wallets.json"
_SM_POSITIONS_FILE = _DATA_DIR / "smart_money_positions.json"

_DEFAULT_STALE_DAYS = 3
_DEFAULT_MIN_SCORE = 0.0
_ARCHETYPE_PATHS = [_SMART_WALLETS_SIGNAL_JSON, _SMART_WALLETS_CLOSER_JSON, _SMART_WALLETS_JSON]


# ---------------------------------------------------------------------------
# wallet loading (unions signal + closer archetypes)
# ---------------------------------------------------------------------------

def _load_smart_wallets(
    min_score: float = _DEFAULT_MIN_SCORE,
    stale_days: int = _DEFAULT_STALE_DAYS,
) -> dict[str, float]:
    """Return {proxy_wallet: score} unioned from signal + closer JSONs.

    Uses ``signal_score`` from signal archetype and ``score`` from closer
    archetype; keeps the max across both per wallet.
    """
    candidates: list[dict] = []
    for path in _ARCHETYPE_PATHS:
        if path.exists():
            try:
                data = json.loads(path.read_text())
                candidates.extend(data.get("wallets", []))
            except (OSError, json.JSONDecodeError):
                continue

    cutoff = time.time() - stale_days * 86400
    wallets: dict[str, float] = {}
    for w in candidates:
        addr = str(w.get("proxy_wallet", "")).lower()
        if not addr:
            continue
        score = float(w.get("signal_score") or w.get("score") or 0.0)
        last_active = int(w.get("last_active_ts") or 0)
        if score < min_score:
            continue
        if last_active and last_active < cutoff:
            continue
        if addr not in wallets or score > wallets[addr]:
            wallets[addr] = score

    logger.info("smart_wallets_loaded", count=len(wallets), min_score=min_score)
    return wallets


# ---------------------------------------------------------------------------
# sm-position persistence
# ---------------------------------------------------------------------------

def _load_sm_positions() -> dict[str, set[str]]:
    """Load {token_id: {wallet_addr, ...}} from disk."""
    if not _SM_POSITIONS_FILE.exists():
        return {}
    try:
        data = json.loads(_SM_POSITIONS_FILE.read_text())
        return {k: set(v) for k, v in data.items()}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_sm_positions(positions: dict[str, set[str]]) -> None:
    try:
        _SM_POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SM_POSITIONS_FILE.write_text(
            json.dumps({k: sorted(v) for k, v in positions.items()}, indent=2)
        )
    except OSError:
        pass


# ---------------------------------------------------------------------------
# strategy
# ---------------------------------------------------------------------------

class SmartMoneyStrategy(BaseStrategy):
    NAME = "smart_money"

    def __init__(self) -> None:
        self._goldsky: GoldskyClient | None = None
        # Cycle-level caches (cleared by reset_cycle_cache)
        self._cycle_events: list[OrderFilledEvent] | None = None
        self._cycle_events_by_token: dict[str, list[OrderFilledEvent]] | None = None
        # Wallet scores — loaded once per cycle, mtime-invalidated between cycles
        self._wallet_scores: dict[str, float] | None = None
        self._wallet_mtime: float = 0.0
        # Persistent cross-cycle state
        self._sm_positions: dict[str, set[str]] = _load_sm_positions()
        self._cooldown: dict[str, float] = {}

    # ------------------------------------------------------------------
    # lifecycle hooks (called by engine)
    # ------------------------------------------------------------------

    def reset_cycle_cache(self) -> None:
        """Clear per-cycle event cache. Engine calls this at start of each cycle."""
        self._cycle_events = None
        self._cycle_events_by_token = None

    def pre_cycle(self, positions: list[Position], cfg: dict) -> list[str]:
        """Warm Goldsky cache and return token_ids where a triggering whale exited.

        Engine calls this once per cycle AFTER reset_cycle_cache, BEFORE
        per-market evaluate() calls. Returned token_ids are closed by the engine.
        """
        wallet_scores = self._refresh_wallet_scores(cfg)
        if not wallet_scores:
            return []

        lookback_minutes = int(cfg.get("lookback_minutes", 60))
        self._warm_events(wallet_scores, lookback_minutes)
        return self._detect_whale_exits(positions, wallet_scores)

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _goldsky_client(self) -> GoldskyClient:
        if self._goldsky is None:
            self._goldsky = GoldskyClient()
        return self._goldsky

    def _refresh_wallet_scores(self, cfg: dict) -> dict[str, float]:
        min_score = float(cfg.get("min_wallet_score", _DEFAULT_MIN_SCORE))
        stale_days = int(cfg.get("stale_days", _DEFAULT_STALE_DAYS))
        mtime = max(
            (p.stat().st_mtime for p in _ARCHETYPE_PATHS if p.exists()),
            default=0.0,
        )
        if self._wallet_scores is None or mtime > self._wallet_mtime:
            dynamic = _load_smart_wallets(min_score=min_score, stale_days=stale_days)
            self._wallet_scores = dynamic or {
                str(w).lower(): 1.0 for w in cfg.get("wallets", [])
            }
            self._wallet_mtime = mtime
        return self._wallet_scores

    def _warm_events(self, wallet_scores: dict[str, float], lookback_minutes: int) -> None:
        if self._cycle_events is not None:
            return
        wallets = list(wallet_scores.keys())
        self._cycle_events = self._goldsky_client().recent_events_for_wallets(
            wallets=wallets, lookback_minutes=lookback_minutes
        )
        by_token: dict[str, list[OrderFilledEvent]] = {}
        for ev in self._cycle_events:
            by_token.setdefault(ev.non_usdc_asset_id, []).append(ev)
        self._cycle_events_by_token = by_token

    def _detect_whale_exits(
        self,
        positions: list[Position],
        wallet_scores: dict[str, float],
    ) -> list[str]:
        if not self._sm_positions:
            return []
        held = {p.token_id for p in positions}
        to_close: list[str] = []
        by_token = self._cycle_events_by_token or {}

        stale = [t for t in self._sm_positions if t not in held]
        for t in stale:
            del self._sm_positions[t]

        for token_id, triggering_wallets in list(self._sm_positions.items()):
            for ev in by_token.get(token_id, []):
                # Taker sold token (direction SELL) or maker received USDC (taker bought)
                seller: str | None = None
                if ev.taker_direction == "SELL" and ev.taker.lower() in triggering_wallets:
                    seller = ev.taker.lower()
                elif ev.taker_direction == "BUY" and ev.maker.lower() in triggering_wallets:
                    seller = ev.maker.lower()
                if seller:
                    logger.info("smart_money_whale_exit", token_id=token_id, wallet=seller)
                    to_close.append(token_id)
                    del self._sm_positions[token_id]
                    break

        if stale or to_close:
            _save_sm_positions(self._sm_positions)
        return to_close

    # ------------------------------------------------------------------
    # evaluate
    # ------------------------------------------------------------------

    def evaluate(self, ctx: StrategyContext) -> SignalSet:
        cfg = ctx.config
        wallet_scores = self._refresh_wallet_scores(cfg)

        if not wallet_scores:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale="No tracked wallets configured",
            )

        lookback_minutes = int(cfg.get("lookback_minutes", 60))
        min_confluence_weight = Decimal(str(cfg.get("min_confluence_weight", "1.5")))
        min_wallet_buys = int(cfg.get("min_wallet_buys", 2))
        max_price = Decimal(str(cfg.get("max_entry_price", "0.75")))
        base_size_usdc = Decimal(str(cfg.get("size_usdc", "10")))
        cooldown_minutes = int(cfg.get("cooldown_minutes", 15))
        max_weight_cap = float(cfg.get("max_weight_cap", 5.0))
        max_slippage_pct = float(cfg.get("max_price_slippage_pct", 0.10))

        # Per-market cooldown
        now = time.time()
        last_ts = self._cooldown.get(ctx.market.condition_id, 0.0)
        if now - last_ts < cooldown_minutes * 60:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale=f"Cooldown active ({(now - last_ts) / 60:.1f}m remaining)",
            )

        # Ensure events are warmed (pre_cycle may not have been called in tests)
        self._warm_events(wallet_scores, lookback_minutes)
        if not self._cycle_events:
            return SignalSet(
                market_condition_id=ctx.market.condition_id,
                rationale="No recent whale activity",
            )

        token_ids = {o.token_id for o in ctx.market.outcomes}
        by_token = self._cycle_events_by_token or {}

        buyers_by_token: dict[str, set[str]] = {t: set() for t in token_ids}
        whale_prices_by_token: dict[str, list[Decimal]] = {t: [] for t in token_ids}

        for token_id in token_ids:
            for ev in by_token.get(token_id, []):
                buyer: str | None = None
                if ev.taker_direction == "BUY" and ev.taker.lower() in wallet_scores:
                    buyer = ev.taker.lower()
                elif ev.taker_direction == "SELL" and ev.maker.lower() in wallet_scores:
                    buyer = ev.maker.lower()
                if buyer:
                    buyers_by_token[token_id].add(buyer)
                    if ev.token_amount > 0:
                        whale_prices_by_token[token_id].append(ev.price)

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

            # Slippage guard: don't chase price far past whale's avg fill
            whale_prices = whale_prices_by_token.get(outcome.token_id, [])
            if whale_prices:
                avg_whale_price = sum(whale_prices) / len(whale_prices)
                slippage = abs(float(entry_price) - float(avg_whale_price)) / max(float(avg_whale_price), 1e-9)
                if slippage > max_slippage_pct:
                    logger.info(
                        "smart_money_skip_slippage",
                        token_id=outcome.token_id,
                        entry=str(entry_price),
                        whale_avg=str(avg_whale_price),
                        slippage=f"{slippage:.2%}",
                    )
                    continue

            # Confluence-proportional sizing
            weight_factor = Decimal(str(min(total_weight, max_weight_cap) / max_weight_cap))
            size_usdc = max((base_size_usdc * weight_factor).quantize(Decimal("0.01")), Decimal("1"))
            size_shares = (size_usdc / entry_price).quantize(Decimal("0.01"))
            if size_shares <= 0:
                continue

            # Record position provenance + cooldown
            self._sm_positions.setdefault(outcome.token_id, set()).update(buyers)
            _save_sm_positions(self._sm_positions)
            self._cooldown[ctx.market.condition_id] = now

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
                    f"{wallet_count} wallet(s) (weight={float(total_weight):.2f}) "
                    f"bought {outcome.label} in last {lookback_minutes}m; "
                    f"entering at {entry_price} (${float(size_usdc):.2f})"
                ),
                confidence=confidence,
            )

        return SignalSet(
            market_condition_id=ctx.market.condition_id,
            rationale="No wallet confluence",
        )
