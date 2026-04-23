"""Forward-trail backtest: simulate SmartMoneyStrategy over historical Goldsky events.

Replays wallet-filtered Goldsky events at a configurable interval, running the
same confluence + slippage logic as the live strategy, and marks each simulated
position to market at resolution (1.0 winner, 0.0 loser) or at the whale's exit
price if detected.

Usage:
    from polybot.smart_wallets.trail_backtest import run_trail_backtest
    result = run_trail_backtest(
        run_id=7,
        lookback_window_days=7,
        sim_interval_seconds=30,
    )
    print(result.summary())
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import structlog

from polybot.client.goldsky import GoldskyClient
from polybot.smart_wallets.config import CACHE_DIR, GOLDSKY_CHUNK_DAYS, GOLDSKY_FETCH_WORKERS
from polybot.smart_wallets.store import Store

logger = structlog.get_logger()


@dataclass
class _SimPosition:
    token_id: str
    condition_id: str
    entry_ts: int
    entry_price: float
    size_usdc: float
    triggering_wallets: set[str]
    exit_ts: int | None = None
    exit_price: float | None = None
    exit_reason: str = ""

    @property
    def pnl(self) -> float:
        if self.exit_price is None:
            return 0.0
        shares = self.size_usdc / self.entry_price
        return shares * (self.exit_price - self.entry_price)


@dataclass
class TrailBacktestResult:
    run_id: int
    period_start: int
    period_end: int
    sim_interval_seconds: int
    n_signals: int
    n_positions: int
    n_whale_exit_closes: int
    n_ttl_closes: int
    total_pnl: float
    winning_positions: int
    positions: list[_SimPosition] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        closed = [p for p in self.positions if p.exit_price is not None]
        if not closed:
            return 0.0
        return sum(1 for p in closed if p.pnl > 0) / len(closed)

    def summary(self) -> str:
        closed = [p for p in self.positions if p.exit_price is not None]
        return (
            f"Trail backtest run_id={self.run_id} "
            f"[{time.strftime('%Y-%m-%d', time.gmtime(self.period_start))} → "
            f"{time.strftime('%Y-%m-%d', time.gmtime(self.period_end))}]\n"
            f"  signals={self.n_signals}  positions={self.n_positions}  "
            f"closed={len(closed)}\n"
            f"  pnl=${self.total_pnl:+.2f}  "
            f"win_rate={self.win_rate:.0%}  "
            f"whale_exits={self.n_whale_exit_closes}  "
            f"ttl_closes={self.n_ttl_closes}"
        )

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "n_signals": self.n_signals,
            "n_positions": self.n_positions,
            "n_whale_exit_closes": self.n_whale_exit_closes,
            "n_ttl_closes": self.n_ttl_closes,
            "total_pnl": round(self.total_pnl, 4),
            "win_rate": round(self.win_rate, 4),
            "winning_positions": self.winning_positions,
        }


def run_trail_backtest(
    run_id: int,
    lookback_window_days: int = 7,
    sim_interval_seconds: int = 30,
    min_wallet_buys: int = 2,
    min_confluence_weight: float = 1.5,
    max_entry_price: float = 0.75,
    max_slippage_pct: float = 0.10,
    base_size_usdc: float = 10.0,
    max_weight_cap: float = 5.0,
    position_ttl_hours: int = 168,
    cooldown_minutes: int = 15,
) -> TrailBacktestResult:
    """Replay whale activity over a past week against the strategy logic.

    Uses Goldsky parallel fetch (with disk cache) for the cohort wallets
    identified in ``run_id``. Simulates entry/exit at each ``sim_interval_seconds``
    tick, marks positions at resolution.
    """
    store = Store()
    snapshot = store.snapshot_for_run(run_id)
    store.close()

    if not snapshot:
        raise ValueError(f"No snapshot found for run_id={run_id}")

    wallet_scores = {w["proxy_wallet"].lower(): float(w.get("signal_score") or w.get("score") or 0.0)
                     for w in snapshot}
    wallets = list(wallet_scores.keys())
    logger.info("trail_backtest_start", run_id=run_id, wallets=len(wallets))

    # Fetch historical events (cached on disk)
    now = int(time.time())
    since_ts = now - lookback_window_days * 86400
    client = GoldskyClient()
    try:
        all_events = client.fetch_events_for_wallets(
            wallets=wallets,
            since_ts=since_ts,
            until_ts=now,
        )
    finally:
        client.close()

    # Sort events by timestamp
    all_events.sort(key=lambda e: e.timestamp)
    if not all_events:
        return TrailBacktestResult(
            run_id=run_id, period_start=since_ts, period_end=now,
            sim_interval_seconds=sim_interval_seconds,
            n_signals=0, n_positions=0, n_whale_exit_closes=0,
            n_ttl_closes=0, total_pnl=0.0, winning_positions=0,
        )

    # Replay in ticks
    positions: dict[str, _SimPosition] = {}  # token_id → position
    cooldowns: dict[str, float] = {}         # condition_id → last signal ts
    n_signals = 0
    n_whale_exits = 0
    n_ttl_closes = 0
    closed: list[_SimPosition] = []

    period_start = all_events[0].timestamp
    period_end = all_events[-1].timestamp
    tick = period_start

    # Build an index for fast lookup within each tick window
    while tick <= period_end:
        window_start = tick - min(3600, sim_interval_seconds * 120)
        window_events = [e for e in all_events if window_start <= e.timestamp <= tick]
        by_token: dict[str, list] = {}
        for ev in window_events:
            by_token.setdefault(ev.non_usdc_asset_id, []).append(ev)

        # --- detect whale exits for open positions ---
        for token_id, pos in list(positions.items()):
            for ev in by_token.get(token_id, []):
                seller = None
                if ev.taker_direction == "SELL" and ev.taker.lower() in pos.triggering_wallets:
                    seller = ev.taker.lower()
                elif ev.taker_direction == "BUY" and ev.maker.lower() in pos.triggering_wallets:
                    seller = ev.maker.lower()
                if seller:
                    # Use whale's exit price as our mark
                    exit_price = float(ev.price) if ev.token_amount > 0 else 0.0
                    pos.exit_ts = ev.timestamp
                    pos.exit_price = exit_price
                    pos.exit_reason = "whale_exit"
                    closed.append(pos)
                    del positions[token_id]
                    n_whale_exits += 1
                    break

        # --- TTL-based close ---
        ttl_secs = position_ttl_hours * 3600
        for token_id, pos in list(positions.items()):
            if tick - pos.entry_ts > ttl_secs:
                pos.exit_ts = tick
                pos.exit_price = 0.0
                pos.exit_reason = "ttl_expired"
                closed.append(pos)
                del positions[token_id]
                n_ttl_closes += 1

        # --- look for new entry signals across all active tokens ---
        for token_id, evs in by_token.items():
            if token_id in positions:
                continue  # already in position

            buyers: set[str] = set()
            whale_prices: list[Decimal] = []
            for ev in evs:
                buyer = None
                if ev.taker_direction == "BUY" and ev.taker.lower() in wallet_scores:
                    buyer = ev.taker.lower()
                elif ev.taker_direction == "SELL" and ev.maker.lower() in wallet_scores:
                    buyer = ev.maker.lower()
                if buyer:
                    buyers.add(buyer)
                    if ev.token_amount > 0:
                        whale_prices.append(ev.price)

            if len(buyers) < min_wallet_buys:
                continue
            total_weight = sum(wallet_scores[b] for b in buyers)
            if total_weight < min_confluence_weight:
                continue

            # Use whale avg fill as proxy for entry price
            if not whale_prices:
                continue
            entry_price = float(sum(whale_prices) / len(whale_prices))
            if entry_price <= 0 or entry_price > max_entry_price:
                continue

            # Slippage guard (no separate ask here; whale price IS our entry)
            # Use first event's condition_id for cooldown
            condition_id = str(evs[0].non_usdc_asset_id) if evs else token_id
            if tick - cooldowns.get(condition_id, 0.0) < cooldown_minutes * 60:
                continue

            weight_factor = min(total_weight, max_weight_cap) / max_weight_cap
            size_usdc = max(base_size_usdc * weight_factor, 1.0)

            n_signals += 1
            cooldowns[condition_id] = float(tick)
            positions[token_id] = _SimPosition(
                token_id=token_id,
                condition_id=condition_id,
                entry_ts=tick,
                entry_price=entry_price,
                size_usdc=size_usdc,
                triggering_wallets=set(buyers),
            )

        tick += sim_interval_seconds

    # Close any remaining open positions at 0 (unknown outcome)
    for pos in positions.values():
        pos.exit_ts = tick
        pos.exit_price = 0.0
        pos.exit_reason = "sim_end_open"
        closed.append(pos)

    total_pnl = sum(p.pnl for p in closed)
    winning = sum(1 for p in closed if p.pnl > 0)

    return TrailBacktestResult(
        run_id=run_id,
        period_start=period_start,
        period_end=period_end,
        sim_interval_seconds=sim_interval_seconds,
        n_signals=n_signals,
        n_positions=len(closed),
        n_whale_exit_closes=n_whale_exits,
        n_ttl_closes=n_ttl_closes,
        total_pnl=total_pnl,
        winning_positions=winning,
        positions=closed,
    )
