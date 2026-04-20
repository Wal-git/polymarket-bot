"""Forward-validation backtest for smart-wallet selections.

Takes a historical run's selected wallets and evaluates how that cohort
actually performed in the window *after* the run. Provides the pipeline's
ground-truth quality KPI: did last week's Top-K outperform a random/all
baseline this week?
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import structlog

from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.metrics import compute_all_metrics
from polybot.smart_wallets.store import Store

logger = structlog.get_logger()


@dataclass
class BacktestResult:
    run_id: int
    forward_days: int
    cohort_size: int
    cohort_mean_pnl: float
    cohort_median_pnl: float
    cohort_hit_rate: float
    baseline_size: int
    baseline_mean_pnl: float
    baseline_median_pnl: float
    baseline_hit_rate: float
    per_wallet: list[dict] = field(default_factory=list)

    @property
    def edge_vs_baseline(self) -> float:
        return self.cohort_mean_pnl - self.baseline_mean_pnl

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "forward_days": self.forward_days,
            "cohort_size": self.cohort_size,
            "cohort_mean_pnl": self.cohort_mean_pnl,
            "cohort_median_pnl": self.cohort_median_pnl,
            "cohort_hit_rate": self.cohort_hit_rate,
            "baseline_size": self.baseline_size,
            "baseline_mean_pnl": self.baseline_mean_pnl,
            "baseline_median_pnl": self.baseline_median_pnl,
            "baseline_hit_rate": self.baseline_hit_rate,
            "edge_vs_baseline": self.edge_vs_baseline,
        }


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def _forward_pnl(
    wallet: str,
    client: DataAPIClient,
    start_ts: int,
    end_ts: int,
) -> float:
    trades = client.activity(wallet, start=start_ts, end=end_ts, activity_type="TRADE")
    redeems = client.activity(wallet, start=start_ts, end=end_ts, activity_type="REDEEM")
    positions = client.positions(wallet)
    shell = {
        "raw_trades": trades,
        "raw_redeems": redeems,
        "raw_positions": positions,
    }
    metrics = compute_all_metrics(shell)
    return float(metrics["pnl_realized"])


def evaluate_run(
    run_id: int,
    forward_days: int = 7,
    baseline_wallets: list[str] | None = None,
) -> BacktestResult:
    """Evaluate the forward-N-day realized PnL of a run's selected wallets."""
    store = Store()
    client = DataAPIClient()
    try:
        run_row = store._con.execute(  # noqa: SLF001 — internal use
            "SELECT started_at FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if not run_row:
            raise ValueError(f"Run {run_id} not found")

        started_at = datetime.fromisoformat(run_row["started_at"].replace("Z", "+00:00"))
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        start_ts = int(started_at.timestamp())
        end_ts = min(int(time.time()), start_ts + forward_days * 86400)

        cohort = [r["proxy_wallet"] for r in store.snapshot_for_run(run_id)]
        if not cohort:
            raise ValueError(f"No snapshot wallets for run {run_id}")

        cohort_pnl: list[float] = []
        per_wallet: list[dict] = []
        for wallet in cohort:
            pnl = _forward_pnl(wallet, client, start_ts, end_ts)
            cohort_pnl.append(pnl)
            per_wallet.append({"wallet": wallet, "pnl": pnl})

        cohort_hits = sum(1 for p in cohort_pnl if p > 0) / max(len(cohort_pnl), 1)
        cohort_mean = sum(cohort_pnl) / max(len(cohort_pnl), 1)

        if baseline_wallets:
            baseline_pnl = [
                _forward_pnl(w, client, start_ts, end_ts) for w in baseline_wallets
            ]
        else:
            baseline_pnl = []
        baseline_hits = (
            sum(1 for p in baseline_pnl if p > 0) / len(baseline_pnl)
            if baseline_pnl
            else 0.0
        )
        baseline_mean = sum(baseline_pnl) / len(baseline_pnl) if baseline_pnl else 0.0

        result = BacktestResult(
            run_id=run_id,
            forward_days=forward_days,
            cohort_size=len(cohort),
            cohort_mean_pnl=cohort_mean,
            cohort_median_pnl=_median(cohort_pnl),
            cohort_hit_rate=cohort_hits,
            baseline_size=len(baseline_pnl),
            baseline_mean_pnl=baseline_mean,
            baseline_median_pnl=_median(baseline_pnl),
            baseline_hit_rate=baseline_hits,
            per_wallet=per_wallet,
        )
        logger.info(
            "backtest_done",
            run_id=run_id,
            forward_days=forward_days,
            cohort_mean=cohort_mean,
            baseline_mean=baseline_mean,
        )
        return result
    finally:
        client.close()
        store.close()
