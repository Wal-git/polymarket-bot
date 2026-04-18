import signal
import time
from decimal import Decimal
from pathlib import Path

import structlog
from rich.console import Console
from rich.table import Table

from polybot.client.clob import CLOBClient
from polybot.client.gamma import GammaClient
from polybot.execution.order_manager import OrderManager
from polybot.models.types import SignalSet
from polybot.monitoring.event_log import EventLog
from polybot.monitoring.tracker import PositionTracker
from polybot.safety.risk_manager import RiskManager
from polybot.strategies.base import BaseStrategy, StrategyContext

logger = structlog.get_logger()
console = Console()


class Engine:
    def __init__(
        self,
        strategies: list[BaseStrategy],
        gamma: GammaClient,
        clob: CLOBClient,
        order_manager: OrderManager,
        tracker: PositionTracker,
        risk_manager: RiskManager,
        poll_interval: int = 30,
        dry_run: bool = True,
        halt_file: str = "./HALT",
        event_log: EventLog | None = None,
    ):
        self._strategies = strategies
        self._gamma = gamma
        self._clob = clob
        self._order_manager = order_manager
        self._tracker = tracker
        self._risk = risk_manager
        self._poll_interval = poll_interval
        self._dry_run = dry_run
        self._halt_file = Path(halt_file)
        self._events = event_log or EventLog()
        self._running = False

    def start(self):
        self._running = True
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        mode = "DRY RUN" if self._dry_run else "LIVE"
        console.print(f"\n[bold green]Bot started[/] — mode: [bold]{mode}[/]")
        console.print(f"Strategies: {[s.NAME for s in self._strategies]}")
        console.print(f"Poll interval: {self._poll_interval}s\n")

        while self._running:
            if self._halt_file.exists():
                console.print("[bold red]HALT file detected — stopping all trading[/]")
                break

            try:
                self._run_cycle()
            except Exception as e:
                logger.error("cycle_error", error=str(e))

            if self._running:
                time.sleep(self._poll_interval)

        console.print("[bold yellow]Bot stopped[/]")

    def run_once(self):
        self._run_cycle()

    def _run_cycle(self):
        cycle_start = time.monotonic()
        markets_scanned = 0
        signals_generated = 0
        signals_approved = 0
        balance = Decimal("0")

        try:
            markets = self._gamma.fetch_active_markets()
            markets_scanned = len(markets)
            if not markets:
                logger.info("no_markets")
                return

            balance = self._clob.get_balance() if not self._dry_run else Decimal("10000")
            positions = self._tracker.positions

            all_signals: list[SignalSet] = []
            signal_meta: list[dict] = []

            for strategy in self._strategies:
                reset = getattr(strategy, "reset_cycle_cache", None)
                if callable(reset):
                    reset()
                config = getattr(strategy, "_config", {})

                # Pass 1: evaluate all markets using Gamma prices — no CLOB calls.
                # This is fast and filters down to only markets worth enriching.
                candidate_markets = []
                for market in markets:
                    if not strategy.filter_market(market):
                        continue
                    try:
                        ctx = StrategyContext(
                            market=market,
                            open_positions=positions,
                            portfolio_balance=balance,
                            historical_prices=[],
                            config=config,
                        )
                        preliminary = strategy.evaluate(ctx)
                        if preliminary.orders:
                            candidate_markets.append(market)
                    except Exception as e:
                        logger.warning(
                            "strategy_eval_failed",
                            strategy=strategy.NAME,
                            market=market.condition_id,
                            error=str(e),
                        )

                logger.info(
                    "candidates_found",
                    strategy=strategy.NAME,
                    count=len(candidate_markets),
                )

                # Pass 2: enrich only candidate markets with live CLOB bid/ask,
                # then re-evaluate to get accurate entry prices.
                for market in candidate_markets:
                    try:
                        if not self._dry_run:
                            enriched_outcomes = self._clob.enrich_outcomes(market.outcomes)
                            if not enriched_outcomes:
                                continue
                            market = market.model_copy(update={"outcomes": enriched_outcomes})

                        ctx = StrategyContext(
                            market=market,
                            open_positions=positions,
                            portfolio_balance=balance,
                            historical_prices=[],
                            config=config,
                        )
                        signal_set = strategy.evaluate(ctx)
                        if signal_set.orders:
                            all_signals.append(signal_set)
                            signal_meta.append(
                                {"strategy": strategy.NAME, "market_question": market.question}
                            )
                    except Exception as e:
                        logger.warning(
                            "strategy_eval_failed",
                            strategy=strategy.NAME,
                            market=market.condition_id,
                            error=str(e),
                        )

            approved, rejections = self._risk.validate_signals(all_signals, positions, balance)
            approved_ids = {s.market_condition_id for s in approved}

            for sig, meta in zip(all_signals, signal_meta):
                is_approved = sig.market_condition_id in approved_ids
                self._events.emit_signal(
                    strategy=meta["strategy"],
                    market_condition_id=sig.market_condition_id,
                    market_question=meta["market_question"],
                    rationale=sig.rationale,
                    confidence=sig.confidence,
                    approved=is_approved,
                    reject_reason=rejections.get(sig.market_condition_id),
                    orders=[
                        {
                            "token_id": o.token_id,
                            "side": o.side.value,
                            "order_type": o.order_type.value,
                            "size": str(o.size),
                            "limit_price": str(o.limit_price) if o.limit_price else None,
                        }
                        for o in sig.orders
                    ],
                )

            signals_generated = len(all_signals)
            signals_approved = len(approved)

            if approved:
                self._order_manager.execute_signals(approved)

            stop_loss_tokens = self._risk.check_stop_losses(positions)
            for token_id in stop_loss_tokens:
                self._order_manager.close_position(token_id)

            self._print_cycle_summary(markets, all_signals, approved)
        finally:
            self._events.emit_cycle(
                markets_scanned=markets_scanned,
                signals_generated=signals_generated,
                signals_approved=signals_approved,
                open_positions=len(self._tracker.positions),
                balance=str(balance) if balance > 0 else None,
                total_pnl=str(self._tracker.total_pnl()),
                duration_ms=int((time.monotonic() - cycle_start) * 1000),
                dry_run=self._dry_run,
            )

    def _print_cycle_summary(
        self,
        markets: list,
        signals: list[SignalSet],
        approved: list[SignalSet],
    ):
        table = Table(title="Cycle Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Markets scanned", str(len(markets)))
        table.add_row("Signals generated", str(len(signals)))
        table.add_row("Signals approved", str(len(approved)))
        table.add_row("Open positions", str(len(self._tracker.positions)))
        table.add_row("Total P&L", str(self._tracker.total_pnl()))
        console.print(table)

    def _handle_shutdown(self, signum, frame):
        logger.info("shutdown_signal", signal=signum)
        self._running = False
        if not self._dry_run:
            try:
                self._clob.cancel_all()
            except Exception as e:
                logger.error("cancel_all_failed", error=str(e))
