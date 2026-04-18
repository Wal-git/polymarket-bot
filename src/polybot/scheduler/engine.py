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
        markets = self._gamma.fetch_active_markets()
        if not markets:
            logger.info("no_markets")
            return

        balance = self._clob.get_balance() if not self._dry_run else Decimal("10000")
        positions = self._tracker.positions

        all_signals: list[SignalSet] = []

        for strategy in self._strategies:
            reset = getattr(strategy, "reset_cycle_cache", None)
            if callable(reset):
                reset()
            config = getattr(strategy, "_config", {})
            for market in markets:
                if not strategy.filter_market(market):
                    continue

                try:
                    enriched_outcomes = self._clob.enrich_outcomes(market.outcomes) if not self._dry_run else market.outcomes
                    enriched_market = market.model_copy(update={"outcomes": enriched_outcomes})

                    ctx = StrategyContext(
                        market=enriched_market,
                        open_positions=positions,
                        portfolio_balance=balance,
                        historical_prices=[],
                        config=config,
                    )
                    signal_set = strategy.evaluate(ctx)
                    if signal_set.orders:
                        all_signals.append(signal_set)
                except Exception as e:
                    logger.warning(
                        "strategy_eval_failed",
                        strategy=strategy.NAME,
                        market=market.condition_id,
                        error=str(e),
                    )

        approved = self._risk.validate_signals(all_signals, positions, balance)

        if approved:
            self._order_manager.execute_signals(approved)

        stop_loss_tokens = self._risk.check_stop_losses(positions)
        for token_id in stop_loss_tokens:
            self._order_manager.close_position(token_id)

        self._print_cycle_summary(markets, all_signals, approved)

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
