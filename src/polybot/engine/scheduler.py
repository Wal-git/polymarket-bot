import asyncio
import time
from pathlib import Path

import structlog
from rich.console import Console

from polybot.account.balance import invalidate_cache
from polybot.auth.wallet import get_private_key
from polybot.client.clob import CLOBClient
from polybot.engine.discovery import fetch_slot_details, get_slug, get_slot_ts
from polybot.engine.lifecycle import LifecycleState, MarketLifecycle
from polybot.execution.redeem import maybe_redeem
from polybot.monitoring.tracker import PositionTracker

logger = structlog.get_logger()
console = Console()

_REDEEM_INTERVAL_SECS = 900  # re-scan for redeemable positions every 15 minutes


class BtcEngine:
    """Asyncio supervisor for 5-minute BTC market lifecycles.

    One lifecycle per slot. Creates the next lifecycle ~30s before the current
    slot ends so the WebSocket and orderbook can warm up before the entry window.
    """

    def __init__(
        self,
        clob: CLOBClient,
        tracker: PositionTracker,
        dry_run: bool,
        config: dict,
        halt_file: str = "./HALT",
        daily_loss_limit: float = 100.0,
    ) -> None:
        self._clob = clob
        self._tracker = tracker
        self._dry_run = dry_run
        self._config = config
        self._halt_file = Path(halt_file)
        self._daily_loss_limit = daily_loss_limit
        self._daily_pnl = 0.0
        self._trades_today = 0
        self._running = False

    def _run_redeem(self) -> None:
        """Redeem any resolved positions and sync CLOB balance."""
        from polybot.monitoring.event_log import emit_result
        slug_confidence = {
            p.market_question: p.confidence
            for p in self._tracker.positions
            if p.confidence is not None
        }
        count, outcomes = maybe_redeem(get_private_key(), self._clob.client)
        if count:
            self._clob.sync_balance_allowance()
            invalidate_cache()
        for outcome in outcomes:
            if outcome.get("slug"):
                emit_result(
                    slug=outcome["slug"],
                    won=outcome["won"],
                    pnl=outcome["pnl"],
                    shares=outcome["shares"],
                    entry_price=outcome["entry_price"],
                    exit_reason="HOLD_TO_RESOLUTION",
                    exit_price=1.0 if outcome["won"] else 0.0,
                    confidence=slug_confidence.get(outcome["slug"]),
                )

    async def run(self) -> None:
        self._running = True
        mode = "DRY RUN" if self._dry_run else "LIVE"
        console.print(f"\n[bold green]BTC 5-min engine started[/] — mode: [bold]{mode}[/]")

        # Redeem any positions left over from previous runs
        self._run_redeem()

        loop = asyncio.get_running_loop()
        import signal as _signal
        for sig in (_signal.SIGINT, _signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._handle_shutdown)
            except NotImplementedError:
                pass  # Windows

        active: dict[str, MarketLifecycle] = {}
        attempted: set[str] = set()  # slugs already tried this run
        last_redeem_ts = time.time()

        while self._running:
            if self._halt_file.exists():
                console.print("[bold red]HALT file detected — stopping[/]")
                break

            if self._daily_pnl <= -self._daily_loss_limit:
                console.print(
                    f"[bold red]Daily loss limit hit (${self._daily_pnl:.2f}) — stopping new entries[/]"
                )
                break

            now_ms = time.time() * 1000
            current_slug = get_slug(0)
            _, current_end_ms = get_slot_ts(0)

            # Reap resolved lifecycles and collect PnL
            for slug, lc in list(active.items()):
                if lc.state == LifecycleState.RESOLVED:
                    if lc.pnl is not None:
                        self._daily_pnl += lc.pnl
                        self._trades_today += 1
                        color = "green" if lc.pnl >= 0 else "red"
                        sign = "+" if lc.pnl >= 0 else ""
                        console.print(
                            f"[{color}][{slug}] PnL: {sign}${lc.pnl:.2f} "
                            f"| Daily: ${self._daily_pnl:.2f} | Trades: {self._trades_today}[/]"
                        )
                    del active[slug]

            # Launch lifecycle for current slot if not already running or attempted
            if current_slug not in active and current_slug not in attempted and now_ms < current_end_ms:
                slot = await fetch_slot_details(current_slug)
                if slot:
                    console.print(
                        f"[dim][{current_slug}] Market open "
                        f"— Price to Beat: ${slot.price_to_beat:,.2f}[/]"
                    )
                    lc = MarketLifecycle(
                        slot=slot,
                        clob=self._clob,
                        tracker=self._tracker,
                        dry_run=self._dry_run,
                        config=self._config,
                    )
                    active[current_slug] = lc
                    attempted.add(current_slug)
                    lc.start()
                else:
                    attempted.add(current_slug)
                    logger.warning("slot_unavailable", slug=current_slug)

            # Pre-warm next slot 30s before current ends
            next_slug = get_slug(1)
            time_to_next_slot_ms = get_slot_ts(1)[0] - now_ms
            if time_to_next_slot_ms < 30_000 and next_slug not in active and next_slug not in attempted:
                slot = await fetch_slot_details(next_slug)
                if slot:
                    lc = MarketLifecycle(
                        slot=slot,
                        clob=self._clob,
                        tracker=self._tracker,
                        dry_run=self._dry_run,
                        config=self._config,
                    )
                    active[next_slug] = lc
                    attempted.add(next_slug)
                    lc.start()

            # Periodic redemption scan for positions resolved outside normal lifecycle
            if time.time() - last_redeem_ts >= _REDEEM_INTERVAL_SECS:
                self._run_redeem()
                last_redeem_ts = time.time()

            await asyncio.sleep(1)

        # Shutdown all active lifecycles
        for lc in active.values():
            lc.shutdown()
        console.print("[bold yellow]Engine stopped[/]")

    def _handle_shutdown(self) -> None:
        logger.info("shutdown_signal_received")
        self._running = False
