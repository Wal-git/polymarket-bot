import asyncio
import time
from pathlib import Path

import structlog
from rich.console import Console

from polybot.client.clob import CLOBClient
from polybot.engine.discovery import fetch_slot_details, get_slug, get_slot_ts
from polybot.engine.lifecycle import LifecycleState, MarketLifecycle
from polybot.monitoring.tracker import PositionTracker

logger = structlog.get_logger()
console = Console()


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

    async def run(self) -> None:
        self._running = True
        mode = "DRY RUN" if self._dry_run else "LIVE"
        console.print(f"\n[bold green]BTC 5-min engine started[/] — mode: [bold]{mode}[/]")

        loop = asyncio.get_running_loop()
        import signal as _signal
        for sig in (_signal.SIGINT, _signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._handle_shutdown)
            except NotImplementedError:
                pass  # Windows

        active: dict[str, MarketLifecycle] = {}

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

            # Launch lifecycle for current slot if not already running
            if current_slug not in active and now_ms < current_end_ms:
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
                    lc.start()
                else:
                    logger.warning("slot_unavailable", slug=current_slug)

            # Pre-warm next slot 30s before current ends
            next_slug = get_slug(1)
            time_to_next_slot_ms = get_slot_ts(1)[0] - now_ms
            if time_to_next_slot_ms < 30_000 and next_slug not in active:
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
                    lc.start()

            await asyncio.sleep(1)

        # Shutdown all active lifecycles
        for lc in active.values():
            lc.shutdown()
        console.print("[bold yellow]Engine stopped[/]")

    def _handle_shutdown(self) -> None:
        logger.info("shutdown_signal_received")
        self._running = False
