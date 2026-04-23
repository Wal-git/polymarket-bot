import asyncio
import subprocess
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(name="polybot", help="BTC 5-minute Polymarket signal engine")
console = Console()


@app.command()
def setup():
    """One-time wallet and API key setup wizard."""
    from polybot.auth.wallet import run_setup_wizard
    run_setup_wizard()


@app.command()
def run(config: str = typer.Option("config/default.yaml", help="Path to config file")):
    """Start the 5-minute BTC engine (honours dry_run in config)."""
    from polybot.bot import build_engine
    engine = build_engine(config)
    asyncio.run(engine.run())


@app.command()
def balance(config: str = typer.Option("config/default.yaml", help="Path to config file")):
    """Show live USDC balance and open positions."""
    from polybot.auth.wallet import load_env
    from polybot.bot import load_config
    from polybot.client.clob import CLOBClient
    from polybot.monitoring.tracker import PositionTracker

    load_env()
    cfg = load_config(config)
    clob = CLOBClient()
    usdc = clob.get_balance()
    console.print(f"\n[bold]USDC Balance:[/] ${usdc:,.2f}")

    tracker = PositionTracker(cfg.get("bot", {}).get("state_file", "./data/state.json"))
    if not tracker.positions:
        console.print("[dim]No open positions[/]")
        return

    table = Table(title="Open Positions")
    table.add_column("Slug", style="cyan", max_width=30)
    table.add_column("Direction", style="green")
    table.add_column("Shares", justify="right")
    table.add_column("Avg Entry", justify="right")
    table.add_column("Unrealized P&L", justify="right")
    for pos in tracker.positions:
        pnl_style = "green" if pos.unrealized_pnl >= 0 else "red"
        table.add_row(
            pos.market_question[:30],
            pos.outcome_label,
            str(pos.shares),
            f"${pos.avg_entry_price:.4f}",
            f"[{pnl_style}]${pos.unrealized_pnl:.2f}[/]",
        )
    console.print(table)
    console.print(f"\nTotal P&L: ${tracker.total_pnl():.2f}")


@app.command()
def backtest(
    days: int = typer.Option(30, help="Number of historical days to replay"),
    config: str = typer.Option("config/default.yaml", help="Path to config file"),
):
    """Replay historical BTC 5-min markets to validate strategy parameters."""
    from polybot.backtest.harness import run_backtest
    from polybot.bot import load_config
    cfg = load_config(config)
    asyncio.run(run_backtest(days=days, config=cfg))


@app.command(name="cli")
def cli_proxy(args: list[str] = typer.Argument(None, help="polymarket-cli subcommand and args")):
    """Proxy to the Rust polymarket-cli binary (tools/polymarket-cli)."""
    binary = Path(__file__).parent.parent.parent / "tools" / "polymarket-cli" / "target" / "release" / "polymarket-cli"
    if not binary.exists():
        # Fall back to PATH
        binary = Path("polymarket-cli")
    try:
        result = subprocess.run([str(binary)] + (args or []))
        raise typer.Exit(result.returncode)
    except FileNotFoundError:
        console.print(
            "[red]polymarket-cli binary not found. "
            "Run: cd tools/polymarket-cli && cargo build --release[/]"
        )
        raise typer.Exit(1)


@app.command()
def agents(args: list[str] = typer.Argument(None, help="agents module subcommand")):
    """Proxy to the agents/ LLM analysis module (requires [llm] extra)."""
    try:
        from polybot.agents.cli import agents_app
        agents_app(args or [], standalone_mode=False)
    except ImportError:
        console.print(
            "[red]agents module not available. Install with: pip install -e '.[llm]'[/]"
        )
        raise typer.Exit(1)


@app.command()
def dashboard(
    port: int = typer.Option(8501, help="Streamlit port"),
    address: str = typer.Option("localhost", help="Bind address"),
):
    """Launch the Streamlit monitoring dashboard."""
    app_path = Path(__file__).parent / "dashboard" / "app.py"
    if not app_path.exists():
        console.print(f"[red]Dashboard not found at {app_path}[/]")
        raise typer.Exit(1)
    try:
        subprocess.run(
            [
                sys.executable, "-m", "streamlit", "run", str(app_path),
                "--server.port", str(port),
                "--server.address", address,
                "--browser.gatherUsageStats", "false",
            ],
            check=True,
        )
    except FileNotFoundError:
        console.print("[red]Streamlit not installed. Run: pip install -e '.[dashboard]'[/]")
        raise typer.Exit(1)
    except subprocess.CalledProcessError as e:
        raise typer.Exit(e.returncode)


@app.command(name="cancel-all")
def cancel_all():
    """Emergency: cancel all open orders on the CLOB."""
    from polybot.auth.wallet import load_env
    from polybot.client.clob import CLOBClient
    load_env()
    CLOBClient().cancel_all()
    console.print("[bold green]All orders cancelled[/]")


if __name__ == "__main__":
    app()
