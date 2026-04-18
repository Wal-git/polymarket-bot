import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(name="polybot", help="Automated Polymarket trading bot")
console = Console()


@app.command()
def setup():
    """One-time wallet and API key setup wizard."""
    from polybot.auth.wallet import run_setup_wizard

    run_setup_wizard()


@app.command()
def run(config: str = typer.Option("config/default.yaml", help="Path to config file")):
    """Start the bot with the main polling loop."""
    from polybot.bot import build_bot

    engine = build_bot(config)
    engine.start()


@app.command(name="dry-run")
def dry_run(config: str = typer.Option("config/default.yaml", help="Path to config file")):
    """Run one cycle in dry-run mode — no real orders."""
    from polybot.bot import build_bot

    engine = build_bot(config)
    engine.run_once()


@app.command()
def positions(config: str = typer.Option("config/default.yaml", help="Path to config file")):
    """Show open positions and P&L."""
    from polybot.bot import load_config
    from polybot.monitoring.tracker import PositionTracker

    cfg = load_config(config)
    tracker = PositionTracker(state_file=cfg.get("bot", {}).get("state_file", "./data/state.json"))

    if not tracker.positions:
        console.print("[yellow]No open positions[/]")
        return

    table = Table(title="Open Positions")
    table.add_column("Market", style="cyan", max_width=40)
    table.add_column("Outcome", style="green")
    table.add_column("Shares", justify="right")
    table.add_column("Avg Entry", justify="right")
    table.add_column("Current", justify="right")
    table.add_column("Unrealized P&L", justify="right")

    for pos in tracker.positions:
        pnl_style = "green" if pos.unrealized_pnl >= 0 else "red"
        table.add_row(
            pos.market_question[:40],
            pos.outcome_label,
            str(pos.shares),
            f"${pos.avg_entry_price:.4f}",
            f"${pos.current_price:.4f}",
            f"[{pnl_style}]${pos.unrealized_pnl:.2f}[/]",
        )

    console.print(table)
    console.print(f"\nTotal P&L: ${tracker.total_pnl():.2f}")


@app.command()
def dashboard(
    port: int = typer.Option(8501, help="Port to serve the Streamlit dashboard"),
    address: str = typer.Option("localhost", help="Bind address (use 0.0.0.0 to expose)"),
):
    """Launch the Streamlit dashboard for monitoring signals and positions."""
    import subprocess
    import sys
    from pathlib import Path

    app_path = Path(__file__).parent / "dashboard" / "app.py"
    if not app_path.exists():
        console.print(f"[red]Dashboard app not found at {app_path}[/]")
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
    """Emergency: cancel all open orders."""
    from polybot.auth.wallet import load_env
    from polybot.client.clob import CLOBClient

    load_env()
    clob = CLOBClient()
    clob.cancel_all()
    console.print("[bold green]All orders cancelled[/]")


if __name__ == "__main__":
    app()
