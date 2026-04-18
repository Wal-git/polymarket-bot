from pathlib import Path

import yaml

from polybot.auth.wallet import load_env
from polybot.client.clob import CLOBClient
from polybot.client.gamma import GammaClient
from polybot.execution.order_manager import OrderManager
from polybot.monitoring.logger import setup_logging
from polybot.monitoring.tracker import PositionTracker
from polybot.safety.risk_manager import RiskManager
from polybot.scheduler.engine import Engine
from polybot.strategies.registry import load_strategies


def load_config(config_path: str = "config/default.yaml") -> dict:
    return yaml.safe_load(Path(config_path).read_text())


def build_bot(config_path: str = "config/default.yaml") -> Engine:
    config = load_config(config_path)
    bot_cfg = config.get("bot", {})
    risk_cfg = config.get("risk", {})
    market_cfg = config.get("market_filter", {})

    setup_logging(bot_cfg.get("log_level", "INFO"))
    load_env()

    dry_run = bot_cfg.get("dry_run", True)

    gamma = GammaClient(
        min_volume=market_cfg.get("min_volume_24h_usdc", 1000),
        max_markets=market_cfg.get("max_markets_per_cycle", 20),
    )
    clob = CLOBClient()
    tracker = PositionTracker(state_file=bot_cfg.get("state_file", "./data/state.json"))
    risk_manager = RiskManager(**risk_cfg)
    order_manager = OrderManager(clob=clob, tracker=tracker, dry_run=dry_run)
    strategies = load_strategies(config.get("strategies", []))

    return Engine(
        strategies=strategies,
        gamma=gamma,
        clob=clob,
        order_manager=order_manager,
        tracker=tracker,
        risk_manager=risk_manager,
        poll_interval=bot_cfg.get("poll_interval_seconds", 30),
        dry_run=dry_run,
        halt_file=bot_cfg.get("halt_file", "./HALT"),
    )
