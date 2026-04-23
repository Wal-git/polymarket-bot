from pathlib import Path

import yaml

from polybot.auth.wallet import load_env
from polybot.client.clob import CLOBClient
from polybot.engine.scheduler import BtcEngine
from polybot.monitoring.logger import setup_logging
from polybot.monitoring.tracker import PositionTracker


def load_config(config_path: str = "config/default.yaml") -> dict:
    return yaml.safe_load(Path(config_path).read_text())


def build_engine(config_path: str = "config/default.yaml") -> BtcEngine:
    config = load_config(config_path)
    bot_cfg = config.get("bot", {})
    risk_cfg = config.get("risk", {})

    setup_logging(bot_cfg.get("log_level", "INFO"))
    load_env()

    clob = CLOBClient()
    state_file = bot_cfg.get("state_file", "./data/state.json")
    tracker = PositionTracker(state_file=state_file)

    return BtcEngine(
        clob=clob,
        tracker=tracker,
        dry_run=bot_cfg.get("dry_run", True),
        config=config,
        halt_file=bot_cfg.get("halt_file", "./HALT"),
        daily_loss_limit=float(risk_cfg.get("daily_loss_limit_usdc", 100.0)),
    )
