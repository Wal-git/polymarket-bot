import importlib
import sys
from pathlib import Path

import structlog

from polybot.strategies.base import BaseStrategy

logger = structlog.get_logger()


def load_strategy(name: str, module_path: str) -> BaseStrategy:
    if module_path.startswith("strategies."):
        strategies_dir = Path("strategies").resolve()
        if str(strategies_dir) not in sys.path:
            sys.path.insert(0, str(strategies_dir.parent))

    mod = importlib.import_module(module_path)

    for attr_name in dir(mod):
        attr = getattr(mod, attr_name)
        if (
            isinstance(attr, type)
            and issubclass(attr, BaseStrategy)
            and attr is not BaseStrategy
            and getattr(attr, "NAME", "") == name
        ):
            instance = attr()
            logger.info("strategy_loaded", name=name, module=module_path)
            return instance

    raise ValueError(f"No strategy with NAME='{name}' found in {module_path}")


def load_strategies(strategy_configs: list[dict]) -> list[BaseStrategy]:
    strategies: list[BaseStrategy] = []
    for cfg in strategy_configs:
        if not cfg.get("enabled", True):
            continue
        try:
            strategy = load_strategy(cfg["name"], cfg["module"])
            strategy._config = cfg.get("config", {})
            strategies.append(strategy)
        except Exception as e:
            logger.error("strategy_load_failed", name=cfg.get("name"), error=str(e))
    return strategies
