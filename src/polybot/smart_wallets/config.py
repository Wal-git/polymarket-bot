"""Thresholds, weights, and paths for the smart-wallet pipeline."""
from __future__ import annotations

from pathlib import Path

# --- filter thresholds (tune after first run) ---
MIN_RESOLVED_MARKETS: int = 30
MIN_WIN_RATE: float = 0.55
MIN_REALIZED_PNL_USD: float = 5_000.0
MIN_VOLUME_USD: float = 50_000.0
MAX_DAYS_INACTIVE: int = 14
MIN_TRADES_COUNT: int = 50
TOP_K: int = 100

# --- metric weights for composite score (must sum to 1.0) ---
SCORE_WEIGHTS: dict[str, float] = {
    "pnl_realized": 0.35,
    "win_rate": 0.25,
    "volume": 0.15,
    "resolved_markets": 0.15,
    "recency": 0.10,
}

# --- data API ---
DATA_API_BASE: str = "https://data-api.polymarket.com"
LOOKBACK_DAYS: int = 60
RECENCY_DAYS: int = 14
LEADERBOARD_LIMIT: int = 500
ENRICH_WORKERS: int = 8
MAX_RPS: float = 5.0  # requests per second cap

# --- paths (relative to repo root; cli.py resolves to absolute) ---
REPO_ROOT: Path = Path(__file__).resolve().parents[3]
DATA_DIR: Path = REPO_ROOT / "data"
CACHE_DIR: Path = DATA_DIR / ".cache"
SMART_WALLETS_JSON: Path = DATA_DIR / "smart_wallets.json"
SMART_WALLETS_DB: Path = DATA_DIR / "smart_wallets.db"
