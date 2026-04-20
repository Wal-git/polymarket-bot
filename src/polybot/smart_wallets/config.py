"""Thresholds, weights, and paths for the smart-wallet pipeline."""
from __future__ import annotations

from pathlib import Path

# --- hard floors (always enforced regardless of percentile calibration) ---
MIN_RESOLVED_MARKETS: int = 20
MIN_TRADES_COUNT: int = 30
MIN_VOLUME_USD: float = 10_000.0
MIN_REALIZED_PNL_USD: float = 1_000.0
MIN_WIN_RATE: float = 0.50
MAX_DAYS_INACTIVE: int = 14
TOP_K: int = 100

# --- percentile-based calibration (applied on top of hard floors) ---
# Set to None to disable a given percentile filter.
PCT_CUTOFF_SHARPE: float | None = 0.50
PCT_CUTOFF_EDGE: float | None = 0.50
PCT_CUTOFF_VOLUME: float | None = 0.25
MIN_SIGNIFICANCE_Z: float = 1.0  # ~p<0.16 one-sided; not extreme but screens luck

# --- metric weights for composite score (percentile-ranked then weighted) ---
SCORE_WEIGHTS: dict[str, float] = {
    "edge": 0.25,
    "sharpe": 0.20,
    "pnl_realized": 0.15,
    "win_rate": 0.15,
    "volume": 0.10,
    "resolved_markets": 0.10,
    "recency": 0.05,
}

# Weights for the early-signal archetype ranking.
SIGNAL_SCORE_WEIGHTS: dict[str, float] = {
    "early_signal": 0.40,
    "edge": 0.20,
    "sharpe": 0.15,
    "volume": 0.10,
    "resolved_markets": 0.10,
    "recency": 0.05,
}

# --- data API ---
DATA_API_BASE: str = "https://data-api.polymarket.com"
LOOKBACK_DAYS: int = 90
RECENCY_DAYS: int = 14
LEADERBOARD_LIMIT: int = 500
ENRICH_WORKERS: int = 8
MAX_RPS: float = 5.0  # requests per second cap

# --- Goldsky-based seed (wallets with high recent volume) ---
GOLDSKY_SEED_DAYS: int = 90
GOLDSKY_SEED_TOP_N: int = 1000  # top-N by buy-volume over the window

# --- sybil / wash clustering ---
SYBIL_JACCARD_THRESHOLD: float = 0.6  # trade-market overlap above → collapse
SYBIL_TIMING_THRESHOLD: float = 0.4  # co-trade rate within 60s window

# --- paths (relative to repo root; cli.py resolves to absolute) ---
REPO_ROOT: Path = Path(__file__).resolve().parents[3]
DATA_DIR: Path = REPO_ROOT / "data"
CACHE_DIR: Path = DATA_DIR / ".cache"
SMART_WALLETS_JSON: Path = DATA_DIR / "smart_wallets.json"
SMART_WALLETS_SIGNAL_JSON: Path = DATA_DIR / "smart_wallets_signal.json"
SMART_WALLETS_CLOSER_JSON: Path = DATA_DIR / "smart_wallets_closer.json"
SMART_WALLETS_DB: Path = DATA_DIR / "smart_wallets.db"
