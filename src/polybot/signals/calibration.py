"""Empirical win-rate calibration.

Replaces the linear ``0.6 + mean(|delta|)/250`` confidence formula with a
historical lookup keyed on (delta_bucket, entry_bucket, hour_utc).

Hierarchy of fallback when a bucket has too few trials (``min_n``):
    delta × entry × hour  →  delta × entry  →  delta  →  global

All rates are Laplace-smoothed: ``(wins + 1) / (trials + 2)``. With 0 trials,
the smoothed rate is 0.5 — which is then dropped in favor of a coarser bucket.

Building the table is offline (see ``scripts/build_calibration.py``); this
module is the runtime lookup.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


# Bucket boundaries (chosen to match the analysis breakdown that motivated this work).
_DELTA_BUCKETS = [(0, 75, "<75"), (75, 100, "75-100"), (100, 150, "100-150"),
                  (150, 200, "150-200"), (200, 300, "200-300"), (300, float("inf"), "300+")]
_ENTRY_BUCKETS = [(0.0, 0.50, "<0.50"), (0.50, 0.60, "0.50-0.60"),
                  (0.60, 0.70, "0.60-0.70"), (0.70, 0.80, "0.70-0.80"),
                  (0.80, 0.85, "0.80-0.85"), (0.85, 0.90, "0.85-0.90"),
                  (0.90, 1.01, "0.90+")]


def bucket_delta(max_abs_delta: float) -> str:
    for lo, hi, name in _DELTA_BUCKETS:
        if lo <= max_abs_delta < hi:
            return name
    return _DELTA_BUCKETS[-1][2]


def bucket_entry(entry_price: float) -> str:
    for lo, hi, name in _ENTRY_BUCKETS:
        if lo <= entry_price < hi:
            return name
    return _ENTRY_BUCKETS[-1][2]


def smoothed_rate(wins: int, trials: int) -> float:
    """Laplace-smoothed win rate: (wins+1)/(trials+2)."""
    return (wins + 1) / (trials + 2)


def lookup_win_rate(
    table: dict,
    max_abs_delta: float,
    entry_price: float,
    hour_utc: int,
    min_n: int = 5,
    fallback: float = 0.5,
) -> tuple[float, str]:
    """Return ``(rate, source)`` from the calibration table.

    Tries the most-specific bucket first and falls back to coarser ones when the
    cell has fewer than ``min_n`` trials. ``source`` indicates which level
    answered (``delta_x_entry_x_hour`` / ``delta_x_entry`` / ``delta`` / ``global``
    / ``fallback``).
    """
    db = bucket_delta(max_abs_delta)
    eb = bucket_entry(entry_price)

    levels = (
        ("delta_x_entry_x_hour", f"{db}_{eb}_{int(hour_utc)}"),
        ("delta_x_entry", f"{db}_{eb}"),
        ("delta", db),
    )
    bucket_groups = table.get("buckets", {})
    for level_name, key in levels:
        cell = bucket_groups.get(level_name, {}).get(key)
        if cell and cell.get("trials", 0) >= min_n:
            return smoothed_rate(cell["wins"], cell["trials"]), level_name

    # Global fallback: smoothed overall rate
    if "global" in table:
        g = table["global"]
        if g.get("trials", 0) >= 1:
            return smoothed_rate(g["wins"], g["trials"]), "global"

    return fallback, "fallback"


# Module-level cache so we don't re-read the file every signal evaluation
_table_cache: tuple[Path, dict] | None = None


def load_table(path: str | Path) -> Optional[dict]:
    """Read the calibration table from disk (cached). Returns None if missing/invalid."""
    global _table_cache
    p = Path(path)
    if not p.exists():
        return None
    if _table_cache and _table_cache[0] == p:
        return _table_cache[1]
    try:
        data = json.loads(p.read_text())
        _table_cache = (p, data)
        return data
    except (OSError, json.JSONDecodeError):
        return None


def reset_cache() -> None:
    """Test hook — clears the module-level cache."""
    global _table_cache
    _table_cache = None
