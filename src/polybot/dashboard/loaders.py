"""Cached file loaders for the Polybot dashboard.

All disk reads go through this module. Loaders are cached with ``st.cache_data``
so the autorefresh + page navigation don't hammer the disk. Each loader returns
a safe empty default when the file is absent — pages show a waiting state
rather than crashing.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import streamlit as st
import yaml

_STATE_FILE = Path("data/state.json")
_CYCLES_FILE = Path("data/cycles.jsonl")
_SIGNALS_FILE = Path("data/signals.jsonl")
_EVALS_FILE = Path("data/evaluations.jsonl")
_RESULTS_FILE = Path("data/results.jsonl")
_BOT_LOG_FILE = Path("data/bot.log")
_BALANCE_FILE = Path("data/balance.json")
_CONFIG_FILE = Path("config/default.yaml")

# 143.00 initial deposit (2026-05-04) + 250.86 from converting ~2687 POL -> pUSD (2026-06-03)
STARTING_BALANCE = 393.86


@st.cache_data(ttl=5)
def load_state() -> dict[str, Any]:
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@st.cache_data(ttl=5)
def load_cycles(last_n: int = 200) -> list[dict]:
    return _tail_jsonl(_CYCLES_FILE, last_n)


@st.cache_data(ttl=5)
def load_signals(last_n: int = 200) -> list[dict]:
    return _tail_jsonl(_SIGNALS_FILE, last_n)


@st.cache_data(ttl=10)
def load_balance() -> dict[str, Any]:
    try:
        return json.loads(_BALANCE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@st.cache_data(ttl=5)
def load_evaluations(last_n: int = 200) -> list[dict]:
    return _tail_jsonl(_EVALS_FILE, last_n)


@st.cache_data(ttl=5)
def load_results() -> dict[str, dict]:
    """Return results keyed by slug for O(1) lookup in card rendering."""
    records = _tail_jsonl(_RESULTS_FILE, 500)
    return {r["slug"]: r for r in records if "slug" in r}


@st.cache_data(ttl=30)
def load_results_deduped() -> list[dict]:
    """All resolved trade results, deduplicated by slug (latest per slug), sorted by ts."""
    try:
        lines = _RESULTS_FILE.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    seen: dict[str, dict] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "slug" in r:
            r.setdefault("asset", "BTC")
            seen[r["slug"]] = r
    return sorted(seen.values(), key=lambda r: r.get("ts", ""))


@st.cache_data(ttl=5)
def load_bot_log(last_n: int = 100) -> list[str]:
    try:
        lines = _BOT_LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-last_n:]
    except FileNotFoundError:
        return []


@st.cache_data(ttl=30)
def load_config() -> dict[str, Any]:
    try:
        return yaml.safe_load(_CONFIG_FILE.read_text(encoding="utf-8")) or {}
    except (FileNotFoundError, yaml.YAMLError):
        return {}


def _tail_jsonl(path: Path, last_n: int) -> list[dict]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    out: list[dict] = []
    for line in reversed(lines[-last_n:]):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        # Records written before the multi-asset refactor lack the "asset"
        # field — default to BTC so dashboard rendering stays consistent.
        rec.setdefault("asset", "BTC")
        out.append(rec)
    return out


def get_halt_path() -> Path:
    cfg = load_config()
    return Path(cfg.get("bot", {}).get("halt_file", "./HALT"))


def latest_cycle() -> dict:
    cycles = load_cycles(last_n=1)
    return cycles[0] if cycles else {}


def latest_evaluation() -> dict:
    evals = load_evaluations(last_n=1)
    return evals[0] if evals else {}


def cycle_age_seconds() -> float | None:
    # Prefer evaluations (new BTC engine) over cycles (old engine)
    record = latest_evaluation() or latest_cycle()
    ts = record.get("ts")
    if not ts:
        return None
    try:
        cycle_time = datetime.fromisoformat(ts)
    except ValueError:
        return None
    return (datetime.now(cycle_time.tzinfo or timezone.utc) - cycle_time).total_seconds()
