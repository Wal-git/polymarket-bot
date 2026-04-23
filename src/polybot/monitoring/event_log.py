"""JSONL event emitter for the dashboard.

Two append-only streams under ``data/``:
  * ``cycles.jsonl``  — one record per polling cycle
  * ``signals.jsonl`` — one record per generated signal (approved or rejected)

The dashboard tail-reads these files; rotation is not needed for typical volumes.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Not JSON serializable: {type(obj).__name__}")


class EventLog:
    def __init__(self, data_dir: str | Path = "./data") -> None:
        self._dir = Path(data_dir)
        self._cycles = self._dir / "cycles.jsonl"
        self._signals = self._dir / "signals.jsonl"
        self._dir.mkdir(parents=True, exist_ok=True)

    def emit_cycle(self, **fields: Any) -> None:
        self._append(self._cycles, {"ts": _now_iso(), **fields})

    def emit_signal(self, **fields: Any) -> None:
        self._append(self._signals, {"ts": _now_iso(), **fields})

    def emit_evaluation(self, **fields: Any) -> None:
        evals = self._dir / "evaluations.jsonl"
        self._append(evals, {"ts": _now_iso(), **fields})

    def _append(self, path: Path, record: dict[str, Any]) -> None:
        line = json.dumps(record, default=_json_default)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


_DEFAULT_EVALS = Path("./data/evaluations.jsonl")
_DEFAULT_RESULTS = Path("./data/results.jsonl")


def emit_evaluation(**fields: Any) -> None:
    """Module-level convenience — writes to ./data/evaluations.jsonl."""
    _DEFAULT_EVALS.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"ts": _now_iso(), **fields}, default=_json_default)
    with _DEFAULT_EVALS.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def emit_result(**fields: Any) -> None:
    """Write a resolved trade outcome to ./data/results.jsonl."""
    _DEFAULT_RESULTS.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"ts": _now_iso(), **fields}, default=_json_default)
    with _DEFAULT_RESULTS.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
