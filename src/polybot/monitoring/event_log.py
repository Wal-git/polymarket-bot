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
_DEFAULT_EXECUTIONS = Path("./data/executions.jsonl")

# Slug-dedup state for emit_result. Warm-loaded from the existing file on first call
# so duplicate redeems across process restarts don't double-write.
_emitted_result_slugs: set[str] | None = None


def emit_evaluation(**fields: Any) -> None:
    """Module-level convenience — writes to ./data/evaluations.jsonl."""
    _DEFAULT_EVALS.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"ts": _now_iso(), **fields}, default=_json_default)
    with _DEFAULT_EVALS.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _load_emitted_result_slugs() -> set[str]:
    seen: set[str] = set()
    if not _DEFAULT_RESULTS.exists():
        return seen
    try:
        with _DEFAULT_RESULTS.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    slug = rec.get("slug")
                    if slug:
                        seen.add(slug)
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return seen


def _lookup_confidence_from_executions(slug: str) -> float | None:
    """Scan executions.jsonl for the most recent confidence on this slug."""
    if not _DEFAULT_EXECUTIONS.exists():
        return None
    found: float | None = None
    try:
        with _DEFAULT_EXECUTIONS.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or slug not in line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("slug") == slug and rec.get("confidence") is not None:
                    found = float(rec["confidence"])  # last write wins
    except OSError:
        return None
    return found


def emit_result(**fields: Any) -> None:
    """Write a resolved trade outcome to ./data/results.jsonl.

    Dedupes by slug (per-slug single-write) so repeated redemption scans don't
    bloat the file. Falls back to executions.jsonl for confidence when missing.
    """
    global _emitted_result_slugs
    if _emitted_result_slugs is None:
        _emitted_result_slugs = _load_emitted_result_slugs()

    slug = fields.get("slug")
    if slug and slug in _emitted_result_slugs:
        return  # already written for this slug

    if slug and fields.get("confidence") is None:
        fallback = _lookup_confidence_from_executions(slug)
        if fallback is not None:
            fields["confidence"] = fallback

    _DEFAULT_RESULTS.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"ts": _now_iso(), **fields}, default=_json_default)
    with _DEFAULT_RESULTS.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    if slug:
        _emitted_result_slugs.add(slug)


def reset_result_dedup_cache() -> None:
    """Test hook — clears the in-memory dedup cache."""
    global _emitted_result_slugs
    _emitted_result_slugs = None


def emit_execution(**fields: Any) -> None:
    """Write an order placement or block record to ./data/executions.jsonl."""
    _DEFAULT_EXECUTIONS.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"ts": _now_iso(), **fields}, default=_json_default)
    with _DEFAULT_EXECUTIONS.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
