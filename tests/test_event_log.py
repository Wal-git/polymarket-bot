import json
from pathlib import Path
from unittest.mock import patch

import pytest

import polybot.monitoring.event_log as event_log


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(event_log, "_DEFAULT_RESULTS", tmp_path / "results.jsonl")
    monkeypatch.setattr(event_log, "_DEFAULT_EXECUTIONS", tmp_path / "executions.jsonl")
    monkeypatch.setattr(event_log, "_DEFAULT_EVALS", tmp_path / "evaluations.jsonl")
    event_log.reset_result_dedup_cache()
    yield tmp_path
    event_log.reset_result_dedup_cache()


def _read_results(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


class TestEmitResultDedup:
    def test_first_write_persists(self, isolated_data_dir):
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0, confidence=0.9)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert len(rows) == 1
        assert rows[0]["slug"] == "btc-updown-5m-1"
        assert rows[0]["confidence"] == 0.9

    def test_duplicate_slug_is_dropped(self, isolated_data_dir):
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0, confidence=0.9)
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0, confidence=0.9)
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert len(rows) == 1

    def test_distinct_slugs_both_persist(self, isolated_data_dir):
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0, confidence=0.9)
        event_log.emit_result(slug="btc-updown-5m-2", won=False, pnl=-3.0, confidence=0.8)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert len(rows) == 2
        assert {r["slug"] for r in rows} == {"btc-updown-5m-1", "btc-updown-5m-2"}

    def test_dedup_warm_loads_from_existing_file(self, isolated_data_dir):
        # Simulate prior process writing a result
        existing = isolated_data_dir / "results.jsonl"
        existing.write_text(json.dumps({"slug": "btc-updown-5m-1", "won": True, "pnl": 5.0}) + "\n")
        event_log.reset_result_dedup_cache()

        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0)
        rows = _read_results(existing)
        # Original 1 row, no duplicate appended
        assert len(rows) == 1


class TestConfidenceFallback:
    def test_pulls_confidence_from_executions_when_missing(self, isolated_data_dir):
        # Seed an execution with confidence
        event_log.emit_execution(
            slug="btc-updown-5m-1",
            status="filled",
            confidence=0.92,
            fill_price=0.78,
        )
        # Emit result without confidence
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert len(rows) == 1
        assert rows[0]["confidence"] == 0.92

    def test_does_not_overwrite_explicit_confidence(self, isolated_data_dir):
        event_log.emit_execution(slug="btc-updown-5m-1", status="filled", confidence=0.92)
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0, confidence=0.85)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert rows[0]["confidence"] == 0.85

    def test_no_executions_file_means_no_confidence(self, isolated_data_dir):
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert rows[0].get("confidence") is None

    def test_uses_latest_execution_when_multiple(self, isolated_data_dir):
        event_log.emit_execution(slug="btc-updown-5m-1", status="filled", confidence=0.70)
        event_log.emit_execution(slug="btc-updown-5m-1", status="filled", confidence=0.95)
        event_log.emit_result(slug="btc-updown-5m-1", won=True, pnl=5.0)
        rows = _read_results(isolated_data_dir / "results.jsonl")
        assert rows[0]["confidence"] == 0.95
