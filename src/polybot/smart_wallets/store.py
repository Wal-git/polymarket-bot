"""SQLite persistence and smart_wallets JSON writers."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import structlog

from polybot.smart_wallets.config import (
    LOOKBACK_DAYS,
    SMART_WALLETS_CLOSER_JSON,
    SMART_WALLETS_DB,
    SMART_WALLETS_JSON,
    SMART_WALLETS_SIGNAL_JSON,
)
from polybot.smart_wallets.sybil import SybilCluster

logger = structlog.get_logger()

_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    status       TEXT NOT NULL DEFAULT 'running',
    n_candidates INTEGER,
    n_selected   INTEGER
);

CREATE TABLE IF NOT EXISTS wallet_snapshot (
    run_id              INTEGER NOT NULL REFERENCES runs(run_id),
    proxy_wallet        TEXT    NOT NULL,
    username            TEXT,
    volume              REAL,
    pnl_realized        REAL,
    pnl_unrealized      REAL,
    win_rate            REAL,
    resolved_markets    INTEGER,
    trades_count        INTEGER,
    avg_position_usd    REAL,
    max_drawdown        REAL,
    last_active_ts      INTEGER,
    sharpe              REAL,
    edge                REAL,
    significance_z      REAL,
    early_signal        REAL,
    score               REAL,
    signal_score        REAL,
    PRIMARY KEY (run_id, proxy_wallet)
);

CREATE TABLE IF NOT EXISTS wallet_history (
    proxy_wallet    TEXT PRIMARY KEY,
    first_seen_run  INTEGER NOT NULL,
    last_seen_run   INTEGER NOT NULL,
    times_selected  INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS wallet_rejects (
    run_id       INTEGER NOT NULL REFERENCES runs(run_id),
    proxy_wallet TEXT    NOT NULL,
    reason       TEXT    NOT NULL,
    stage        TEXT    NOT NULL,
    PRIMARY KEY (run_id, proxy_wallet)
);

CREATE TABLE IF NOT EXISTS sybil_clusters (
    run_id         INTEGER NOT NULL REFERENCES runs(run_id),
    representative TEXT    NOT NULL,
    members_json   TEXT    NOT NULL,
    jaccard        REAL,
    timing_overlap REAL,
    PRIMARY KEY (run_id, representative)
);
"""


class Store:
    def __init__(
        self,
        db_path: Path | None = None,
        json_path: Path | None = None,
        closer_json_path: Path | None = None,
        signal_json_path: Path | None = None,
    ):
        # Resolve lazily so tests/monkeypatching of module-level paths take effect.
        from polybot.smart_wallets import store as _self_mod
        self._db_path = db_path or _self_mod.SMART_WALLETS_DB
        self._json_path = json_path or _self_mod.SMART_WALLETS_JSON
        self._closer_json_path = closer_json_path or _self_mod.SMART_WALLETS_CLOSER_JSON
        self._signal_json_path = signal_json_path or _self_mod.SMART_WALLETS_SIGNAL_JSON
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(self._db_path))
        self._con.row_factory = sqlite3.Row
        self._con.executescript(_DDL)
        # Schema migration for existing DBs missing newer columns.
        self._migrate_snapshot_columns()
        self._con.commit()

    def _migrate_snapshot_columns(self) -> None:
        cols = {r["name"] for r in self._con.execute("PRAGMA table_info(wallet_snapshot)").fetchall()}
        for col, sql_type in [
            ("sharpe", "REAL"),
            ("edge", "REAL"),
            ("significance_z", "REAL"),
            ("early_signal", "REAL"),
            ("signal_score", "REAL"),
        ]:
            if col not in cols:
                self._con.execute(f"ALTER TABLE wallet_snapshot ADD COLUMN {col} {sql_type}")

    # ------------------------------------------------------------------
    # run lifecycle
    # ------------------------------------------------------------------

    def start_run(self) -> int:
        cur = self._con.execute(
            "INSERT INTO runs (started_at, status) VALUES (?, 'running')",
            (_now(),),
        )
        self._con.commit()
        run_id = cur.lastrowid
        logger.info("run_started", run_id=run_id)
        return run_id

    def finish_run(self, run_id: int, n_candidates: int, n_selected: int, status: str = "ok") -> None:
        self._con.execute(
            "UPDATE runs SET finished_at=?, status=?, n_candidates=?, n_selected=? WHERE run_id=?",
            (_now(), status, n_candidates, n_selected, run_id),
        )
        self._con.commit()
        logger.info("run_finished", run_id=run_id, status=status, selected=n_selected)

    # ------------------------------------------------------------------
    # snapshot persistence
    # ------------------------------------------------------------------

    def save_snapshot(self, run_id: int, wallets: list[dict]) -> None:
        rows = [
            (
                run_id,
                w["proxy_wallet"],
                w.get("username", ""),
                w.get("volume"),
                w.get("pnl_realized"),
                w.get("pnl_unrealized"),
                w.get("win_rate"),
                w.get("resolved_markets"),
                w.get("trades_count"),
                w.get("avg_position_usd"),
                w.get("max_drawdown"),
                w.get("last_active_ts"),
                w.get("sharpe"),
                w.get("edge"),
                w.get("significance_z"),
                w.get("early_signal"),
                w.get("score"),
                w.get("signal_score"),
            )
            for w in wallets
        ]
        self._con.executemany(
            """INSERT OR REPLACE INTO wallet_snapshot
               (run_id, proxy_wallet, username, volume, pnl_realized, pnl_unrealized,
                win_rate, resolved_markets, trades_count, avg_position_usd,
                max_drawdown, last_active_ts, sharpe, edge, significance_z,
                early_signal, score, signal_score)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        for w in wallets:
            pw = w["proxy_wallet"]
            self._con.execute(
                """INSERT INTO wallet_history (proxy_wallet, first_seen_run, last_seen_run, times_selected)
                   VALUES (?, ?, ?, 1)
                   ON CONFLICT(proxy_wallet) DO UPDATE SET
                     last_seen_run = excluded.last_seen_run,
                     times_selected = times_selected + 1""",
                (pw, run_id, run_id),
            )
        self._con.commit()

    def save_rejects(self, run_id: int, rejects: list[dict]) -> None:
        if not rejects:
            return
        rows = [
            (run_id, r["wallet"], r["reason"], r.get("stage", "hard_floor"))
            for r in rejects
        ]
        self._con.executemany(
            "INSERT OR REPLACE INTO wallet_rejects (run_id, proxy_wallet, reason, stage) VALUES (?,?,?,?)",
            rows,
        )
        self._con.commit()

    def save_sybil_clusters(self, run_id: int, clusters: list[SybilCluster]) -> None:
        if not clusters:
            return
        rows = [
            (run_id, c.representative, json.dumps(list(c.members)), c.jaccard, c.timing_overlap)
            for c in clusters
        ]
        self._con.executemany(
            """INSERT OR REPLACE INTO sybil_clusters
               (run_id, representative, members_json, jaccard, timing_overlap)
               VALUES (?,?,?,?,?)""",
            rows,
        )
        self._con.commit()

    # ------------------------------------------------------------------
    # JSON output
    # ------------------------------------------------------------------

    def write_json(self, wallets: list[dict], lookback_days: int = LOOKBACK_DAYS) -> None:
        payload = _wallets_payload(wallets, lookback_days)
        self._json_path.parent.mkdir(parents=True, exist_ok=True)
        self._json_path.write_text(json.dumps(payload, indent=2))
        logger.info("json_written", path=str(self._json_path), count=len(wallets))

    def write_archetype_json(
        self,
        closer: list[dict],
        signal: list[dict],
        lookback_days: int = LOOKBACK_DAYS,
    ) -> None:
        closer_payload = _wallets_payload(closer, lookback_days)
        signal_payload = _wallets_payload(signal, lookback_days, score_field="signal_score")
        self._closer_json_path.parent.mkdir(parents=True, exist_ok=True)
        self._closer_json_path.write_text(json.dumps(closer_payload, indent=2))
        self._signal_json_path.write_text(json.dumps(signal_payload, indent=2))
        logger.info(
            "archetype_json_written",
            closer=len(closer),
            signal=len(signal),
        )

    # ------------------------------------------------------------------
    # query / status
    # ------------------------------------------------------------------

    def recent_runs(self, limit: int = 5) -> list[dict]:
        rows = self._con.execute(
            "SELECT * FROM runs ORDER BY run_id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def wallet_diff(self, run_id: int) -> dict:
        cur = self._con.execute(
            "SELECT proxy_wallet FROM wallet_snapshot WHERE run_id=?", (run_id,)
        ).fetchall()
        current = {r["proxy_wallet"] for r in cur}

        prev_run = self._con.execute(
            "SELECT run_id FROM runs WHERE run_id < ? ORDER BY run_id DESC LIMIT 1", (run_id,)
        ).fetchone()
        if not prev_run:
            return {"added": list(current), "removed": [], "retained": []}

        prev = self._con.execute(
            "SELECT proxy_wallet FROM wallet_snapshot WHERE run_id=?", (prev_run["run_id"],)
        ).fetchall()
        previous = {r["proxy_wallet"] for r in prev}
        return {
            "added": list(current - previous),
            "removed": list(previous - current),
            "retained": list(current & previous),
        }

    def reject_histogram(self, run_id: int) -> dict[str, int]:
        rows = self._con.execute(
            "SELECT reason, COUNT(*) AS n FROM wallet_rejects WHERE run_id=? GROUP BY reason",
            (run_id,),
        ).fetchall()
        return {r["reason"]: r["n"] for r in rows}

    def snapshot_for_run(self, run_id: int) -> list[dict]:
        rows = self._con.execute(
            "SELECT * FROM wallet_snapshot WHERE run_id=?", (run_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self._con.close()


def _wallets_payload(
    wallets: list[dict],
    lookback_days: int,
    score_field: str = "score",
) -> dict:
    return {
        "generated_at": _now(),
        "lookback_days": lookback_days,
        "wallets": [
            {
                "proxy_wallet": w["proxy_wallet"],
                "username": w.get("username", ""),
                "score": w.get(score_field, 0.0),
                "signal_score": w.get("signal_score", 0.0),
                "closer_score": w.get("score", 0.0),
                "pnl_realized": w.get("pnl_realized", 0.0),
                "edge": w.get("edge", 0.0),
                "sharpe": w.get("sharpe", 0.0),
                "win_rate": w.get("win_rate", 0.0),
                "resolved_markets": w.get("resolved_markets", 0),
                "volume": w.get("volume", 0.0),
                "early_signal": w.get("early_signal", 0.0),
                "last_active_ts": w.get("last_active_ts", 0),
            }
            for w in wallets
        ],
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
