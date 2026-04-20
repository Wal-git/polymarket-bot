"""Orchestrates seed → enrich → metrics → filter → score → sybil → persist."""
from __future__ import annotations

import time
from dataclasses import dataclass, field

import structlog

from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.config import (
    LOOKBACK_DAYS,
    MAX_DAYS_INACTIVE,
    MIN_REALIZED_PNL_USD,
    MIN_RESOLVED_MARKETS,
    MIN_SIGNIFICANCE_Z,
    MIN_TRADES_COUNT,
    MIN_VOLUME_USD,
    MIN_WIN_RATE,
    PCT_CUTOFF_EDGE,
    PCT_CUTOFF_SHARPE,
    PCT_CUTOFF_VOLUME,
    SCORE_WEIGHTS,
    SIGNAL_SCORE_WEIGHTS,
    TOP_K,
)
from polybot.smart_wallets.enrich import enrich_candidates, enrich_candidates_via_asyncio
from polybot.smart_wallets.metrics import compute_all_metrics, score_wallets
from polybot.smart_wallets.seed import fetch_candidates
from polybot.smart_wallets.store import Store
from polybot.smart_wallets.sybil import collapse_clusters, find_clusters

logger = structlog.get_logger()


@dataclass
class _RunResult:
    run_id: int
    n_candidates: int
    n_selected: int
    dry_run: bool
    wallets: list[dict]
    signal_wallets: list[dict]
    rejects: list[dict] = field(default_factory=list)
    sybil_dropped: set[str] = field(default_factory=set)


def run(
    top_k: int = TOP_K,
    lookback_days: int = LOOKBACK_DAYS,
    dry_run: bool = False,
    candidate_limit: int | None = None,
    async_enrich: bool = False,
) -> dict:
    """Execute the full pipeline and return a result summary dict."""
    store = Store()
    client = DataAPIClient()
    run_id = store.start_run()

    try:
        # 1. Seed
        candidates = fetch_candidates(client)
        if candidate_limit:
            candidates = candidates[:candidate_limit]
        n_candidates = len(candidates)
        logger.info("pipeline_seed_done", n_candidates=n_candidates)

        # 2. Enrich (thread-pool by default; async is opt-in for speed)
        if async_enrich:
            enriched = enrich_candidates_via_asyncio(candidates, lookback_days=lookback_days)
        else:
            enriched = enrich_candidates(candidates, client, lookback_days=lookback_days)

        # 3. Metrics
        for w in enriched:
            w.update(compute_all_metrics(w))

        # 4. Filter with reject-reason capture
        passed, rejects = _filter_with_reasons(enriched)
        logger.info("pipeline_filter_done", passed=len(passed), rejects=len(rejects))

        # 5. Score both archetypes
        closer_scored = score_wallets(passed, weights=SCORE_WEIGHTS, score_key="score")
        signal_scored = score_wallets(
            passed, weights=SIGNAL_SCORE_WEIGHTS, score_key="signal_score"
        )

        # Merge signal_score into the closer_scored list so each wallet has both.
        signal_by_wallet = {w["proxy_wallet"]: w.get("signal_score", 0.0) for w in signal_scored}
        merged = [
            {**w, "signal_score": signal_by_wallet.get(w["proxy_wallet"], 0.0)}
            for w in closer_scored
        ]

        # 6. Sybil clustering on the scored list
        clusters = find_clusters(merged)
        kept, dropped = collapse_clusters(merged, clusters)
        if dropped:
            logger.info("sybil_collapsed", dropped=len(dropped))

        # 7. Rank + top-K for both archetypes
        closer_top = sorted(kept, key=lambda w: w["score"], reverse=True)[:top_k]
        signal_top = sorted(kept, key=lambda w: w["signal_score"], reverse=True)[:top_k]

        n_selected = len(closer_top)
        logger.info("pipeline_rank_done", closer=n_selected, signal=len(signal_top))

        # 8. Persist
        if not dry_run:
            store.save_snapshot(run_id, closer_top)
            store.save_rejects(run_id, rejects)
            store.save_sybil_clusters(run_id, clusters)
            store.write_json(closer_top, lookback_days=lookback_days)
            store.write_archetype_json(
                closer_top, signal_top, lookback_days=lookback_days
            )
            store.finish_run(
                run_id,
                n_candidates=n_candidates,
                n_selected=n_selected,
                status="ok",
            )
        else:
            store.finish_run(
                run_id,
                n_candidates=n_candidates,
                n_selected=n_selected,
                status="dry_run",
            )
            logger.info("dry_run_skipped_write")

        return {
            "run_id": run_id,
            "n_candidates": n_candidates,
            "n_selected": n_selected,
            "dry_run": dry_run,
            "wallets": closer_top,
            "signal_wallets": signal_top,
            "rejects": rejects,
            "sybil_dropped": sorted(dropped),
        }

    except Exception as exc:
        store.finish_run(run_id, n_candidates=0, n_selected=0, status=f"error: {exc}")
        logger.error("pipeline_failed", error=str(exc))
        raise
    finally:
        client.close()
        store.close()


# ---------------------------------------------------------------------------
# filtering
# ---------------------------------------------------------------------------

def _filter_with_reasons(
    wallets: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Apply hard floors + percentile cuts. Returns (passed, rejects_with_reason)."""
    now = time.time()
    cutoff_ts = now - MAX_DAYS_INACTIVE * 86400

    # Hard-floor pass 1.
    stage1: list[dict] = []
    rejects: list[dict] = []
    for w in wallets:
        reason = _hard_floor_reason(w, cutoff_ts)
        if reason is None:
            stage1.append(w)
        else:
            rejects.append({"wallet": w["proxy_wallet"], "reason": reason, "stage": "hard_floor"})

    if not stage1:
        return [], rejects

    # Percentile cuts on the surviving distribution.
    sharpe_cut = _percentile_threshold(stage1, "sharpe", PCT_CUTOFF_SHARPE)
    edge_cut = _percentile_threshold(stage1, "edge", PCT_CUTOFF_EDGE)
    vol_cut = _percentile_threshold(stage1, "volume", PCT_CUTOFF_VOLUME)

    passed: list[dict] = []
    for w in stage1:
        if sharpe_cut is not None and w.get("sharpe", 0.0) < sharpe_cut:
            rejects.append({"wallet": w["proxy_wallet"], "reason": "below_sharpe_percentile", "stage": "pct_cut"})
            continue
        if edge_cut is not None and w.get("edge", 0.0) < edge_cut:
            rejects.append({"wallet": w["proxy_wallet"], "reason": "below_edge_percentile", "stage": "pct_cut"})
            continue
        if vol_cut is not None and w.get("volume", 0.0) < vol_cut:
            rejects.append({"wallet": w["proxy_wallet"], "reason": "below_volume_percentile", "stage": "pct_cut"})
            continue
        if w.get("significance_z", 0.0) < MIN_SIGNIFICANCE_Z:
            rejects.append({"wallet": w["proxy_wallet"], "reason": "insignificant_win_rate", "stage": "pct_cut"})
            continue
        passed.append(w)
    return passed, rejects


def _hard_floor_reason(w: dict, cutoff_ts: float) -> str | None:
    if w.get("trades_count", 0) < MIN_TRADES_COUNT:
        return "below_min_trades"
    if w.get("resolved_markets", 0) < MIN_RESOLVED_MARKETS:
        return "below_min_resolved_markets"
    if w.get("win_rate", 0.0) < MIN_WIN_RATE:
        return "below_min_win_rate"
    if w.get("pnl_realized", 0.0) < MIN_REALIZED_PNL_USD:
        return "below_min_pnl"
    if w.get("volume", 0.0) < MIN_VOLUME_USD:
        return "below_min_volume"
    if w.get("last_active_ts", 0) < cutoff_ts:
        return "stale"
    return None


def _percentile_threshold(
    wallets: list[dict], key: str, pct: float | None
) -> float | None:
    if pct is None:
        return None
    vals = sorted(w.get(key, 0.0) for w in wallets)
    if not vals:
        return None
    idx = max(0, min(len(vals) - 1, int(pct * len(vals))))
    return vals[idx]
