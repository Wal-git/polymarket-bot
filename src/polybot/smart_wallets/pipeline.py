"""Orchestrates seed → enrich → score → filter → persist."""
from __future__ import annotations

import structlog

from polybot.smart_wallets.api import DataAPIClient
from polybot.smart_wallets.config import (
    LOOKBACK_DAYS,
    MAX_DAYS_INACTIVE,
    MIN_REALIZED_PNL_USD,
    MIN_RESOLVED_MARKETS,
    MIN_VOLUME_USD,
    MIN_WIN_RATE,
    TOP_K,
)
from polybot.smart_wallets.enrich import enrich_candidates
from polybot.smart_wallets.metrics import compute_all_metrics, score_wallets
from polybot.smart_wallets.seed import fetch_candidates
from polybot.smart_wallets.store import Store

logger = structlog.get_logger()


def run(
    top_k: int = TOP_K,
    lookback_days: int = LOOKBACK_DAYS,
    dry_run: bool = False,
    candidate_limit: int | None = None,
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

        # 2. Enrich
        enriched = enrich_candidates(candidates, client, lookback_days=lookback_days)

        # 3. Compute metrics
        for w in enriched:
            w.update(compute_all_metrics(w))

        # 4. Filter
        import time as _time
        now = _time.time()
        cutoff_ts = now - MAX_DAYS_INACTIVE * 86400

        passed = [
            w for w in enriched
            if (
                w["resolved_markets"] >= MIN_RESOLVED_MARKETS
                and w["win_rate"] >= MIN_WIN_RATE
                and w["pnl_realized"] >= MIN_REALIZED_PNL_USD
                and w["volume"] >= MIN_VOLUME_USD
                and w["last_active_ts"] >= cutoff_ts
            )
        ]
        logger.info("pipeline_filter_done", passed=len(passed), total=len(enriched))

        # 5. Score
        scored = score_wallets(passed)
        selected = sorted(scored, key=lambda w: w["score"], reverse=True)[:top_k]
        n_selected = len(selected)
        logger.info("pipeline_score_done", selected=n_selected)

        # 6. Persist
        if not dry_run:
            store.save_snapshot(run_id, selected)
            store.write_json(selected, lookback_days=lookback_days)
            store.finish_run(run_id, n_candidates=n_candidates, n_selected=n_selected)
        else:
            store.finish_run(run_id, n_candidates=n_candidates, n_selected=n_selected, status="dry_run")
            logger.info("dry_run_skipped_write")

        return {
            "run_id": run_id,
            "n_candidates": n_candidates,
            "n_selected": n_selected,
            "dry_run": dry_run,
            "wallets": selected,
        }

    except Exception as exc:
        store.finish_run(run_id, n_candidates=0, n_selected=0, status=f"error: {exc}")
        logger.error("pipeline_failed", error=str(exc))
        raise
    finally:
        client.close()
        store.close()
