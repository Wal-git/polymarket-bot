"""Sybil / wash-trade clustering.

Detects groups of wallets that behave as one operator:
  - Strong market-set overlap (Jaccard on traded conditionIds).
  - High co-trade timing (fraction of trades on same market within 60s window).

For each cluster, only the wallet with the highest score is kept; the others
are recorded on the cluster mapping for audit.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import structlog

from polybot.smart_wallets.config import (
    SYBIL_JACCARD_THRESHOLD,
    SYBIL_TIMING_THRESHOLD,
)
from polybot.smart_wallets.metrics import _ensure_ledger

logger = structlog.get_logger()

CO_TRADE_WINDOW_SECONDS = 60


@dataclass(frozen=True)
class SybilCluster:
    representative: str
    members: tuple[str, ...]
    jaccard: float
    timing_overlap: float


def _market_set(wallet: Mapping[str, object]) -> set[str]:
    return set(_ensure_ledger(dict(wallet)).keys())


def _trade_times_by_market(wallet: Mapping[str, object]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for ev in wallet.get("raw_trades") or []:  # type: ignore[union-attr]
        mid = str(
            ev.get("conditionId")
            or ev.get("market")
            or ev.get("marketId")
            or ""
        )
        try:
            ts = int(ev.get("timestamp") or 0)
        except (TypeError, ValueError):
            ts = 0
        if mid and ts:
            out.setdefault(mid, []).append(ts)
    for k in out:
        out[k].sort()
    return out


def _timing_overlap(
    a_times: Mapping[str, list[int]],
    b_times: Mapping[str, list[int]],
) -> float:
    """Fraction of a's trades that co-occur with a b-trade within the window."""
    a_total = sum(len(v) for v in a_times.values())
    if a_total == 0:
        return 0.0
    hits = 0
    for mid, a_list in a_times.items():
        b_list = b_times.get(mid)
        if not b_list:
            continue
        j = 0
        for ta in a_list:
            while j < len(b_list) and b_list[j] < ta - CO_TRADE_WINDOW_SECONDS:
                j += 1
            if j < len(b_list) and b_list[j] <= ta + CO_TRADE_WINDOW_SECONDS:
                hits += 1
    return hits / a_total


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def find_clusters(
    wallets: list[dict],
    jaccard_threshold: float = SYBIL_JACCARD_THRESHOLD,
    timing_threshold: float = SYBIL_TIMING_THRESHOLD,
) -> list[SybilCluster]:
    """Return clusters of sybil-suspicious wallets. Singletons are omitted."""
    if len(wallets) < 2:
        return []

    markets = [(_market_set(w), _trade_times_by_market(w)) for w in wallets]
    n = len(wallets)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    edge_meta: dict[tuple[int, int], tuple[float, float]] = {}

    for i in range(n):
        mi, ti = markets[i]
        if not mi:
            continue
        for j in range(i + 1, n):
            mj, tj = markets[j]
            if not mj:
                continue
            jac = _jaccard(mi, mj)
            if jac < jaccard_threshold:
                continue
            timing = _timing_overlap(ti, tj)
            if timing < timing_threshold:
                continue
            union(i, j)
            edge_meta[(i, j)] = (jac, timing)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)

    clusters: list[SybilCluster] = []
    for members in groups.values():
        if len(members) < 2:
            continue
        # Representative = highest score; fall back to highest realized PnL.
        rep_idx = max(
            members,
            key=lambda i: (
                wallets[i].get("score", 0.0),
                wallets[i].get("pnl_realized", 0.0),
            ),
        )
        member_wallets = tuple(wallets[i]["proxy_wallet"] for i in members)
        max_jac = max(
            (edge_meta.get((min(a, b), max(a, b)), (0.0, 0.0))[0]
             for a in members for b in members if a != b),
            default=0.0,
        )
        max_timing = max(
            (edge_meta.get((min(a, b), max(a, b)), (0.0, 0.0))[1]
             for a in members for b in members if a != b),
            default=0.0,
        )
        clusters.append(
            SybilCluster(
                representative=wallets[rep_idx]["proxy_wallet"],
                members=member_wallets,
                jaccard=max_jac,
                timing_overlap=max_timing,
            )
        )
    if clusters:
        logger.info("sybil_clusters_found", count=len(clusters))
    return clusters


def collapse_clusters(
    wallets: list[dict],
    clusters: list[SybilCluster],
) -> tuple[list[dict], set[str]]:
    """Drop non-representative cluster members. Returns (kept, dropped_wallets)."""
    dropped: set[str] = set()
    for c in clusters:
        for m in c.members:
            if m != c.representative:
                dropped.add(m)
    kept = [w for w in wallets if w["proxy_wallet"] not in dropped]
    return kept, dropped
