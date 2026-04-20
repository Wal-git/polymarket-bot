"""Tests for sybil clustering."""
from __future__ import annotations

from polybot.smart_wallets.sybil import (
    collapse_clusters,
    find_clusters,
)


def _make_wallet(addr: str, trades: list[tuple[str, int, str]], score: float = 0.5) -> dict:
    """trades = [(condition_id, timestamp, side), ...]"""
    return {
        "proxy_wallet": addr,
        "raw_trades": [
            {"conditionId": c, "timestamp": t, "side": s, "usdcSize": 100.0, "asset": f"{c}-tok"}
            for c, t, s in trades
        ],
        "raw_redeems": [],
        "raw_positions": [],
        "score": score,
    }


def test_no_clusters_when_no_overlap():
    w1 = _make_wallet("0x1", [("m1", 1000, "BUY"), ("m2", 2000, "BUY")])
    w2 = _make_wallet("0x2", [("m3", 3000, "BUY"), ("m4", 4000, "BUY")])
    clusters = find_clusters([w1, w2])
    assert clusters == []


def test_cluster_detected_with_matching_markets_and_timing():
    trades = [("m1", 1000, "BUY"), ("m2", 2000, "BUY"), ("m3", 3000, "BUY")]
    w1 = _make_wallet("0x1", trades, score=0.9)
    # Same markets, timestamps within the 60s co-trade window.
    trades2 = [("m1", 1010, "BUY"), ("m2", 2020, "BUY"), ("m3", 3030, "BUY")]
    w2 = _make_wallet("0x2", trades2, score=0.5)
    clusters = find_clusters([w1, w2], jaccard_threshold=0.5, timing_threshold=0.5)
    assert len(clusters) == 1
    c = clusters[0]
    # Higher score wins representative election.
    assert c.representative == "0x1"
    assert set(c.members) == {"0x1", "0x2"}


def test_collapse_clusters_drops_non_representative():
    trades = [("m1", 1000, "BUY"), ("m2", 2000, "BUY")]
    w1 = _make_wallet("0x1", trades, score=0.9)
    w2 = _make_wallet("0x2", [("m1", 1005, "BUY"), ("m2", 2005, "BUY")], score=0.5)
    w3 = _make_wallet("0x3", [("m100", 5_000_000, "BUY")], score=0.5)
    clusters = find_clusters([w1, w2, w3], jaccard_threshold=0.5, timing_threshold=0.5)
    kept, dropped = collapse_clusters([w1, w2, w3], clusters)
    assert {w["proxy_wallet"] for w in kept} == {"0x1", "0x3"}
    assert dropped == {"0x2"}
