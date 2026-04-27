"""Per-asset configuration carried through engine, feeds, and signals.

An ``AssetSpec`` captures everything that varies between BTC, ETH, and any
future asset Polymarket lists 5-min markets for: the slug prefix used by
discovery, the slot grid anchor, the spot-feed URL set, the Binance futures
symbol URL, the Chainlink aggregator address on Polygon, and per-asset
signal thresholds (which need to be scaled to the asset's price level).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class AssetThresholds:
    """Per-asset signal/sizing overrides.

    Values that are None fall back to the strategy_defaults block in config.
    Anything price-level-sensitive (USD-denominated gaps, calibration buckets)
    must be set per-asset; ratio-based thresholds (imbalance, kelly_fraction)
    typically don't need overrides.
    """
    min_gap_usd: Optional[float] = None
    max_gap_usd: Optional[float] = None
    fast_pass_usd: Optional[float] = None
    double_min_above_usd: Optional[float] = None
    delta_buckets: Optional[tuple[float, ...]] = None  # USD-denominated calibration bucket edges


@dataclass(frozen=True)
class AssetSpec:
    """All asset-specific knobs the engine, feeds, and signals consume."""
    name: str                                  # "BTC", "ETH"
    slug_prefix: str                           # "btc-updown-5m", "ethereum-updown-5m"
    slot_base_timestamp: int                   # unix seconds, anchor for the 5-min grid
    slot_interval_s: int = 300
    spot_urls: dict[str, str] = field(default_factory=dict)  # exchange_name -> URL
    futures_url: Optional[str] = None          # Binance futures premiumIndex
    chainlink_aggregator: Optional[str] = None # Polygon aggregator proxy address
    chainlink_rpc_url: Optional[str] = None
    calibration_table_path: Optional[str] = None  # data/calibration_table.{asset}.json
    thresholds: AssetThresholds = field(default_factory=AssetThresholds)
