from pathlib import Path

import yaml

from polybot.auth.wallet import load_env
from polybot.client.clob import CLOBClient
from polybot.engine.scheduler import MultiAssetEngine
from polybot.models.asset import AssetSpec, AssetThresholds
from polybot.monitoring.logger import setup_logging
from polybot.monitoring.tracker import PositionTracker


def load_config(config_path: str = "config/default.yaml") -> dict:
    return yaml.safe_load(Path(config_path).read_text())


# Hardcoded BTC fallback used when ``assets:`` block is missing from config.
# Mirrors the values that lived inline in discovery.py and the old config blocks.
_BTC_FALLBACK = {
    "slug_prefix": "btc-updown-5m",
    "slot_base_timestamp": 1772568900,
    "slot_interval_s": 300,
    "spot_urls": {
        "binance": "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
        "coinbase": "https://api.coinbase.com/v2/prices/BTC-USD/spot",
        "kraken": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
        "bitstamp": "https://www.bitstamp.net/api/v2/ticker/btcusd/",
        "okx": "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT",
    },
    "futures_url": "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT",
    "chainlink_aggregator": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "chainlink_rpc_url": "https://1rpc.io/matic",
}


def _build_asset_specs(config: dict) -> list[AssetSpec]:
    """Build AssetSpec list from the ``assets:`` block, or fall back to a
    BTC-only spec assembled from legacy config keys.
    """
    assets_block = config.get("assets")
    if not assets_block:
        return [_btc_default_spec()]

    specs: list[AssetSpec] = []
    for name, body in assets_block.items():
        if not body.get("enabled", True):
            continue
        thresholds_block = body.get("thresholds") or {}
        specs.append(
            AssetSpec(
                name=name,
                slug_prefix=body["slug_prefix"],
                slot_base_timestamp=int(body["slot_base_timestamp"]),
                slot_interval_s=int(body.get("slot_interval_s", 300)),
                spot_urls=dict(body.get("spot_urls", {})),
                futures_url=body.get("futures_url"),
                chainlink_aggregator=body.get("chainlink_aggregator"),
                chainlink_rpc_url=body.get("chainlink_rpc_url"),
                calibration_table_path=body.get("calibration_table_path"),
                eval_only=bool(body.get("eval_only", False)),
                thresholds=AssetThresholds(
                    min_gap_usd=thresholds_block.get("min_gap_usd"),
                    max_gap_usd=thresholds_block.get("max_gap_usd"),
                    fast_pass_usd=thresholds_block.get("fast_pass_usd"),
                    double_min_above_usd=thresholds_block.get("double_min_above_usd"),
                    delta_buckets=tuple(thresholds_block["delta_buckets"])
                    if thresholds_block.get("delta_buckets") else None,
                    deep_gap_usd=thresholds_block.get("deep_gap_usd"),
                    deep_gap_min_entry=thresholds_block.get("deep_gap_min_entry"),
                    min_confidence=thresholds_block.get("min_confidence"),
                    min_agreement=thresholds_block.get("min_agreement"),
                ),
            )
        )
    if not specs:
        raise ValueError("No enabled assets in config — set at least one assets.* block enabled: true")
    return specs


def _btc_default_spec() -> AssetSpec:
    f = _BTC_FALLBACK
    return AssetSpec(
        name="BTC",
        slug_prefix=f["slug_prefix"],
        slot_base_timestamp=f["slot_base_timestamp"],
        slot_interval_s=f["slot_interval_s"],
        spot_urls=dict(f["spot_urls"]),
        futures_url=f["futures_url"],
        chainlink_aggregator=f["chainlink_aggregator"],
        chainlink_rpc_url=f["chainlink_rpc_url"],
    )


def build_engine(config_path: str = "config/default.yaml") -> MultiAssetEngine:
    config = load_config(config_path)
    bot_cfg = config.get("bot", {})
    risk_cfg = config.get("risk", {})

    setup_logging(bot_cfg.get("log_level", "INFO"))
    load_env()

    clob = CLOBClient()
    state_file = bot_cfg.get("state_file", "./data/state.json")
    tracker = PositionTracker(state_file=state_file)

    return MultiAssetEngine(
        clob=clob,
        tracker=tracker,
        dry_run=bot_cfg.get("dry_run", True),
        config=config,
        assets=_build_asset_specs(config),
        halt_file=bot_cfg.get("halt_file", "./HALT"),
        daily_loss_limit=float(risk_cfg.get("daily_loss_limit_usdc", 100.0)),
    )
