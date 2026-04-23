from typing import Optional

import structlog

from polybot.feeds.orderbook_ws import OrderBookWS
from polybot.models.btc_market import BtcPrices, Direction, SlotInfo, TradeSignal
from polybot.monitoring.event_log import emit_evaluation
from polybot.signals.divergence import detect_divergence
from polybot.signals.imbalance import calculate_imbalance, detect_smart_entry

logger = structlog.get_logger()


def should_trade(
    prices: BtcPrices,
    book_ws: OrderBookWS,
    slot: SlotInfo,
    bankroll: float,
    config: dict,
) -> Optional[TradeSignal]:
    """Two-signal confluence filter. Returns None for ~85% of 5-min windows.

    Both price divergence AND order-book imbalance must agree on direction.
    """
    sig_cfg = config.get("signals", {})
    div_cfg = sig_cfg.get("divergence", {})
    imb_cfg = sig_cfg.get("imbalance", {})
    siz_cfg = config.get("sizing", {})

    min_gap = float(div_cfg.get("min_gap_usd", 50.0))
    threshold_buy = float(imb_cfg.get("buy_threshold", 1.8))
    threshold_sell = float(imb_cfg.get("sell_threshold", 0.55))
    window_cfg = imb_cfg.get("detection_window_seconds", [30, 90])
    window = (float(window_cfg[0]), float(window_cfg[1]))
    depth = int(imb_cfg.get("depth_levels", 10))

    binance_delta = round(prices.binance - slot.price_to_beat, 2)
    coinbase_delta = round(prices.coinbase - slot.price_to_beat, 2)

    _base = dict(
        slug=slot.slug,
        price_to_beat=slot.price_to_beat,
        binance=round(prices.binance, 2),
        coinbase=round(prices.coinbase, 2),
        binance_delta=binance_delta,
        coinbase_delta=coinbase_delta,
    )

    # Always capture orderbook state for diagnostics
    history = book_ws.get_imbalance_history()
    window_readings = [r for r in history if window[0] <= r.seconds_since_open <= window[1]]
    window_ratio = round(max((r.ratio for r in window_readings), default=0.0), 3) if window_readings else None
    latest_ratio = round(history[-1].ratio, 3) if history else None

    # Signal 1: price divergence
    div_direction = detect_divergence(prices, slot.price_to_beat, min_gap_usd=min_gap)
    if div_direction is None:
        logger.debug("no_divergence", slug=slot.slug)
        emit_evaluation(
            **_base,
            div_direction=None,
            imb_direction=None,
            imbalance_ratio=window_ratio,
            latest_imbalance_ratio=latest_ratio,
            imbalance_readings=len(history),
            window_readings=len(window_readings),
            confluence=False,
            confidence=None,
            size_usdc=None,
            direction=None,
            reject_reason="no_divergence",
        )
        return None

    imb_direction = detect_smart_entry(history, threshold_buy, threshold_sell, window)
    if imb_direction is None or imb_direction != div_direction:
        reject = "no_imbalance" if imb_direction is None else "direction_mismatch"
        logger.debug(
            "no_imbalance_confluence",
            slug=slot.slug,
            div=div_direction,
            imb=imb_direction,
        )
        emit_evaluation(
            **_base,
            div_direction=div_direction.value,
            imb_direction=imb_direction.value if imb_direction else None,
            imbalance_ratio=window_ratio,
            latest_imbalance_ratio=latest_ratio,
            imbalance_readings=len(history),
            window_readings=len(window_readings),
            confluence=False,
            confidence=None,
            size_usdc=None,
            direction=None,
            reject_reason=reject,
        )
        return None

    # Both agree — compute confidence and Kelly size
    token_id = slot.up_token_id if div_direction == Direction.UP else slot.down_token_id
    snapshot = book_ws.get_snapshot(token_id)
    imbalance = calculate_imbalance(snapshot, depth=depth)
    confidence = min(0.95, 0.6 + abs(binance_delta) / 500.0 + abs(imbalance - 1.0) / 5.0)

    from polybot.execution.sizing import kelly_size
    size = kelly_size(
        confidence=confidence,
        entry_price=book_ws.best_ask(div_direction) or 0.5,
        bankroll=bankroll,
        kelly_fraction=float(siz_cfg.get("kelly_fraction", 0.25)),
        min_usdc=float(siz_cfg.get("min_trade_usdc", 10.0)),
        max_usdc=float(siz_cfg.get("max_trade_usdc", 200.0)),
    )

    logger.info(
        "signal_confluence",
        slug=slot.slug,
        direction=div_direction.value,
        confidence=round(confidence, 3),
        size_usdc=round(size, 2),
        imbalance=imbalance,
        binance_delta=binance_delta,
    )

    emit_evaluation(
        **_base,
        div_direction=div_direction.value,
        imb_direction=div_direction.value,
        imbalance_ratio=round(imbalance, 3),
        latest_imbalance_ratio=latest_ratio,
        imbalance_readings=len(history),
        window_readings=len(window_readings),
        confluence=True,
        confidence=round(confidence, 3),
        size_usdc=round(size, 2),
        direction=div_direction.value,
        reject_reason=None,
    )

    return TradeSignal(direction=div_direction, confidence=round(confidence, 3), size_usdc=size)
