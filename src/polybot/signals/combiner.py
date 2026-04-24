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
    """Divergence-only signal with a high-conviction fast-pass.

    Fires when both Binance and Coinbase exceed min_gap_usd in the same
    direction, OR when either exchange exceeds fast_pass_usd (no min_gap
    required on the other exchange, but both must agree on direction).
    Imbalance data is still captured for diagnostics but not used as a gate.
    """
    sig_cfg = config.get("signals", {})
    div_cfg = sig_cfg.get("divergence", {})
    imb_cfg = sig_cfg.get("imbalance", {})
    siz_cfg = config.get("sizing", {})

    min_gap = float(div_cfg.get("min_gap_usd", 75.0))
    fast_pass = float(div_cfg.get("fast_pass_usd", 200.0))
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

    _imb_diag = dict(
        imbalance_ratio=window_ratio,
        latest_imbalance_ratio=latest_ratio,
        imbalance_readings=len(history),
        window_readings=len(window_readings),
    )

    _thresholds = dict(
        min_gap_usd=min_gap,
        fast_pass_usd=fast_pass,
        buy_threshold=threshold_buy,
        sell_threshold=threshold_sell,
        bankroll=round(bankroll, 2),
    )

    # Determine direction: fast-pass (either > fast_pass_usd, both same sign)
    # or normal divergence (both > min_gap_usd)
    both_positive = binance_delta > 0 and coinbase_delta > 0
    both_negative = binance_delta < 0 and coinbase_delta < 0
    fast_pass_triggered = (
        (abs(binance_delta) >= fast_pass or abs(coinbase_delta) >= fast_pass)
        and (both_positive or both_negative)
    )

    if fast_pass_triggered:
        direction = Direction.UP if both_positive else Direction.DOWN
        logger.info("fast_pass_triggered", slug=slot.slug, direction=direction.value,
                    binance_delta=binance_delta, coinbase_delta=coinbase_delta)
    else:
        direction = detect_divergence(prices, slot.price_to_beat, min_gap_usd=min_gap)

    if direction is None:
        logger.debug("no_divergence", slug=slot.slug)
        emit_evaluation(
            **_base,
            **_imb_diag,
            **_thresholds,
            div_direction=None,
            imb_direction=None,
            confluence=False,
            fast_pass=False,
            confidence=None,
            size_usdc=None,
            direction=None,
            reject_reason="no_divergence",
        )
        return None

    # Capture imbalance snapshot for diagnostics (not a gate)
    imb_direction = detect_smart_entry(history, threshold_buy, threshold_sell, window)
    token_id = slot.up_token_id if direction == Direction.UP else slot.down_token_id
    snapshot = book_ws.get_snapshot(token_id)
    imbalance = calculate_imbalance(snapshot, depth=depth)

    confidence = min(0.95, 0.6 + abs(binance_delta) / 500.0 + abs(coinbase_delta) / 500.0)

    min_confidence = float(siz_cfg.get("min_confidence", 0.0))
    if confidence < min_confidence:
        logger.debug("confidence_below_threshold", slug=slot.slug,
                     confidence=round(confidence, 3), min_confidence=min_confidence)
        emit_evaluation(
            **_base,
            **_imb_diag,
            **_thresholds,
            div_direction=direction.value,
            imb_direction=imb_direction.value if imb_direction else None,
            snapshot_imbalance=round(imbalance, 3),
            confluence=False,
            fast_pass=fast_pass_triggered,
            confidence=round(confidence, 3),
            size_usdc=None,
            direction=None,
            reject_reason="low_confidence",
        )
        return None

    base_min_usdc = float(siz_cfg.get("min_trade_usdc", 10.0))
    double_min_threshold = float(siz_cfg.get("double_min_above_usd", 200.0))
    large_move = max(abs(binance_delta), abs(coinbase_delta)) >= double_min_threshold
    effective_min_usdc = base_min_usdc * 2 if large_move else base_min_usdc

    from polybot.execution.sizing import kelly_size
    size = kelly_size(
        confidence=confidence,
        entry_price=book_ws.best_ask(direction) or 0.5,
        bankroll=bankroll,
        kelly_fraction=float(siz_cfg.get("kelly_fraction", 0.25)),
        min_usdc=effective_min_usdc,
        max_usdc=float(siz_cfg.get("max_trade_usdc", 200.0)),
    )

    logger.info(
        "signal_fired",
        slug=slot.slug,
        direction=direction.value,
        confidence=round(confidence, 3),
        size_usdc=round(size, 2),
        binance_delta=binance_delta,
        coinbase_delta=coinbase_delta,
        fast_pass=fast_pass_triggered,
        doubled_min=large_move,
    )

    emit_evaluation(
        **_base,
        **_imb_diag,
        **_thresholds,
        div_direction=direction.value,
        imb_direction=imb_direction.value if imb_direction else None,
        snapshot_imbalance=round(imbalance, 3),
        confluence=True,
        fast_pass=fast_pass_triggered,
        doubled_min=large_move,
        confidence=round(confidence, 3),
        size_usdc=round(size, 2),
        direction=direction.value,
        reject_reason=None,
        confidence_price_contrib=round(abs(binance_delta) / 500.0, 4),
        confidence_coinbase_contrib=round(abs(coinbase_delta) / 500.0, 4),
        best_ask=round(book_ws.best_ask(direction) or 0.5, 4),
    )

    return TradeSignal(direction=direction, confidence=round(confidence, 3), size_usdc=size)
