import time
from typing import Optional

import structlog

from polybot.feeds.orderbook_ws import OrderBookWS
from polybot.models.asset import AssetSpec
from polybot.models.market import (
    ChainlinkRound,
    Direction,
    FuturesSnapshot,
    MacroSnapshot,
    SlotInfo,
    SpotPrices,
    TradeSignal,
)
from polybot.monitoring.event_log import emit_evaluation
from polybot.signals import calibration as calibration_mod
from polybot.signals.divergence import detect_divergence
from polybot.signals.imbalance import calculate_imbalance, detect_smart_entry

logger = structlog.get_logger()


def should_trade(
    prices: SpotPrices,
    book_ws: OrderBookWS,
    slot: SlotInfo,
    bankroll: float,
    config: dict,
    asset: Optional[AssetSpec] = None,
    chainlink: Optional[ChainlinkRound] = None,
    futures: Optional[FuturesSnapshot] = None,
    macro: Optional[MacroSnapshot] = None,
) -> Optional[TradeSignal]:
    """N-of-M divergence signal with a high-conviction fast-pass.

    Fires when at least ``min_agreement`` exchanges exceed ``min_gap_usd`` past the
    price_to_beat in the same direction (with no exchanges in the opposite
    direction past min_gap), OR when any single exchange exceeds ``fast_pass_usd``
    and all available exchanges agree on direction.

    Imbalance, Chainlink, and Futures data are captured for diagnostics but not
    used as gates.
    """
    sig_cfg = config.get("signals", {})
    div_cfg = sig_cfg.get("divergence", {})
    imb_cfg = sig_cfg.get("imbalance", {})
    siz_cfg = config.get("sizing", {})

    # Per-asset threshold overrides take precedence over the strategy block.
    asset_thresholds = asset.thresholds if asset is not None else None

    def _override(name: str, fallback: float) -> float:
        if asset_thresholds is not None:
            v = getattr(asset_thresholds, name, None)
            if v is not None:
                return float(v)
        return float(fallback)

    min_gap = _override("min_gap_usd", div_cfg.get("min_gap_usd", 75.0))
    max_gap = _override("max_gap_usd", div_cfg.get("max_gap_usd", 0.0))  # 0 = disabled
    fast_pass = _override("fast_pass_usd", div_cfg.get("fast_pass_usd", 200.0))
    fast_pass_enabled = bool(div_cfg.get("fast_pass_enabled", True))
    min_agreement = int(
        asset_thresholds.min_agreement
        if (asset_thresholds is not None and asset_thresholds.min_agreement is not None)
        else div_cfg.get("min_agreement", 2)
    )
    threshold_buy = float(imb_cfg.get("buy_threshold", 1.8))
    threshold_sell = float(imb_cfg.get("sell_threshold", 0.55))
    window_cfg = imb_cfg.get("detection_window_seconds", [30, 90])
    window = (float(window_cfg[0]), float(window_cfg[1]))
    depth = int(imb_cfg.get("depth_levels", 10))

    # Per-exchange deltas (only for available exchanges)
    available = prices.exchange_prices()
    deltas: dict[str, float] = {
        name: round(price - slot.price_to_beat, 2)
        for name, price in available.items()
    }

    # Per-exchange diag fields for evaluations: e.g. {binance: 95100, binance_delta: 100, ...}
    _per_exchange_diag: dict[str, Optional[float]] = {}
    for name in ("binance", "coinbase", "kraken", "bitstamp", "okx"):
        if name in available:
            _per_exchange_diag[name] = round(available[name], 2)
            _per_exchange_diag[f"{name}_delta"] = deltas[name]
        else:
            _per_exchange_diag[name] = None
            _per_exchange_diag[f"{name}_delta"] = None

    # Capture best_ask for both sides upfront so every evaluation row records
    # the orderbook entry price — needed for the entry-price gate below and for
    # offline analysis of slots that didn't fire.
    up_best_ask = book_ws.best_ask(Direction.UP)
    down_best_ask = book_ws.best_ask(Direction.DOWN)

    _base = dict(
        slug=slot.slug,
        asset=asset.name if asset is not None else None,
        price_to_beat=slot.price_to_beat,
        sources_available=len(available),
        up_best_ask=round(up_best_ask, 4) if up_best_ask is not None else None,
        down_best_ask=round(down_best_ask, 4) if down_best_ask is not None else None,
        **_per_exchange_diag,
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

    # Chainlink diagnostics — logged into every evaluation, never used as a gate.
    if chainlink is not None:
        cl_lag = round(time.time() - chainlink.updated_at, 1)
        cl_vs_ptb = round(slot.price_to_beat - chainlink.answer, 2)
        # Per-exchange chainlink-vs-spot gaps for any available exchange
        cl_vs = {
            f"chainlink_vs_{name}": round(price - chainlink.answer, 2)
            for name, price in available.items()
        }
        _chainlink_diag = dict(
            chainlink_price=round(chainlink.answer, 2),
            chainlink_round_id=chainlink.round_id,
            chainlink_updated_at=chainlink.updated_at,
            chainlink_lag_s=cl_lag,
            chainlink_vs_price_to_beat=cl_vs_ptb,
            **cl_vs,
        )
    else:
        _chainlink_diag = dict(
            chainlink_price=None,
            chainlink_round_id=None,
            chainlink_updated_at=None,
            chainlink_lag_s=None,
            chainlink_vs_price_to_beat=None,
        )

    # Binance Futures diagnostics — logged into every evaluation, never used as a gate.
    if futures is not None:
        ms_now = time.time() * 1000
        funding_until_next_s = round((futures.next_funding_time_ms - ms_now) / 1000, 1)
        # Compare mark to spot consensus (mean of available spot exchanges)
        avail_prices = list(prices.exchange_prices().values())
        spot_mean = sum(avail_prices) / len(avail_prices) if avail_prices else None
        mark_minus_spot = (
            round(futures.mark_price - spot_mean, 2) if spot_mean is not None else None
        )
        mark_minus_index = round(futures.mark_price - futures.index_price, 2)
        _futures_diag = dict(
            futures_mark_price=round(futures.mark_price, 2),
            futures_index_price=round(futures.index_price, 2),
            futures_mark_minus_index=mark_minus_index,
            futures_mark_minus_spot=mark_minus_spot,
            futures_funding_rate=round(futures.last_funding_rate, 8),
            futures_funding_until_next_s=funding_until_next_s,
        )
    else:
        _futures_diag = dict(
            futures_mark_price=None,
            futures_index_price=None,
            futures_mark_minus_index=None,
            futures_mark_minus_spot=None,
            futures_funding_rate=None,
            futures_funding_until_next_s=None,
        )

    # Macro diagnostics — logged into every evaluation, never used as a gate.
    # Yahoo Finance can return None for any individual field when a market is
    # closed or the API hiccups; that's fine — we log what we have.
    if macro is not None:
        _macro_diag = dict(
            vix=round(macro.vix, 3) if macro.vix is not None else None,
            dxy=round(macro.dxy, 3) if macro.dxy is not None else None,
            es_price=round(macro.es_price, 2) if macro.es_price is not None else None,
            es_pct_change_1h=macro.es_pct_change_1h,
        )
    else:
        _macro_diag = dict(vix=None, dxy=None, es_price=None, es_pct_change_1h=None)

    deep_gap_usd = _override("deep_gap_usd", siz_cfg.get("deep_gap_usd", 0.0))
    deep_gap_min_entry = _override("deep_gap_min_entry", siz_cfg.get("deep_gap_min_entry", 0.0))

    _thresholds = dict(
        min_gap_usd=min_gap,
        max_gap_usd=max_gap,
        fast_pass_usd=fast_pass,
        fast_pass_enabled=fast_pass_enabled,
        min_agreement=min_agreement,
        buy_threshold=threshold_buy,
        sell_threshold=threshold_sell,
        bankroll=round(bankroll, 2),
        deep_gap_usd=deep_gap_usd,
        deep_gap_min_entry=deep_gap_min_entry,
    )

    def _emit(**extra) -> None:
        emit_evaluation(
            **_base, **_imb_diag, **_thresholds, **_chainlink_diag, **_futures_diag,
            **_macro_diag, **extra
        )

    # If no exchanges responded at all, we can't evaluate
    if not deltas:
        logger.warning("no_exchange_prices", slug=slot.slug)
        _emit(
            div_direction=None, imb_direction=None, confluence=False, fast_pass=False,
            confidence=None, size_usdc=None, direction=None,
            reject_reason="no_exchange_prices",
        )
        return None

    abs_deltas = [abs(d) for d in deltas.values()]
    max_abs_delta = max(abs_deltas)

    # Reject over-extended divergences: when ANY exchange exceeds max_gap, the
    # market has already over-priced the move and tends to revert within 5m.
    if max_gap > 0 and max_abs_delta > max_gap:
        logger.debug("divergence_too_large", slug=slot.slug,
                     deltas=deltas, max_gap=max_gap)
        _emit(
            div_direction=None, imb_direction=None, confluence=False, fast_pass=False,
            confidence=None, size_usdc=None, direction=None,
            reject_reason="divergence_too_large",
        )
        return None

    # Direction logic: count votes past min_gap on each side.
    up_votes = sum(1 for d in deltas.values() if d > min_gap)
    down_votes = sum(1 for d in deltas.values() if d < -min_gap)

    # Fast pass: any exchange exceeds fast_pass AND no exchange contradicts
    # past min_gap. Direction follows the dominant side. Off when disabled.
    if fast_pass_enabled:
        fast_pass_up = any(d >= fast_pass for d in deltas.values())
        fast_pass_down = any(d <= -fast_pass for d in deltas.values())
        fast_pass_triggered = (fast_pass_up and down_votes == 0) or (
            fast_pass_down and up_votes == 0
        )
    else:
        fast_pass_up = fast_pass_down = fast_pass_triggered = False

    if fast_pass_triggered:
        direction = Direction.UP if fast_pass_up else Direction.DOWN
        logger.info("fast_pass_triggered", slug=slot.slug, direction=direction.value,
                    deltas=deltas)
    else:
        direction = detect_divergence(
            prices, slot.price_to_beat,
            min_gap_usd=min_gap, min_agreement=min_agreement,
        )

    if direction is None:
        logger.debug("no_divergence", slug=slot.slug, up_votes=up_votes, down_votes=down_votes)
        _emit(
            div_direction=None, imb_direction=None, confluence=False, fast_pass=False,
            confidence=None, size_usdc=None, direction=None,
            reject_reason="no_divergence",
        )
        return None

    # Capture imbalance snapshot for diagnostics (not a gate)
    imb_direction = detect_smart_entry(history, threshold_buy, threshold_sell, window)
    token_id = slot.up_token_id if direction == Direction.UP else slot.down_token_id
    snapshot = book_ws.get_snapshot(token_id)
    imbalance = calculate_imbalance(snapshot, depth=depth)
    entry_price = book_ws.best_ask(direction) or 0.5

    # mean_abs_delta is needed for both the deep-gap entry-price check and
    # the confidence formula below.
    mean_abs_delta = sum(abs_deltas) / len(abs_deltas)

    # Entry-price gate: tiered floor based on divergence strength.
    # Normal floor (min_entry_price) applies unless every exchange's average
    # delta exceeds deep_gap_usd — in that case the floor drops to
    # deep_gap_min_entry, allowing high-conviction signals to trade at lower
    # market prices where the upside is larger.
    min_entry_price = float(siz_cfg.get("min_entry_price", 0.0))
    max_entry_price = float(siz_cfg.get("max_entry_price", 0.0))
    deep_gap_triggered = (
        deep_gap_usd > 0
        and mean_abs_delta >= deep_gap_usd
        and deep_gap_min_entry > 0
    )
    effective_min_entry = deep_gap_min_entry if deep_gap_triggered else min_entry_price
    if effective_min_entry > 0 and entry_price < effective_min_entry:
        logger.debug("entry_price_below_floor", slug=slot.slug,
                     entry_price=round(entry_price, 4), floor=effective_min_entry,
                     deep_gap_triggered=deep_gap_triggered)
        _emit(
            div_direction=direction.value,
            imb_direction=imb_direction.value if imb_direction else None,
            snapshot_imbalance=round(imbalance, 3),
            confluence=False, fast_pass=fast_pass_triggered,
            confidence=None, size_usdc=None, direction=None,
            best_ask=round(entry_price, 4),
            mean_abs_delta=round(mean_abs_delta, 2),
            deep_gap_triggered=deep_gap_triggered,
            reject_reason="entry_price_too_low",
        )
        return None
    if max_entry_price > 0 and entry_price > max_entry_price:
        logger.debug("entry_price_above_ceiling", slug=slot.slug,
                     entry_price=round(entry_price, 4), ceiling=max_entry_price)
        _emit(
            div_direction=direction.value,
            imb_direction=imb_direction.value if imb_direction else None,
            snapshot_imbalance=round(imbalance, 3),
            confluence=False, fast_pass=fast_pass_triggered,
            confidence=None, size_usdc=None, direction=None,
            best_ask=round(entry_price, 4),
            mean_abs_delta=round(mean_abs_delta, 2),
            deep_gap_triggered=deep_gap_triggered,
            reject_reason="entry_price_too_high",
        )
        return None

    # Confidence — calibrated lookup if enabled and table available, else formula.
    # Formula uses percentage delta (mean_abs_delta / price_to_beat) so it is
    # asset-agnostic: a 0.2% move on BTC and a 0.2% move on ETH both yield the
    # same confidence score, regardless of their dollar sizes.
    # Scaling constant 0.004 (0.4% ≈ full confidence above the 0.60 base):
    #   BTC $150 delta @ $75k  (0.20%) → 0.95  (capped)
    #   ETH $2.0 delta @ $2.25k (0.089%) → 0.82  (passes 0.80 gate)
    #   ETH $1.5 delta @ $2.25k (0.067%) → 0.77  (blocked — matches 77.8% empirical)
    _pct_delta = mean_abs_delta / slot.price_to_beat if slot.price_to_beat else 0.0
    cal_cfg = sig_cfg.get("calibration", {})
    confidence_source = "formula"
    if cal_cfg.get("enabled", False):
        table_path = (
            asset.calibration_table_path
            if (asset is not None and asset.calibration_table_path)
            else cal_cfg.get("table_path", "data/calibration_table.json")
        )
        table = calibration_mod.load_table(table_path)
        if table is not None:
            cal_min_n = int(cal_cfg.get("min_n", 5))
            cal_fallback = float(cal_cfg.get("fallback_confidence", 0.75))
            rate, source = calibration_mod.lookup_win_rate(
                table,
                max_abs_delta=max_abs_delta,
                entry_price=entry_price,
                hour_utc=int(time.gmtime().tm_hour),
                min_n=cal_min_n,
                fallback=cal_fallback,
            )
            confidence = min(0.95, rate)
            confidence_source = f"calibration:{source}"
        else:
            confidence = min(0.95, 0.6 + _pct_delta / 0.004)
            confidence_source = "formula:no_table"
    else:
        confidence = min(0.95, 0.6 + _pct_delta / 0.004)

    min_confidence = _override("min_confidence", float(siz_cfg.get("min_confidence", 0.0)))
    if confidence < min_confidence:
        logger.debug("confidence_below_threshold", slug=slot.slug,
                     confidence=round(confidence, 3), min_confidence=min_confidence)
        _emit(
            div_direction=direction.value,
            imb_direction=imb_direction.value if imb_direction else None,
            snapshot_imbalance=round(imbalance, 3),
            confluence=False, fast_pass=fast_pass_triggered,
            confidence=round(confidence, 3),
            confidence_source=confidence_source,
            size_usdc=None, direction=None,
            reject_reason="low_confidence",
        )
        return None

    base_min_usdc = _override("min_trade_usdc", float(siz_cfg.get("min_trade_usdc", 10.0)))
    base_max_usdc = _override("max_trade_usdc", float(siz_cfg.get("max_trade_usdc", 200.0)))
    double_min_threshold = _override(
        "double_min_above_usd", siz_cfg.get("double_min_above_usd", 200.0)
    )
    large_move = max_abs_delta >= double_min_threshold
    effective_min_usdc = base_min_usdc * 2 if large_move else base_min_usdc

    from polybot.execution.sizing import kelly_size
    size = kelly_size(
        confidence=confidence,
        entry_price=entry_price,
        bankroll=bankroll,
        kelly_fraction=float(siz_cfg.get("kelly_fraction", 0.25)),
        min_usdc=effective_min_usdc,
        max_usdc=base_max_usdc,
    )

    logger.info(
        "signal_fired",
        slug=slot.slug,
        direction=direction.value,
        confidence=round(confidence, 3),
        confidence_source=confidence_source,
        size_usdc=round(size, 2),
        deltas=deltas,
        up_votes=up_votes,
        down_votes=down_votes,
        fast_pass=fast_pass_triggered,
        doubled_min=large_move,
        deep_gap_triggered=deep_gap_triggered,
    )

    _emit(
        div_direction=direction.value,
        imb_direction=imb_direction.value if imb_direction else None,
        snapshot_imbalance=round(imbalance, 3),
        confluence=True, fast_pass=fast_pass_triggered,
        doubled_min=large_move,
        confidence=round(confidence, 3),
        confidence_source=confidence_source,
        size_usdc=round(size, 2),
        direction=direction.value,
        reject_reason=None,
        up_votes=up_votes, down_votes=down_votes,
        mean_abs_delta=round(mean_abs_delta, 2),
        max_abs_delta=round(max_abs_delta, 2),
        best_ask=round(entry_price, 4),
        deep_gap_triggered=deep_gap_triggered,
    )

    return TradeSignal(direction=direction, confidence=round(confidence, 3), size_usdc=size)
