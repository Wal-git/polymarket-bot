"""Reusable render helpers for the Polybot dashboard.

These replace the hand-written ``st.markdown(unsafe_allow_html=True)`` blocks
that were copy-pasted across pages: KPI tiles, the per-exchange price tiles,
the slot-evaluation card, and the LLM signal card. All color comes from
``PALETTE``; all P&L formatting from ``format``.
"""
from __future__ import annotations

import streamlit as st

from polybot.dashboard.format import pnl_str, to_pdt
from polybot.dashboard.theme import PALETTE

# Order matches the 5 exchanges in polybot.feeds.spot_price._FETCHERS.
EXCHANGE_NAMES: tuple[str, ...] = ("binance", "coinbase", "kraken", "bitstamp", "okx")
EXCHANGE_LABELS: dict[str, str] = {
    "binance": "Binance",
    "coinbase": "Coinbase",
    "kraken": "Kraken",
    "bitstamp": "Bitstamp",
    "okx": "OKX",
}


def kpi_html(label: str, value: str, *, color: str | None = None,
             value_class: str = "", sub: str | None = None,
             sub_color: str | None = None) -> str:
    """Return the HTML for one ``.kpi-block``. ``value_class`` picks a CSS color
    class (``positive``/``negative``/``amber``); ``color`` overrides with an
    inline color. ``sub`` is an optional small line under the value.
    """
    style = f' style="color:{color};"' if color else ""
    cls = f" {value_class}" if value_class else ""
    sub_html = ""
    if sub is not None:
        sc = f"color:{sub_color};" if sub_color else ""
        sub_html = f'<div class="kpi-sub" style="{sc}">{sub}</div>'
    return (
        f'<div class="kpi-block">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value{cls}"{style}>{value}</div>'
        f'{sub_html}</div>'
    )


def render_kpi(label: str, value: str, **kwargs) -> None:
    """Render a single KPI block (see ``kpi_html`` for kwargs)."""
    st.markdown(kpi_html(label, value, **kwargs), unsafe_allow_html=True)


def kpi_row(items: list[dict]) -> None:
    """Render a row of KPI blocks across equal columns. Each item is a dict of
    ``kpi_html`` kwargs (``label`` and ``value`` required).
    """
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        with col:
            st.markdown(kpi_html(**item), unsafe_allow_html=True)


def render_exchange_tiles(ev: dict) -> str:
    """Return HTML for one tile per exchange in EXCHANGE_NAMES order.

    When an exchange returned None for a particular evaluation, the tile is
    dimmed with an em-dash placeholder rather than omitted, so column
    positions stay stable between cards.
    """
    parts = []
    for name in EXCHANGE_NAMES:
        price = ev.get(name)
        label = EXCHANGE_LABELS[name]
        if price is None:
            parts.append(
                f'<div class="tile" style="opacity:0.4;">'
                f'<div class="tile-label">{label}</div>'
                f'<div class="tile-value" style="color:{PALETTE.GREY};">—</div>'
                f'<div style="font-size:0.7rem;color:{PALETTE.GREY};">—</div>'
                f'</div>'
            )
            continue
        d = float(ev.get(f"{name}_delta") or 0)
        delta_color = PALETTE.GREEN if d > 0 else (PALETTE.RED if d < 0 else PALETTE.GREY)
        parts.append(
            f'<div class="tile">'
            f'<div class="tile-label">{label}</div>'
            f'<div class="tile-value" style="color:{PALETTE.WHITE};">${float(price):,.2f}</div>'
            f'<div style="font-size:0.7rem;color:{delta_color};">{d:+.2f}</div>'
            f'</div>'
        )
    return "".join(parts)


def _ptb_tile(ptb: float) -> str:
    return (
        f'<div class="tile">'
        f'<div class="tile-label">P-T-B</div>'
        f'<div class="tile-value" style="color:{PALETTE.AMBER};">${ptb:,.2f}</div>'
        f'</div>'
    )


def render_eval_card(ev: dict, result: dict | None = None) -> None:
    """Render one slot evaluation as a card, with an outcome row when the slot
    fired a trade. Used by both the home page and the live feed.
    """
    is_conf = ev.get("confluence", False)
    reject = ev.get("reject_reason") or ""
    accent = PALETTE.GREEN if is_conf else (PALETTE.AMBER if reject == "direction_mismatch" else PALETTE.RED)
    accent_cls = "green" if is_conf else ("amber" if reject == "direction_mismatch" else "red")
    badge_text = "TRADE" if is_conf else reject.upper().replace("_", " ")

    ts = to_pdt(ev.get("ts") or "", "clock")
    slug = ev.get("slug", "—")
    asset = ev.get("asset", "BTC")
    ptb = ev.get("price_to_beat") or 0
    div_dir = ev.get("div_direction")
    div_icon = "✓" if div_dir else "✗"
    div_color = PALETTE.GREEN if div_dir else PALETTE.RED

    st.markdown(f"""
    <div class="card {accent_cls}" style="margin-bottom:0;border-radius:4px 4px 0 0;">
        <div class="card-head">
            <div>
                <span style="font-family:'Inter',sans-serif;font-size:0.65rem;font-weight:700;
                             color:{PALETTE.AMBER};background:rgba(240,185,11,0.12);
                             padding:0.1rem 0.4rem;border-radius:3px;letter-spacing:0.08em;
                             margin-right:0.5rem;">{asset}</span>
                <span style="font-family:'Barlow Condensed',sans-serif;font-size:0.95rem;
                             font-weight:700;color:{PALETTE.WHITE};letter-spacing:0.06em;">{slug}</span>
                <span style="font-family:'Inter',sans-serif;font-size:0.68rem;
                             color:{PALETTE.GREY};margin-left:0.6rem;">{ts}</span>
            </div>
            <span class="card-badge" style="color:{accent};">{badge_text}</span>
        </div>
        <div class="tile-grid">
            {render_exchange_tiles(ev)}
            {_ptb_tile(float(ptb))}
        </div>
        <div style="display:flex;gap:1.25rem;font-family:'Inter',sans-serif;font-size:0.78rem;">
            <span style="color:{div_color};">{div_icon} Divergence: <strong>{div_dir or 'none'}</strong></span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    confidence = ev.get("confidence")
    if is_conf and confidence is not None:
        direction = ev.get("direction")
        size_usdc = ev.get("size_usdc")
        dir_color = PALETTE.GREEN if direction == "UP" else PALETTE.RED
        parts = [
            f'<span style="color:{dir_color};font-weight:700;">▲ {direction}</span>',
            f'<span style="color:{PALETTE.GREY};">·</span> <span style="color:{PALETTE.AMBER};font-weight:600;">${size_usdc:.2f} USDC</span>',
            f'<span style="color:{PALETTE.GREY};">·</span> <span style="color:{PALETTE.WHITE};">confidence {confidence:.1%}</span>',
        ]
        if result is not None:
            won = result.get("won", False)
            pnl = result.get("pnl", 0)
            oc = PALETTE.GREEN if won else PALETTE.RED
            parts.append(
                f'<span style="color:{PALETTE.GREY};">·</span> '
                f'<span style="color:{oc};font-weight:700;">{"WIN" if won else "LOSS"} {pnl_str(pnl)}</span>'
            )
        st.markdown(
            f'<div class="card {accent_cls}" style="margin-top:0;border-radius:0 0 4px 4px;'
            f'font-family:\'Inter\',sans-serif;font-size:0.82rem;">{" &nbsp; ".join(parts)}</div>',
            unsafe_allow_html=True,
        )


def render_signal_card(sig: dict) -> None:
    """Render a single LLM signal as a styled card."""
    approved = sig.get("approved", False)
    klass = "approved" if approved else "rejected"
    badge_color = PALETTE.GREEN if approved else PALETTE.RED
    badge_text = "APPROVED" if approved else "REJECTED"
    confidence = float(sig.get("confidence") or 0)
    conf_pct = int(confidence * 100)
    question = sig.get("market_question") or "(unknown market)"
    rationale = sig.get("rationale") or ""
    strategy = sig.get("strategy") or ""
    ts = (sig.get("ts") or "")[:19].replace("T", " ")

    orders_html = ""
    for o in sig.get("orders", []):
        side = o.get("side", "")
        size = o.get("size", "")
        price = o.get("limit_price") or "—"
        side_color = PALETTE.GREEN if side == "BUY" else PALETTE.RED
        orders_html += (
            f'<span style="color:{side_color};font-weight:600;">{side}</span> '
            f'{size} @ ${price} &nbsp;'
        )

    reject_reason = sig.get("reject_reason") or ""
    reject_line = (
        f'<div class="signal-rationale" style="color:{PALETTE.RED};font-style:normal;">⊗ {reject_reason}</div>'
        if reject_reason else ""
    )

    st.markdown(f"""
    <div class="signal-card {klass}">
        <div class="signal-meta">
            <span style="color:{badge_color};font-weight:700;">{badge_text}</span>
            &nbsp;·&nbsp; {strategy} &nbsp;·&nbsp; {ts}
            <span class="confidence-bar"><span class="confidence-fill" style="width:{conf_pct}%;"></span></span>
            <span style="color:{PALETTE.AMBER};font-weight:600;">{conf_pct}%</span>
        </div>
        <div class="signal-question">{question}</div>
        <div class="signal-rationale">{rationale}</div>
        <div class="signal-orders">{orders_html}</div>
        {reject_line}
    </div>
    """, unsafe_allow_html=True)
