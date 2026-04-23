"""Live signal evaluation feed — one card per 5-min slot evaluated."""
from __future__ import annotations

import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Live Feed — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import inject_styles, load_evaluations, load_results, render_sidebar  # noqa: E402

inject_styles()
st_autorefresh(interval=5_000, key="live_feed_refresh")
render_sidebar()

st.markdown('<div class="page-header">◇ LIVE SIGNAL FEED</div>', unsafe_allow_html=True)


def _render_eval_card(ev: dict, result: dict | None = None) -> None:
    is_conf = ev.get("confluence", False)
    reject = ev.get("reject_reason") or ""
    border = "#0ECB81" if is_conf else ("#F0B90B" if reject == "direction_mismatch" else "#F6465D")
    badge_color = "#0ECB81" if is_conf else ("#F0B90B" if reject == "direction_mismatch" else "#F6465D")
    badge_text = "TRADE" if is_conf else reject.upper().replace("_", " ")

    ts = (ev.get("ts") or "")[:19].replace("T", " ")
    slug = ev.get("slug", "—")
    ptb = ev.get("price_to_beat") or 0
    binance = ev.get("binance") or 0
    coinbase = ev.get("coinbase") or 0
    b_delta = ev.get("binance_delta") or 0
    c_delta = ev.get("coinbase_delta") or 0
    div_dir = ev.get("div_direction")
    imb_dir = ev.get("imb_direction")
    imb_ratio = ev.get("imbalance_ratio")
    confidence = ev.get("confidence")
    size_usdc = ev.get("size_usdc")
    direction = ev.get("direction")

    div_icon = "✓" if div_dir else "✗"
    div_color = "#0ECB81" if div_dir else "#F6465D"
    imb_icon = "✓" if imb_dir else "✗"
    imb_color = "#0ECB81" if imb_dir else "#F6465D"
    b_delta_color = "#0ECB81" if b_delta > 0 else "#F6465D"
    c_delta_color = "#0ECB81" if c_delta > 0 else "#F6465D"
    ratio_str = f"{imb_ratio:.3f}" if imb_ratio is not None else "n/a"

    trade_line = ""
    if is_conf and confidence is not None:
        dir_color = "#0ECB81" if direction == "UP" else "#F6465D"
        outcome_html = ""
        if result is not None:
            won = result.get("won", False)
            pnl = result.get("pnl", 0)
            outcome_color = "#0ECB81" if won else "#F6465D"
            outcome_label = "WIN" if won else "LOSS"
            pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
            outcome_html = (
                f'&nbsp;·&nbsp; <span style="color:{outcome_color};font-weight:700;">'
                f'{outcome_label} {pnl_str}</span>'
            )
        trade_line = f"""
        <div style="margin-top:0.5rem;padding:0.4rem 0.6rem;background:rgba(14,203,129,0.06);border-radius:3px;font-family:'Inter',sans-serif;font-size:0.82rem;">
            <span style="color:{dir_color};font-weight:700;">▲ {direction}</span>
            &nbsp;·&nbsp; <span style="color:#F0B90B;font-weight:600;">${size_usdc:.2f}</span> USDC
            &nbsp;·&nbsp; confidence <span style="color:#0ECB81;font-weight:600;">{confidence:.1%}</span>
            {outcome_html}
        </div>"""

    st.markdown(f"""
    <div style="padding:0.75rem 1rem;margin:0.4rem 0;background:rgba(255,255,255,0.02);
                border-left:3px solid {border};border-radius:4px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
            <div>
                <span style="font-family:'Barlow Condensed',sans-serif;font-size:0.95rem;
                             font-weight:700;color:#EAECEF;letter-spacing:0.06em;">{slug}</span>
                <span style="font-family:'Inter',sans-serif;font-size:0.68rem;
                             color:#848E9C;margin-left:0.6rem;">{ts}</span>
            </div>
            <span style="font-family:'Inter',sans-serif;font-size:0.75rem;font-weight:700;
                         color:{badge_color};background:rgba(0,0,0,0.25);
                         padding:0.2rem 0.5rem;border-radius:3px;">{badge_text}</span>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.4rem;margin-bottom:0.5rem;">
            <div style="font-family:'Inter',sans-serif;font-size:0.78rem;">
                <span style="color:#848E9C;">Binance</span>&nbsp;
                <span style="color:#EAECEF;font-variant-numeric:tabular-nums;">${binance:,.2f}</span>&nbsp;
                <span style="color:{b_delta_color};font-size:0.72rem;">{b_delta:+.2f}</span>
            </div>
            <div style="font-family:'Inter',sans-serif;font-size:0.78rem;">
                <span style="color:#848E9C;">Coinbase</span>&nbsp;
                <span style="color:#EAECEF;font-variant-numeric:tabular-nums;">${coinbase:,.2f}</span>&nbsp;
                <span style="color:{c_delta_color};font-size:0.72rem;">{c_delta:+.2f}</span>
            </div>
            <div style="font-family:'Inter',sans-serif;font-size:0.78rem;">
                <span style="color:#848E9C;">P-T-B</span>&nbsp;
                <span style="color:#F0B90B;font-variant-numeric:tabular-nums;">${ptb:,.2f}</span>
            </div>
        </div>
        <div style="display:flex;gap:1.25rem;font-family:'Inter',sans-serif;font-size:0.78rem;">
            <span style="color:{div_color};">{div_icon} Divergence: <strong>{div_dir or 'none'}</strong></span>
            <span style="color:{imb_color};">{imb_icon} Imbalance: <strong>{ratio_str}</strong> → {imb_dir or 'none'}</span>
        </div>
        {trade_line}
    </div>
    """, unsafe_allow_html=True)


col1, col2 = st.columns([3, 1])
with col1:
    filter_opt = st.selectbox("Show", ["all", "trades only", "skipped only"], index=0, label_visibility="collapsed")
with col2:
    last_n = st.number_input("Last N", min_value=10, max_value=500, value=50, step=10, label_visibility="collapsed")

evals = load_evaluations(last_n=int(last_n))

if filter_opt == "trades only":
    evals = [e for e in evals if e.get("confluence")]
elif filter_opt == "skipped only":
    evals = [e for e in evals if not e.get("confluence")]

st.caption(f"{len(evals)} evaluation(s) · auto-refreshes every 5s")

if not evals:
    st.info("No evaluations yet — bot will log signal decisions here as each 5-min slot is evaluated.")
else:
    results = load_results()
    for ev in evals:
        slug = ev.get("slug", "")
        _render_eval_card(ev, result=results.get(slug))
