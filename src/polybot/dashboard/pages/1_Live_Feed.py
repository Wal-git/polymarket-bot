"""Live signal evaluation feed — one card per 5-min slot evaluated."""
from __future__ import annotations

import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Live Feed — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    apply_asset_filter,
    inject_styles,
    load_evaluations,
    load_results,
    render_exchange_tiles,
    render_sidebar,
)

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

    try:
        from datetime import datetime, timezone, timedelta
        _PDT = timezone(timedelta(hours=-7))
        ts = datetime.fromisoformat(ev.get("ts") or "").astimezone(_PDT).strftime("%-I:%M %p PDT")
    except Exception:
        ts = (ev.get("ts") or "")[:19].replace("T", " ")
    slug = ev.get("slug", "—")
    asset = ev.get("asset", "BTC")
    ptb = ev.get("price_to_beat") or 0
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
    ratio_str = f"{imb_ratio:.3f}" if imb_ratio is not None else "n/a"

    exchange_tiles = render_exchange_tiles(ev)

    st.markdown(f"""
    <div style="padding:0.75rem 1rem;margin:0.4rem 0 0 0;background:rgba(255,255,255,0.02);
                border-left:3px solid {border};border-radius:4px 4px 0 0;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
            <div>
                <span style="font-family:'Inter',sans-serif;font-size:0.65rem;font-weight:700;
                             color:#F0B90B;background:rgba(240,185,11,0.12);
                             padding:0.1rem 0.4rem;border-radius:3px;letter-spacing:0.08em;
                             margin-right:0.5rem;">{asset}</span>
                <span style="font-family:'Barlow Condensed',sans-serif;font-size:0.95rem;
                             font-weight:700;color:#EAECEF;letter-spacing:0.06em;">{slug}</span>
                <span style="font-family:'Inter',sans-serif;font-size:0.68rem;
                             color:#848E9C;margin-left:0.6rem;">{ts}</span>
            </div>
            <span style="font-family:'Inter',sans-serif;font-size:0.75rem;font-weight:700;
                         color:{badge_color};background:rgba(0,0,0,0.25);
                         padding:0.2rem 0.5rem;border-radius:3px;">{badge_text}</span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:0.4rem;margin-bottom:0.5rem;">
            {exchange_tiles}
            <div style="background:rgba(0,0,0,0.15);padding:0.5rem;border-radius:3px;">
                <div style="font-size:0.65rem;color:#848E9C;text-transform:uppercase;letter-spacing:0.08em;">P-T-B</div>
                <div style="font-family:'Barlow Condensed',sans-serif;font-size:1rem;color:#F0B90B;">${ptb:,.2f}</div>
            </div>
        </div>
        <div style="display:flex;gap:1.25rem;font-family:'Inter',sans-serif;font-size:0.78rem;">
            <span style="color:{div_color};">{div_icon} Divergence: <strong>{div_dir or 'none'}</strong></span>
            <span style="color:{imb_color};">{imb_icon} Imbalance: <strong>{ratio_str}</strong> → {imb_dir or 'none'}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if is_conf and confidence is not None:
        dir_color = "#0ECB81" if direction == "UP" else "#F6465D"
        outcome_parts = [
            f'<span style="color:{dir_color};font-weight:700;">▲ {direction}</span>',
            f'<span style="color:#848E9C;">·</span> <span style="color:#F0B90B;font-weight:600;">${size_usdc:.2f} USDC</span>',
            f'<span style="color:#848E9C;">·</span> <span style="color:#EAECEF;">confidence {confidence:.1%}</span>',
        ]
        if result is not None:
            won = result.get("won", False)
            pnl = result.get("pnl", 0)
            outcome_color = "#0ECB81" if won else "#F6465D"
            outcome_label = "WIN" if won else "LOSS"
            pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
            outcome_parts.append(
                f'<span style="color:#848E9C;">·</span> '
                f'<span style="color:{outcome_color};font-weight:700;">{outcome_label} {pnl_str}</span>'
            )
        row = " &nbsp; ".join(outcome_parts)
        st.markdown(
            f'<div style="padding:0.4rem 1rem 0.75rem 1rem;margin:0 0 0.4rem 0;'
            f'background:rgba(255,255,255,0.02);border-left:3px solid {border};'
            f'border-radius:0 0 4px 4px;font-family:\'Inter\',sans-serif;font-size:0.82rem;">'
            f'{row}</div>',
            unsafe_allow_html=True,
        )


col1, col2 = st.columns([3, 1])
with col1:
    filter_opt = st.selectbox("Show", ["all", "trades only", "skipped only"], index=0, label_visibility="collapsed")
with col2:
    last_n = st.number_input("Last N", min_value=10, max_value=500, value=50, step=10, label_visibility="collapsed")

evals = apply_asset_filter(load_evaluations(last_n=int(last_n)))

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
