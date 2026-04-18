"""Open positions, recent fills, and P&L breakdown."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Positions — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    inject_styles,
    load_state,
    render_sidebar,
)

inject_styles()
st_autorefresh(interval=10_000, key="positions_refresh")
render_sidebar()

st.markdown('<div class="page-header">◇ POSITIONS</div>', unsafe_allow_html=True)

state = load_state()
positions = state.get("positions", [])
trades = state.get("trades", [])

if not positions:
    st.info("No open positions.")
else:
    rows = []
    total_cost = 0.0
    total_unrealized = 0.0
    for p in positions:
        shares = float(p.get("shares") or 0)
        entry = float(p.get("avg_entry_price") or 0)
        current = float(p.get("current_price") or 0)
        unrealized = float(p.get("unrealized_pnl") or 0)
        cost_basis = shares * entry
        total_cost += cost_basis
        total_unrealized += unrealized
        rows.append({
            "Market": (p.get("market_question") or "")[:60],
            "Outcome": p.get("outcome_label") or "",
            "Shares": f"{shares:.4f}",
            "Avg Entry": f"${entry:.4f}",
            "Current": f"${current:.4f}" if current else "—",
            "Cost Basis": f"${cost_basis:,.2f}",
            "Unrealized P&L": f"${unrealized:+,.2f}",
        })

    col1, col2 = st.columns(2)
    pnl_class = "positive" if total_unrealized >= 0 else "negative"
    pnl_str = f"+${total_unrealized:,.2f}" if total_unrealized >= 0 else f"-${abs(total_unrealized):,.2f}"
    with col1:
        st.markdown(f"""
        <div class="kpi-block">
            <div class="kpi-label">Total Cost Basis</div>
            <div class="kpi-value">${total_cost:,.2f}</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="kpi-block">
            <div class="kpi-label">Unrealized P&L</div>
            <div class="kpi-value {pnl_class}">{pnl_str}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">RECENT TRADES</div>', unsafe_allow_html=True)

if not trades:
    st.info("No trades recorded yet.")
else:
    rows = []
    for t in reversed(trades[-50:]):
        rows.append({
            "Time": (t.get("timestamp") or "")[:19].replace("T", " "),
            "Side": t.get("side") or "",
            "Size": float(t.get("size") or 0),
            "Price": f"${float(t.get('price') or 0):.4f}",
            "Market": (t.get("market_question") or "")[:60],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
