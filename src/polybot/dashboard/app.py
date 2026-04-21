"""Polybot Dashboard — main entry point.

Run via: ``polybot dashboard``
Or directly: ``streamlit run src/polybot/dashboard/app.py``
"""
from __future__ import annotations

import streamlit as st
# from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="POLYBOT",
    page_icon="◇",
    layout="wide",
    initial_sidebar_state="expanded",
)

from polybot.dashboard.data_loader import inject_styles  # noqa: E402

inject_styles()

# st_autorefresh(interval=10_000, key="polybot_home_refresh")
# Auto-refresh disabled: use browser refresh (F5) to see live updates

from polybot.dashboard.data_loader import (  # noqa: E402
    latest_cycle,
    load_signals,
    load_state,
    render_sidebar,
    render_signal_card,
)

render_sidebar()

state = load_state()
cycle = latest_cycle()
recent_signals = load_signals(last_n=20)
positions = state.get("positions", [])

st.markdown('<div class="page-header">◇ POLYBOT DASHBOARD</div>', unsafe_allow_html=True)


def _pnl_class(val: float) -> str:
    return "positive" if val >= 0 else "negative"


def _fmt_pnl(val: float) -> str:
    return f"+${val:,.2f}" if val >= 0 else f"-${abs(val):,.2f}"


realized = sum(float(p.get("realized_pnl") or 0) for p in positions)
unrealized = sum(float(p.get("unrealized_pnl") or 0) for p in positions)
total_pnl = realized + unrealized
balance = float(cycle.get("balance") or 0)
markets_scanned = cycle.get("markets_scanned", 0)
signals_generated = cycle.get("signals_generated", 0)
signals_approved = cycle.get("signals_approved", 0)
signals_rejected = max(signals_generated - signals_approved, 0)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Total P&L</div>
        <div class="kpi-value {_pnl_class(total_pnl)}">{_fmt_pnl(total_pnl)}</div>
    </div>""", unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Open Positions</div>
        <div class="kpi-value">{len(positions)}</div>
    </div>""", unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Balance (USDC.e)</div>
        <div class="kpi-value amber">${balance:,.2f}</div>
    </div>""", unsafe_allow_html=True)
with col4:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Markets Scanned</div>
        <div class="kpi-value">{markets_scanned}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Signals — Last Cycle</div>
        <div class="kpi-value">{signals_generated}</div>
    </div>""", unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Approved by Risk</div>
        <div class="kpi-value positive">{signals_approved}</div>
    </div>""", unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Rejected by Risk</div>
        <div class="kpi-value negative">{signals_rejected}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">RECENT SIGNALS</div>', unsafe_allow_html=True)

if not recent_signals:
    st.info("No signals captured yet — once the bot completes a cycle with strategy hits, signals appear here.")
else:
    for sig in recent_signals[:5]:
        render_signal_card(sig)

if not state and not cycle:
    st.warning("No bot state yet — start the bot with `polybot run` (or `polybot dry-run` for one-shot test).")
