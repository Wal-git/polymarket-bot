"""Polybot Dashboard — BTC 5-min engine home."""
from __future__ import annotations

import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="POLYBOT",
    page_icon="◇",
    layout="wide",
    initial_sidebar_state="expanded",
)

from polybot.dashboard.theme import inject_styles  # noqa: E402

inject_styles()

st_autorefresh(interval=10_000, key="polybot_home_refresh")

from polybot.dashboard.components import kpi_html, render_eval_card  # noqa: E402
from polybot.dashboard.format import fmt_pct, pnl_color, pnl_str  # noqa: E402
from polybot.dashboard.theme import PALETTE  # noqa: E402
from polybot.dashboard.loaders import (  # noqa: E402
    STARTING_BALANCE,
    latest_evaluation,
    load_balance,
    load_config,
    load_evaluations,
    load_state,
)
from polybot.dashboard.sidebar import render_sidebar  # noqa: E402

render_sidebar()

cfg = load_config()
state = load_state()
evals = load_evaluations(last_n=200)
last_eval = latest_evaluation()
bal = load_balance()

positions = state.get("positions", [])
trades = state.get("trades", [])

bal_f = float(bal.get("balance", 0)) if bal else 0.0
total_value_f = float(bal.get("total_value", bal_f)) if bal else 0.0
total_pnl = (total_value_f - STARTING_BALANCE) if (bal and total_value_f > 0) else 0.0

st.markdown('<div class="page-header">◇ POLYBOT — BTC 5-MIN ENGINE</div>', unsafe_allow_html=True)

# ── KPI row ──────────────────────────────────────────────────────────────────
total_evals = len(evals)
confluences = sum(1 for e in evals if e.get("confluence"))
conf_rate = f"{100 * confluences / total_evals:.1f}%" if total_evals else "—"
pnl_pct = total_pnl / STARTING_BALANCE * 100

cols = st.columns(6)
kpis = [
    {"label": "Total P&L", "value": pnl_str(total_pnl),
     "color": pnl_color(total_pnl), "sub": fmt_pct(pnl_pct), "sub_color": pnl_color(total_pnl)},
    {"label": "Open Positions", "value": str(len(positions))},
    {"label": "Total Trades", "value": str(len(trades))},
    {"label": "Slots Evaluated", "value": str(total_evals)},
    {"label": "Signal Rate", "value": conf_rate, "value_class": "amber"},
    {"label": "Wallet Balance (USDC)", "value": f"${bal_f:,.2f}" if bal else "—", "value_class": "amber"},
]
for col, kpi in zip(cols, kpis):
    with col:
        st.markdown(kpi_html(**kpi), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Rejection breakdown ───────────────────────────────────────────────────────
if evals:
    reasons: dict[str, int] = {}
    for e in evals:
        r = e.get("reject_reason") or ("confluence" if e.get("confluence") else "unknown")
        reasons[r] = reasons.get(r, 0) + 1

    st.markdown('<div class="page-header">SIGNAL FILTER BREAKDOWN</div>', unsafe_allow_html=True)
    label_map = {
        "confluence": ("TRADE FIRED", PALETTE.GREEN),
        "no_divergence": ("NO DIVERGENCE", PALETTE.RED),
        "direction_mismatch": ("DIRECTION MISMATCH", PALETTE.AMBER),
    }
    items = sorted(reasons.items())
    cols = st.columns(len(items))
    for col, (reason, count) in zip(cols, items):
        label, color = label_map.get(reason, (reason.upper(), PALETTE.GREY))
        pct = 100 * count / total_evals
        with col:
            st.markdown(
                kpi_html(label, str(count), color=color, sub=f"{pct:.1f}%"),
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

# ── Last evaluation ───────────────────────────────────────────────────────────
st.markdown('<div class="page-header">LAST SLOT EVALUATION</div>', unsafe_allow_html=True)

if not last_eval:
    st.info("No slot evaluations yet — bot is running and will appear here once a 5-min slot opens.")
else:
    render_eval_card(last_eval)

# ── Open positions ────────────────────────────────────────────────────────────
if positions:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="page-header">OPEN POSITIONS</div>', unsafe_allow_html=True)
    import pandas as pd
    rows = []
    for p in positions:
        shares = float(p.get("shares") or 0)
        entry = float(p.get("avg_entry_price") or 0)
        unrealized = float(p.get("unrealized_pnl") or 0)
        conf = p.get("confidence")
        rows.append({
            "Market": (p.get("market_question") or "")[:50],
            "Direction": p.get("outcome_label") or "",
            "Shares": f"{shares:.4f}",
            "Entry": f"${entry:.4f}",
            "Unrealized P&L": f"${unrealized:+,.2f}",
            "Confidence": f"{conf:.1%}" if conf is not None else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
