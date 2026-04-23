"""Polybot Dashboard — BTC 5-min engine home."""
from __future__ import annotations

import time

import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="POLYBOT",
    page_icon="◇",
    layout="wide",
    initial_sidebar_state="expanded",
)

from polybot.dashboard.data_loader import inject_styles  # noqa: E402

inject_styles()

st_autorefresh(interval=10_000, key="polybot_home_refresh")

from polybot.dashboard.data_loader import (  # noqa: E402
    latest_evaluation,
    load_config,
    load_evaluations,
    load_state,
    render_sidebar,
)

render_sidebar()

cfg = load_config()
state = load_state()
evals = load_evaluations(last_n=200)
last_eval = latest_evaluation()

positions = state.get("positions", [])
trades = state.get("trades", [])

realized = sum(float(p.get("realized_pnl") or 0) for p in positions)
unrealized = sum(float(p.get("unrealized_pnl") or 0) for p in positions)
total_pnl = realized + unrealized

dry_run = cfg.get("bot", {}).get("dry_run", True)
mode_str = "DRY RUN" if dry_run else "LIVE"

st.markdown('<div class="page-header">◇ POLYBOT — BTC 5-MIN ENGINE</div>', unsafe_allow_html=True)

# ── KPI row ──────────────────────────────────────────────────────────────────
def _pnl_class(v: float) -> str:
    return "positive" if v >= 0 else "negative"

def _fmt_pnl(v: float) -> str:
    return f"+${v:,.2f}" if v >= 0 else f"-${abs(v):,.2f}"

col1, col2, col3, col4, col5 = st.columns(5)

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
        <div class="kpi-label">Total Trades</div>
        <div class="kpi-value">{len(trades)}</div>
    </div>""", unsafe_allow_html=True)

# Confluence rate from evaluations
total_evals = len(evals)
confluences = sum(1 for e in evals if e.get("confluence"))
conf_rate = f"{100 * confluences / total_evals:.1f}%" if total_evals else "—"

with col4:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Slots Evaluated</div>
        <div class="kpi-value">{total_evals}</div>
    </div>""", unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Signal Rate</div>
        <div class="kpi-value amber">{conf_rate}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Rejection breakdown ───────────────────────────────────────────────────────
if evals:
    reasons = {}
    for e in evals:
        r = e.get("reject_reason") or ("confluence" if e.get("confluence") else "unknown")
        reasons[r] = reasons.get(r, 0) + 1

    st.markdown('<div class="page-header">SIGNAL FILTER BREAKDOWN</div>', unsafe_allow_html=True)
    cols = st.columns(len(reasons))
    label_map = {
        "confluence": ("TRADE FIRED", "#0ECB81"),
        "no_divergence": ("NO DIVERGENCE", "#F6465D"),
        "no_imbalance": ("NO IMBALANCE", "#F6465D"),
        "direction_mismatch": ("DIRECTION MISMATCH", "#F0B90B"),
    }
    for i, (reason, count) in enumerate(sorted(reasons.items())):
        label, color = label_map.get(reason, (reason.upper(), "#848E9C"))
        pct = 100 * count / total_evals
        with cols[i]:
            st.markdown(f"""
            <div class="kpi-block">
                <div class="kpi-label" style="color:{color};">{label}</div>
                <div class="kpi-value" style="color:{color};">{count}</div>
                <div style="font-size:0.75rem;color:#848E9C;">{pct:.1f}%</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

# ── Last evaluation ───────────────────────────────────────────────────────────
st.markdown('<div class="page-header">LAST SLOT EVALUATION</div>', unsafe_allow_html=True)

if not last_eval:
    st.info("No slot evaluations yet — bot is running and will appear here once a 5-min slot opens.")
else:
    is_confluence = last_eval.get("confluence", False)
    reject = last_eval.get("reject_reason", "")
    border = "#0ECB81" if is_confluence else ("#F0B90B" if reject == "direction_mismatch" else "#F6465D")
    result_label = "TRADE FIRED" if is_confluence else (reject or "SKIPPED").upper().replace("_", " ")
    result_color = "#0ECB81" if is_confluence else ("#F0B90B" if reject == "direction_mismatch" else "#F6465D")

    ts = (last_eval.get("ts") or "")[:19].replace("T", " ")
    slug = last_eval.get("slug", "")
    ptb = last_eval.get("price_to_beat", 0)
    binance = last_eval.get("binance", 0)
    coinbase = last_eval.get("coinbase", 0)
    b_delta = last_eval.get("binance_delta", 0)
    c_delta = last_eval.get("coinbase_delta", 0)
    div_dir = last_eval.get("div_direction")
    imb_dir = last_eval.get("imb_direction")
    imb_ratio = last_eval.get("imbalance_ratio")
    confidence = last_eval.get("confidence")
    size_usdc = last_eval.get("size_usdc")
    direction = last_eval.get("direction")

    div_icon = "✓" if div_dir else "✗"
    div_color = "#0ECB81" if div_dir else "#F6465D"
    imb_icon = "✓" if imb_dir else "✗"
    imb_color = "#0ECB81" if imb_dir else "#F6465D"
    conf_html = f'<span style="color:#0ECB81;font-weight:700;">{confidence:.1%}</span> confidence · <span style="color:#F0B90B;">${size_usdc:.2f}</span> USDC · <span style="color:#0ECB81;">{direction}</span>' if is_confluence else ""

    st.markdown(f"""
    <div style="padding:1rem 1.25rem;background:rgba(255,255,255,0.02);border-left:3px solid {border};border-radius:4px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.75rem;">
            <span style="font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;color:#848E9C;letter-spacing:0.1em;">{slug}</span>
            <span style="font-family:'Inter',sans-serif;font-size:0.8rem;font-weight:700;color:{result_color};background:rgba(0,0,0,0.2);padding:0.25rem 0.6rem;border-radius:3px;">{result_label}</span>
        </div>
        <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:#848E9C;margin-bottom:0.6rem;">{ts}</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:0.5rem;margin-bottom:0.75rem;">
            <div style="background:rgba(0,0,0,0.15);padding:0.5rem;border-radius:3px;">
                <div style="font-size:0.65rem;color:#848E9C;text-transform:uppercase;letter-spacing:0.08em;">Binance</div>
                <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;color:#EAECEF;">${binance:,.2f}</div>
                <div style="font-size:0.75rem;color:{'#0ECB81' if b_delta > 0 else '#F6465D'};">{b_delta:+.2f}</div>
            </div>
            <div style="background:rgba(0,0,0,0.15);padding:0.5rem;border-radius:3px;">
                <div style="font-size:0.65rem;color:#848E9C;text-transform:uppercase;letter-spacing:0.08em;">Coinbase</div>
                <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;color:#EAECEF;">${coinbase:,.2f}</div>
                <div style="font-size:0.75rem;color:{'#0ECB81' if c_delta > 0 else '#F6465D'};">{c_delta:+.2f}</div>
            </div>
            <div style="background:rgba(0,0,0,0.15);padding:0.5rem;border-radius:3px;">
                <div style="font-size:0.65rem;color:#848E9C;text-transform:uppercase;letter-spacing:0.08em;">Price to Beat</div>
                <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;color:#F0B90B;">${ptb:,.2f}</div>
            </div>
        </div>
        <div style="display:flex;gap:1rem;font-family:'Inter',sans-serif;font-size:0.82rem;">
            <span style="color:{div_color};">{div_icon} Divergence: {div_dir or 'none'}</span>
            <span style="color:{imb_color};">{imb_icon} Imbalance: {f'{imb_ratio:.3f}' if imb_ratio is not None else 'n/a'} ({imb_dir or 'none'})</span>
            {'<span style="color:#0ECB81;">→ ' + conf_html + '</span>' if is_confluence else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)

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
        rows.append({
            "Market": (p.get("market_question") or "")[:50],
            "Direction": p.get("outcome_label") or "",
            "Shares": f"{shares:.4f}",
            "Entry": f"${entry:.4f}",
            "Unrealized P&L": f"${unrealized:+,.2f}",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
