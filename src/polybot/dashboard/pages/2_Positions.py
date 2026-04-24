"""Trade history with outcomes, timestamps in Pacific time."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Positions — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    inject_styles,
    load_results,
    load_state,
    render_sidebar,
)

inject_styles()
st_autorefresh(interval=10_000, key="positions_refresh")
render_sidebar()

st.markdown('<div class="page-header">◇ TRADE HISTORY</div>', unsafe_allow_html=True)

PDT = timezone(timedelta(hours=-7))

def _to_pdt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso).astimezone(PDT)
        return dt.strftime("%-I:%M %p")
    except Exception:
        return iso[:19]

state = load_state()
trades = state.get("trades", [])
results = load_results()

if not trades:
    st.info("No trades recorded yet.")
else:
    rows = []
    total_pnl = 0.0
    wins = 0
    losses = 0

    for t in reversed(trades):
        slug = t.get("market_question", "")
        result = results.get(slug)
        won = result.get("won") if result else None
        pnl = result.get("pnl") if result else None

        if pnl is not None:
            total_pnl += pnl
            if won:
                wins += 1
            else:
                losses += 1

        if won is True:
            outcome = "✅ WIN"
        elif won is False:
            outcome = "❌ LOSS"
        else:
            outcome = "⏳ Pending"

        pnl_str = f"+${pnl:.2f}" if pnl is not None and pnl >= 0 else (f"-${abs(pnl):.2f}" if pnl is not None else "—")

        confidence = result.get("confidence") if result else None

        rows.append({
            "Time (PDT)": _to_pdt(t.get("timestamp", "")),
            "Market": slug.replace("btc-updown-5m-", ""),
            "Direction": t.get("side", ""),
            "Shares": f"{float(t.get('size') or 0):.2f}",
            "Entry": f"${float(t.get('price') or 0):.2f}",
            "Stake": f"${float(t.get('size') or 0) * float(t.get('price') or 0):.2f}",
            "Confidence": f"{confidence:.1%}" if confidence is not None else "—",
            "Outcome": outcome,
            "P&L": pnl_str,
        })

    # Summary KPIs
    c1, c2, c3, c4 = st.columns(4)
    pnl_color = "#0ECB81" if total_pnl >= 0 else "#F6465D"
    pnl_sign = "+" if total_pnl >= 0 else ""
    with c1:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Trades</div>
        <div class="kpi-value">{len(rows)}</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Wins</div>
        <div class="kpi-value" style="color:#0ECB81">{wins}</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Losses</div>
        <div class="kpi-value" style="color:#F6465D">{losses}</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Net P&L</div>
        <div class="kpi-value" style="color:{pnl_color}">{pnl_sign}${total_pnl:.2f}</div></div>""",
        unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# Open positions (if any)
positions = state.get("positions", [])
if positions:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="page-header">OPEN POSITIONS</div>', unsafe_allow_html=True)
    rows = []
    for p in positions:
        shares = float(p.get("shares") or 0)
        entry = float(p.get("avg_entry_price") or 0)
        conf = p.get("confidence")
        rows.append({
            "Market": (p.get("market_question") or "")[-10:],
            "Shares": f"{shares:.2f}",
            "Entry": f"${entry:.2f}",
            "Cost Basis": f"${shares * entry:.2f}",
            "Confidence": f"{conf:.1%}" if conf is not None else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
