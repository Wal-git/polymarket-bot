"""Cycle history — see what the bot did each polling iteration."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Cycles — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    inject_styles,
    load_cycles,
    render_sidebar,
)

inject_styles()
st_autorefresh(interval=10_000, key="cycles_refresh")
render_sidebar()

st.markdown('<div class="page-header">◇ CYCLE HISTORY</div>', unsafe_allow_html=True)

last_n = st.number_input("Show last N cycles", min_value=10, max_value=500, value=50, step=10)
cycles = load_cycles(last_n=int(last_n))

if not cycles:
    st.info("No cycles recorded yet.")
else:
    rows = []
    for c in cycles:
        rows.append({
            "Time": (c.get("ts") or "")[:19].replace("T", " "),
            "Mode": "DRY" if c.get("dry_run") else "LIVE",
            "Markets": c.get("markets_scanned", 0),
            "Signals": c.get("signals_generated", 0),
            "Approved": c.get("signals_approved", 0),
            "Open Pos": c.get("open_positions", 0),
            "Balance": f"${float(c.get('balance') or 0):,.2f}" if c.get("balance") else "—",
            "Total P&L": f"${float(c.get('total_pnl') or 0):+,.2f}",
            "Duration (ms)": c.get("duration_ms", 0),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
