"""Live signal evaluation feed — one card per 5-min slot evaluated."""
from __future__ import annotations

import streamlit as st

from polybot.dashboard.theme import page_setup

page_setup("Live Feed — POLYBOT", refresh_ms=5_000)

from polybot.dashboard.components import render_eval_card  # noqa: E402
from polybot.dashboard.loaders import load_evaluations, load_results  # noqa: E402
from polybot.dashboard.sidebar import apply_asset_filter  # noqa: E402

st.markdown('<div class="page-header">◇ LIVE SIGNAL FEED</div>', unsafe_allow_html=True)

col1, col2 = st.columns([3, 1])
with col1:
    filter_opt = st.selectbox("Show", ["all", "trades only", "skipped only"], index=0,
                              label_visibility="collapsed")
with col2:
    last_n = st.number_input("Last N", min_value=10, max_value=500, value=50, step=10,
                             label_visibility="collapsed")

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
        render_eval_card(ev, result=results.get(ev.get("slug", "")))
