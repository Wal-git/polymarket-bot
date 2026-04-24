"""Daily P&L history with date-range filter."""
from __future__ import annotations

from collections import defaultdict
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="P&L — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    STARTING_BALANCE,
    inject_styles,
    load_balance,
    load_results_deduped,
    render_sidebar,
)

inject_styles()
st_autorefresh(interval=30_000, key="pnl_refresh")
render_sidebar()

PDT = timezone(timedelta(hours=-7))

st.markdown('<div class="page-header">◇ P&L HISTORY</div>', unsafe_allow_html=True)

results = load_results_deduped()
bal = load_balance()

# Group by PDT date
daily: dict[date_cls, float] = defaultdict(float)
for r in results:
    ts = r.get("ts", "")
    if not ts:
        continue
    try:
        dt = datetime.fromisoformat(ts).astimezone(PDT)
        daily[dt.date()] += float(r.get("pnl", 0))
    except Exception:
        continue

all_dates = sorted(daily.keys())
today = datetime.now(PDT).date()

# ── Summary KPIs ──────────────────────────────────────────────────────────────
total_value_f = float(bal.get("total_value", 0)) if bal else 0.0
net_pnl = (total_value_f - STARTING_BALANCE) if (bal and total_value_f > 0) else 0.0
trade_pnl = sum(daily.values())
best_day = max(daily.values()) if daily else 0.0
worst_day = min(daily.values()) if daily else 0.0
winning_days = sum(1 for v in daily.values() if v > 0)
total_trading_days = len(daily)

pnl_color = "#0ECB81" if net_pnl >= 0 else "#F6465D"
pnl_str = f"+${net_pnl:,.2f}" if net_pnl >= 0 else f"-${abs(net_pnl):,.2f}"
trade_color = "#0ECB81" if trade_pnl >= 0 else "#F6465D"
trade_str = f"+${trade_pnl:,.2f}" if trade_pnl >= 0 else f"-${abs(trade_pnl):,.2f}"

c1, c2, c3, c4, c5 = st.columns(5)
net_pct = net_pnl / STARTING_BALANCE * 100
trade_pct = trade_pnl / STARTING_BALANCE * 100

with c1:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Total P&L</div>
        <div class="kpi-value" style="color:{pnl_color};">{pnl_str}</div>
        <div style="font-size:0.75rem;color:{pnl_color};margin-top:0.15rem;">{net_pct:+.1f}% vs ${STARTING_BALANCE:.0f} start</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Closed Trade P&L</div>
        <div class="kpi-value" style="color:{trade_color};">{trade_str}</div>
        <div style="font-size:0.75rem;color:{trade_color};margin-top:0.15rem;">{trade_pct:+.1f}%</div>
    </div>""", unsafe_allow_html=True)
with c3:
    best_str = f"+${best_day:,.2f}" if best_day >= 0 else f"-${abs(best_day):,.2f}"
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Best Day</div>
        <div class="kpi-value positive">{best_str}</div>
    </div>""", unsafe_allow_html=True)
with c4:
    worst_str = f"-${abs(worst_day):,.2f}" if worst_day < 0 else f"+${worst_day:,.2f}"
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Worst Day</div>
        <div class="kpi-value negative">{worst_str}</div>
    </div>""", unsafe_allow_html=True)
with c5:
    wr_str = f"{winning_days}/{total_trading_days}" if total_trading_days else "—"
    st.markdown(f"""
    <div class="kpi-block">
        <div class="kpi-label">Winning Days</div>
        <div class="kpi-value amber">{wr_str}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Filter controls ────────────────────────────────────────────────────────────
col_f1, col_f2 = st.columns([1, 3])
with col_f1:
    preset = st.selectbox(
        "Time range",
        ["All time", "Last 7 days", "Last 14 days", "Last 30 days", "Custom"],
        index=0,
        label_visibility="collapsed",
    )

if not all_dates:
    st.info("No resolved trades yet — P&L chart will appear once markets resolve.")
    st.stop()

if preset == "Custom":
    with col_f2:
        date_range = st.date_input(
            "Select range",
            value=(all_dates[0], today),
            min_value=all_dates[0],
            max_value=today,
            label_visibility="collapsed",
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_date, end_date = date_range[0], date_range[1]
        else:
            start_date = end_date = today
elif preset == "Last 7 days":
    start_date, end_date = today - timedelta(days=6), today
elif preset == "Last 14 days":
    start_date, end_date = today - timedelta(days=13), today
elif preset == "Last 30 days":
    start_date, end_date = today - timedelta(days=29), today
else:
    start_date, end_date = all_dates[0], today

# Build continuous date range with zero-fill for days with no trades
filtered_dates: list[date_cls] = []
d = start_date
while d <= end_date:
    filtered_dates.append(d)
    d += timedelta(days=1)

df = pd.DataFrame({
    "date": [str(d) for d in filtered_dates],
    "daily_pnl": [daily.get(d, 0.0) for d in filtered_dates],
})
df["cumulative_pnl"] = df["daily_pnl"].cumsum()

# ── Daily P&L bar chart ────────────────────────────────────────────────────────
st.markdown('<div class="page-header">DAILY P&L</div>', unsafe_allow_html=True)

bar_records = df[["date", "daily_pnl"]].to_dict("records")
vega_bar = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "height": 260,
    "data": {"values": bar_records},
    "mark": {"type": "bar", "cornerRadiusTopLeft": 2, "cornerRadiusTopRight": 2},
    "encoding": {
        "x": {
            "field": "date",
            "type": "temporal",
            "axis": {
                "labelColor": "#848E9C",
                "gridColor": "rgba(255,255,255,0.04)",
                "domainColor": "rgba(255,255,255,0.1)",
                "tickColor": "transparent",
                "labelAngle": -30,
                "labelFontSize": 11,
            },
            "title": None,
        },
        "y": {
            "field": "daily_pnl",
            "type": "quantitative",
            "axis": {
                "labelColor": "#848E9C",
                "gridColor": "rgba(255,255,255,0.06)",
                "domainColor": "transparent",
                "tickColor": "transparent",
                "format": "$,.2f",
                "labelFontSize": 11,
            },
            "title": None,
        },
        "color": {
            "condition": {"test": "datum.daily_pnl >= 0", "value": "#0ECB81"},
            "value": "#F6465D",
        },
        "tooltip": [
            {"field": "date", "type": "temporal", "title": "Date", "format": "%Y-%m-%d"},
            {"field": "daily_pnl", "type": "quantitative", "title": "Daily P&L", "format": "$,.2f"},
        ],
    },
    "config": {"background": "transparent", "view": {"stroke": "transparent"}},
}

st.vega_lite_chart(vega_bar, use_container_width=True, theme=None)

# ── Cumulative P&L line chart ──────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">CUMULATIVE TRADE P&L</div>', unsafe_allow_html=True)

line_records = df[["date", "cumulative_pnl"]].to_dict("records")
last_cum = df["cumulative_pnl"].iloc[-1] if not df.empty else 0.0
line_color = "#0ECB81" if last_cum >= 0 else "#F6465D"

vega_line = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "height": 200,
    "data": {"values": line_records},
    "layer": [
        {
            "mark": {"type": "area", "opacity": 0.08, "color": line_color},
            "encoding": {
                "x": {"field": "date", "type": "temporal", "title": None,
                      "axis": {"labelColor": "#848E9C", "gridColor": "rgba(255,255,255,0.04)",
                               "domainColor": "rgba(255,255,255,0.1)", "tickColor": "transparent",
                               "labelAngle": -30, "labelFontSize": 11}},
                "y": {"field": "cumulative_pnl", "type": "quantitative", "title": None,
                      "axis": {"labelColor": "#848E9C", "gridColor": "rgba(255,255,255,0.06)",
                               "domainColor": "transparent", "tickColor": "transparent",
                               "format": "$,.2f", "labelFontSize": 11}},
                "color": {"value": line_color},
            },
        },
        {
            "mark": {"type": "line", "strokeWidth": 2, "color": line_color},
            "encoding": {
                "x": {"field": "date", "type": "temporal"},
                "y": {"field": "cumulative_pnl", "type": "quantitative"},
                "color": {"value": line_color},
                "tooltip": [
                    {"field": "date", "type": "temporal", "title": "Date", "format": "%Y-%m-%d"},
                    {"field": "cumulative_pnl", "type": "quantitative", "title": "Cumulative P&L", "format": "$,.2f"},
                ],
            },
        },
    ],
    "config": {"background": "transparent", "view": {"stroke": "transparent"}},
}

st.vega_lite_chart(vega_line, use_container_width=True, theme=None)

# ── Daily breakdown table ──────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">DAILY BREAKDOWN</div>', unsafe_allow_html=True)

table_rows = df[df["daily_pnl"] != 0.0].copy().sort_values("date", ascending=False)
if table_rows.empty:
    st.info("No trades in the selected date range.")
else:
    def _fmt(v: float) -> str:
        return f"+${v:,.2f}" if v >= 0 else f"-${abs(v):,.2f}"

    display_df = pd.DataFrame({
        "Date": table_rows["date"].values,
        "Daily P&L": [_fmt(v) for v in table_rows["daily_pnl"].values],
        "Daily %": [f"{v / STARTING_BALANCE * 100:+.1f}%" for v in table_rows["daily_pnl"].values],
        "Cumulative P&L": [_fmt(v) for v in table_rows["cumulative_pnl"].values],
        "Cumulative %": [f"{v / STARTING_BALANCE * 100:+.1f}%" for v in table_rows["cumulative_pnl"].values],
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)
