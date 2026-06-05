"""Daily P&L history with date-range filter."""
from __future__ import annotations

from collections import defaultdict
from datetime import date as date_cls
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from polybot.dashboard.theme import PALETTE, page_setup

page_setup("P&L — POLYBOT", refresh_ms=30_000)

from polybot.dashboard.components import kpi_row  # noqa: E402
from polybot.dashboard.format import PDT, fmt_pct, pnl_color, pnl_str  # noqa: E402
from polybot.dashboard.loaders import STARTING_BALANCE, load_balance, load_results_deduped  # noqa: E402


def _x_axis(angle: int = -30, fontsize: int = 11) -> dict:
    return {"labelColor": PALETTE.GREY, "gridColor": "rgba(255,255,255,0.04)",
            "domainColor": "rgba(255,255,255,0.1)", "tickColor": "transparent",
            "labelAngle": angle, "labelFontSize": fontsize}


def _y_axis(fmt: str = "$,.2f") -> dict:
    return {"labelColor": PALETTE.GREY, "gridColor": "rgba(255,255,255,0.06)",
            "domainColor": "transparent", "tickColor": "transparent",
            "format": fmt, "labelFontSize": 11}


_VEGA_CONFIG = {"background": "transparent", "view": {"stroke": "transparent"}}

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

net_pct = net_pnl / STARTING_BALANCE * 100
trade_pct = trade_pnl / STARTING_BALANCE * 100

kpi_row([
    {"label": "Total P&L", "value": pnl_str(net_pnl), "color": pnl_color(net_pnl),
     "sub": f"{fmt_pct(net_pct)} vs ${STARTING_BALANCE:.0f} start", "sub_color": pnl_color(net_pnl)},
    {"label": "Closed Trade P&L", "value": pnl_str(trade_pnl), "color": pnl_color(trade_pnl),
     "sub": fmt_pct(trade_pct), "sub_color": pnl_color(trade_pnl)},
    {"label": "Best Day", "value": pnl_str(best_day), "value_class": "positive"},
    {"label": "Worst Day", "value": pnl_str(worst_day), "value_class": "negative"},
    {"label": "Winning Days",
     "value": f"{winning_days}/{total_trading_days}" if total_trading_days else "—",
     "value_class": "amber"},
])

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

vega_bar = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "height": 260,
    "data": {"values": df[["date", "daily_pnl"]].to_dict("records")},
    "mark": {"type": "bar", "cornerRadiusTopLeft": 2, "cornerRadiusTopRight": 2},
    "encoding": {
        "x": {"field": "date", "type": "temporal", "axis": _x_axis(), "title": None},
        "y": {"field": "daily_pnl", "type": "quantitative", "axis": _y_axis(), "title": None},
        "color": {
            "condition": {"test": "datum.daily_pnl >= 0", "value": PALETTE.GREEN},
            "value": PALETTE.RED,
        },
        "tooltip": [
            {"field": "date", "type": "temporal", "title": "Date", "format": "%Y-%m-%d"},
            {"field": "daily_pnl", "type": "quantitative", "title": "Daily P&L", "format": "$,.2f"},
        ],
    },
    "config": _VEGA_CONFIG,
}
st.vega_lite_chart(vega_bar, use_container_width=True, theme=None)

# ── Cumulative P&L line chart ──────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">CUMULATIVE TRADE P&L</div>', unsafe_allow_html=True)

last_cum = df["cumulative_pnl"].iloc[-1] if not df.empty else 0.0
line_color = pnl_color(last_cum)
line_records = df[["date", "cumulative_pnl"]].to_dict("records")

vega_line = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "height": 200,
    "data": {"values": line_records},
    "layer": [
        {
            "mark": {"type": "area", "opacity": 0.08, "color": line_color},
            "encoding": {
                "x": {"field": "date", "type": "temporal", "title": None, "axis": _x_axis()},
                "y": {"field": "cumulative_pnl", "type": "quantitative", "title": None, "axis": _y_axis()},
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
    "config": _VEGA_CONFIG,
}
st.vega_lite_chart(vega_line, use_container_width=True, theme=None)

# ── Daily breakdown table ──────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">DAILY BREAKDOWN</div>', unsafe_allow_html=True)

table_rows = df[df["daily_pnl"] != 0.0].copy().sort_values("date", ascending=False)
if table_rows.empty:
    st.info("No trades in the selected date range.")
else:
    display_df = pd.DataFrame({
        "Date": table_rows["date"].values,
        "Daily P&L": [pnl_str(v) for v in table_rows["daily_pnl"].values],
        "Daily %": [fmt_pct(v / STARTING_BALANCE * 100) for v in table_rows["daily_pnl"].values],
        "Cumulative P&L": [pnl_str(v) for v in table_rows["cumulative_pnl"].values],
        "Cumulative %": [fmt_pct(v / STARTING_BALANCE * 100) for v in table_rows["cumulative_pnl"].values],
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ── P&L by hour of day (Pacific) ───────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="page-header">P&L BY HOUR OF DAY (PACIFIC)</div>', unsafe_allow_html=True)

# Aggregate every resolved trade into 24 Pacific-hour buckets
hourly_pnl: dict[int, float] = {h: 0.0 for h in range(24)}
hourly_wins: dict[int, int] = {h: 0 for h in range(24)}
hourly_count: dict[int, int] = {h: 0 for h in range(24)}
for r in results:
    ts = r.get("ts", "")
    if not ts:
        continue
    try:
        h = datetime.fromisoformat(ts).astimezone(PDT).hour
    except Exception:
        continue
    hourly_pnl[h] += float(r.get("pnl", 0))
    hourly_count[h] += 1
    if r.get("won"):
        hourly_wins[h] += 1

if sum(hourly_count.values()) == 0:
    st.info("No resolved trades yet — hourly chart will appear once markets resolve.")
else:
    hour_records = []
    for h in range(24):
        n = hourly_count[h]
        wr = (hourly_wins[h] / n) if n else 0.0
        hour_records.append({
            "hour": f"{h:02d}:00",
            "hour_int": h,
            "pnl": round(hourly_pnl[h], 2),
            "trades": n,
            "wins": hourly_wins[h],
            "win_rate": round(wr, 3),
        })

    vega_hour_pnl = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "height": 240,
        "data": {"values": hour_records},
        "mark": {"type": "bar", "cornerRadiusTopLeft": 2, "cornerRadiusTopRight": 2},
        "encoding": {
            "x": {"field": "hour", "type": "ordinal", "axis": _x_axis(angle=-45, fontsize=10),
                  "title": None, "sort": [f"{h:02d}:00" for h in range(24)]},
            "y": {"field": "pnl", "type": "quantitative", "axis": _y_axis(), "title": None},
            "color": {
                "condition": {"test": "datum.pnl >= 0", "value": PALETTE.GREEN},
                "value": PALETTE.RED,
            },
            "tooltip": [
                {"field": "hour", "type": "ordinal", "title": "Hour (PT)"},
                {"field": "trades", "type": "quantitative", "title": "Trades"},
                {"field": "wins", "type": "quantitative", "title": "Wins"},
                {"field": "win_rate", "type": "quantitative", "title": "Win Rate", "format": ".1%"},
                {"field": "pnl", "type": "quantitative", "title": "Total P&L", "format": "$,.2f"},
            ],
        },
        "config": _VEGA_CONFIG,
    }
    st.vega_lite_chart(vega_hour_pnl, use_container_width=True, theme=None)

    # Companion table — only hours that had activity
    active_rows = [r for r in hour_records if r["trades"] > 0]
    if active_rows:
        active_rows.sort(key=lambda r: r["pnl"])  # worst first
        hour_df = pd.DataFrame({
            "Hour (PT)": [r["hour"] for r in active_rows],
            "Trades": [r["trades"] for r in active_rows],
            "Win Rate": [f"{r['wins']}/{r['trades']} ({r['win_rate']:.0%})" for r in active_rows],
            "Total P&L": [pnl_str(r["pnl"]) for r in active_rows],
            "Avg P&L / trade": [pnl_str(r["pnl"] / r["trades"]) for r in active_rows],
        })
        st.dataframe(hour_df, use_container_width=True, hide_index=True)
