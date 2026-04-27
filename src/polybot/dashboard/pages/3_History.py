"""Historical slot evaluations — table view with rejection breakdown chart."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

_PDT = timezone(timedelta(hours=-7))


def _to_pdt(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso).astimezone(_PDT).strftime("%-I:%M %p PDT")
    except Exception:
        return iso[:19].replace("T", " ")

st.set_page_config(page_title="History — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    apply_asset_filter,
    inject_styles,
    load_evaluations,
    render_sidebar,
)

inject_styles()
st_autorefresh(interval=15_000, key="history_refresh")
render_sidebar()

st.markdown('<div class="page-header">◇ SLOT HISTORY</div>', unsafe_allow_html=True)

last_n = st.number_input("Show last N evaluations", min_value=10, max_value=1000, value=100, step=10)
evals = apply_asset_filter(load_evaluations(last_n=int(last_n)))

if not evals:
    st.info("No evaluations recorded yet.")
else:
    # ── Summary metrics ───────────────────────────────────────────────────────
    total = len(evals)
    confluences = [e for e in evals if e.get("confluence")]
    no_div = sum(1 for e in evals if e.get("reject_reason") == "no_divergence")
    no_imb = sum(1 for e in evals if e.get("reject_reason") == "no_imbalance")
    mismatch = sum(1 for e in evals if e.get("reject_reason") == "direction_mismatch")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Total Slots</div>
        <div class="kpi-value">{total}</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Trades Fired</div>
        <div class="kpi-value positive">{len(confluences)}</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">No Divergence</div>
        <div class="kpi-value negative">{no_div}</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">No Imbalance</div>
        <div class="kpi-value negative">{no_imb}</div></div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class="kpi-block"><div class="kpi-label">Dir Mismatch</div>
        <div class="kpi-value amber">{mismatch}</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Bar chart of rejection reasons ────────────────────────────────────────
    reason_counts = {
        "Trade": len(confluences),
        "No Divergence": no_div,
        "No Imbalance": no_imb,
        "Dir Mismatch": mismatch,
    }
    chart_df = pd.DataFrame({"Outcome": list(reason_counts.keys()), "Count": list(reason_counts.values())})
    st.bar_chart(chart_df.set_index("Outcome"), color="#F0B90B")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Detailed table ────────────────────────────────────────────────────────
    st.markdown('<div class="page-header">EVALUATION LOG</div>', unsafe_allow_html=True)

    rows = []
    for e in evals:
        reject = e.get("reject_reason") or ""
        result = "TRADE" if e.get("confluence") else reject.replace("_", " ").upper()
        rows.append({
            "Time": _to_pdt(e.get("ts") or ""),
            "Asset": e.get("asset") or "BTC",
            "Slot": (e.get("slug") or "")[-10:],
            "P-T-B": f"${float(e.get('price_to_beat') or 0):,.0f}",
            "Binance Δ": f"{float(e['binance_delta']):+.0f}" if e.get("binance_delta") is not None else "—",
            "Coinbase Δ": f"{float(e['coinbase_delta']):+.0f}" if e.get("coinbase_delta") is not None else "—",
            "Kraken Δ": f"{float(e['kraken_delta']):+.0f}" if e.get("kraken_delta") is not None else "—",
            "Bitstamp Δ": f"{float(e['bitstamp_delta']):+.0f}" if e.get("bitstamp_delta") is not None else "—",
            "OKX Δ": f"{float(e['okx_delta']):+.0f}" if e.get("okx_delta") is not None else "—",
            "Divergence": e.get("div_direction") or "—",
            "Imbalance": f"{e.get('imbalance_ratio'):.3f}" if e.get("imbalance_ratio") is not None else "—",
            "Imb Dir": e.get("imb_direction") or "—",
            "Result": result,
            "Confidence": f"{e.get('confidence'):.1%}" if e.get("confidence") else "—",
            "Size $": f"${e.get('size_usdc'):.2f}" if e.get("size_usdc") else "—",
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Imbalance ratio distribution ──────────────────────────────────────────
    ratios = [e.get("imbalance_ratio") for e in evals if e.get("imbalance_ratio") is not None]
    if ratios:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="page-header">IMBALANCE RATIO DISTRIBUTION</div>', unsafe_allow_html=True)
        ratio_df = pd.DataFrame({"ratio": ratios})
        st.bar_chart(ratio_df["ratio"].value_counts(bins=20).sort_index())
