"""Historical slot evaluations — table view with rejection breakdown chart."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from polybot.dashboard.theme import PALETTE, page_setup

page_setup("History — POLYBOT", refresh_ms=15_000)

from polybot.dashboard.components import kpi_row  # noqa: E402
from polybot.dashboard.format import to_pdt  # noqa: E402
from polybot.dashboard.loaders import load_evaluations  # noqa: E402
from polybot.dashboard.sidebar import apply_asset_filter  # noqa: E402

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
    mismatch = sum(1 for e in evals if e.get("reject_reason") == "direction_mismatch")

    kpi_row([
        {"label": "Total Slots", "value": str(total)},
        {"label": "Trades Fired", "value": str(len(confluences)), "value_class": "positive"},
        {"label": "No Divergence", "value": str(no_div), "value_class": "negative"},
        {"label": "Dir Mismatch", "value": str(mismatch), "value_class": "amber"},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Bar chart of rejection reasons ────────────────────────────────────────
    reason_counts = {
        "Trade": len(confluences),
        "No Divergence": no_div,
        "Dir Mismatch": mismatch,
    }
    chart_df = pd.DataFrame({"Outcome": list(reason_counts.keys()), "Count": list(reason_counts.values())})
    st.bar_chart(chart_df.set_index("Outcome"), color=PALETTE.AMBER)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Detailed table ────────────────────────────────────────────────────────
    st.markdown('<div class="page-header">EVALUATION LOG</div>', unsafe_allow_html=True)

    rows = []
    for e in evals:
        reject = e.get("reject_reason") or ""
        result = "TRADE" if e.get("confluence") else reject.replace("_", " ").upper()
        rows.append({
            "Time": to_pdt(e.get("ts") or "", "clock"),
            "Asset": e.get("asset") or "BTC",
            "Slot": (e.get("slug") or "")[-10:],
            "P-T-B": f"${float(e.get('price_to_beat') or 0):,.0f}",
            "Binance Δ": f"{float(e['binance_delta']):+.0f}" if e.get("binance_delta") is not None else "—",
            "Coinbase Δ": f"{float(e['coinbase_delta']):+.0f}" if e.get("coinbase_delta") is not None else "—",
            "Kraken Δ": f"{float(e['kraken_delta']):+.0f}" if e.get("kraken_delta") is not None else "—",
            "Bitstamp Δ": f"{float(e['bitstamp_delta']):+.0f}" if e.get("bitstamp_delta") is not None else "—",
            "OKX Δ": f"{float(e['okx_delta']):+.0f}" if e.get("okx_delta") is not None else "—",
            "Divergence": e.get("div_direction") or "—",
            "Result": result,
            "Confidence": f"{e.get('confidence'):.1%}" if e.get("confidence") else "—",
            "Size $": f"${e.get('size_usdc'):.2f}" if e.get("size_usdc") else "—",
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
