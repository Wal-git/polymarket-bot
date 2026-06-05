"""Trade history with outcomes, timestamps in Pacific time."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from polybot.dashboard.theme import PALETTE, page_setup

page_setup("Positions — POLYBOT", refresh_ms=10_000)

from polybot.dashboard.components import kpi_row  # noqa: E402
from polybot.dashboard.format import fmt_pct, pnl_color, pnl_str, strip_slug_prefix, to_pdt  # noqa: E402
from polybot.dashboard.loaders import STARTING_BALANCE, load_results, load_state  # noqa: E402

st.markdown('<div class="page-header">◇ TRADE HISTORY</div>', unsafe_allow_html=True)

state = load_state()
trades = state.get("trades", [])
results = load_results()

if not trades:
    st.info("No trades recorded yet.")
else:
    rows = []
    for t in reversed(trades):
        if t.get("side") != "BUY":
            continue
        slug = t.get("market_question", "")
        result = results.get(slug)
        if slug.startswith("eth-"):
            asset = "ETH"
        elif slug.startswith("btc-"):
            asset = "BTC"
        else:
            asset = (result or {}).get("asset", "BTC") if result else "BTC"
        won = result.get("won") if result else None
        pnl = result.get("pnl") if result else None

        if won is True:
            outcome = "✅ WIN"
        elif won is False:
            outcome = "❌ LOSS"
        else:
            outcome = "⏳ Pending"

        pnl_display = pnl_str(pnl) if pnl is not None else "—"
        confidence = result.get("confidence") if result else None
        stake = float(t.get("size") or 0) * float(t.get("price") or 0)
        pnl_pct_str = fmt_pct(pnl / stake * 100) if (pnl is not None and stake > 0) else "—"

        rows.append({
            "Time (PDT)": to_pdt(t.get("timestamp", ""), "datetime"),
            "Asset": asset,
            "Market": strip_slug_prefix(slug),
            "Direction": t.get("side", ""),
            "Shares": f"{float(t.get('size') or 0):.2f}",
            "Entry": f"${float(t.get('price') or 0):.2f}",
            "Stake": f"${stake:.2f}",
            "Confidence": f"{confidence:.1%}" if confidence is not None else "—",
            "Outcome": outcome,
            "P&L": pnl_display,
            "P&L %": pnl_pct_str,
        })

    # Summary KPIs — use all results as source of truth (includes trades
    # no longer in state.json, e.g. old slots or manual bets).
    all_wins = sum(1 for r in results.values() if r.get("won"))
    all_losses = sum(1 for r in results.values() if not r.get("won"))
    total_pnl = sum(r.get("pnl", 0) for r in results.values())
    net_pct = total_pnl / STARTING_BALANCE * 100

    kpi_row([
        {"label": "Trades", "value": str(all_wins + all_losses)},
        {"label": "Wins", "value": str(all_wins), "color": PALETTE.GREEN},
        {"label": "Losses", "value": str(all_losses), "color": PALETTE.RED},
        {"label": "Net P&L", "value": pnl_str(total_pnl), "color": pnl_color(total_pnl),
         "sub": fmt_pct(net_pct), "sub_color": pnl_color(total_pnl)},
    ])

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
