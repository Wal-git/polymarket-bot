"""Smart-wallet pipeline overview — latest run, archetype rankings, rejects, backtest."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Smart Wallets — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import inject_styles, render_sidebar  # noqa: E402
from polybot.smart_wallets.config import (  # noqa: E402
    SMART_WALLETS_CLOSER_JSON,
    SMART_WALLETS_SIGNAL_JSON,
)
from polybot.smart_wallets.store import Store  # noqa: E402

inject_styles()
render_sidebar()

st.markdown('<div class="page-header">◇ SMART WALLETS</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# latest run metadata
# ---------------------------------------------------------------------------

def _safe_store() -> Store | None:
    try:
        return Store()
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not open smart-wallet DB: {exc}")
        return None


store = _safe_store()
latest_run = None
if store is not None:
    runs = store.recent_runs(limit=10)
    if runs:
        latest_run = runs[0]

if latest_run is None:
    st.info("No smart-wallet runs recorded. Run `polybot.smart_wallets.cli run` to populate.")
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest run", f"#{latest_run['run_id']}")
    c2.metric("Status", latest_run["status"])
    c3.metric("Candidates", latest_run["n_candidates"] or 0)
    c4.metric("Selected", latest_run["n_selected"] or 0)

    # Diff vs. previous run.
    diff = store.wallet_diff(latest_run["run_id"])
    d1, d2, d3 = st.columns(3)
    d1.metric("Added", len(diff["added"]))
    d2.metric("Removed", len(diff["removed"]))
    d3.metric("Retained", len(diff["retained"]))

    # Reject histogram.
    st.markdown("### Reject reasons (latest run)")
    hist = store.reject_histogram(latest_run["run_id"])
    if hist:
        st.bar_chart(pd.DataFrame({"count": hist}))
    else:
        st.caption("No rejects recorded.")


# ---------------------------------------------------------------------------
# archetype tabs
# ---------------------------------------------------------------------------

def _load_json(path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


closer_data = _load_json(SMART_WALLETS_CLOSER_JSON)
signal_data = _load_json(SMART_WALLETS_SIGNAL_JSON)

tabs = st.tabs(["Closer (resolution PnL)", "Signal (early-entry edge)"])

for tab, data, label in zip(
    tabs,
    [closer_data, signal_data],
    ["closer", "signal"],
):
    with tab:
        wallets = data.get("wallets") or []
        if not wallets:
            st.info(f"No {label} archetype wallets written yet.")
            continue
        df = pd.DataFrame(wallets)
        df["last_active"] = df["last_active_ts"].apply(
            lambda ts: datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
            if ts else "—"
        )
        cols = [
            "proxy_wallet", "username", "score", "signal_score", "closer_score",
            "pnl_realized", "edge", "sharpe", "win_rate", "resolved_markets",
            "volume", "early_signal", "last_active",
        ]
        display = df[[c for c in cols if c in df.columns]].copy()
        for money_col in ("pnl_realized", "volume"):
            if money_col in display.columns:
                display[money_col] = display[money_col].apply(lambda v: f"${float(v):,.0f}")
        for pct_col in ("win_rate",):
            if pct_col in display.columns:
                display[pct_col] = display[pct_col].apply(lambda v: f"{float(v):.0%}")
        for f4_col in ("edge", "sharpe", "early_signal", "score", "signal_score", "closer_score"):
            if f4_col in display.columns:
                display[f4_col] = display[f4_col].apply(lambda v: f"{float(v):.3f}")
        st.dataframe(display, use_container_width=True, hide_index=True)
        st.caption(
            f"Generated {data.get('generated_at','?')} · lookback {data.get('lookback_days','?')}d"
            f" · {len(wallets)} wallets"
        )


# ---------------------------------------------------------------------------
# backtest runner
# ---------------------------------------------------------------------------

st.markdown("### Forward-validation backtest")
if latest_run is not None:
    col1, col2 = st.columns([1, 3])
    run_choice = col1.selectbox(
        "Run",
        options=[r["run_id"] for r in runs],
        index=0,
    )
    forward_days = col1.number_input("Forward days", min_value=1, max_value=30, value=7)
    if col1.button("Run backtest", type="primary"):
        from polybot.smart_wallets.backtest import evaluate_run

        with st.spinner(f"Running forward-{forward_days}d backtest for run #{run_choice}…"):
            try:
                result = evaluate_run(run_id=int(run_choice), forward_days=int(forward_days))
                col2.json(result.to_dict())
            except Exception as exc:  # noqa: BLE001
                col2.error(f"Backtest failed: {exc}")


# ---------------------------------------------------------------------------
# trail backtest
# ---------------------------------------------------------------------------

st.markdown("### Trail-strategy backtest")
st.caption(
    "Replays wallet-filtered Goldsky events over a past window, simulates "
    "the SmartMoneyStrategy at configurable cadence, and reports simulated PnL."
)
if latest_run is not None:
    tb1, tb2 = st.columns([1, 3])
    trail_run = tb1.selectbox(
        "Cohort run",
        options=[r["run_id"] for r in runs],
        index=0,
        key="trail_run",
    )
    trail_days = tb1.number_input("Window days", min_value=1, max_value=30, value=7, key="trail_days")
    trail_interval = tb1.number_input("Sim interval (s)", min_value=10, max_value=3600, value=30, key="trail_interval")
    if tb1.button("Run trail backtest", type="primary"):
        from polybot.smart_wallets.trail_backtest import run_trail_backtest

        with st.spinner(f"Replaying {trail_days}d of events for run #{trail_run}…"):
            try:
                tr = run_trail_backtest(
                    run_id=int(trail_run),
                    lookback_window_days=int(trail_days),
                    sim_interval_seconds=int(trail_interval),
                )
                m1, m2, m3, m4 = tb2.columns(4)
                m1.metric("Signals", tr.n_signals)
                m2.metric("Positions", tr.n_positions)
                m3.metric("Total PnL", f"${tr.total_pnl:+.2f}")
                m4.metric("Win rate", f"{tr.win_rate:.0%}")
                d1, d2 = tb2.columns(2)
                d1.metric("Whale-exit closes", tr.n_whale_exit_closes)
                d2.metric("TTL closes", tr.n_ttl_closes)
                if tr.positions:
                    import pandas as pd
                    rows = [
                        {
                            "token_id": p.token_id[:12] + "…",
                            "entry_price": round(p.entry_price, 4),
                            "exit_price": round(p.exit_price, 4) if p.exit_price is not None else None,
                            "size_usdc": round(p.size_usdc, 2),
                            "pnl": round(p.pnl, 4),
                            "exit_reason": p.exit_reason,
                        }
                        for p in tr.positions
                    ]
                    tb2.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            except Exception as exc:  # noqa: BLE001
                tb2.error(f"Trail backtest failed: {exc}")

if store is not None:
    store.close()
