"""Troubleshoot — live log tail, config viewer, bot controls."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

from polybot.dashboard.theme import PALETTE, page_setup

page_setup("Troubleshoot — POLYBOT", refresh_ms=5_000)

from polybot.dashboard.loaders import (  # noqa: E402
    get_halt_path,
    load_bot_log,
    load_config,
    load_evaluations,
)

st.markdown('<div class="page-header">◇ TROUBLESHOOT</div>', unsafe_allow_html=True)

tab_log, tab_config, tab_controls, tab_signals = st.tabs(["Bot Log", "Config", "Controls", "Signal Debug"])

# ── Tab 1: Bot Log ────────────────────────────────────────────────────────────
with tab_log:
    st.markdown('<div class="page-header">LIVE BOT LOG</div>', unsafe_allow_html=True)

    n_lines = st.slider("Lines to show", 20, 200, 80, step=20)
    log_lines = load_bot_log(last_n=n_lines)

    if not log_lines:
        st.warning("No log data yet — bot may not have started.")
    else:
        # Colour-code by level
        coloured = []
        for line in log_lines:
            if "error" in line.lower() or "ERROR" in line:
                coloured.append(f'<span style="color:{PALETTE.RED};">{line}</span>')
            elif "warning" in line.lower() or "WARNING" in line:
                coloured.append(f'<span style="color:{PALETTE.AMBER};">{line}</span>')
            elif "signal_confluence" in line or "signal_fired" in line:
                coloured.append(f'<span style="color:{PALETTE.GREEN};font-weight:600;">{line}</span>')
            else:
                coloured.append(f'<span style="color:{PALETTE.MUTED};">{line}</span>')

        st.markdown(
            f'<div class="log-panel">{"<br>".join(coloured)}</div>',
            unsafe_allow_html=True,
        )

# ── Tab 2: Config ─────────────────────────────────────────────────────────────
with tab_config:
    st.markdown('<div class="page-header">ACTIVE CONFIGURATION</div>', unsafe_allow_html=True)
    cfg = load_config()

    if not cfg:
        st.warning("Could not load config/default.yaml")
    else:
        # Highlight key thresholds
        strategy = cfg.get("strategy", {})
        signals = strategy.get("signals", {})
        entry = strategy.get("entry", {})
        exit_cfg = strategy.get("exit", {})
        sizing = strategy.get("sizing", {})

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**Divergence Signal**")
            st.metric("Min Gap (USD)", signals.get("divergence", {}).get("min_gap_usd", "—"))
            st.markdown("**Entry Window**")
            window = entry.get("window_seconds", [60, 180])
            st.metric("Entry Window (s)", f"{window[0]}–{window[1]}")

        with col2:
            st.markdown("**Confidence Calibration**")
            cal = signals.get("calibration", {})
            st.metric("Enabled", str(cal.get("enabled", False)))
            st.metric("Cap", cal.get("cap", "—"))
            st.metric("Min trials/bucket", cal.get("min_n", "—"))

        with col3:
            st.markdown("**Exit & Sizing**")
            st.metric("Profit Target", exit_cfg.get("profit_target", "—"))
            st.metric("Stop Loss", exit_cfg.get("stop_loss", "—"))
            st.metric("Kelly Fraction", sizing.get("kelly_fraction", "—"))
            st.metric("Max Trade (USDC)", sizing.get("max_trade_usdc", "—"))

        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("Full YAML config"):
            st.code(yaml.dump(cfg, default_flow_style=False), language="yaml")

# ── Tab 3: Controls ───────────────────────────────────────────────────────────
with tab_controls:
    st.markdown('<div class="page-header">BOT CONTROLS</div>', unsafe_allow_html=True)

    halt_path = get_halt_path()
    is_halted = halt_path.exists()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Emergency Stop**")
        if is_halted:
            st.success("Bot is HALTED. No new entries will be placed.")
            if st.button("▶ Resume Bot", type="secondary", use_container_width=True):
                halt_path.unlink(missing_ok=True)
                st.cache_data.clear()
                st.rerun()
        else:
            st.info("Bot is running normally.")
            if st.button("⏹ HALT Bot", type="primary", use_container_width=True):
                halt_path.write_text("halt\n", encoding="utf-8")
                st.cache_data.clear()
                st.rerun()
        st.caption("HALT file: " + str(halt_path))

    with col2:
        st.markdown("**PM2 Process Status**")
        try:
            result = subprocess.run(["pm2", "jlist"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                for proc in json.loads(result.stdout):
                    name = proc.get("name", "")
                    status = proc.get("pm2_env", {}).get("status", "?")
                    restarts = proc.get("pm2_env", {}).get("restart_time", 0)
                    status_color = PALETTE.GREEN if status == "online" else PALETTE.RED
                    st.markdown(
                        f'<div style="padding:0.4rem 0.6rem;margin:0.2rem 0;'
                        f'background:rgba(0,0,0,0.15);border-radius:3px;font-family:monospace;font-size:0.8rem;">'
                        f'<span style="color:{PALETTE.WHITE};">{name}</span> &nbsp;'
                        f'<span style="color:{status_color};font-weight:700;">{status}</span> &nbsp;'
                        f'<span style="color:{PALETTE.GREY};">↺ {restarts}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
        except Exception as e:
            st.warning(f"Could not query PM2: {e}")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Data Files**")

    data_dir = Path("data")
    file_info = []
    for fname in ["bot.log", "cycles.jsonl", "signals.jsonl", "evaluations.jsonl", "state.json"]:
        p = data_dir / fname
        if p.exists():
            size_kb = p.stat().st_size / 1024
            lines = str(p.read_text(errors="replace").count("\n")) if p.suffix in (".jsonl", ".log") else "—"
            file_info.append({"File": fname, "Size (KB)": f"{size_kb:.1f}", "Lines": lines})
        else:
            file_info.append({"File": fname, "Size (KB)": "—", "Lines": "missing"})

    st.dataframe(pd.DataFrame(file_info), use_container_width=True, hide_index=True)

    with st.expander("Clear evaluations log"):
        st.warning("This will delete data/evaluations.jsonl permanently.")
        if st.button("Delete evaluations.jsonl", type="primary"):
            Path("data/evaluations.jsonl").unlink(missing_ok=True)
            st.cache_data.clear()
            st.success("Deleted.")

# ── Tab 4: Signal Debug ───────────────────────────────────────────────────────
with tab_signals:
    st.markdown('<div class="page-header">SIGNAL THRESHOLD SIMULATOR</div>', unsafe_allow_html=True)
    st.caption("Test what the divergence check produces at different prices.")

    cfg = load_config()
    sig_cfg = cfg.get("strategy", {}).get("signals", {})

    st.markdown("**Divergence Check**")
    div_cfg = sig_cfg.get("divergence", {})
    min_gap = float(div_cfg.get("min_gap_usd", 50.0))
    min_agreement = int(div_cfg.get("min_agreement", 3))
    sim_ptb = st.number_input("Price to Beat ($)", value=93000.0, step=100.0)
    sim_prices = {
        "Binance":  st.number_input("Binance Price ($)",  value=93000.0, step=10.0, key="sim_binance"),
        "Coinbase": st.number_input("Coinbase Price ($)", value=93000.0, step=10.0, key="sim_coinbase"),
        "Kraken":   st.number_input("Kraken Price ($)",   value=93000.0, step=10.0, key="sim_kraken"),
        "Bitstamp": st.number_input("Bitstamp Price ($)", value=93000.0, step=10.0, key="sim_bitstamp"),
        "OKX":      st.number_input("OKX Price ($)",      value=93000.0, step=10.0, key="sim_okx"),
    }
    sim_deltas = {name: price - sim_ptb for name, price in sim_prices.items()}
    up_votes = sum(1 for d in sim_deltas.values() if d > min_gap)
    down_votes = sum(1 for d in sim_deltas.values() if d < -min_gap)
    div_up = up_votes >= min_agreement and down_votes == 0
    div_dn = down_votes >= min_agreement and up_votes == 0
    div_result = "UP" if div_up else ("DOWN" if div_dn else "NO SIGNAL")
    div_color = PALETTE.GREEN if div_up or div_dn else PALETTE.RED
    deltas_str = " · ".join(f"{n} **{d:+.0f}**" for n, d in sim_deltas.items())
    st.markdown(deltas_str)
    st.markdown(
        f"Min gap: ±{min_gap} · Agreement: {min_agreement}-of-5 "
        f"· UP votes: {up_votes} · DOWN votes: {down_votes}"
    )
    st.markdown(f'Result: <span style="color:{div_color};font-weight:700;">{div_result}</span>',
                unsafe_allow_html=True)

    st.markdown("---")
    if div_up or div_dn:
        st.success("Divergence signal present. Trade would fire (subject to entry-price/confidence gates).")
    else:
        st.error("No divergence signal. Trade skipped.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Recent Evaluation Stats**")
    evals = load_evaluations(last_n=200)
    if evals:
        with_div = [e for e in evals if e.get("div_direction")]

        # Per-exchange average |delta| over the same window — only counts
        # evaluations where that exchange returned a value, so an outage
        # doesn't drag its average toward zero.
        exchange_labels = [
            ("binance", "Binance"), ("coinbase", "Coinbase"), ("kraken", "Kraken"),
            ("bitstamp", "Bitstamp"), ("okx", "OKX"),
        ]
        avg_deltas: dict[str, float | None] = {}
        for key, label in exchange_labels:
            vals = [abs(e.get(f"{key}_delta") or 0) for e in evals if e.get(f"{key}_delta") is not None]
            avg_deltas[label] = (sum(vals) / len(vals)) if vals else None

        cols = st.columns(5)
        for col, (_, label) in zip(cols, exchange_labels):
            v = avg_deltas[label]
            with col:
                st.metric(f"Avg |{label} Δ|", f"${v:.2f}" if v is not None else "—")

        st.metric("Divergence hit rate", f"{100*len(with_div)/len(evals):.1f}%")
