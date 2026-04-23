"""Troubleshoot — live log tail, config viewer, bot controls."""
from __future__ import annotations

import subprocess
from pathlib import Path

import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Troubleshoot — POLYBOT", page_icon="◇", layout="wide")

from polybot.dashboard.data_loader import (  # noqa: E402
    get_halt_path,
    inject_styles,
    load_bot_log,
    load_config,
    load_evaluations,
    render_sidebar,
)

inject_styles()
st_autorefresh(interval=5_000, key="troubleshoot_refresh")
render_sidebar()

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
                coloured.append(f'<span style="color:#F6465D;">{line}</span>')
            elif "warning" in line.lower() or "WARNING" in line:
                coloured.append(f'<span style="color:#F0B90B;">{line}</span>')
            elif "signal_confluence" in line or "signal_fired" in line:
                coloured.append(f'<span style="color:#0ECB81;font-weight:600;">{line}</span>')
            else:
                coloured.append(f'<span style="color:#B7BDC6;">{line}</span>')

        log_html = "<br>".join(coloured)
        st.markdown(
            f'<div style="font-family:monospace;font-size:0.75rem;line-height:1.6;'
            f'background:#0d0f12;padding:1rem;border-radius:6px;overflow-x:auto;">'
            f'{log_html}</div>',
            unsafe_allow_html=True,
        )

# ── Tab 2: Config ─────────────────────────────────────────────────────────────
with tab_config:
    st.markdown('<div class="page-header">ACTIVE CONFIGURATION</div>', unsafe_allow_html=True)
    cfg = load_config()

    if not cfg:
        st.warning("Could not load config/default.yaml")
    else:
        import yaml

        # Highlight key thresholds
        strategy = cfg.get("strategy", {})
        signals = strategy.get("signals", {})
        entry = strategy.get("entry", {})
        exit_cfg = strategy.get("exit", {})
        sizing = strategy.get("sizing", {})
        risk = cfg.get("risk", {})

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**Divergence Signal**")
            st.metric("Min Gap (USD)", signals.get("divergence", {}).get("min_gap_usd", "—"))
            st.markdown("**Entry Window**")
            window = entry.get("window_seconds", [60, 180])
            st.metric("Entry Window (s)", f"{window[0]}–{window[1]}")

        with col2:
            st.markdown("**Imbalance Signal**")
            imb = signals.get("imbalance", {})
            st.metric("Buy Threshold", imb.get("buy_threshold", "—"))
            st.metric("Sell Threshold", imb.get("sell_threshold", "—"))
            det_window = imb.get("detection_window_seconds", [30, 90])
            st.metric("Detection Window (s)", f"{det_window[0]}–{det_window[1]}")

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
            result = subprocess.run(
                ["pm2", "jlist"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                import json
                processes = json.loads(result.stdout)
                for proc in processes:
                    name = proc.get("name", "")
                    status = proc.get("pm2_env", {}).get("status", "?")
                    restarts = proc.get("pm2_env", {}).get("restart_time", 0)
                    uptime = proc.get("pm2_env", {}).get("pm_uptime", 0)
                    status_color = "#0ECB81" if status == "online" else "#F6465D"
                    st.markdown(
                        f'<div style="padding:0.4rem 0.6rem;margin:0.2rem 0;'
                        f'background:rgba(0,0,0,0.15);border-radius:3px;font-family:monospace;font-size:0.8rem;">'
                        f'<span style="color:#EAECEF;">{name}</span> &nbsp;'
                        f'<span style="color:{status_color};font-weight:700;">{status}</span> &nbsp;'
                        f'<span style="color:#848E9C;">↺ {restarts}</span>'
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
            lines = p.read_text(errors="replace").count("\n") if p.suffix in (".jsonl", ".log") else "—"
            file_info.append({"File": fname, "Size (KB)": f"{size_kb:.1f}", "Lines": lines})
        else:
            file_info.append({"File": fname, "Size (KB)": "—", "Lines": "missing"})

    import pandas as pd
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
    st.caption("Test what the divergence and imbalance checks produce at different prices.")

    cfg = load_config()
    sig_cfg = cfg.get("strategy", {}).get("signals", {})

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Divergence Check**")
        min_gap = float(sig_cfg.get("divergence", {}).get("min_gap_usd", 50.0))
        sim_ptb = st.number_input("Price to Beat ($)", value=93000.0, step=100.0)
        sim_binance = st.number_input("Binance Price ($)", value=93000.0, step=10.0)
        sim_coinbase = st.number_input("Coinbase Price ($)", value=93000.0, step=10.0)
        b_gap = sim_binance - sim_ptb
        c_gap = sim_coinbase - sim_ptb
        div_up = b_gap > min_gap and c_gap > min_gap
        div_dn = b_gap < -min_gap and c_gap < -min_gap
        div_result = "UP" if div_up else ("DOWN" if div_dn else "NO SIGNAL")
        div_color = "#0ECB81" if div_up or div_dn else "#F6465D"
        st.markdown(f"Binance Δ: **{b_gap:+.2f}** · Coinbase Δ: **{c_gap:+.2f}** · Min gap: ±{min_gap}")
        st.markdown(f'Result: <span style="color:{div_color};font-weight:700;">{div_result}</span>', unsafe_allow_html=True)

    with col2:
        st.markdown("**Imbalance Check**")
        buy_thresh = float(sig_cfg.get("imbalance", {}).get("buy_threshold", 1.8))
        sell_thresh = float(sig_cfg.get("imbalance", {}).get("sell_threshold", 0.55))
        sim_ratio = st.number_input("Imbalance Ratio (bid/ask)", value=1.0, step=0.05, min_value=0.0, max_value=10.0)
        imb_up = sim_ratio >= buy_thresh
        imb_dn = sim_ratio <= sell_thresh
        imb_result = "UP (buy dominant)" if imb_up else ("DOWN (sell dominant)" if imb_dn else "NO SIGNAL")
        imb_color = "#0ECB81" if imb_up or imb_dn else "#F6465D"
        st.markdown(f"Buy threshold: ≥{buy_thresh} · Sell threshold: ≤{sell_thresh}")
        st.markdown(f'Result: <span style="color:{imb_color};font-weight:700;">{imb_result}</span>', unsafe_allow_html=True)

    st.markdown("---")
    confluence_ok = (div_up and imb_up) or (div_dn and imb_dn)
    if confluence_ok:
        st.success("CONFLUENCE — both signals agree. Trade would fire.")
    elif (div_up or div_dn) and not (imb_up or imb_dn):
        st.warning("Divergence signal present but no imbalance confluence. Trade skipped.")
    elif not (div_up or div_dn) and (imb_up or imb_dn):
        st.warning("Imbalance signal present but no price divergence. Trade skipped.")
    elif div_up and imb_dn or div_dn and imb_up:
        st.error("Direction mismatch — signals disagree. Trade skipped.")
    else:
        st.error("No signals. Trade skipped.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Recent Evaluation Stats**")
    evals = load_evaluations(last_n=200)
    if evals:
        with_div = [e for e in evals if e.get("div_direction")]
        avg_b_delta = sum(abs(e.get("binance_delta") or 0) for e in evals) / len(evals)
        avg_ratio = [e.get("imbalance_ratio") for e in evals if e.get("imbalance_ratio") is not None]
        avg_r = sum(avg_ratio) / len(avg_ratio) if avg_ratio else 0

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.metric("Avg |Binance Δ|", f"${avg_b_delta:.2f}")
        with sc2:
            st.metric("Divergence hit rate", f"{100*len(with_div)/len(evals):.1f}%")
        with sc3:
            st.metric("Avg imbalance ratio", f"{avg_r:.3f}")
