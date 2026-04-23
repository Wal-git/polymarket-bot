"""Shared loaders, styling, and sidebar for the Polybot dashboard.

All file reads go through this module. Loaders are cached with ``st.cache_data``
so the autorefresh + page navigation don't hammer the disk. Each loader returns
a safe empty default when the file is absent — pages show a waiting state
rather than crashing.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import streamlit as st
import yaml

_STATE_FILE = Path("data/state.json")
_CYCLES_FILE = Path("data/cycles.jsonl")
_SIGNALS_FILE = Path("data/signals.jsonl")
_EVALS_FILE = Path("data/evaluations.jsonl")
_RESULTS_FILE = Path("data/results.jsonl")
_BOT_LOG_FILE = Path("data/bot.log")
_BALANCE_FILE = Path("data/balance.json")
_CONFIG_FILE = Path("config/default.yaml")


@st.cache_data(ttl=5)
def load_state() -> dict[str, Any]:
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@st.cache_data(ttl=5)
def load_cycles(last_n: int = 200) -> list[dict]:
    return _tail_jsonl(_CYCLES_FILE, last_n)


@st.cache_data(ttl=5)
def load_signals(last_n: int = 200) -> list[dict]:
    return _tail_jsonl(_SIGNALS_FILE, last_n)


@st.cache_data(ttl=10)
def load_balance() -> dict[str, Any]:
    try:
        return json.loads(_BALANCE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@st.cache_data(ttl=5)
def load_evaluations(last_n: int = 200) -> list[dict]:
    return _tail_jsonl(_EVALS_FILE, last_n)


@st.cache_data(ttl=5)
def load_results() -> dict[str, dict]:
    """Return results keyed by slug for O(1) lookup in card rendering."""
    records = _tail_jsonl(_RESULTS_FILE, 500)
    return {r["slug"]: r for r in records if "slug" in r}


@st.cache_data(ttl=5)
def load_bot_log(last_n: int = 100) -> list[str]:
    try:
        lines = _BOT_LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-last_n:]
    except FileNotFoundError:
        return []


@st.cache_data(ttl=30)
def load_config() -> dict[str, Any]:
    try:
        return yaml.safe_load(_CONFIG_FILE.read_text(encoding="utf-8")) or {}
    except (FileNotFoundError, yaml.YAMLError):
        return {}


def _tail_jsonl(path: Path, last_n: int) -> list[dict]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    out: list[dict] = []
    for line in reversed(lines[-last_n:]):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def get_halt_path() -> Path:
    cfg = load_config()
    return Path(cfg.get("bot", {}).get("halt_file", "./HALT"))


def latest_cycle() -> dict:
    cycles = load_cycles(last_n=1)
    return cycles[0] if cycles else {}


def latest_evaluation() -> dict:
    evals = load_evaluations(last_n=1)
    return evals[0] if evals else {}


def cycle_age_seconds() -> float | None:
    # Prefer evaluations (new BTC engine) over cycles (old engine)
    record = latest_evaluation() or latest_cycle()
    ts = record.get("ts")
    if not ts:
        return None
    try:
        cycle_time = datetime.fromisoformat(ts)
    except ValueError:
        return None
    return (datetime.now(cycle_time.tzinfo or timezone.utc) - cycle_time).total_seconds()


def inject_styles() -> None:
    """Inject Google Fonts + global CSS. Call once per page before any st.* widget."""
    _html = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* Polybot dashboard — Binance-inspired design system */
html, body, [class*="css"] { font-family: 'Inter', Arial, sans-serif !important; }
.block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; }

.status-badge { display:inline-block; font-family:'Inter',sans-serif; font-weight:600;
    font-size:0.85rem; letter-spacing:0.08em; padding:0.35rem 0.75rem; border-radius:4px;
    width:100%; text-align:center; }
.status-badge.live   { background:rgba(14,203,129,0.12); color:#0ECB81;
    border:1px solid rgba(14,203,129,0.25); }
.status-badge.halted { background:rgba(246,70,93,0.12);  color:#F6465D;
    border:1px solid rgba(246,70,93,0.25); }
.status-badge.dryrun { background:rgba(240,185,11,0.12); color:#F0B90B;
    border:1px solid rgba(240,185,11,0.25); }

.kpi-block { padding:0.75rem 0 0.5rem 0; border-bottom:1px solid rgba(255,255,255,0.06); }
.kpi-label { font-family:'Inter',sans-serif; font-size:0.7rem; font-weight:500;
    letter-spacing:0.1em; text-transform:uppercase; color:#848E9C; margin-bottom:0.2rem; }
.kpi-value { font-family:'Barlow Condensed',sans-serif; font-size:2rem; font-weight:700;
    font-variant-numeric:tabular-nums; line-height:1; color:#EAECEF; }
.kpi-value.positive { color:#0ECB81; }
.kpi-value.negative { color:#F6465D; }
.kpi-value.amber    { color:#F0B90B; }

.page-header { font-family:'Barlow Condensed',sans-serif; font-size:1.1rem; font-weight:600;
    letter-spacing:0.12em; text-transform:uppercase; color:#848E9C;
    padding-bottom:0.5rem; border-bottom:1px solid rgba(255,255,255,0.06); margin-bottom:1rem; }

[data-testid="stDataFrame"] table { font-family:'Inter',sans-serif !important;
    font-size:0.82rem !important; font-variant-numeric:tabular-nums; }
[data-testid="stDataFrame"] th { font-size:0.7rem !important; letter-spacing:0.06em;
    text-transform:uppercase; color:#848E9C !important; }

[data-testid="stSidebar"] [data-testid="baseButton-primary"] {
    background-color:#F0B90B !important; border-color:#F0B90B !important;
    color:#1E2026 !important; font-family:'Inter',sans-serif !important;
    font-weight:600 !important; letter-spacing:0.06em; border-radius:6px !important;
    transition:background-color 200ms ease !important; }
[data-testid="stSidebar"] [data-testid="baseButton-primary"]:hover {
    background-color:#D0980B !important; border-color:#D0980B !important; }

.signal-card {
    padding: 0.75rem 1rem;
    margin: 0.5rem 0;
    background: rgba(255,255,255,0.02);
    border-left: 3px solid #848E9C;
    border-radius: 4px;
}
.signal-card.approved { border-left-color: #0ECB81; }
.signal-card.rejected { border-left-color: #F6465D; opacity: 0.78; }
.signal-meta {
    font-family: 'Inter', sans-serif;
    font-size: 0.7rem;
    color: #848E9C;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}
.signal-question {
    font-family: 'Inter', sans-serif;
    font-size: 0.95rem;
    color: #EAECEF;
    font-weight: 500;
    margin: 0.25rem 0;
}
.signal-rationale {
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    color: #B7BDC6;
    margin: 0.25rem 0;
    font-style: italic;
}
.signal-orders {
    font-family: 'Barlow Condensed', sans-serif;
    font-size: 0.95rem;
    color: #EAECEF;
    font-variant-numeric: tabular-nums;
    margin-top: 0.25rem;
}
.confidence-bar {
    display: inline-block;
    width: 80px;
    height: 6px;
    background: rgba(255,255,255,0.08);
    border-radius: 2px;
    overflow: hidden;
    vertical-align: middle;
    margin: 0 0.5rem;
}
.confidence-fill {
    height: 100%;
    background: #F0B90B;
}
</style>
"""
    try:
        st.html(_html)
    except AttributeError:
        st.markdown(_html, unsafe_allow_html=True)


def render_sidebar() -> None:
    """Render the persistent sidebar: status, HALT toggle, last eval, balance."""
    halt_path = get_halt_path()
    is_halted = halt_path.exists()
    cfg = load_config()
    dry_run = cfg.get("bot", {}).get("dry_run", True)
    age = cycle_age_seconds()
    state = load_state()
    bal = load_balance()

    with st.sidebar:
        if is_halted:
            st.markdown(
                '<div class="status-badge halted">⊗ &nbsp;BOT HALTED</div>',
                unsafe_allow_html=True,
            )
        elif dry_run:
            st.markdown(
                '<div class="status-badge dryrun">◉ &nbsp;DRY RUN</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="status-badge live">● &nbsp;LIVE</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")

        if is_halted:
            if st.button("▶ RESUME BOT", use_container_width=True, type="secondary"):
                halt_path.unlink(missing_ok=True)
                st.cache_data.clear()
                st.rerun()
            st.caption("Bot is paused. Click to resume.")
        else:
            if st.button("⏹ HALT BOT", use_container_width=True, type="primary"):
                halt_path.write_text("halt\n", encoding="utf-8")
                st.cache_data.clear()
                st.rerun()
            st.caption("Takes effect after the current slot.")

        st.markdown("---")

        if age is None:
            st.markdown("**Last eval** — no data yet")
        elif age < 60:
            st.markdown(f"**Last eval** — {int(age)}s ago")
        elif age < 600:
            st.markdown(f"**Last eval** — {int(age / 60)}m ago")
        else:
            st.markdown(
                f'<span style="color:#F6465D">**Last eval** — {int(age / 60)}m ago ⚠ stale</span>',
                unsafe_allow_html=True,
            )

        positions = state.get("positions", [])
        trades = state.get("trades", [])
        realized = sum(float(p.get("realized_pnl") or 0) for p in positions)
        unrealized = sum(float(p.get("unrealized_pnl") or 0) for p in positions)
        total_pnl = realized + unrealized
        pnl_color = "#0ECB81" if total_pnl >= 0 else "#F6465D"
        pnl_str = f"+${total_pnl:,.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.2f}"

        st.markdown(f"""
<div style="margin:0.5rem 0 0.25rem 0;">
  <div style="font-family:'Inter',sans-serif;font-size:0.65rem;font-weight:500;
              letter-spacing:0.1em;text-transform:uppercase;color:#848E9C;margin-bottom:0.15rem;">
    Total P&L</div>
  <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.6rem;
              font-weight:700;color:{pnl_color};font-variant-numeric:tabular-nums;line-height:1;">
    {pnl_str}</div>
</div>""", unsafe_allow_html=True)

        if bal:
            try:
                clob_f = float(bal["balance"])
                portfolio_f = float(bal["portfolio_value"]) if "portfolio_value" in bal else None
                total_f = float(bal["total_value"]) if "total_value" in bal else clob_f
                import time as _time
                bal_age = int(_time.time() - float(bal.get("ts", 0)))
                age_str = f"{bal_age}s ago" if bal_age < 120 else f"{bal_age // 60}m ago"
                portfolio_row = ""
                if portfolio_f is not None:
                    portfolio_row = f"""
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-top:0.3rem;">
    <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:#848E9C;">Polymarket wallet</div>
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:600;
                color:#848E9C;font-variant-numeric:tabular-nums;">${portfolio_f:,.2f}</div>
  </div>
  <div style="display:flex;justify-content:space-between;align-items:baseline;
              border-top:1px solid rgba(255,255,255,0.06);margin-top:0.3rem;padding-top:0.3rem;">
    <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:#EAECEF;font-weight:600;">Total</div>
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;
                color:#EAECEF;font-variant-numeric:tabular-nums;">${total_f:,.2f}</div>
  </div>"""
                st.markdown(f"""
<div style="margin:0.5rem 0 0.25rem 0;">
  <div style="font-family:'Inter',sans-serif;font-size:0.65rem;font-weight:500;
              letter-spacing:0.1em;text-transform:uppercase;color:#848E9C;margin-bottom:0.4rem;">
    Balance (USDC)</div>
  <div style="display:flex;justify-content:space-between;align-items:baseline;">
    <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:#848E9C;">CLOB (tradeable)</div>
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.6rem;
                font-weight:700;color:#F0B90B;font-variant-numeric:tabular-nums;line-height:1;">
      ${clob_f:,.2f}</div>
  </div>
  {portfolio_row}
  <div style="font-family:'Inter',sans-serif;font-size:0.65rem;color:#848E9C;margin-top:0.25rem;">
    updated {age_str}</div>
</div>""", unsafe_allow_html=True)
            except (TypeError, ValueError):
                pass

        st.markdown(f"**Open positions** — {len(positions)}")
        st.markdown(f"**Total trades** — {len(trades)}")

        st.markdown("---")
        st.caption("Auto-refreshes every 10s")
        if st.button("↺ Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()


def render_signal_card(sig: dict) -> None:
    """Render a single signal as a styled card."""
    approved = sig.get("approved", False)
    klass = "approved" if approved else "rejected"
    badge_color = "#0ECB81" if approved else "#F6465D"
    badge_text = "APPROVED" if approved else "REJECTED"
    confidence = float(sig.get("confidence") or 0)
    conf_pct = int(confidence * 100)
    question = sig.get("market_question") or "(unknown market)"
    rationale = sig.get("rationale") or ""
    strategy = sig.get("strategy") or ""
    ts = (sig.get("ts") or "")[:19].replace("T", " ")

    orders_html = ""
    for o in sig.get("orders", []):
        side = o.get("side", "")
        size = o.get("size", "")
        price = o.get("limit_price") or "—"
        side_color = "#0ECB81" if side == "BUY" else "#F6465D"
        orders_html += (
            f'<span style="color:{side_color};font-weight:600;">{side}</span> '
            f'{size} @ ${price} &nbsp;'
        )

    reject_reason = sig.get("reject_reason") or ""
    reject_line = (
        f'<div class="signal-rationale" style="color:#F6465D;font-style:normal;">⊗ {reject_reason}</div>'
        if reject_reason else ""
    )

    st.markdown(f"""
    <div class="signal-card {klass}">
        <div class="signal-meta">
            <span style="color:{badge_color};font-weight:700;">{badge_text}</span>
            &nbsp;·&nbsp; {strategy} &nbsp;·&nbsp; {ts}
            <span class="confidence-bar"><span class="confidence-fill" style="width:{conf_pct}%;"></span></span>
            <span style="color:#F0B90B;font-weight:600;">{conf_pct}%</span>
        </div>
        <div class="signal-question">{question}</div>
        <div class="signal-rationale">{rationale}</div>
        <div class="signal-orders">{orders_html}</div>
        {reject_line}
    </div>
    """, unsafe_allow_html=True)
