"""Persistent sidebar and the asset-filter session state it owns."""
from __future__ import annotations

import time

import streamlit as st

from polybot.dashboard.components import kpi_html
from polybot.dashboard.format import fmt_pct, pnl_color, pnl_str
from polybot.dashboard.loaders import (
    STARTING_BALANCE,
    cycle_age_seconds,
    get_halt_path,
    load_balance,
    load_config,
    load_state,
)
from polybot.dashboard.theme import PALETTE

_ASSET_FILTER_KEY = "selected_asset"
_ASSET_FILTER_ALL = "All"


def selected_asset() -> str | None:
    """Return the asset filter selected in the sidebar, or None for 'All'."""
    val = st.session_state.get(_ASSET_FILTER_KEY, _ASSET_FILTER_ALL)
    return None if val == _ASSET_FILTER_ALL else val


def apply_asset_filter(records: list[dict], asset: str | None = None) -> list[dict]:
    """Filter records by asset. Records without an explicit ``asset`` field
    were written before the multi-asset refactor and default to BTC.
    """
    if asset is None:
        asset = selected_asset()
    if asset is None:
        return records
    return [r for r in records if r.get("asset", "BTC") == asset]


def configured_asset_names() -> list[str]:
    """Names of assets enabled in config — used to populate the sidebar filter."""
    cfg = load_config()
    return [
        name for name, body in (cfg.get("assets") or {}).items()
        if body.get("enabled", True)
    ]


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
            st.markdown('<div class="status-badge halted">⊗ &nbsp;BOT HALTED</div>',
                        unsafe_allow_html=True)
        elif dry_run:
            st.markdown('<div class="status-badge dryrun">◉ &nbsp;DRY RUN</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="status-badge live">● &nbsp;LIVE</div>',
                        unsafe_allow_html=True)

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

        # Asset filter — only render when more than one asset is configured.
        asset_names = configured_asset_names()
        if len(asset_names) > 1:
            st.markdown("---")
            options = [_ASSET_FILTER_ALL] + asset_names
            current = st.session_state.get(_ASSET_FILTER_KEY, _ASSET_FILTER_ALL)
            if current not in options:
                current = _ASSET_FILTER_ALL
            st.selectbox("Asset", options, index=options.index(current),
                         key=_ASSET_FILTER_KEY)

        st.markdown("---")

        if age is None:
            st.markdown("**Last eval** — no data yet")
        elif age < 60:
            st.markdown(f"**Last eval** — {int(age)}s ago")
        elif age < 600:
            st.markdown(f"**Last eval** — {int(age / 60)}m ago")
        else:
            st.markdown(
                f'<span style="color:{PALETTE.RED}">**Last eval** — {int(age / 60)}m ago ⚠ stale</span>',
                unsafe_allow_html=True,
            )

        positions = state.get("positions", [])
        trades = state.get("trades", [])
        total_value = float(bal.get("total_value", 0)) if bal else 0.0
        total_pnl = (total_value - STARTING_BALANCE) if (bal and total_value > 0) else 0.0
        pnl_pct = total_pnl / STARTING_BALANCE * 100
        st.markdown(
            kpi_html("Total P&L", pnl_str(total_pnl), color=pnl_color(total_pnl),
                     sub=fmt_pct(pnl_pct), sub_color=pnl_color(total_pnl)),
            unsafe_allow_html=True,
        )

        if bal:
            _render_balance_block(bal)

        st.markdown(f"**Open positions** — {len(positions)}")
        st.markdown(f"**Total trades** — {len(trades)}")

        st.markdown("---")
        st.caption("Auto-refreshes every 10s")
        if st.button("↺ Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()


def _render_balance_block(bal: dict) -> None:
    """Render the CLOB / Polymarket-wallet / Total balance summary."""
    try:
        clob_f = float(bal["balance"])
        portfolio_f = float(bal["portfolio_value"]) if "portfolio_value" in bal else None
        total_f = float(bal["total_value"]) if "total_value" in bal else clob_f
        bal_age = int(time.time() - float(bal.get("ts", 0)))
        age_str = f"{bal_age}s ago" if bal_age < 120 else f"{bal_age // 60}m ago"
    except (TypeError, ValueError, KeyError):
        return

    portfolio_row = ""
    if portfolio_f is not None:
        portfolio_row = f"""
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-top:0.3rem;">
    <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:{PALETTE.GREY};">Polymarket wallet</div>
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:600;
                color:{PALETTE.GREY};font-variant-numeric:tabular-nums;">${portfolio_f:,.2f}</div>
  </div>
  <div style="display:flex;justify-content:space-between;align-items:baseline;
              border-top:1px solid rgba(255,255,255,0.06);margin-top:0.3rem;padding-top:0.3rem;">
    <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:{PALETTE.WHITE};font-weight:600;">Total</div>
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;
                color:{PALETTE.WHITE};font-variant-numeric:tabular-nums;">${total_f:,.2f}</div>
  </div>"""

    st.markdown(f"""
<div style="margin:0.5rem 0 0.25rem 0;">
  <div style="font-family:'Inter',sans-serif;font-size:0.65rem;font-weight:500;
              letter-spacing:0.1em;text-transform:uppercase;color:{PALETTE.GREY};margin-bottom:0.4rem;">
    Balance (USDC)</div>
  <div style="display:flex;justify-content:space-between;align-items:baseline;">
    <div style="font-family:'Inter',sans-serif;font-size:0.7rem;color:{PALETTE.GREY};">CLOB (tradeable)</div>
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:1.6rem;
                font-weight:700;color:{PALETTE.AMBER};font-variant-numeric:tabular-nums;line-height:1;">
      ${clob_f:,.2f}</div>
  </div>
  {portfolio_row}
  <div style="font-family:'Inter',sans-serif;font-size:0.65rem;color:{PALETTE.GREY};margin-top:0.25rem;">
    updated {age_str}</div>
</div>""", unsafe_allow_html=True)
