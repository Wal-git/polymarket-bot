"""Design system for the Polybot dashboard: palette, CSS, page setup.

Every color used anywhere in the dashboard lives in ``PALETTE`` so the look is
defined in one place. ``inject_styles`` ships the Google Fonts + global CSS
(built from the palette, so the stylesheet and inline styles can never drift).
``page_setup`` is the one-line per-page header that wires config + styles +
autorefresh + sidebar.
"""
from __future__ import annotations

import streamlit as st


class PALETTE:
    """Binance-inspired dark palette. Single source of truth for color."""

    GREEN = "#0ECB81"   # positive / up / live
    RED = "#F6465D"     # negative / down / halted
    AMBER = "#F0B90B"   # accent / highlight / dry-run
    GREY = "#848E9C"    # labels, muted text
    WHITE = "#EAECEF"   # primary text
    MUTED = "#B7BDC6"   # secondary text (rationale, log body)
    BG = "#1E2026"      # app background
    BG_DARK = "#0d0f12"  # log/console panel background


# CSS is built from the palette so a color change in PALETTE propagates here too.
_CSS = f"""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* Polybot dashboard — Binance-inspired design system */
html, body, [class*="css"] {{ font-family: 'Inter', Arial, sans-serif !important; }}
.block-container {{ padding-top: 1rem !important; padding-bottom: 1rem !important; }}

.status-badge {{ display:inline-block; font-family:'Inter',sans-serif; font-weight:600;
    font-size:0.85rem; letter-spacing:0.08em; padding:0.35rem 0.75rem; border-radius:4px;
    width:100%; text-align:center; }}
.status-badge.live   {{ background:rgba(14,203,129,0.12); color:{PALETTE.GREEN};
    border:1px solid rgba(14,203,129,0.25); }}
.status-badge.halted {{ background:rgba(246,70,93,0.12);  color:{PALETTE.RED};
    border:1px solid rgba(246,70,93,0.25); }}
.status-badge.dryrun {{ background:rgba(240,185,11,0.12); color:{PALETTE.AMBER};
    border:1px solid rgba(240,185,11,0.25); }}

.kpi-block {{ padding:0.75rem 0 0.5rem 0; border-bottom:1px solid rgba(255,255,255,0.06); }}
.kpi-label {{ font-family:'Inter',sans-serif; font-size:0.7rem; font-weight:500;
    letter-spacing:0.1em; text-transform:uppercase; color:{PALETTE.GREY}; margin-bottom:0.2rem; }}
.kpi-value {{ font-family:'Barlow Condensed',sans-serif; font-size:2rem; font-weight:700;
    font-variant-numeric:tabular-nums; line-height:1; color:{PALETTE.WHITE}; }}
.kpi-value.positive {{ color:{PALETTE.GREEN}; }}
.kpi-value.negative {{ color:{PALETTE.RED}; }}
.kpi-value.amber    {{ color:{PALETTE.AMBER}; }}
.kpi-sub {{ font-size:0.75rem; margin-top:0.15rem; color:{PALETTE.GREY}; }}

.page-header {{ font-family:'Barlow Condensed',sans-serif; font-size:1.1rem; font-weight:600;
    letter-spacing:0.12em; text-transform:uppercase; color:{PALETTE.GREY};
    padding-bottom:0.5rem; border-bottom:1px solid rgba(255,255,255,0.06); margin-bottom:1rem; }}

/* Shared card surface — replaces per-call inline padding/background/border blocks. */
.card {{ padding:0.75rem 1rem; margin:0.5rem 0; background:rgba(255,255,255,0.02);
    border-left:3px solid {PALETTE.GREY}; border-radius:4px;
    transition:background 160ms ease, border-color 160ms ease; }}
.card:hover {{ background:rgba(255,255,255,0.035); }}
.card.green {{ border-left-color:{PALETTE.GREEN}; }}
.card.red   {{ border-left-color:{PALETTE.RED}; }}
.card.amber {{ border-left-color:{PALETTE.AMBER}; }}
.card-head {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:0.5rem; }}
.card-badge {{ font-family:'Inter',sans-serif; font-size:0.75rem; font-weight:700;
    background:rgba(0,0,0,0.25); padding:0.2rem 0.5rem; border-radius:3px; }}
.tile-grid {{ display:grid; grid-template-columns:repeat(6,1fr); gap:0.4rem; margin-bottom:0.5rem; }}
.tile {{ background:rgba(0,0,0,0.15); padding:0.5rem; border-radius:3px; }}
.tile-label {{ font-size:0.65rem; color:{PALETTE.GREY}; text-transform:uppercase; letter-spacing:0.08em; }}
.tile-value {{ font-family:'Barlow Condensed',sans-serif; font-size:1rem; font-variant-numeric:tabular-nums; }}

.log-panel {{ font-family:monospace; font-size:0.75rem; line-height:1.6;
    background:{PALETTE.BG_DARK}; padding:1rem; border-radius:6px; overflow-x:auto; }}

[data-testid="stDataFrame"] table {{ font-family:'Inter',sans-serif !important;
    font-size:0.82rem !important; font-variant-numeric:tabular-nums; }}
[data-testid="stDataFrame"] th {{ font-size:0.7rem !important; letter-spacing:0.06em;
    text-transform:uppercase; color:{PALETTE.GREY} !important; }}

[data-testid="stSidebar"] [data-testid="baseButton-primary"] {{
    background-color:{PALETTE.AMBER} !important; border-color:{PALETTE.AMBER} !important;
    color:{PALETTE.BG} !important; font-family:'Inter',sans-serif !important;
    font-weight:600 !important; letter-spacing:0.06em; border-radius:6px !important;
    transition:background-color 200ms ease !important; }}
[data-testid="stSidebar"] [data-testid="baseButton-primary"]:hover {{
    background-color:#D0980B !important; border-color:#D0980B !important; }}

.signal-card {{ padding:0.75rem 1rem; margin:0.5rem 0; background:rgba(255,255,255,0.02);
    border-left:3px solid {PALETTE.GREY}; border-radius:4px; }}
.signal-card.approved {{ border-left-color:{PALETTE.GREEN}; }}
.signal-card.rejected {{ border-left-color:{PALETTE.RED}; opacity:0.78; }}
.signal-meta {{ font-family:'Inter',sans-serif; font-size:0.7rem; color:{PALETTE.GREY};
    letter-spacing:0.06em; text-transform:uppercase; }}
.signal-question {{ font-family:'Inter',sans-serif; font-size:0.95rem; color:{PALETTE.WHITE};
    font-weight:500; margin:0.25rem 0; }}
.signal-rationale {{ font-family:'Inter',sans-serif; font-size:0.85rem; color:{PALETTE.MUTED};
    margin:0.25rem 0; font-style:italic; }}
.signal-orders {{ font-family:'Barlow Condensed',sans-serif; font-size:0.95rem; color:{PALETTE.WHITE};
    font-variant-numeric:tabular-nums; margin-top:0.25rem; }}
.confidence-bar {{ display:inline-block; width:80px; height:6px; background:rgba(255,255,255,0.08);
    border-radius:2px; overflow:hidden; vertical-align:middle; margin:0 0.5rem; }}
.confidence-fill {{ height:100%; background:{PALETTE.AMBER}; }}
</style>
"""


def inject_styles() -> None:
    """Inject Google Fonts + global CSS. Call once per page before any st.* widget."""
    try:
        st.html(_CSS)
    except AttributeError:
        st.markdown(_CSS, unsafe_allow_html=True)


def page_setup(title: str, *, refresh_ms: int | None = None, icon: str = "◇") -> None:
    """One-line per-page header: config + styles + optional autorefresh + sidebar.

    Must run before any other ``st.*`` call on the page (``set_page_config`` and
    ``inject_styles`` both require it).
    """
    st.set_page_config(page_title=title, page_icon=icon, layout="wide")
    inject_styles()
    if refresh_ms is not None:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=refresh_ms, key=f"refresh_{title}")
    # Imported lazily to avoid a circular import (sidebar imports from theme).
    from polybot.dashboard.sidebar import render_sidebar
    render_sidebar()
