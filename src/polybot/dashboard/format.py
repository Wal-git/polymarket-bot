"""Formatting helpers shared across dashboard pages.

One PDT timezone, one timestamp formatter (with format presets), and the
P&L color/string logic that used to be copy-pasted into every page.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from polybot.dashboard.loaders import load_config
from polybot.dashboard.theme import PALETTE

# Pacific Daylight Time. The bot/dashboard report in Pacific year-round.
PDT = timezone(timedelta(hours=-7))

# Named output formats for to_pdt — covers the three variants the pages used.
TIME_FORMATS = {
    "clock": "%-I:%M %p PDT",      # 3_History, 1_Live_Feed
    "datetime": "%-m/%-d %-I:%M %p",  # 2_Positions
    "iso": "%Y-%m-%d %H:%M:%S",
}


def to_pdt(iso: str, fmt: str = "clock") -> str:
    """Format an ISO timestamp in Pacific time. ``fmt`` is a key of TIME_FORMATS
    (or a raw strftime string). Falls back to the raw ISO head on parse failure.
    """
    pattern = TIME_FORMATS.get(fmt, fmt)
    try:
        return datetime.fromisoformat(iso).astimezone(PDT).strftime(pattern)
    except Exception:
        return (iso or "")[:19].replace("T", " ")


def pnl_color(value: float) -> str:
    """Green when non-negative, red when negative."""
    return PALETTE.GREEN if value >= 0 else PALETTE.RED


def pnl_str(value: float) -> str:
    """Signed dollar string: ``+$1,234.56`` / ``-$1,234.56``."""
    return f"+${value:,.2f}" if value >= 0 else f"-${abs(value):,.2f}"


def fmt_pct(value: float) -> str:
    """Signed percent string: ``+1.2%`` / ``-1.2%``."""
    return f"{value:+.1f}%"


def strip_slug_prefix(slug: str) -> str:
    """Strip a known asset slug prefix (`btc-updown-5m-`, `eth-updown-5m-`,
    or whatever's in config) so the timestamp tail is human-readable.
    """
    cfg = load_config()
    for body in (cfg.get("assets") or {}).values():
        prefix = body.get("slug_prefix", "")
        if prefix and slug.startswith(prefix + "-"):
            return slug[len(prefix) + 1:]
    return slug
