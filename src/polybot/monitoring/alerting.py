"""Fire-and-forget trade alerts via Telegram.

Requires two env vars (set in your .env file):
  TELEGRAM_BOT_TOKEN   — bot token from @BotFather
  TELEGRAM_CHAT_ID     — your personal or group chat ID

If either var is missing the module silently no-ops so the bot runs
normally without alerting configured.
"""
from __future__ import annotations

import asyncio
import os

import structlog

logger = structlog.get_logger()


def send_alert(message: str) -> None:
    """Schedule a Telegram alert as a background asyncio task (non-blocking).

    Safe to call from any async context. No-ops when not in an event loop
    or when Telegram credentials are absent.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_telegram(message))
    except RuntimeError:
        pass  # no running event loop — skip silently


def blocked_message(
    asset: str,
    slug: str,
    direction: str,
    confidence: float,
    size_usdc: float,
    reason: str,
    detail: str = "",
) -> str:
    lines = [
        f"⚠️ <b>Signal blocked — {asset}</b>",
        f"<code>{slug}</code>",
        f"Direction: {direction} | Confidence: {confidence:.0%} | Size: ${size_usdc:.0f}",
        f"Reason: {reason}",
    ]
    if detail:
        lines.append(detail)
    return "\n".join(lines)


def error_message(asset: str, slug: str, error: str) -> str:
    return "\n".join([
        f"🚨 <b>Lifecycle error — {asset}</b>",
        f"<code>{slug}</code>",
        f"Error: {error[:300]}",
    ])


async def _telegram(message: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=10,
            )
            if not r.is_success:
                logger.warning("telegram_alert_failed", status=r.status_code, body=r.text[:200])
    except Exception as e:
        logger.warning("telegram_alert_error", error=str(e))
