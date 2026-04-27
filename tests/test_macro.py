import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from polybot.feeds import macro
from polybot.models.btc_market import MacroSnapshot


@pytest.fixture(autouse=True)
def reset_caches():
    macro.reset_cache()
    yield
    macro.reset_cache()


def _yahoo_payload(latest: float | None = 100.0, hour_ago: float | None = 99.5):
    """Build a minimal Yahoo chart payload with two bars: 1 hour apart and now."""
    now = int(time.time())
    bars = []
    closes = []
    if hour_ago is not None:
        bars.append(now - 3600 - 30)  # 1h+30s ago — counts as ≤ 1h-ago target
        closes.append(hour_ago)
    if latest is not None:
        bars.append(now)
        closes.append(latest)
    return {
        "meta": {"regularMarketPrice": latest},
        "timestamp": bars,
        "indicators": {"quote": [{"close": closes}]},
    }


def _mock_session_with_payloads(by_symbol: dict[str, dict | None]):
    """Build an aiohttp session mock whose .get() returns the payload matching the URL."""
    def make_resp(payload):
        body = {"chart": {"result": [payload] if payload is not None else None}}
        resp = MagicMock()
        resp.json = AsyncMock(return_value=body)
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=None)
        return resp

    def get(url, **kwargs):
        for sym, payload in by_symbol.items():
            if sym in url:
                return make_resp(payload)
        return make_resp(None)

    session = MagicMock()
    session.get = MagicMock(side_effect=get)
    session.close = AsyncMock()
    return session


class TestFetchMacroSnapshot:
    def test_all_three_succeed(self):
        session = _mock_session_with_payloads({
            "^VIX": _yahoo_payload(latest=18.5, hour_ago=19.0),
            "DX-Y.NYB": _yahoo_payload(latest=104.2, hour_ago=104.0),
            "ES=F": _yahoo_payload(latest=5800.0, hour_ago=5780.0),
        })
        snap = asyncio.run(macro.fetch_macro_snapshot(session=session))
        assert snap.vix == 18.5
        assert snap.dxy == 104.2
        assert snap.es_price == 5800.0
        # 5800 / 5780 - 1 = 0.00346
        assert snap.es_pct_change_1h == pytest.approx(0.00346, abs=1e-4)

    def test_partial_failure_returns_partial_snapshot(self):
        session = _mock_session_with_payloads({
            "^VIX": _yahoo_payload(latest=18.5, hour_ago=19.0),
            "DX-Y.NYB": None,  # API down
            "ES=F": _yahoo_payload(latest=5800.0, hour_ago=5780.0),
        })
        snap = asyncio.run(macro.fetch_macro_snapshot(session=session))
        assert snap.vix == 18.5
        assert snap.dxy is None
        assert snap.es_price == 5800.0

    def test_caches_within_ttl(self):
        session = _mock_session_with_payloads({
            "^VIX": _yahoo_payload(latest=18.5, hour_ago=19.0),
            "DX-Y.NYB": _yahoo_payload(latest=104.2, hour_ago=104.0),
            "ES=F": _yahoo_payload(latest=5800.0, hour_ago=5780.0),
        })
        first = asyncio.run(macro.fetch_macro_snapshot(session=session))
        second = asyncio.run(macro.fetch_macro_snapshot(session=session))
        assert first is second
        # 3 GETs the first time, 0 the second
        assert session.get.call_count == 3

    def test_returns_snapshot_even_when_all_fail(self):
        session = _mock_session_with_payloads({})  # nothing matches → all None
        snap = asyncio.run(macro.fetch_macro_snapshot(session=session))
        assert isinstance(snap, MacroSnapshot)
        assert snap.vix is None
        assert snap.dxy is None
        assert snap.es_price is None
        assert snap.es_pct_change_1h is None


class TestPctChange1h:
    def test_returns_none_for_empty(self):
        assert macro._pct_change_1h(None) is None
        assert macro._pct_change_1h({"timestamp": [], "indicators": {"quote": [{"close": []}]}}) is None

    def test_returns_none_when_only_one_bar(self):
        payload = _yahoo_payload(latest=100.0, hour_ago=None)
        assert macro._pct_change_1h(payload) is None

    def test_handles_null_closes(self):
        # Several null bars at start — should still find the 1h-ago bar
        now = int(time.time())
        payload = {
            "timestamp": [now - 3700, now - 3600, now - 3000, now],
            "indicators": {"quote": [{"close": [None, 100.0, None, 102.0]}]},
        }
        result = macro._pct_change_1h(payload)
        assert result == pytest.approx(0.02)
