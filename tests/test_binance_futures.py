import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polybot.feeds import binance_futures
from polybot.models.btc_market import FuturesSnapshot


@pytest.fixture(autouse=True)
def reset_caches():
    binance_futures.reset_cache()
    yield
    binance_futures.reset_cache()


def _mock_session(payload: dict):
    """Build an aiohttp-like session mock whose .get() returns ``payload`` as JSON."""
    resp = MagicMock()
    resp.json = AsyncMock(return_value=payload)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.get = MagicMock(return_value=resp)
    session.close = AsyncMock()
    return session


_OK_PAYLOAD = {
    "symbol": "BTCUSDT",
    "markPrice": "78900.50",
    "indexPrice": "78920.00",
    "lastFundingRate": "0.00012345",
    "nextFundingTime": 1_777_300_000_000,
    "time": 1_777_265_000_000,
}


class TestFetchFuturesSnapshot:
    def test_returns_decoded_snapshot(self):
        session = _mock_session(_OK_PAYLOAD)
        result = asyncio.run(binance_futures.fetch_futures_snapshot(session=session))
        assert result is not None
        assert result.mark_price == 78_900.50
        assert result.index_price == 78_920.00
        assert result.last_funding_rate == 0.00012345
        assert result.next_funding_time_ms == 1_777_300_000_000

    def test_returns_none_on_http_error(self):
        resp = MagicMock()
        resp.json = AsyncMock(side_effect=RuntimeError("502 bad gateway"))
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=None)
        session = MagicMock()
        session.get = MagicMock(return_value=resp)
        result = asyncio.run(binance_futures.fetch_futures_snapshot(session=session))
        assert result is None

    def test_caches_within_ttl(self):
        session = _mock_session(_OK_PAYLOAD)
        first = asyncio.run(binance_futures.fetch_futures_snapshot(session=session))
        second = asyncio.run(binance_futures.fetch_futures_snapshot(session=session))
        assert first is second  # same cached object
        # Second call must not have hit the session
        assert session.get.call_count == 1
