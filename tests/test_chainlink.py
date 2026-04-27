import time
from unittest.mock import MagicMock, patch

import pytest

from polybot.feeds import chainlink
from polybot.models.btc_market import ChainlinkRound


@pytest.fixture(autouse=True)
def reset_caches():
    chainlink.reset_cache()
    yield
    chainlink.reset_cache()


def _mock_w3_with_round(round_id: int, raw_answer: int, updated_at: int, decimals: int = 8):
    """Build a Web3 mock whose contract returns a controllable latestRoundData()."""
    mock_w3 = MagicMock()
    mock_w3.provider.endpoint_uri = "mock://"
    contract = MagicMock()
    contract.functions.latestRoundData.return_value.call.return_value = (
        round_id, raw_answer, 0, updated_at, round_id
    )
    contract.functions.decimals.return_value.call.return_value = decimals
    mock_w3.eth.contract.return_value = contract
    return mock_w3


class TestFetchChainlinkRoundSync:
    def test_returns_decoded_round(self):
        mock_w3 = _mock_w3_with_round(
            round_id=12345, raw_answer=8_700_000_000_000, updated_at=1_000_000_000
        )
        with patch.object(chainlink, "_get_web3", return_value=mock_w3):
            with patch("polybot.feeds.chainlink.Web3.to_checksum_address", side_effect=lambda x: x):
                result = chainlink.fetch_chainlink_round_sync(
                    rpc_url="mock://", address="0xabc"
                )
        assert result is not None
        assert result.answer == 87_000.0  # 8.7e12 / 1e8
        assert result.updated_at == 1_000_000_000
        assert result.round_id == 12345

    def test_returns_none_on_rpc_failure(self):
        mock_w3 = MagicMock()
        mock_w3.provider.endpoint_uri = "mock://"
        contract = MagicMock()
        contract.functions.decimals.return_value.call.return_value = 8
        contract.functions.latestRoundData.return_value.call.side_effect = RuntimeError("rpc down")
        mock_w3.eth.contract.return_value = contract
        with patch.object(chainlink, "_get_web3", return_value=mock_w3):
            with patch("polybot.feeds.chainlink.Web3.to_checksum_address", side_effect=lambda x: x):
                result = chainlink.fetch_chainlink_round_sync(rpc_url="mock://", address="0xabc")
        assert result is None

    def test_caches_within_ttl(self):
        mock_w3 = _mock_w3_with_round(
            round_id=1, raw_answer=8_500_000_000_000, updated_at=int(time.time())
        )
        contract_factory = mock_w3.eth.contract
        with patch.object(chainlink, "_get_web3", return_value=mock_w3):
            with patch("polybot.feeds.chainlink.Web3.to_checksum_address", side_effect=lambda x: x):
                first = chainlink.fetch_chainlink_round_sync(rpc_url="mock://", address="0xabc")
                second = chainlink.fetch_chainlink_round_sync(rpc_url="mock://", address="0xabc")
        assert first is second  # Same cached object reference
        # decimals() called once, latestRoundData() called once across both calls
        assert contract_factory.return_value.functions.latestRoundData.return_value.call.call_count == 1


class TestAsyncFetchWrapper:
    def test_async_wrapper_returns_round(self):
        import asyncio

        expected = ChainlinkRound(answer=87_000.0, updated_at=123, round_id=42)
        with patch.object(chainlink, "fetch_chainlink_round_sync", return_value=expected):
            result = asyncio.run(
                chainlink.fetch_chainlink_round(rpc_url="mock://", address="0xabc")
            )
        assert result is expected
