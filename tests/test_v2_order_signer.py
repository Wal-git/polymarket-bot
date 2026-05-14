"""Hand-rolled v2_order signer must match SDK byte-for-byte.

If the SDK ever changes its signing logic, this test fails and we know to
either update the fallback or trust the SDK. The fallback is the safety net
for *SDK regressions*, not for forking the protocol — so it must agree.
"""
from polybot.client.v2_order import build_v2_poly_1271_order

from py_clob_client_v2.order_utils.exchange_order_builder_v2 import ExchangeOrderBuilderV2
from py_clob_client_v2.order_utils.model.order_data_v2 import OrderDataV2
from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2
from py_clob_client_v2.signer import Signer

PK = "0x" + "1" * 64
DEPOSIT_WALLET = "0xd31f05af327955214f71f9387c93ed8bd7f5770c"
TOKEN_ID = "78433024518676680431174478322854148606578065650008220678402966840627347604025"


def _sdk_sign(exchange: str, salt: int, side: int, maker_amt: int, taker_amt: int, ts_ms: int):
    signer = Signer(PK, 137)
    builder = ExchangeOrderBuilderV2(exchange, 137, signer, generate_salt=lambda: salt)
    return builder.build_signed_order(OrderDataV2(
        maker=DEPOSIT_WALLET,
        signer=DEPOSIT_WALLET,
        tokenId=TOKEN_ID,
        makerAmount=str(maker_amt),
        takerAmount=str(taker_amt),
        side=side,
        signatureType=SignatureTypeV2.POLY_1271,
        timestamp=str(ts_ms),
        metadata="0x" + "0" * 64,
        builder="0x" + "0" * 64,
        expiration="0",
    ))


def test_handrolled_matches_sdk_buy_regular():
    sdk = _sdk_sign("0xE111180000d2663C0091e4f400237545B87B996B", 12345, 0, 50000, 5000000, 1778728800000)
    ours = build_v2_poly_1271_order(
        private_key=PK, deposit_wallet=DEPOSIT_WALLET,
        token_id=TOKEN_ID, maker_amount=50000, taker_amount=5000000,
        side=0, timestamp_ms=1778728800000, salt=12345, neg_risk=False,
    )
    assert sdk.signature.lower() == ours["signature"].lower()


def test_handrolled_matches_sdk_sell_negrisk():
    sdk = _sdk_sign("0xe2222d279d744050d28e00520010520000310F59", 999, 1, 1_000_000, 100_000_000, 1778729999999)
    ours = build_v2_poly_1271_order(
        private_key=PK, deposit_wallet=DEPOSIT_WALLET,
        token_id=TOKEN_ID, maker_amount=1_000_000, taker_amount=100_000_000,
        side=1, timestamp_ms=1778729999999, salt=999, neg_risk=True,
    )
    assert sdk.signature.lower() == ours["signature"].lower()
