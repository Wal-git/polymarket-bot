"""
Polymarket CLOB V2 order signing.

V2 changes vs V1:
 - New exchange: 0xE111180000d2663C0091e4f400237545B87B996B (regular)
                 0xe2222d279d744050d28e00520010520000310F59 (neg-risk)
 - Domain version: "2"
 - Order struct: salt, maker, signer, tokenId, makerAmount, takerAmount,
                 side, signatureType, timestamp, metadata, builder
   (removed: taker, nonce, feeRateBps, expiration from struct)
 - POST body adds: taker (zero addr), expiration, deferExec fields
"""
from __future__ import annotations

import json
import os
import random
import time
from decimal import Decimal

import httpx
from eth_account import Account
from eth_utils import keccak

from py_clob_client.clob_types import RequestArgs
from py_clob_client.headers.headers import create_level_2_headers
from py_clob_client.signer import Signer

V2_EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
V2_EXCHANGE_NEG_RISK = "0xe2222d279d744050d28e00520010520000310F59"
CLOB_URL = "https://clob.polymarket.com"

BYTES32_ZERO = b"\x00" * 32
ORDER_TYPE_STR = (
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
)
ORDER_TYPEHASH = keccak(text=ORDER_TYPE_STR)

DOMAIN_TYPEHASH = keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")

def _domain_separator(exchange: str) -> bytes:
    return keccak(
        DOMAIN_TYPEHASH
        + keccak(text="Polymarket CTF Exchange")
        + keccak(text="2")
        + (137).to_bytes(32, "big")
        + bytes.fromhex("0" * 24 + exchange[2:].lower())
    )

DOMAIN_SEP_REGULAR  = _domain_separator(V2_EXCHANGE)
DOMAIN_SEP_NEG_RISK = _domain_separator(V2_EXCHANGE_NEG_RISK)


def build_v2_order(
    private_key: str,
    token_id: str,
    maker_amount: int,
    taker_amount: int,
    side: int,          # 0=BUY, 1=SELL
    neg_risk: bool = False,
) -> dict:
    acct = Account.from_key(private_key)
    ds = DOMAIN_SEP_NEG_RISK if neg_risk else DOMAIN_SEP_REGULAR
    salt = random.randint(1, 2**32)
    ts = str(int(time.time() * 1000))

    order_hash = keccak(
        ORDER_TYPEHASH
        + salt.to_bytes(32, "big")
        + bytes.fromhex("0" * 24 + acct.address[2:].lower())  # maker
        + bytes.fromhex("0" * 24 + acct.address[2:].lower())  # signer
        + int(token_id).to_bytes(32, "big")
        + maker_amount.to_bytes(32, "big")
        + taker_amount.to_bytes(32, "big")
        + side.to_bytes(32, "big")
        + (0).to_bytes(32, "big")        # signatureType = EOA
        + int(ts).to_bytes(32, "big")
        + BYTES32_ZERO                   # metadata
        + BYTES32_ZERO                   # builder
    )
    signed = acct.unsafe_sign_hash(keccak(b"\x19\x01" + ds + order_hash))

    return {
        "salt": salt,
        "maker": acct.address,
        "signer": acct.address,
        "taker": "0x" + "0" * 40,
        "tokenId": token_id,
        "makerAmount": str(maker_amount),
        "takerAmount": str(taker_amount),
        "side": "BUY" if side == 0 else "SELL",
        "signatureType": 0,
        "timestamp": ts,
        "expiration": "0",
        "metadata": "0x" + "0" * 64,
        "builder": "0x" + "0" * 64,
        "signature": "0x" + signed.signature.hex(),
    }


def post_v2_order(
    private_key: str,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    order: dict,
    order_type: str = "GTC",
) -> dict:
    from py_clob_client.clob_types import ApiCreds

    body_json = json.dumps({
        "order": order,
        "owner": api_key,
        "orderType": order_type,
        "postOnly": False,
        "deferExec": False,
    })
    signer = Signer(private_key, 137)
    creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    headers = create_level_2_headers(
        signer, creds, RequestArgs(method="POST", request_path="/order", body=body_json)
    )
    r = httpx.post(f"{CLOB_URL}/order", headers=headers, content=body_json, timeout=15)
    if not r.is_success:
        import structlog as _sl
        _sl.get_logger().error(
            "clob_order_rejected",
            status=r.status_code,
            body=r.text[:1000],
        )
    r.raise_for_status()
    return r.json()


def price_size_to_amounts(price: float, size: float, side: int) -> tuple[int, int]:
    """Convert price/size to (makerAmount, takerAmount) in 6-decimal units.

    CLOB precision rules (enforced server-side):
      makerAmount (USDC): max 4 decimal places → must be a multiple of 100
      takerAmount (shares): max 2 decimal places → must be a multiple of 10_000
    """
    if side == 0:  # BUY: pay USDC, receive shares
        maker = round(price * size * 1_000_000 / 100) * 100
        taker = round(size * 1_000_000 / 10_000) * 10_000
    else:  # SELL: pay shares, receive USDC
        maker = round(size * 1_000_000 / 10_000) * 10_000
        taker = round(price * size * 1_000_000 / 100) * 100
    return maker, taker
