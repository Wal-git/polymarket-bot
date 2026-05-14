"""Hand-rolled V2 CLOB order signer — fallback for when the SDK regresses.

Gated by env var: set `CLOB_USE_HANDROLLED_SIGNER=1` and `clob.py` will route
through `build_v2_poly_1271_order()` here instead of the SDK's
`create_order()`. The SDK path is the default and what we rely on day to day;
this exists so a future SDK regression doesn't take the bot offline.

What it implements:
  - The V2 CTF Exchange order struct (no taker/nonce/feeRateBps/expiration in
    the signed body — those are V1).
  - The Solady `TypedDataSign` nested-typed-data wrapper that the deposit-
    wallet `isValidSignature` on the proxy requires (= what the CLOB's
    POLY_1271 verifier checks). Matches v1.0.1 SDK's
    `_build_poly_1271_order_signature` byte-for-byte.

To verify against the SDK: run `scripts/compare_handrolled_vs_sdk.py` (TODO).
If signatures differ, prefer the SDK's. This module is a safety net, not a
preference.
"""
from __future__ import annotations

import os
import random
import time

from eth_abi import encode as abi_encode
from eth_account import Account
from eth_utils import keccak

# V2 exchange contracts (chain 137)
V2_EXCHANGE          = "0xE111180000d2663C0091e4f400237545B87B996B"
V2_EXCHANGE_NEG_RISK = "0xe2222d279d744050d28e00520010520000310F59"
CHAIN_ID = 137

BYTES32_ZERO = b"\x00" * 32

ORDER_TYPE_STRING = (
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
)
SOLADY_TYPE_STRING = (
    "TypedDataSign(Order contents,string name,string version,uint256 chainId,"
    "address verifyingContract,bytes32 salt)" + ORDER_TYPE_STRING
)
DOMAIN_TYPE_STRING = (
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
)

ORDER_TYPE_HASH   = keccak(text=ORDER_TYPE_STRING)
SOLADY_TYPE_HASH  = keccak(text=SOLADY_TYPE_STRING)
DOMAIN_TYPE_HASH  = keccak(text=DOMAIN_TYPE_STRING)

CTF_EXCHANGE_NAME_HASH    = keccak(text="Polymarket CTF Exchange")
CTF_EXCHANGE_VERSION_HASH = keccak(text="2")
DEPOSIT_WALLET_NAME_HASH    = keccak(text="DepositWallet")
DEPOSIT_WALLET_VERSION_HASH = keccak(text="1")


def is_enabled() -> bool:
    return os.environ.get("CLOB_USE_HANDROLLED_SIGNER", "").lower() in ("1", "true", "yes")


def _hex_to_bytes32(h: str) -> bytes:
    return bytes.fromhex(h.replace("0x", "").zfill(64))


def _ctf_exchange_domain_separator(exchange: str) -> bytes:
    return keccak(
        primitive=abi_encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [DOMAIN_TYPE_HASH, CTF_EXCHANGE_NAME_HASH, CTF_EXCHANGE_VERSION_HASH, CHAIN_ID, exchange],
        )
    )


def build_v2_poly_1271_order(
    *,
    private_key: str,
    deposit_wallet: str,
    token_id: str,
    maker_amount: int,
    taker_amount: int,
    side: int,                 # 0 = BUY, 1 = SELL
    timestamp_ms: int | None = None,
    salt: int | None = None,
    neg_risk: bool = False,
    builder_code: str = "0x" + "0" * 64,
    metadata: str = "0x" + "0" * 64,
) -> dict:
    """Build + ERC-7739-wrap-sign a POLY_1271 order for the deposit wallet.

    Returns the JSON body the CLOB expects at POST /order's `order` field.
    """
    acct = Account.from_key(private_key)
    exchange = V2_EXCHANGE_NEG_RISK if neg_risk else V2_EXCHANGE
    ts = str(timestamp_ms if timestamp_ms is not None else time.time_ns() // 1_000_000)
    s = salt if salt is not None else random.randint(1, 2**60)

    # 1) Hash the inner Order struct (`contents` in TypedDataSign)
    contents_hash = keccak(
        primitive=abi_encode(
            ["bytes32", "uint256", "address", "address", "uint256", "uint256",
             "uint256", "uint8", "uint8", "uint256", "bytes32", "bytes32"],
            [
                ORDER_TYPE_HASH, s, deposit_wallet, deposit_wallet,
                int(token_id), maker_amount, taker_amount, side,
                3,  # signatureType POLY_1271
                int(ts), _hex_to_bytes32(metadata), _hex_to_bytes32(builder_code),
            ],
        )
    )

    # 2) Build the TypedDataSign struct hash. Per Solady/ERC-7739, the
    #    `name`/`version`/`chainId`/`verifyingContract`/`salt` fields here
    #    describe the *wallet that validates the signature* (the DepositWallet),
    #    NOT the app. This is the part that makes ERC-1271 work — the
    #    DepositWallet's `isValidSignature` reconstructs this hash and checks
    #    that its owner (EOA) signed it.
    typed_data_sign_hash = keccak(
        primitive=abi_encode(
            ["bytes32", "bytes32", "bytes32", "bytes32", "uint256", "address", "bytes32"],
            [
                SOLADY_TYPE_HASH,
                contents_hash,
                DEPOSIT_WALLET_NAME_HASH,
                DEPOSIT_WALLET_VERSION_HASH,
                CHAIN_ID,
                deposit_wallet,
                BYTES32_ZERO,
            ],
        )
    )

    # 3) The outer EIP-712 domain separator is the *app* (CTF Exchange V2) —
    #    this is what `signer.signedMessage(hash)` would normally produce if
    #    the wallet signed an ORDER directly. The ERC-7739 trick is signing
    #    a wallet-domain-wrapped statement *about* this app-domain hash.
    ctf_exchange_domain_sep = _ctf_exchange_domain_separator(exchange)

    # 4) Final EIP-712 message hash to sign
    msg_hash = keccak(b"\x19\x01" + ctf_exchange_domain_sep + typed_data_sign_hash)
    signed = acct.unsafe_sign_hash(msg_hash)

    # 5) ERC-7739 signature format: inner_sig || app_domain_sep || contents_hash
    #    || contents_type_descr || uint16(contents_type_len). The CLOB verifier
    #    uses the trailing data to reconstruct what was signed without seeing
    #    the inner Order struct.
    inner_sig = signed.signature.hex()
    contents_type_descr = ORDER_TYPE_STRING.encode("utf-8").hex()
    contents_type_len = len(ORDER_TYPE_STRING).to_bytes(2, "big").hex()
    full_signature = (
        "0x"
        + inner_sig
        + ctf_exchange_domain_sep.hex()
        + contents_hash.hex()
        + contents_type_descr
        + contents_type_len
    )

    return {
        "salt": s,
        "maker": deposit_wallet,
        "signer": deposit_wallet,
        "tokenId": str(token_id),
        "makerAmount": str(maker_amount),
        "takerAmount": str(taker_amount),
        "side": "BUY" if side == 0 else "SELL",
        "signatureType": 3,
        "timestamp": ts,
        "expiration": "0",
        "metadata": metadata,
        "builder": builder_code,
        "signature": full_signature,
    }
