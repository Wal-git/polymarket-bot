from decimal import Decimal
from typing import Optional

import structlog

from polybot.auth.wallet import get_builder_code, get_clob_creds, get_private_key
from polybot.models.types import MarketOutcome, OrderRequest, OrderType, Side

logger = structlog.get_logger()

# V2 deposit-wallet proxy for this account. Registered as POLY_1271 (sigType=3)
# during the original UI deposit flow. The CLOB will only accept orders where
# maker=signer=funder=this proxy, signed with the ERC-7739 / Solady TypedDataSign
# wrapper (NOT a plain EIP-712 sig). Requires py-clob-client-v2>=1.0.1, which
# adds `_build_poly_1271_order_signature`; v1.0.0 silently sent unwrapped sigs
# that the CLOB rejected as "invalid signature". Do NOT use POLY_GNOSIS_SAFE
# with a separate Safe here — the CLOB only knows about this proxy for our EOA.
# Allowances (pUSD → V2 exchanges, CTF setApprovalForAll) were set at deposit
# time and are already in place. See memory: project_clob_trading_resolved.
DEPOSIT_WALLET = "0xd31f05af327955214f71f9387c93ed8bd7f5770c"


class CLOBClient:
    def __init__(self):
        self._client = None
        self._tick_cache: dict[str, Decimal] = {}
        self._market_info_cache: dict[str, dict] = {}

    def connect(self):
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds
        from py_clob_client_v2.constants import POLYGON
        from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

        creds = get_clob_creds()
        self._client = ClobClient(
            "https://clob.polymarket.com",
            key=get_private_key(),
            chain_id=POLYGON,
            creds=ApiCreds(
                api_key=creds["api_key"],
                api_secret=creds["api_secret"],
                api_passphrase=creds["passphrase"],
            ),
            signature_type=SignatureTypeV2.POLY_1271,
            funder=DEPOSIT_WALLET,
        )
        logger.info("clob_connected", deposit_wallet=DEPOSIT_WALLET)

    @property
    def client(self):
        if self._client is None:
            self.connect()
        return self._client

    def get_order_book(self, token_id: str) -> dict:
        book = self.client.get_order_book(token_id)
        return {
            "bids": book.bids if hasattr(book, "bids") else [],
            "asks": book.asks if hasattr(book, "asks") else [],
        }

    def get_best_bid_ask(self, token_id: str) -> tuple[Optional[Decimal], Optional[Decimal]]:
        book = self.get_order_book(token_id)
        best_bid = Decimal(str(book["bids"][0].price)) if book["bids"] else None
        best_ask = Decimal(str(book["asks"][0].price)) if book["asks"] else None
        return best_bid, best_ask

    def enrich_outcomes(self, outcomes: list[MarketOutcome]) -> list[MarketOutcome]:
        enriched = []
        for outcome in outcomes:
            try:
                bid, ask = self.get_best_bid_ask(outcome.token_id)
            except Exception:
                return []
            enriched.append(
                outcome.model_copy(update={"best_bid": bid, "best_ask": ask})
            )
        return enriched

    def get_tick_size(self, token_id: str) -> Decimal:
        if token_id not in self._tick_cache:
            self._tick_cache[token_id] = Decimal(str(self.client.get_tick_size(token_id)))
        return self._tick_cache[token_id]

    def get_market_info(self, condition_id: str) -> dict:
        if condition_id not in self._market_info_cache:
            self._market_info_cache[condition_id] = self.client.get_clob_market_info(condition_id)
        return self._market_info_cache[condition_id]

    def _handrolled_sign(self, args):
        """Sign via the hand-rolled v2_order module (env CLOB_USE_HANDROLLED_SIGNER=1).

        Reuses the SDK's amount computation (proven against real markets) but
        substitutes our own ERC-7739 wrapper. Returns a SignedOrderV2 that
        the SDK's post_order will serialize + auth + submit.
        """
        from polybot.auth.wallet import get_private_key
        from polybot.client.v2_order import build_v2_poly_1271_order
        from py_clob_client_v2.order_builder.builder import ROUNDING_CONFIG
        from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2
        from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

        token_id = args.token_id
        tick = str(self.get_tick_size(token_id))
        side_int, maker_amt, taker_amt = self.client.builder.get_order_amounts(
            args.side, args.size, args.price, ROUNDING_CONFIG[tick]
        )
        neg_risk = self.client.get_neg_risk(token_id)

        signed = build_v2_poly_1271_order(
            private_key=get_private_key(),
            deposit_wallet=DEPOSIT_WALLET,
            token_id=token_id,
            maker_amount=maker_amt,
            taker_amount=taker_amt,
            side=int(side_int),
            neg_risk=neg_risk,
            builder_code=args.builder_code,
            metadata=args.metadata,
        )
        return SignedOrderV2(
            salt=str(signed["salt"]),
            maker=signed["maker"],
            signer=signed["signer"],
            tokenId=signed["tokenId"],
            makerAmount=signed["makerAmount"],
            takerAmount=signed["takerAmount"],
            side=int(side_int),
            signatureType=SignatureTypeV2.POLY_1271,
            timestamp=signed["timestamp"],
            metadata=signed["metadata"],
            builder=signed["builder"],
            expiration=signed["expiration"],
            signature=signed["signature"],
        )

    def place_order(self, order: OrderRequest, dry_run: bool = True) -> Optional[str]:
        if dry_run:
            logger.info(
                "dry_run_order",
                token_id=order.token_id,
                side=order.side.value,
                size=str(order.size),
                price=str(order.limit_price),
            )
            return None

        from py_clob_client_v2.clob_types import OrderArgsV2

        if order.limit_price is not None:
            tick = self.get_tick_size(str(order.token_id))
            remainder = (order.limit_price / tick) % 1
            if remainder != 0:
                logger.warning(
                    "order_rejected_tick_size",
                    token_id=order.token_id,
                    price=str(order.limit_price),
                    tick=str(tick),
                )
                return None

        args = {
            "token_id": str(order.token_id),
            "price": float(order.limit_price or 0),
            "size": float(order.size),
            "side": "BUY" if order.side == Side.BUY else "SELL",
        }
        builder = get_builder_code()
        if builder:
            args["builder_code"] = builder

        from polybot.client import v2_order as _v2order
        if _v2order.is_enabled():
            signed_order = self._handrolled_sign(OrderArgsV2(**args))
            logger.info("order_signed_handrolled", token_id=order.token_id)
        else:
            signed_order = self.client.create_order(OrderArgsV2(**args))
        resp = self.client.post_order(signed_order, "GTC")
        order_id = resp.get("orderID") or resp.get("id") or "unknown"
        logger.info("order_placed", order_id=order_id, token_id=order.token_id)
        return order_id

    def cancel_order(self, order_id: str):
        resp = self.client.cancel_orders([order_id])
        cancelled = (resp.get("canceled") or []) if isinstance(resp, dict) else []
        if order_id not in cancelled:
            logger.warning("cancel_not_confirmed", order_id=order_id, resp=resp)
        else:
            logger.info("order_cancelled", order_id=order_id)

    def cancel_all(self):
        self.client.cancel_all()
        logger.info("all_orders_cancelled")

    def get_balance(self) -> Decimal:
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            bal = self.client.get_balance_allowance(params=params)
            return Decimal(str(bal.get("balance", 0))) / Decimal("1e6")
        except Exception:
            logger.warning("balance_fetch_failed")
            return Decimal("0")

    def sync_balance_allowance(self) -> None:
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            self.client.update_balance_allowance(params=params)
            logger.info("balance_allowance_synced")
        except Exception as e:
            logger.warning("balance_allowance_sync_failed", error=str(e))
