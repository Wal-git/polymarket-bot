from decimal import Decimal
from typing import Optional

import structlog

from polybot.auth.wallet import get_clob_creds, get_private_key
from polybot.models.types import MarketOutcome, OrderRequest, OrderType, Side

logger = structlog.get_logger()


class CLOBClient:
    def __init__(self):
        self._client = None

    def connect(self):
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.constants import POLYGON

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
        )
        logger.info("clob_connected")

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
            bid, ask = self.get_best_bid_ask(outcome.token_id)
            enriched.append(
                outcome.model_copy(update={"best_bid": bid, "best_ask": ask})
            )
        return enriched

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

        from py_clob_client.order_builder.constants import BUY as CLOB_BUY, SELL as CLOB_SELL

        side = CLOB_BUY if order.side == Side.BUY else CLOB_SELL

        signed_order = self.client.create_and_post_order(
            {
                "tokenID": order.token_id,
                "price": float(order.limit_price or 0),
                "size": float(order.size),
                "side": side,
            }
        )
        order_id = signed_order.get("orderID", signed_order.get("id", "unknown"))
        logger.info("order_placed", order_id=order_id, token_id=order.token_id)
        return order_id

    def cancel_order(self, order_id: str):
        self.client.cancel(order_id)
        logger.info("order_cancelled", order_id=order_id)

    def cancel_all(self):
        self.client.cancel_all()
        logger.info("all_orders_cancelled")

    def get_balance(self) -> Decimal:
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            bal = self.client.get_balance_allowance(params=params)
            return Decimal(str(bal.get("balance", 0))) / Decimal("1e6")
        except Exception:
            logger.warning("balance_fetch_failed")
            return Decimal("0")
