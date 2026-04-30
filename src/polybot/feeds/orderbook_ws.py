import asyncio
import json
import time
from collections import deque
from typing import Optional

import structlog
import websockets
from websockets.exceptions import ConnectionClosed

from polybot.models.btc_market import Direction, ImbalanceReading, OrderBookSnapshot, OrderLevel

logger = structlog.get_logger()

_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_IMBALANCE_BUFFER = 300


class OrderBookWS:
    """Live Polymarket order book via WebSocket.

    Maintains per-asset depth maps, records imbalance readings on every update.
    Auto-reconnects with exponential backoff.
    """

    def __init__(self) -> None:
        self._up_token: str = ""
        self._down_token: str = ""
        self._books: dict[str, dict[str, dict[float, float]]] = {}
        self._imbalance_history: deque[ImbalanceReading] = deque(maxlen=_IMBALANCE_BUFFER)
        self._slot_start_ts: float = 0.0
        self._ready: asyncio.Event = asyncio.Event()
        self._bid_changed: asyncio.Event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def subscribe(self, up_token_id: str, down_token_id: str, slot_start_ts: float) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
        self._up_token = up_token_id
        self._down_token = down_token_id
        self._books = {}
        self._imbalance_history.clear()
        self._ready.clear()
        self._slot_start_ts = slot_start_ts
        self._task = asyncio.create_task(self._run())

    async def wait_ready(self, timeout: float = 15.0) -> None:
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    def destroy(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    async def _run(self) -> None:
        asset_ids = [self._up_token, self._down_token]
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(_WS_URL, ping_interval=30) as ws:
                    backoff = 1.0
                    await ws.send(json.dumps({"type": "market", "assets_ids": asset_ids}))
                    async for raw in ws:
                        self._handle_message(raw)
            except ConnectionClosed as e:
                logger.warning("orderbook_ws_closed", reason=str(e), backoff=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("orderbook_ws_error", error=str(e), backoff=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _handle_message(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if isinstance(data, list):
            for msg in data:
                if isinstance(msg, dict) and msg.get("event_type") == "book":
                    self._apply_snapshot(msg)
        elif data.get("event_type") == "book":
            self._apply_snapshot(data)
        elif data.get("event_type") == "price_change":
            self._apply_price_change(data)

        if self._up_token in self._books and self._down_token in self._books:
            if not self._ready.is_set():
                self._ready.set()

        self._record_imbalance()

    def _apply_snapshot(self, msg: dict) -> None:
        asset_id = msg.get("asset_id", "")
        self._books[asset_id] = {
            "bids": {float(l["price"]): float(l["size"]) for l in msg.get("bids", [])},
            "asks": {float(l["price"]): float(l["size"]) for l in msg.get("asks", [])},
        }

    def _apply_price_change(self, msg: dict) -> None:
        bid_updated = False
        for change in msg.get("price_changes", []):
            asset_id = change["asset_id"]
            if asset_id not in self._books:
                self._books[asset_id] = {"bids": {}, "asks": {}}
            book = self._books[asset_id]
            side_key = "bids" if change["side"] == "BUY" else "asks"
            price = float(change["price"])
            size = float(change["size"])
            if size == 0:
                book[side_key].pop(price, None)
            else:
                book[side_key][price] = size
            if side_key == "bids":
                bid_updated = True
        if bid_updated:
            self._bid_changed.set()

    def _record_imbalance(self) -> None:
        book = self._books.get(self._up_token)
        if not book:
            return
        top_bids = sorted(book["bids"].values(), reverse=True)[:10]
        top_asks = sorted(book["asks"].values())[:10]
        bid_depth = sum(top_bids)
        ask_depth = sum(top_asks)
        ratio = bid_depth / ask_depth if ask_depth > 0 else float("inf")
        secs_since_open = time.time() - self._slot_start_ts
        self._imbalance_history.append(
            ImbalanceReading(ratio=ratio, seconds_since_open=secs_since_open, ts=time.time())
        )

    def get_snapshot(self, asset_id: str) -> OrderBookSnapshot:
        book = self._books.get(asset_id, {"bids": {}, "asks": {}})
        bids = [OrderLevel(price=p, size=s) for p, s in sorted(book["bids"].items(), reverse=True)]
        asks = [OrderLevel(price=p, size=s) for p, s in sorted(book["asks"].items())]
        return OrderBookSnapshot(asset_id=asset_id, bids=bids, asks=asks)

    def get_imbalance_history(self) -> list[ImbalanceReading]:
        return list(self._imbalance_history)

    async def wait_bid_change(self, timeout: float = 2.0) -> None:
        """Wait until any bid update arrives, or until timeout elapses."""
        self._bid_changed.clear()
        try:
            await asyncio.wait_for(self._bid_changed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    def best_ask(self, direction: Direction) -> Optional[float]:
        token = self._up_token if direction == Direction.UP else self._down_token
        asks = self._books.get(token, {}).get("asks", {})
        return min(asks.keys()) if asks else None

    def best_bid(self, direction: Direction) -> Optional[float]:
        token = self._up_token if direction == Direction.UP else self._down_token
        bids = self._books.get(token, {}).get("bids", {})
        return max(bids.keys()) if bids else None
