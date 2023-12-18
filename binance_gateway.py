import asyncio
import logging
import json
import websockets
import schedule
import time


from binance import Client, AsyncClient
from threading import Thread
from binance.depthcache import FuturesDepthCacheManager
from interface_order import OrderEvent, OrderStatus, ExecutionType, Side
from interface_book import OrderBook, PriceLevel, VenueOrderBook

logging.basicConfig(
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s",
    level=logging.INFO,
)


class BinanceFutureGateway:
    def __init__(
        self, symbol: str, api_key=None, api_secret=None, name="Binance", testnet=True
    ):
        self._symbol = symbol
        self._api_key = api_key
        self._api_secret = api_secret
        self._exchange_name = name
        self.testnet = testnet

        # Client and Async client
        self._client = None
        self._async_client = None
        self._dcm = None
        self._dws = None
        self._listen_key = None

        # Depth cache
        self._depth_cache = None

        # dedicated loop and thread to run all async concurrent tasks
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(target=self.run_async_tasks, daemon=True, name=name)

        # callbacks
        self._depth_callbacks = []
        self._execution_callbacks = []

    def connect(self):
        logging.info("Initializing connection..")

        self._loop.run_until_complete(self._setting_asyncclient())

        logging.info("starting event loop thread..")
        self._thread.start()

        self._client = Client(self._api_key, self._api_secret, testnet=self.testnet)

        schedule.every(15).seconds.do(self.extend_listen_key)
        while True:
            schedule.run_pending()
            time.sleep(1)

    async def _setting_asyncclient(self):
        logging.info("Configuring depth websocket AsyncClient...")
        self._async_client = await AsyncClient.create(
            self._api_key, self._api_secret, testnet=self.testnet
        )

    def extend_listen_key(self):
        logging.info("Extending listen key...")
        self._client.futures_stream_keepalive(self._listen_key)

    def run_async_tasks(self):
        self._loop.create_task(self._listen_depth_forever())
        self._loop.create_task(self._listen_execution_forever())
        self._loop.run_forever()

    async def _listen_depth_forever(self):
        logging.info("Subscribing to depth events...")
        while True:
            if not self._dws:
                logging.info("depth socket not connected, connecting...")
                self._dcm = FuturesDepthCacheManager(
                    self._async_client, symbol=self._symbol
                )
            async with self._dcm as self._dws:
                try:
                    self._depth_cache = await self._dws.recv()

                    if self._depth_callbacks:
                        for _d_callback in self._depth_callbacks:
                            _d_callback(VenueOrderBook(self._exchange_name, self.get_order_book()))

                except Exception as e:
                    logging.info(f"[Error] depth processing error: {e}...")
                    self._dws = None
                    await self._setting_asyncclient()

    def get_order_book(self) -> OrderBook:
        _bids = [
            PriceLevel(price=p, size=s) for (p, s) in self._depth_cache.get_bids()[:5]
        ]
        _asks = [
            PriceLevel(price=p, size=s) for (p, s) in self._depth_cache.get_asks()[:5]
        ]
        return OrderBook(
            timestamp=self._depth_cache.update_time,
            contract_name=self._symbol,
            bids=_bids,
            asks=_asks,
        )

    async def _listen_execution_forever(self):
        logging.info("Subscribing to user data stream...")
        self._listen_key = await self._async_client.futures_stream_get_listen_key()
        if self.testnet:
            url = "wss://stream.binancefuture.com/ws/" + self._listen_key
        else:
            url = "wss://fstream.binance.com/ws/" + self._listen_key

        async with websockets.connect(url) as ws:
            while ws.open:
                _message = await ws.recv()
                _m_data = json.loads(_message)
                _event = _m_data["e"]
                if _event == "ORDER_TRADE_UPDATE":
                    _trade_data = _m_data["o"]
                    _order_id = _trade_data["c"]
                    _symbol = _trade_data["s"]
                    _ex_type = _trade_data["x"]
                    _order_status = _trade_data["X"]
                    _side = _trade_data["S"]
                    _last_filled_px = float(_trade_data["L"])
                    _last_filled_qty = float(_trade_data["l"])

                    # create an order event
                    _order_event = OrderEvent(
                        _symbol,
                        _order_id,
                        ExecutionType[_ex_type],
                        Side[_side],
                        OrderStatus[_order_status],
                    )
                    if _ex_type == "TRADE":
                        _order_event.last_filled_price = _last_filled_px
                        _order_event.last_filled_quantity = _last_filled_qty

                    if self._execution_callbacks:
                        for _ex_callback in self._execution_callbacks:
                            _ex_callback(_order_event)

    """
        Place limit order
    """

    def place_limit_order(self, side: Side, price, quantity, tif="IOC") -> bool:
        try:
            self._client.futures_create_order(
                symbol=self._symbol,
                side=side.name,
                type="LIMIT",
                price=price,
                quantity=quantity,
                timeInForce=tif,
            )
            return True
        except Exception as e:
            logging.info(f"Failed to place order: {e}")
            return False

    """
        Cancel Order
    """

    def cancel_order(self, symbol, order_id) -> bool:
        try:
            self._client.futures_cancel_order(symbol=symbol, origClientOrderId=order_id)
            return True
        except Exception as e:
            logging.info(f"Failed to cancel order: {order_id}, {e}")
            return False

    def register_depth_callback(self, dep_callback):
        self._depth_callbacks.append(dep_callback)

    def register_execution_callback(self, ex_callback):
        self._execution_callbacks.append(ex_callback)
