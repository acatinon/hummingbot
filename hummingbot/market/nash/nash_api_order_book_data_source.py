import pandas as pd
from aiostream import stream
import asyncio
import logging
import time
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.phoenix_channel_websockets import PhoenixChannelWebsocketsTransport

from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.logger import HummingbotLogger

from hummingbot.market.nash.nash_order_book import NashOrderBook

HTTP_URL = "https://app.nash.io/api/graphql"
WS_URL = "wss://app.nash.io/api/socket/websocket"
HEADERS = {"Content-Type": "application/json"}

LIST_MARKETS = gql(
    """
    query {
        listMarkets {
            name
            aUnit
            bUnit
            primary
            status
        }
    }
    """
)

NEW_TRADES = gql(
    """
    subscription NewTrades($marketName: MarketName!) {
        newTrades(marketName: $marketName) {
            id
            direction
            limitPrice {
                amount
            }
            amount {
                amount
            }
            executedAt
        }
    }
    """
)

GET_ORDERBOOK = gql(
    """
    query GetOrderBook($marketName: MarketName!) {
        getOrderBook(marketName: $marketName) {
            updateId
            asks {
                price {
                    amount
                }
                amount {
                    amount
                }
            }
            bids {
                price {
                    amount
                }
                amount {
                    amount
                }
            }
        }
    }
    """
)

UPDATED_ORDERBOOK = gql(
    """
    subscription UpdatedOrderBook($marketName: MarketName!) {
        updatedOrderBook(marketName: $marketName) {
            updateId
            asks {
                price {
                    amount
                }
                amount {
                    amount
                }
            }
            bids {
                price {
                    amount
                }
                amount {
                    amount
                }
            }
        }
    }
    """
)


def _price_to_array(el):
    return [float(el["price"]["amount"]), float(el["amount"]["amount"])]


class NashAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _kraobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._kraobds_logger is None:
            cls._kraobds_logger = logging.getLogger(__name__)
        return cls._kraobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        transport = AIOHTTPTransport(url=HTTP_URL, headers=HEADERS)

        async with Client(transport=transport, fetch_schema_from_transport=True) as client:
            data = await client.execute(LIST_MARKETS)

        all_markets: pd.DataFrame = pd.DataFrame.from_records(data=data["listMarkets"], index="name")

        filtered_markets = all_markets[all_markets.status.eq("RUNNING") & all_markets.primary.eq(True)]

        return filtered_markets.filter(["name", "aUnit", "bUnit"])

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    "Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg="Error getting active exchange information. Check network connection.",
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: Client, trading_pair: str) -> Dict[str, Any]:
        data = await client.execute(GET_ORDERBOOK, {"marketName": trading_pair})

        data["getOrderBook"]["asks"] = list(map(_price_to_array, data["getOrderBook"]["asks"]))
        data["getOrderBook"]["bids"] = list(map(_price_to_array, data["getOrderBook"]["bids"]))
        data["getOrderBook"]["tradingPair"] = trading_pair

        return data["getOrderBook"]

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        transport = AIOHTTPTransport(url=HTTP_URL, headers=HEADERS)

        async with Client(transport=transport, fetch_schema_from_transport=True) as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = NashOrderBook.snapshot_message_from_exchange(
                        snapshot, snapshot_timestamp
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book)
                    self.logger().info(
                        f"Initialized order book for {trading_pair}. " f"{index+1}/{number_of_pairs} completed."
                    )
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5.0)
            return retval

    async def _subscribe(self, client, query, trading_pair):
        variables = {"marketName": trading_pair}
        async for msg in client.subscribe(query, variable_values=variables):
            yield (trading_pair, msg)

    async def _inner_messages(self, query, trading_pairs):
        generators = asyncio.Queue()
        transport = PhoenixChannelWebsocketsTransport(
            url=WS_URL, channel_name="__absinthe__:control", connect_args={"compression": None, "subprotocols": None},
        )

        async with Client(transport=transport, fetch_schema_from_transport=False) as client:
            for trading_pair in trading_pairs:
                await generators.put(self._subscribe(client, query, trading_pair))

            s = stream.flatten(stream.cycle(stream.call(generators.get)))

            async with s.stream() as streamer:
                async for (trading_pair, msg) in streamer:
                    yield (trading_pair, msg)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                async for (trading_pair, msg) in self._inner_messages(NEW_TRADES, trading_pairs):
                    for new_trade in msg["newTrades"]:
                        trade_msg: OrderBookMessage = NashOrderBook.trade_message_from_exchange(
                            new_trade, metadata={"tradingPair": trading_pair}
                        )
                        output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. Retrying after 30 seconds...", exc_info=True
                )

                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                async for (trading_pair, msg) in self._inner_messages(UPDATED_ORDERBOOK, trading_pairs):
                    msg["updatedOrderBook"]["asks"] = list(map(_price_to_array, msg["updatedOrderBook"]["asks"]))
                    msg["updatedOrderBook"]["bids"] = list(map(_price_to_array, msg["updatedOrderBook"]["bids"]))
                    order_book_message: OrderBookMessage = NashOrderBook.diff_message_from_exchange(
                        msg["updatedOrderBook"], time.time(), metadata={"tradingPair": trading_pair}
                    )
                    output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. Retrying after 30 seconds...", exc_info=True
                )
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                transport = AIOHTTPTransport(url=HTTP_URL, headers=HEADERS)

                async with Client(transport=transport, fetch_schema_from_transport=True) as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = NashOrderBook.snapshot_message_from_exchange(
                                snapshot, snapshot_timestamp
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error. ", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error. ", exc_info=True)
                await asyncio.sleep(5.0)
