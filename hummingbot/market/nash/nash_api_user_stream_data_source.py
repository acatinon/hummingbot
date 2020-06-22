import asyncio
from aiostream import stream
import logging
import time

from gql import gql, Client
from gql.transport.phoenix_channel_websockets import PhoenixChannelWebsocketsTransport

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.market.nash.nash_order_book import NashOrderBook
from hummingbot.market.nash.nash_auth import NashAuth
from hummingbot.market.nash.nash_constants import WS_URL

from typing import Optional


UPDATED_ACCOUNT_ORDERS = gql(
    """
    subscription UpdatedAccountOrders($market: String, $payload: UpdatedAccountOrdersParams!, $signature: Signature) {
        updatedAccountOrders(market: $market, payload: $payload, signature: $signature) {
            id
            amount {
                amount
            }
            buyOrSell
            status
            type
        }
    }
    """
)

NEW_ACCOUNT_TRADES = gql(
    """
    subscription NewAccountTrades($market: String, $payload: NewAccountTradesParams!, $signature: Signature) {
        newAccountTrades(market: $market, payload: $payload, signature: $signature) {
            id
            amount {
                amount
            }
        }
    }
    """
)


class NashAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _krausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._krausds_logger is None:
            cls._krausds_logger = logging.getLogger(__name__)
        return cls._krausds_logger

    def __init__(self, nash_auth: NashAuth):
        super().__init__()
        self.nash_auth: NashAuth = nash_auth
        self._last_recv_time: float = 0

    @property
    def order_book_class(self):
        return NashOrderBook

    @property
    def last_recv_time(self):
        return self._last_recv_time

    async def _inner_messages(self):
        authenticated_url = self.nash_auth.append_token_to_url(WS_URL)
        generators = asyncio.Queue()
        transport = PhoenixChannelWebsocketsTransport(
            url=authenticated_url,
            channel_name="__absinthe__:control",
            connect_args={"compression": None, "subprotocols": None},
        )

        async with Client(transport=transport, fetch_schema_from_transport=False) as client:
            await generators.put(client.subscribe(UPDATED_ACCOUNT_ORDERS, variable_values={"payload": {}}))
            await generators.put(client.subscribe(NEW_ACCOUNT_TRADES, variable_values={"payload": {}}))

            s = stream.flatten(stream.cycle(stream.call(generators.get)))

            async with s.stream() as streamer:
                async for msg in streamer:
                    yield msg

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async for msg in self._inner_messages():
                    output.put_nowait(msg)
                    self._last_recv_time = time.time()

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with Nash WebSocket connection. " "Retrying after 30 seconds...", exc_info=True,
                )
                await asyncio.sleep(30.0)
