import asyncio
import logging
import gql
from typing import (
    Dict,
    Optional,
    Any,
    List,
)
import pandas as pd
from mock import patch
import unittest

from test.integration.assets.mock_data.fixture_nash import FixtureNash
from hummingbot.market.nash.nash_api_order_book_data_source import NashAPIOrderBookDataSource, HTTP_URL, HEADERS
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

PATCH_BASE_PATH = "hummingbot.market.nash.nash_api_order_book_data_source.NashAPIOrderBookDataSource.{method}"


class NashAPIOrderBookDataSourceUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_data_source: NashAPIOrderBookDataSource = NashAPIOrderBookDataSource(
            ["btc_usdc", "eth_usdc", "eth_btc"]
        )

    def run_async(self, task):
        return self.ev_loop.run_until_complete(task)

    def test_get_active_exchange_markets(self):
        market_data: pd.DataFrame = self.run_async(self.order_book_data_source.get_active_exchange_markets())
        self.assertIn("btc_usdc", market_data.index)

    def test_get_trading_pairs(self):
        trading_pairs: List[str] = self.run_async(self.order_book_data_source.get_trading_pairs())
        self.assertIn("btc_usdc", trading_pairs)

    async def get_snapshot(self):
        transport = gql.transport.aiohttp.AIOHTTPTransport(url=HTTP_URL, headers=HEADERS)

        async with gql.Client(transport=transport, fetch_schema_from_transport=True) as client:
            trading_pairs: List[str] = await self.order_book_data_source.get_trading_pairs()
            trading_pair: str = trading_pairs[0]
            try:
                snapshot: Dict[str, Any] = await self.order_book_data_source.get_snapshot(client, trading_pair)
                return snapshot
            except Exception:
                return None

    def test_get_snapshot(self):
        snapshot: Optional[Dict[str, Any]] = self.run_async(self.get_snapshot())
        self.assertIsNotNone(snapshot)
        self.assertIn(snapshot["tradingPair"], self.run_async(self.order_book_data_source.get_trading_pairs()))

    def test_get_tracking_pairs(self):
        tracking_pairs: Dict[str, OrderBookTrackerEntry] = self.run_async(
            self.order_book_data_source.get_tracking_pairs()
        )
        self.assertIsInstance(tracking_pairs["btc_usdc"], OrderBookTrackerEntry)

    async def mock_inner_messages_for_listen_for_trades(self, query, trading_pairs):
        await asyncio.sleep(0.1)
        yield FixtureNash.TRADE_1
        await asyncio.sleep(0.2)
        yield FixtureNash.TRADE_2
        await asyncio.sleep(0.1)
        yield FixtureNash.TRADE_3
        await asyncio.sleep(5)

    @patch(PATCH_BASE_PATH.format(method="_inner_messages"), mock_inner_messages_for_listen_for_trades)
    def test_listen_for_trades(self):
        timeout = 2
        loop = asyncio.get_event_loop()

        # Instantiate empty async queue and make sure the initial size is 0
        q = asyncio.Queue()
        self.assertEqual(q.qsize(), 0)

        try:
            loop.run_until_complete(
                # Force exit from event loop after set timeout seconds
                asyncio.wait_for(
                    NashAPIOrderBookDataSource().listen_for_trades(ev_loop=loop, output=q), timeout=timeout
                )
            )
        except asyncio.exceptions.TimeoutError as e:
            print(e)

        self.assertGreaterEqual(q.qsize(), 4)

        # Validate received response has correct data types
        for trades in [FixtureNash.TRADE_1, FixtureNash.TRADE_2, FixtureNash.TRADE_3]:
            for trade in trades[1]["newTrades"]:
                item = q.get_nowait()
                self.assertIsInstance(item, OrderBookMessage)
                self.assertIsInstance(item.type, OrderBookMessageType)
                self.assertEqual(item.trade_id, trade["id"])
                self.assertEqual(item.trading_pair, trades[0])
                self.assertEqual(item.content["amount"], trade["amount"]["amount"])
                self.assertEqual(item.content["price"], trade["limitPrice"]["amount"])

    @patch(PATCH_BASE_PATH.format(method="get_snapshot"))
    @patch(PATCH_BASE_PATH.format(method="get_trading_pairs"))
    def test_listen_for_order_book_snapshots(self, mock_get_trading_pairs, mock_get_snapshot):
        loop = asyncio.get_event_loop()

        # Instantiate empty async queue and make sure the initial size is 0
        q = asyncio.Queue()
        self.assertEqual(q.qsize(), 0)

        # Mock Future() object return value as the request response
        mock_get_snapshot.side_effect = [FixtureNash.SNAPSHOT_BTC_USDC, FixtureNash.SNAPSHOT_ETH_BTC]

        # Mock get trading pairs
        mock_get_trading_pairs.return_value = ["btc_usdc", "eth_btc"]

        # Listening for tracking pairs within the set timeout timeframe
        timeout = 6

        try:
            loop.run_until_complete(
                # Force exit from event loop after set timeout seconds
                asyncio.wait_for(
                    NashAPIOrderBookDataSource().listen_for_order_book_snapshots(ev_loop=loop, output=q),
                    timeout=timeout,
                )
            )
        except asyncio.exceptions.TimeoutError as e:
            print(e)

        # Make sure that the number of items in the queue after certain seconds make sense
        # For instance, when the asyncio sleep time is set to 5 seconds in the method
        # If we configure timeout to be the same length, only 1 item has enough time to be received
        self.assertGreaterEqual(q.qsize(), 1)

        first_item = q.get_nowait()
        # Validate received response has correct data types
        self.assertIsInstance(first_item, OrderBookMessage)
        self.assertIsInstance(first_item.type, OrderBookMessageType)

        # Validate order book message type
        self.assertEqual(first_item.type, OrderBookMessageType.SNAPSHOT)

        # Validate snapshot received matches with the original snapshot received from API
        self.assertEqual(first_item.content["bids"], FixtureNash.SNAPSHOT_BTC_USDC["bids"])
        self.assertEqual(first_item.content["asks"], FixtureNash.SNAPSHOT_BTC_USDC["asks"])

        # Validate the rest of the content
        self.assertEqual(first_item.content["trading_pair"], FixtureNash.SNAPSHOT_BTC_USDC["tradingPair"])

    async def mock_inner_messages_for_listen_for_order_book_diffs(self, query, trading_pairs):
        await asyncio.sleep(0.1)
        yield FixtureNash.DIFF_BTC_USDC
        await asyncio.sleep(0.2)
        yield FixtureNash.DIFF_ETH_BTC
        await asyncio.sleep(0.1)
        yield FixtureNash.DIFF_ETH_USDC
        await asyncio.sleep(5)

    @patch(PATCH_BASE_PATH.format(method="_inner_messages"), mock_inner_messages_for_listen_for_order_book_diffs)
    def test_listen_for_order_book_diffs(self):
        timeout = 2
        loop = asyncio.get_event_loop()

        q = asyncio.Queue()

        try:
            loop.run_until_complete(
                # Force exit from event loop after set timeout seconds
                asyncio.wait_for(
                    NashAPIOrderBookDataSource().listen_for_order_book_diffs(ev_loop=loop, output=q), timeout=timeout
                )
            )
        except asyncio.exceptions.TimeoutError as e:
            print(e)

        for diff in [FixtureNash.DIFF_BTC_USDC, FixtureNash.DIFF_ETH_BTC, FixtureNash.DIFF_ETH_USDC]:
            event = q.get_nowait()

            # Validate the data inject into async queue is in Liquid order book message type
            self.assertIsInstance(event, OrderBookMessage)

            # Validate the event type is equal to DIFF
            self.assertEqual(event.type, OrderBookMessageType.DIFF)

            # Validate the actual content injected is dict type
            self.assertIsInstance(event.content, dict)

            # Validate the trading pair is correct
            self.assertEqual(event.trading_pair, diff[0])


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
