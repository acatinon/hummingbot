#!/usr/bin/env python

import logging
from typing import List, Optional

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker, OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.remote_api_order_book_data_source import RemoteAPIOrderBookDataSource
from hummingbot.market.nash.nash_api_order_book_data_source import NashAPIOrderBookDataSource


class NashOrderBookTracker(OrderBookTracker):
    _krobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._krobt_logger is None:
            cls._krobt_logger = logging.getLogger(__name__)
        return cls._krobt_logger

    def __init__(
        self,
        data_source_type: OrderBookTrackerDataSourceType = OrderBookTrackerDataSourceType.EXCHANGE_API,
        trading_pairs: Optional[List[str]] = None,
    ):
        super().__init__(data_source_type=data_source_type)

        self._data_source: Optional[OrderBookTrackerDataSource] = None
        self._trading_pairs: Optional[List[str]] = trading_pairs

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        if not self._data_source:
            if self._data_source_type is OrderBookTrackerDataSourceType.REMOTE_API:
                self._data_source = RemoteAPIOrderBookDataSource()
            elif self._data_source_type is OrderBookTrackerDataSourceType.EXCHANGE_API:
                self._data_source = NashAPIOrderBookDataSource(trading_pairs=self._trading_pairs)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "nash"
