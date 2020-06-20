#!/usr/bin/env python
import dateutil.parser
import datetime
import logging
from typing import (
    Dict,
    Optional
)
import ujson

from aiokafka import ConsumerRecord
from sqlalchemy.engine import RowProxy

from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType
)

_krob_logger = None


cdef class NashOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _krob_logger
        if _krob_logger is None:
            _krob_logger = logging.getLogger(__name__)
        return _krob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["tradingPair"],
            "update_id": msg["updateId"],
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp * 1e-3)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["tradingPair"],
            "update_id": msg["updateId"],
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp * 1e-3)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        created_at = dateutil.parser.parse(msg["executedAt"])
        ts = created_at.timestamp()
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["tradingPair"],
            "trade_type": float(TradeType.SELL.value) if msg["direction"] == "SELL" else float(TradeType.BUY.value),
            "trade_id": msg["id"],
            "update_id": msg["id"],
            "price": msg["limitPrice"]["amount"],
            "amount": msg["amount"]["amount"]
        }, timestamp=ts)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        retval = NashOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval
