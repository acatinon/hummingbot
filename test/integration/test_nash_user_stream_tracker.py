#!/usr/bin/env python

from hummingbot.market.nash.nash_user_stream_tracker import NashUserStreamTracker
from hummingbot.market.nash.nash_auth import NashAuth
from hummingbot.core.utils.async_utils import safe_ensure_future
import asyncio
import logging
import unittest
import conf


class NashUserStreamTrackerUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.nash_auth = NashAuth(conf.nash_api_key, conf.nash_secret_key)
        cls.user_stream_tracker: NashUserStreamTracker = NashUserStreamTracker(nash_auth=cls.nash_auth)
        cls.user_stream_tracker_task: asyncio.Task = safe_ensure_future(cls.user_stream_tracker.start())

    def run_async(self, task):
        return self.ev_loop.run_until_complete(task)

    def test_user_stream(self):
        self.ev_loop.run_until_complete(asyncio.sleep(20.0))
        print(self.user_stream_tracker.user_stream)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
