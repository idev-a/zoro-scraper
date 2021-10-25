import os
import time
from os import path
import unittest
from datetime import timedelta
from typing import Optional

from sgscrape.pause_resume import CrawlState, CrawlStateSingleton, SerializableRequest


class PauseResumeTest(unittest.TestCase):

    def setUp(self) -> None:
        if path.exists('state.json'):
            os.remove('state.json')

    def tearDown(self) -> None:
        if path.exists('state.json'):
            os.remove('state.json')

    @staticmethod
    def __fresh_state_instance(new_save_time: Optional[timedelta] = None):
        CrawlStateSingleton._delete_instance()
        if new_save_time is not None:
            CrawlStateSingleton.set_minimum_time_between_saves(new_save_time)
        return CrawlStateSingleton.get_instance()

    @staticmethod
    def populate_state(state: CrawlState) -> CrawlState:
        state.push_request(SerializableRequest(url='http://example.com/1'))
        state.push_request(SerializableRequest(url='http://example.com/2'))
        state.push_request(SerializableRequest(url='http://example.com/3'))

        state.increment_visited_coords('us')
        state.increment_visited_coords('ca')
        state.increment_visited_coords('uk')

        state.set_misc_value('field_value', 'zero')
        state.set_misc_value('y', 2)
        state.set_misc_value('z', 3.1)

        return state

    def test_pause_resume(self):
        state = PauseResumeTest.__fresh_state_instance(timedelta(milliseconds=0))
        self.populate_state(state)
        self.populate_state(state)  # attempts to insert deplucates.
        state.push_request(SerializableRequest(url='http://example.com/2'))  # out of order duplicate

        # re-read from disk
        state2 = PauseResumeTest.__fresh_state_instance()

        urls = [req.url for req in state2.request_stack_iter()]

        self.assertEqual(['http://example.com/3', 'http://example.com/2', 'http://example.com/1'], urls)
        self.assertEqual(2, state2.get_visited_coords('us'))  # called populate_state twice with 'us'
        self.assertEqual(2, state2.get_visited_coords('ca'))  # called populate_state twice with 'ca'
        self.assertEqual(2, state2.get_visited_coords('uk'))  # called populate_state twice with 'uk'
        self.assertEqual(0, state2.get_visited_coords('cn'))  # not called with 'cn'

        self.assertEqual('zero', state2.get_misc_value('field_value'))
        self.assertEqual(2, state2.get_misc_value('y'))
        self.assertEqual(3.1, state2.get_misc_value('z'))

    def test_default_ctor(self):
        state = PauseResumeTest.__fresh_state_instance(timedelta(seconds=0))
        key = "XXX"
        value = "YYY"

        value_1 = state.get_misc_value(key, default_factory=lambda: value)
        self.assertEqual(value, value_1)

        state2 = PauseResumeTest.__fresh_state_instance()
        self.assertEqual(value, state2.get_misc_value(key))

    def test_save_within_timeline(self):
        state = PauseResumeTest.__fresh_state_instance(timedelta(seconds=1))
        time.sleep(0.5)
        state.set_misc_value('field_value', 'y')

        state2 = PauseResumeTest.__fresh_state_instance()

        self.assertIsNone(state2.get_misc_value('field_value'))

        time.sleep(1.5)
        state2.set_misc_value('field_value', 'y')

        state3 = PauseResumeTest.__fresh_state_instance()

        self.assertEqual('y', state3.get_misc_value('field_value'))

    def test_dup_streak(self):
        state = PauseResumeTest.__fresh_state_instance(timedelta(seconds=0))

        self.assertEqual(0, state.get_duplicate_streak())
        self.assertEqual(1, state.increment_and_get_duplicate_streak())
        self.assertEqual(0, state.reset_and_get_duplicate_streak())
        self.assertEqual(1, state.increment_and_get_duplicate_streak())

        state2 = PauseResumeTest.__fresh_state_instance()
        self.assertEqual(1, state2.get_duplicate_streak())


if __name__ == "__main__":
    unittest.main()
