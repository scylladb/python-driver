"""
Unit tests for lazy initialization of _callbacks/_errbacks in ResponseFuture.
"""
import unittest
from unittest.mock import Mock, patch, PropertyMock
from threading import Lock, Event

from cassandra.cluster import ResponseFuture, _NOT_SET
from cassandra.query import SimpleStatement
from cassandra.policies import RetryPolicy


def make_response_future():
    """Create a minimal ResponseFuture for testing."""
    session = Mock()
    session.cluster._default_load_balancing_policy = Mock()
    session.cluster._default_load_balancing_policy.make_query_plan.return_value = iter([])
    session.row_factory = Mock()
    session.cluster.connect_timeout = 5
    session._create_clock.return_value = None

    message = Mock()
    query = SimpleStatement("SELECT 1")
    return ResponseFuture(session, message, query, timeout=10.0,
                          retry_policy=RetryPolicy())


class TestLazyInitCallbacks(unittest.TestCase):

    def test_callbacks_initially_none(self):
        """_callbacks and _errbacks should be None after __init__."""
        rf = make_response_future()
        self.assertIsNone(rf._callbacks)
        self.assertIsNone(rf._errbacks)

    def test_add_callback_lazy_inits(self):
        """add_callback should create the list on first use."""
        rf = make_response_future()
        self.assertIsNone(rf._callbacks)
        rf.add_callback(lambda result: None)
        self.assertIsNotNone(rf._callbacks)
        self.assertEqual(len(rf._callbacks), 1)

    def test_add_errback_lazy_inits(self):
        """add_errback should create the list on first use."""
        rf = make_response_future()
        self.assertIsNone(rf._errbacks)
        rf.add_errback(lambda exc: None)
        self.assertIsNotNone(rf._errbacks)
        self.assertEqual(len(rf._errbacks), 1)

    def test_set_final_result_no_callbacks(self):
        """_set_final_result should work when _callbacks is None."""
        rf = make_response_future()
        self.assertIsNone(rf._callbacks)
        # Should not raise
        rf._set_final_result("some result")
        self.assertEqual(rf._final_result, "some result")
        self.assertTrue(rf._event.is_set())

    def test_set_final_exception_no_errbacks(self):
        """_set_final_exception should work when _errbacks is None."""
        rf = make_response_future()
        self.assertIsNone(rf._errbacks)
        exc = Exception("test error")
        # Should not raise
        rf._set_final_exception(exc)
        self.assertIs(rf._final_exception, exc)
        self.assertTrue(rf._event.is_set())

    def test_set_final_result_with_callbacks(self):
        """_set_final_result should invoke registered callbacks."""
        rf = make_response_future()
        results = []
        rf.add_callback(lambda result: results.append(result))
        rf._set_final_result("data")
        self.assertEqual(results, ["data"])

    def test_set_final_exception_with_errbacks(self):
        """_set_final_exception should invoke registered errbacks."""
        rf = make_response_future()
        errors = []
        rf.add_errback(lambda exc: errors.append(exc))
        exc = Exception("fail")
        rf._set_final_exception(exc)
        self.assertEqual(errors, [exc])

    def test_multiple_callbacks(self):
        """Multiple callbacks should all be invoked."""
        rf = make_response_future()
        r1, r2 = [], []
        rf.add_callback(lambda result: r1.append(result))
        rf.add_callback(lambda result: r2.append(result))
        rf._set_final_result("ok")
        self.assertEqual(r1, ["ok"])
        self.assertEqual(r2, ["ok"])

    def test_clear_callbacks_resets_to_none(self):
        """clear_callbacks should set both back to None."""
        rf = make_response_future()
        rf.add_callback(lambda r: None)
        rf.add_errback(lambda e: None)
        self.assertIsNotNone(rf._callbacks)
        self.assertIsNotNone(rf._errbacks)
        rf.clear_callbacks()
        self.assertIsNone(rf._callbacks)
        self.assertIsNone(rf._errbacks)

    def test_add_callback_after_result(self):
        """add_callback after _set_final_result should run immediately."""
        rf = make_response_future()
        rf._set_final_result("data")
        results = []
        rf.add_callback(lambda result: results.append(result))
        self.assertEqual(results, ["data"])

    def test_add_errback_after_exception(self):
        """add_errback after _set_final_exception should run immediately."""
        rf = make_response_future()
        exc = Exception("fail")
        rf._set_final_exception(exc)
        errors = []
        rf.add_errback(lambda e: errors.append(e))
        self.assertEqual(errors, [exc])


if __name__ == '__main__':
    unittest.main()
