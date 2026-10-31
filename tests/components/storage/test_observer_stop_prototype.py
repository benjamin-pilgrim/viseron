"""Shutdown race with scheduler cleanup."""

import logging
import queue
import threading
from unittest.mock import Mock
from apscheduler.jobstores.base import JobLookupError
from viseron.components.storage.tier_handler import TierHandler


def test_stop_continues_if_scheduler_already_removed_job():
    """Stopping remains safe when the scheduler has removed its job."""
    handler = object.__new__(TierHandler)
    handler._logger = logging.getLogger("test")
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_token = "/tmp/nonexistent-viseron-retry-test-token"
    handler._source_intent_job = Mock()
    handler._source_intent_job.remove.side_effect = JobLookupError("job")
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_paths = set()
    handler._journal_retry_timer = None
    handler._check_tier_lock = threading.Lock()
    handler._deletion_retry_stop = threading.Event()
    handler._tier_check_retry_timer = None
    handler._observer_lock = threading.Lock()
    handler._observer = Mock()
    handler._pending_updates = {}
    handler._event_queue = queue.Queue()
    handler._event_thread = Mock()
    handler._stop_observer()
    assert handler._source_intent_job is None
    handler._observer.stop.assert_called_once()
    handler._observer.join.assert_called_once()
