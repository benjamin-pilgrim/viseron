"""Pending journal replay advances even if early entries remain blocked."""

import queue
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch
from viseron.components.storage.pending_journal import PREFIX
from viseron.components.storage.tier_handler import TierHandler


def test_bounded_replay_reaches_later_markers(tmp_path):
    """Blocked early markers must not starve later pending publications."""
    run = tmp_path / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir()
    for i in range(5):
        (run / f"{PREFIX}{i}.m4s.json").write_text("{}")
    handler = object.__new__(TierHandler)
    handler._path = str(tmp_path)
    handler._storage = SimpleNamespace(tier_available=lambda _: True)
    handler._event_queue = queue.Queue()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_reported_issues = set()
    handler._logger = Mock()
    handler._journal_retry_stopping = False
    handler._journal_retry_timer = None
    handler._journal_retry_rescan = False
    handler._journal_replay_after = None
    observed = []
    with (
        patch("viseron.components.storage.tier_handler.JOURNAL_BATCH_LIMIT", 2),
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        for _ in range(3):
            handler._replay_pending_publications()
            while not handler._event_queue.empty():
                observed.append(handler._event_queue.get().src_path)
    assert {str(run / f"{i}.m4s") for i in range(5)} == set(observed)
