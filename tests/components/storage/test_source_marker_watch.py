"""Source move bookkeeping must never be indexed as media."""

from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileMovedEvent
from viseron.components.storage.tier_handler import TierHandler


def test_source_move_events_are_never_indexed(tmp_path):
    """Ignore direct and moved source intents but retain real media events."""
    handler = object.__new__(TierHandler)
    handler._path = str(tmp_path)
    handler._storage = SimpleNamespace(config={}, ignored_files=[])
    handler._event_queue = Queue()
    handler._on_created_impl = Mock()
    marker = str(tmp_path / ".viseron-source-move-a.m4s.json")
    staged = str(tmp_path / ".viseron-tier-move-abcdef12")
    media = str(tmp_path / "a.m4s")
    for event in (
        FileCreatedEvent(marker),
        FileModifiedEvent(marker),
        FileMovedEvent(staged, marker),
    ):
        handler.on_any_event(event)
    assert handler._event_queue.empty()
    handler._on_created(FileCreatedEvent(marker))
    handler._on_created_impl.assert_not_called()
    handler.on_any_event(FileMovedEvent(staged, media))
    assert handler._event_queue.get_nowait().src_path == media
