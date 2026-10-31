"""Real watcher regression for storage private-file filtering."""

import logging
import threading
import time
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock

from sqlalchemy import select
from watchdog.events import FileMovedEvent
from watchdog.observers import Observer

from viseron.components.storage.models import Files
from viseron.components.storage.source_move_intent import (
    prepare_source_intent,
    set_source_digest,
)
from viseron.components.storage.tier_handler import TierHandler


def test_real_watcher_does_not_index_private_holds(tmp_path, get_db_session):
    """A real watcher indexes footage but ignores its private source hold."""
    camera = tmp_path / "camera"
    run = camera / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="private-watch")
    handler._storage = SimpleNamespace(
        get_session=get_db_session,
        config={},
        ignored_files={"init.mp4"},
        temporary_files_meta={}, pending_moves_lock=threading.Lock(),
        tier_available=lambda _: True,
    )
    handler._path = str(camera)
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._logger = logging.getLogger("private-watch")
    handler._event_queue = Queue()
    handler._vis = Mock()
    handler.check_tier = Mock()
    handler._on_modified = Mock()
    handler._schedule_journal_retry = Mock()
    # Observe actual callbacks, not just direct calls to the filter.
    handler.on_any_event = Mock(wraps=handler.on_any_event)
    worker = threading.Thread(target=handler._process_events, daemon=True)
    observer = Observer()
    observer.schedule(handler, str(camera), recursive=True)
    observer.start()
    worker.start()
    try:
        media = run / "1.m4s"
        media.write_bytes(b"video")
        limit = time.monotonic() + 5
        while time.monotonic() < limit:
            with get_db_session() as session:
                if session.execute(
                    select(Files.id).where(Files.path == str(media))
                ).first():
                    break
            time.sleep(0.05)
        else:
            raise AssertionError("real media was not indexed")
        assert worker.is_alive()
        marker = prepare_source_intent(
            str(media),
            str(tmp_path / "other" / run.name / media.name),
            media.stat(),
            42,
        )
        set_source_digest(marker, "a" * 64, "b" * 64)
        limit = time.monotonic() + 5
        while time.monotonic() < limit:
            events = [call.args[0] for call in handler.on_any_event.call_args_list]
            if any(".viseron-source-move-" in str(event.src_path) for event in events):
                break
            time.sleep(0.05)
        else:
            raise AssertionError("observer did not see source hold events")
        observer.stop()
        observer.join(5)
        assert worker.is_alive()
        handler._event_queue.put(None)
        worker.join(5)
        assert not worker.is_alive()
        with get_db_session() as session:
            paths = (
                session.execute(
                    select(Files.path).where(Files.camera_identifier == "private-watch")
                )
                .scalars()
                .all()
            )
        assert paths == [str(media)]
    finally:
        observer.stop()
        observer.join(5)
        if worker.is_alive():
            handler._event_queue.put(None)
            worker.join(5)


def test_private_to_private_move_is_never_published(tmp_path):
    """A private bookkeeping rename never creates a media event."""
    handler = object.__new__(TierHandler)
    handler._path = str(tmp_path)
    handler._storage = SimpleNamespace(config={}, ignored_files=set())
    handler._event_queue = Queue()
    handler.on_any_event(
        FileMovedEvent(
            str(tmp_path / ".viseron-tier-move-123"),
            str(tmp_path / ".viseron-source-move-1.m4s.json"),
        )
    )
    assert handler._event_queue.empty()
