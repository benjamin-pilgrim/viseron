"""Regression tests for failed storage tier moves."""

import logging
import os
import queue
import shutil
import time
from errno import ETIMEDOUT
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from watchdog.events import FileCreatedEvent, FileMovedEvent
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from viseron.components.storage.check_tier import delete_file, move_file
from viseron.components.storage.jobs import (
    OrphanedEventClipsCleanup,
    OrphanedFilesCleanup,
    OrphanedThumbnailsCleanup,
    ZeroSizeFilesCleanup,
    file_is_missing,
)
from viseron.components.storage.tier_handler import TierHandler
from viseron.components.storage.util import TIER_MOVE_TEMP_PREFIX


def test_failed_destination_copy_preserves_source_and_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A destination write error must not destroy the only copy or its record."""
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = tmp_path / "tier" / "destination.m4s"
    get_session = MagicMock()

    def fail_copy(*_args: object, **_kwargs: object) -> None:
        raise OSError("destination unavailable")

    monkeypatch.setattr("shutil.copy", fail_copy)
    monkeypatch.setattr("shutil.copyfileobj", fail_copy)
    with pytest.raises(OSError, match="destination unavailable"):
        move_file(get_session, str(source), str(destination), logging.getLogger())

    assert source.read_bytes() == b"recording"
    assert not destination.exists()
    get_session.assert_not_called()


def test_failed_source_removal_preserves_database_and_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed unlink after publication must retain both copies and their record."""
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = tmp_path / "tier" / "destination.m4s"
    get_session = MagicMock()
    original_remove = os.remove

    def fail_source_removal(path: str) -> None:
        if path == str(source):
            raise OSError("source unavailable")
        original_remove(path)

    monkeypatch.setattr(os, "remove", fail_source_removal)
    with pytest.raises(OSError, match="source unavailable"):
        move_file(get_session, str(source), str(destination), logging.getLogger())

    assert source.read_bytes() == b"recording"
    assert destination.read_bytes() == b"recording"
    get_session.assert_not_called()


def test_failed_delete_preserves_database_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unavailable file must retain its record for a later retry."""
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    get_session = MagicMock()

    def fail_remove(_path: str) -> None:
        raise OSError("tier unavailable")

    monkeypatch.setattr(os, "remove", fail_remove)
    with pytest.raises(OSError, match="tier unavailable"):
        delete_file(get_session, str(source), logging.getLogger())

    assert source.read_bytes() == b"recording"
    get_session.assert_not_called()


def test_inaccessible_file_is_not_considered_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A remote stat error must not be treated as a missing database file."""
    path = tmp_path / "source.m4s"

    def fail_stat(_path: str) -> None:
        raise OSError(ETIMEDOUT, "tier unavailable")

    monkeypatch.setattr(os, "stat", fail_stat)
    assert not file_is_missing(str(path))


def test_zero_size_cleanup_retains_row_when_stat_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient stat error is not evidence that a file is empty."""
    runner = MagicMock()
    runner.kill_event.is_set.return_value = False
    session = runner.get_session.return_value.__enter__.return_value
    row = MagicMock(id=1, path="/remote/segment.m4s")
    first, last = MagicMock(), MagicMock()
    first.scalars.return_value.all.return_value = [row]
    last.scalars.return_value.all.return_value = []
    session.execute.side_effect = [first, last]

    def fail_stat(_path: str) -> int:
        raise OSError(ETIMEDOUT, "tier unavailable")

    monkeypatch.setattr(os.path, "getsize", fail_stat)
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    ZeroSizeFilesCleanup.work(runner)
    assert session.execute.call_count == 2


def test_missing_source_does_not_delete_database_record(tmp_path: Path) -> None:
    """A missing source may be an unavailable mount and is not proof of deletion."""
    get_session = MagicMock()
    with pytest.raises(FileNotFoundError):
        move_file(
            get_session,
            str(tmp_path / "missing.m4s"),
            str(tmp_path / "tier" / "destination.m4s"),
            logging.getLogger(),
        )
    get_session.assert_not_called()


def test_destination_is_published_only_after_copy_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Readers must not observe a partially copied destination."""
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = tmp_path / "tier" / "destination.m4s"
    original_replace = os.replace

    def check_before_publish(src: str, dst: str) -> None:
        assert not destination.exists()
        assert Path(src).read_bytes() == b"recording"
        original_replace(src, dst)

    monkeypatch.setattr(os, "replace", check_before_publish)
    move_file(MagicMock(), str(source), str(destination), logging.getLogger())

    assert destination.read_bytes() == b"recording"
    assert not source.exists()
    assert not list(destination.parent.glob(f"{TIER_MOVE_TEMP_PREFIX}*"))


def test_watcher_ignores_temporary_file_and_handles_publication(tmp_path: Path) -> None:
    """Register the finished destination, never the in-progress temp file."""
    handler = TierHandler.__new__(TierHandler)
    handler._path = str(tmp_path)
    handler._storage = MagicMock()
    handler._storage.ignored_files = []
    handler._event_queue = MagicMock()
    temp = str(tmp_path / f"{TIER_MOVE_TEMP_PREFIX}123")
    destination = str(tmp_path / "segment.m4s")

    handler.on_any_event(FileCreatedEvent(temp))
    handler._event_queue.put.assert_not_called()
    handler.on_any_event(FileMovedEvent(temp, destination))
    handler._event_queue.put.assert_called_once_with(FileCreatedEvent(destination))


def test_rename_error_after_publication_keeps_source_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ambiguous rename result must not make the source disposable."""
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = tmp_path / "tier" / "destination.m4s"
    original_replace = os.replace

    def publish_then_fail(src: str, dst: str) -> None:
        original_replace(src, dst)
        raise OSError("rename response lost")

    monkeypatch.setattr(os, "replace", publish_then_fail)
    get_session = MagicMock()
    with pytest.raises(OSError, match="rename response lost"):
        move_file(get_session, str(source), str(destination), logging.getLogger())

    assert source.read_bytes() == b"recording"
    assert destination.read_bytes() == b"recording"
    get_session.assert_not_called()


def test_reject_copy_when_source_grows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not publish or remove a source that changed during copying."""
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = tmp_path / "tier" / "destination.m4s"
    original_copy = shutil.copyfileobj

    def copy_then_extend(src: object, dst: object) -> None:
        original_copy(src, dst)  # type: ignore[arg-type]
        with source.open("ab") as writer:
            writer.write(b"more")

    monkeypatch.setattr(shutil, "copyfileobj", copy_then_extend)
    with pytest.raises(OSError, match="Incomplete copy"):
        move_file(MagicMock(), str(source), str(destination), logging.getLogger())

    assert source.read_bytes() == b"recordingmore"
    assert not destination.exists()


@pytest.mark.parametrize(
    "job",
    [OrphanedFilesCleanup, OrphanedEventClipsCleanup, OrphanedThumbnailsCleanup],
)
def test_cleanup_ignores_in_progress_copy(tmp_path: Path, job: object) -> None:
    """Cleanup must not remove a temporary file while a copy is in progress."""
    temp = tmp_path / f"{TIER_MOVE_TEMP_PREFIX}123"
    temp.write_bytes(b"in progress")
    runner = MagicMock()
    runner.paths = [str(tmp_path)]
    runner.ignored_files = frozenset()
    runner.kill_event.is_set.return_value = False
    session = runner.get_session.return_value.__enter__.return_value
    session.execute.return_value.first.return_value = None
    job.work(runner)  # type: ignore[attr-defined]
    assert temp.read_bytes() == b"in progress"


@pytest.mark.parametrize("observer_type", [Observer, PollingObserver])
def test_observer_sees_only_published_destination(
    tmp_path: Path, observer_type: object
) -> None:
    """Both native and polling observers register a completed tier move."""
    tier = tmp_path / "tier"
    tier.mkdir()
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = tier / "segment.m4s"
    handler = TierHandler.__new__(TierHandler)
    handler._path = str(tier)
    handler._storage = MagicMock()
    handler._storage.ignored_files = []
    handler._event_queue = queue.Queue()
    observer = observer_type(timeout=0.05)  # type: ignore[operator]
    observer.schedule(handler, str(tier), recursive=True)
    observer.start()
    try:
        move_file(MagicMock(), str(source), str(destination), logging.getLogger())
        deadline = time.monotonic() + 3
        seen_destination = False
        while time.monotonic() < deadline and not seen_destination:
            try:
                event = handler._event_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            assert not os.path.basename(event.src_path).startswith(
                TIER_MOVE_TEMP_PREFIX
            )
            seen_destination = isinstance(event, FileCreatedEvent) and (
                event.src_path == str(destination)
            )
        assert seen_destination
        assert destination.read_bytes() == b"recording"
    finally:
        observer.stop()
        observer.join()
