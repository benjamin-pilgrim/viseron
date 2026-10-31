"""Tests for isolated init identity experiment."""
import pytest
from viseron.components.storage.run_init_prototype import ensure_init, MARKER

def test_published_identity_replay_and_collision(tmp_path):
    source = tmp_path/'source'/'init.mp4'; source.parent.mkdir()
    source.write_bytes(b'initial'); run = tmp_path/'run'
    ensure_init(source, run)
    ensure_init(source, run)
    assert (run/'init.mp4').read_bytes() == b'initial'
    (run/'init.mp4').unlink(); (run/'init.mp4').write_bytes(b'other')
    with pytest.raises(ValueError): ensure_init(source, run)
    assert source.read_bytes() == b'initial'

def test_existing_unowned_init_fails_closed(tmp_path):
    source = tmp_path/'source'; source.write_bytes(b'new')
    run = tmp_path/'run'; run.mkdir(); (run/'init.mp4').write_bytes(b'old')
    with pytest.raises(FileExistsError): ensure_init(source, run)
    assert (run/'init.mp4').read_bytes() == b'old'
    assert not (run/MARKER).exists()

def test_interrupted_publication_replays(tmp_path, monkeypatch):
    from viseron.components.storage import run_init_prototype as mod
    source = tmp_path/'source'; source.write_bytes(b'new')
    run = tmp_path/'run'
    publish = mod._publish
    def interrupted(src, dst):
        if dst.name == 'init.mp4':
            raise OSError('crash before rename')
        return publish(src, dst)
    with monkeypatch.context() as patcher:
        patcher.setattr(mod, '_publish', interrupted)
        with pytest.raises(OSError): ensure_init(source, run)
    assert (run/MARKER).exists()
    ensure_init(source, run)
    assert (run/'init.mp4').read_bytes() == b'new'

def test_reclaim_init_marker_only_when_empty_and_unreferenced(tmp_path):
    from viseron.components.storage.run_lifecycle import reclaim_empty_run
    from viseron.components.storage.run_init_prototype import MARKER
    camera = tmp_path/'camera'; run = camera/'20261031T143215.482000Z-a7c93e12ab34'
    run.mkdir(parents=True)
    (run/'init.mp4').write_bytes(b'init'); (run/MARKER).write_text('{}')
    check = lambda: None
    assert not reclaim_empty_run(str(tmp_path/'locks'), str(camera), str(run), check, lambda _: True)
    extra = run/'pending'; extra.write_text('pending')
    assert not reclaim_empty_run(str(tmp_path/'locks'), str(camera), str(run), check, lambda _: False)
    extra.unlink()
    assert reclaim_empty_run(str(tmp_path/'locks'), str(camera), str(run), check, lambda _: False)
    assert not run.exists()


def test_watcher_ignores_init_marker_temporary(tmp_path):
    """Private init files must never be queued as media by the real handler."""
    from queue import Queue
    from types import SimpleNamespace
    from unittest.mock import patch
    from watchdog.events import FileCreatedEvent, FileMovedEvent
    from viseron.components.storage import run_init_prototype as mod
    from viseron.components.storage.tier_handler import TierHandler
    source = tmp_path/'source'; source.write_bytes(b'new')
    run = tmp_path/'run'; camera = run.parent
    handler = object.__new__(TierHandler)
    handler._path = str(camera)
    handler._storage = SimpleNamespace(config={}, ignored_files=['init.mp4'])
    handler._event_queue = Queue()
    original = mod.tempfile.mkstemp
    names = []
    def capture(*args, **kwargs):
        fd, name = original(*args, **kwargs); names.append(name); return fd, name
    with patch.object(mod.tempfile, 'mkstemp', side_effect=capture):
        ensure_init(source, run)
    for name in names:
        handler.on_any_event(FileCreatedEvent(name))
        handler.on_any_event(FileMovedEvent(name, str(run/MARKER)))
    assert handler._event_queue.empty()
