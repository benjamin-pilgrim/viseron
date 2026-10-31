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


def test_init_remains_usable_when_storage_changes_mtime_during_rename(tmp_path, monkeypatch):
    """A share that rewrites metadata on rename can still publish the init."""
    import os
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init bytes")
    run = tmp_path / "run"
    publish = mod._publish

    def rename_with_metadata_change(src, dst):
        publish(src, dst)
        if dst.name == "init.mp4":
            st = dst.stat()
            os.utime(dst, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))

    monkeypatch.setattr(mod, "_publish", rename_with_metadata_change)
    mod.ensure_init(source, run)
    assert (run / "init.mp4").read_bytes() == source.read_bytes()
    mod.ensure_init(source, run)


def test_tier_promotion_reuses_verified_init_without_routine_readback(tmp_path, monkeypatch):
    """Repeated fragment promotion does not repeatedly read the network init."""
    import builtins
    from viseron.components.storage.check_tier import publish_run_init

    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    source = tmp_path / "local" / run_id
    destination = tmp_path / "remote" / run_id
    source.mkdir(parents=True)
    (source / "init.mp4").write_bytes(b"camera init bytes")
    publish_run_init(str(source / "000.m4s"), str(destination / "000.m4s"), {})
    open_file = builtins.open

    def no_destination_read(path, mode="r", *args, **kwargs):
        if str(path) == str(destination / "init.mp4") and "r" in mode:
            pytest.fail("unchanged destination init was read back")
        return open_file(path, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", no_destination_read)
    publish_run_init(str(source / "001.m4s"), str(destination / "001.m4s"), {})


def test_existing_tier_init_is_adopted_only_when_contents_match(tmp_path):
    from viseron.components.storage.check_tier import publish_run_init

    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    source = tmp_path / "local" / run_id
    destination = tmp_path / "remote" / run_id
    source.mkdir(parents=True)
    destination.mkdir(parents=True)
    (source / "init.mp4").write_bytes(b"correct")
    target = destination / "init.mp4"
    target.write_bytes(b"wrong!!")
    with pytest.raises(ValueError, match="conflicting existing"):
        publish_run_init(str(source / "000.m4s"), str(destination / "000.m4s"), {})
    assert target.read_bytes() == b"wrong!!"
    target.write_bytes(b"correct")
    publish_run_init(str(source / "000.m4s"), str(destination / "000.m4s"), {})
    assert (destination / MARKER).exists()


def test_tier_promotion_accepts_recreated_matching_source_init(tmp_path):
    """A drained first-tier run may later be recreated from the recorder spool."""
    from viseron.components.storage.check_tier import publish_run_init

    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    source = tmp_path / "local" / run_id
    destination = tmp_path / "remote" / run_id
    source.mkdir(parents=True)
    init = source / "init.mp4"
    init.write_bytes(b"correct run init")
    publish_run_init(str(source / "000.m4s"), str(destination / "000.m4s"), {})
    replacement = source / "replacement"
    replacement.write_bytes(b"correct run init")
    replacement.replace(init)
    publish_run_init(str(source / "001.m4s"), str(destination / "001.m4s"), {})

    replacement.write_bytes(b"different format")
    replacement.replace(init)
    with pytest.raises(ValueError, match="source content changed"):
        publish_run_init(str(source / "002.m4s"), str(destination / "002.m4s"), {})


def test_interrupted_after_init_rename_replays(tmp_path, monkeypatch):
    """A crash after rename leaves an owned init that can be recovered."""
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init")
    run = tmp_path / "run"
    publish = mod._publish

    def interrupt_after_rename(src, dst):
        publish(src, dst)
        if dst.name == "init.mp4":
            raise OSError("crash after rename")

    with monkeypatch.context() as patcher:
        patcher.setattr(mod, "_publish", interrupt_after_rename)
        with pytest.raises(OSError, match="crash after rename"):
            mod.ensure_init(source, run)
    assert (run / MARKER).exists()
    mod.ensure_init(source, run)
    assert (run / "init.mp4").read_bytes() == b"camera init"


def test_rename_metadata_change_does_not_accept_corrupt_init(tmp_path, monkeypatch):
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"valid init")
    run = tmp_path / "run"
    publish = mod._publish

    def rename_with_corruption(src, dst):
        publish(src, dst)
        if dst.name == "init.mp4":
            dst.write_bytes(b"bad!! init")

    monkeypatch.setattr(mod, "_publish", rename_with_corruption)
    with pytest.raises(ValueError, match="destination content changed"):
        mod.ensure_init(source, run)
    assert source.read_bytes() == b"valid init"


def test_identical_init_remains_usable_after_share_identity_changes(tmp_path, monkeypatch):
    """A remounted share may report a different device or inode for the same init."""
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init")
    run = tmp_path / "run"
    mod.ensure_init(source, run)
    target = run / "init.mp4"
    original_id = mod._id

    def remounted_identity(path):
        identity = original_id(path)
        if path == target:
            identity[0] += 1
            identity[1] += 1
        return identity

    monkeypatch.setattr(mod, "_id", remounted_identity)
    mod.ensure_init(source, run)
    monkeypatch.setattr(mod, "_hash", lambda _: pytest.fail("unnecessary readback"))
    mod.ensure_init(source, run)


def test_changed_init_is_rejected_after_share_identity_changes(tmp_path, monkeypatch):
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init")
    run = tmp_path / "run"
    mod.ensure_init(source, run)
    target = run / "init.mp4"
    target.write_bytes(b"wrong! init")
    original_id = mod._id

    def remounted_identity(path):
        identity = original_id(path)
        if path == target:
            identity[0] += 1
            identity[1] += 1
        return identity

    monkeypatch.setattr(mod, "_id", remounted_identity)
    with pytest.raises(ValueError, match="destination content changed"):
        mod.ensure_init(source, run)


def test_prepared_init_recovers_after_share_identity_changes(tmp_path, monkeypatch):
    """An init prepared before a crash survives a later share remount."""
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init")
    run = tmp_path / "run"
    publish = mod._publish

    def interrupt(src, dst):
        if dst.name == "init.mp4":
            raise OSError("interrupted before publish")
        publish(src, dst)

    with monkeypatch.context() as patcher:
        patcher.setattr(mod, "_publish", interrupt)
        with pytest.raises(OSError, match="interrupted"):
            mod.ensure_init(source, run)
    original_id = mod._id

    def remounted_identity(path):
        identity = original_id(path)
        if path.name.startswith(".viseron-tier-move-init-"):
            identity[0] += 1
            identity[1] += 1
        return identity

    monkeypatch.setattr(mod, "_id", remounted_identity)
    mod.ensure_init(source, run)
    assert (run / "init.mp4").read_bytes() == b"camera init"


def test_remount_verification_rejects_init_that_changes_during_read(tmp_path, monkeypatch):
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init")
    run = tmp_path / "run"
    mod.ensure_init(source, run)
    target = run / "init.mp4"
    original_id = mod._id
    count = 0

    def unstable_identity(path):
        nonlocal count
        identity = original_id(path)
        if path == target:
            count += 1
            identity[0] += count
        return identity

    monkeypatch.setattr(mod, "_id", unstable_identity)
    with pytest.raises(ValueError, match="destination content changed"):
        mod.ensure_init(source, run)


def test_legacy_marker_recovers_when_source_identity_changes(tmp_path, monkeypatch):
    """Legacy markers gain a digest after verifying both sides on a remount."""
    import json
    from viseron.components.storage import run_init_prototype as mod

    source = tmp_path / "source"
    source.write_bytes(b"camera init")
    run = tmp_path / "run"
    mod.ensure_init(source, run)
    marker = run / MARKER
    record = json.loads(marker.read_text())
    record["version"] = 1
    del record["digest"]
    marker.write_text(json.dumps(record))
    original_id = mod._id

    def remounted_source(path):
        identity = original_id(path)
        if path == source:
            identity[0] += 1
        return identity

    monkeypatch.setattr(mod, "_id", remounted_source)
    mod.ensure_init(source, run)
    assert json.loads(marker.read_text())["version"] == 2
