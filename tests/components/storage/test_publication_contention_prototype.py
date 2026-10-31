"""Expected publication contention must remain retryable without log storms."""
import datetime
import logging
from viseron.components.storage.run_lifecycle import camera_lock
from viseron.domains.camera.fragmenter_process import FragmenterWork

RUN='20261031T143215.482000Z-a7c93e12ab34'

def make_work(tmp_path):
    stage=tmp_path/'stage'; run=stage/RUN; run.mkdir(parents=True)
    (run/'init.mp4').write_bytes(b'init')
    source=run/'1.m4s'; source.write_bytes(b'video')
    target=tmp_path/'segments'/'cam'
    return FragmenterWork('cam',str(stage),str(target),None,None,None), source, target

def test_expected_lock_contention_retains_then_retries(tmp_path, caplog):
    worker,source,camera=make_work(tmp_path)
    timestamp=datetime.datetime.now(datetime.timezone.utc)
    with caplog.at_level(logging.WARNING):
        with camera_lock('/tmp/viseron-run-locks',str(camera)):
            worker._move_to_segments_folder(f'{RUN}/1.m4s',timestamp,1.5)
        assert not caplog.records
        assert source.read_bytes()==b'video' and not (camera/RUN/source.name).exists()
        worker._move_to_segments_folder(f'{RUN}/1.m4s',timestamp,1.5)
    assert (camera/RUN/source.name).read_bytes()==b'video' and source.read_bytes()==b'video'

def test_destination_collision_is_reported_and_keeps_source(tmp_path, caplog):
    worker,source,camera=make_work(tmp_path)
    run=camera/RUN; run.mkdir(parents=True)
    from viseron.components.storage.run_init_prototype import ensure_init
    ensure_init(source.parent/'init.mp4',run)
    (run/'1.m4s').write_bytes(b'old')
    with caplog.at_level(logging.WARNING):
        worker._move_to_segments_folder(f'{RUN}/1.m4s',datetime.datetime.now(datetime.timezone.utc),1.5)
    assert 'already exists' in caplog.text
    assert source.read_bytes()==b'video' and (run/'1.m4s').read_bytes()==b'old'

def test_unrelated_eagain_is_not_mistaken_for_lock_contention(tmp_path, caplog, monkeypatch):
    import errno
    worker,source,_=make_work(tmp_path)
    def storage_busy(*_): raise BlockingIOError(errno.EAGAIN, 'storage busy')
    monkeypatch.setattr('viseron.domains.camera.fragmenter_process.ensure_init',storage_busy)
    with caplog.at_level(logging.WARNING):
        worker._move_to_segments_folder(f'{RUN}/1.m4s',datetime.datetime.now(datetime.timezone.utc),1.5)
    assert 'Cannot publish' in caplog.text and source.read_bytes()==b'video'
