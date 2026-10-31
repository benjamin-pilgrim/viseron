"""Tests for writer lease and conservative spool reclamation."""
import os
import signal
from pathlib import Path
from viseron.components.ffmpeg.pipe import _leased_command_for_run
from viseron.components.storage.run_lifecycle import reclaim_spool_run
from viseron.watchdog.subprocess_watchdog import RestartablePopen


def test_running_subprocess_holds_lease_and_sigkill_releases(tmp_path):
    camera = tmp_path/'spool'/'cam'; camera.mkdir(parents=True)
    template = ['python3', '-c', 'import time; time.sleep(60)', str(camera/'__VISERON_RUN__'/'index.m3u8')]
    process = RestartablePopen(template, command_factory=lambda: _leased_command_for_run(template), register=False)
    run = next(camera.iterdir()); lockroot = str(tmp_path/'locks')
    (run/'init.mp4').write_bytes(b'init'); (run/'index.m3u8').write_text('index')
    try:
        assert not reclaim_spool_run(lockroot, str(camera), str(run))
        process.subprocess.kill(); process.subprocess.wait(timeout=5)
        media = run/'1.m4s'; media.write_bytes(b'video')
        assert not reclaim_spool_run(lockroot, str(camera), str(run))
        media.unlink()
        unknown = run/'.viseron-source-move-1.m4s.json'; unknown.write_text('{}')
        assert not reclaim_spool_run(lockroot, str(camera), str(run))
        unknown.unlink()
        assert reclaim_spool_run(lockroot, str(camera), str(run))
        assert not run.exists()
    finally:
        if process.subprocess.poll() is None:
            process.subprocess.kill(); process.subprocess.wait(timeout=5)


def test_missing_lease_is_retained(tmp_path):
    camera = tmp_path/'cam'; run = camera/'20261031T143215.482000Z-a7c93e12ab34'; run.mkdir(parents=True)
    (run/'init.mp4').write_bytes(b'init')
    assert not reclaim_spool_run(str(tmp_path/'locks'), str(camera), str(run))
    assert (run/'init.mp4').exists()


def test_actual_ffmpeg_keeps_lease(tmp_path):
    camera = tmp_path/'cam'; camera.mkdir()
    template = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-re', '-f', 'lavfi', '-i', 'testsrc2=size=96x96:rate=10', '-c:v', 'libx264', '-g', '10', '-f', 'hls', '-hls_time', '1', '-hls_segment_type', 'fmp4', '-hls_segment_filename', str(camera/'__VISERON_RUN__'/'%s.m4s'), str(camera/'__VISERON_RUN__'/'index.m3u8')]
    proc = RestartablePopen(template, command_factory=lambda: _leased_command_for_run(template), register=False, stdout=-3, stderr=-3)
    run = next(camera.iterdir())
    try:
        assert proc.subprocess.poll() is None
        assert not reclaim_spool_run(str(tmp_path/'locks'), str(camera), str(run))
    finally:
        proc.subprocess.kill(); proc.subprocess.wait(timeout=5)


def test_live_ffmpeg_playlist_retains_delayed_segments(tmp_path):
    """A delayed fragmenter can still find the earliest segment after 15s."""
    import time
    camera = tmp_path/'cam'; camera.mkdir()
    template = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-re', '-f', 'lavfi', '-i', 'testsrc2=size=96x96:rate=10', '-t', '15', '-c:v', 'libx264', '-g', '10', '-f', 'hls', '-hls_time', '1', '-hls_segment_type', 'fmp4', '-hls_list_size', '0', '-hls_flags', 'program_date_time', '-strftime', '1', '-hls_segment_filename', str(camera/'__VISERON_RUN__'/'%s.m4s'), str(camera/'__VISERON_RUN__'/'index.m3u8')]
    proc = RestartablePopen(template, command_factory=lambda: _leased_command_for_run(template), register=False, stdout=-3, stderr=-3)
    run = next(camera.iterdir())
    try:
        assert proc.subprocess.wait(timeout=25) == 0
        playlist = (run/'index.m3u8').read_text()
        assert playlist.count('#EXTINF:') >= 14
        assert len(list(run.glob('*.m4s'))) >= 14
        assert not reclaim_spool_run(str(tmp_path/'locks'), str(camera), str(run))
    finally:
        if proc.subprocess.poll() is None:
            proc.subprocess.kill(); proc.subprocess.wait(timeout=5)


def test_dedicated_rawvideo_fd_still_creates_leased_run(tmp_path):
    import logging
    from viseron.components.ffmpeg.const import ORIN_RAWVIDEO_OUTPUT
    from viseron.components.ffmpeg.pipe import FFmpegPipe
    camera = tmp_path/'cam'; camera.mkdir()
    command = ['python3', '-c', 'import time; time.sleep(60)', str(camera/'__VISERON_RUN__'/'index.m3u8'), ORIN_RAWVIDEO_OUTPUT]
    pipe = FFmpegPipe('camera', logging.getLogger('test'), logging.ERROR, decoder_command=command, dedicated_rawvideo_fd=True)
    try:
        pipe.start()
        run = next(camera.iterdir())
        assert (run/'.viseron-writer-lease').exists()
        assert not reclaim_spool_run(str(tmp_path/'locks'), str(camera), str(run))
    finally:
        pipe.close()
