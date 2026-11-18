"""Generated hardware defaults preserve exact timing across playlist rotation."""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("vaapi", ["false", "true"])
def test_writer_captures_timing_with_generated_hardware_defaults(tmp_path, vaapi):
    """A freshly configured writer must recover footage after its playlist rotates."""
    # Camera defaults are resolved when the module is imported, just as in the
    # service. Use a fresh interpreter with the actual hardware environment.
    script = """
import logging,multiprocessing,queue,sys,threading
from pathlib import Path
from viseron.components.ffmpeg.camera import CAMERA_SCHEMA
from viseron.components.ffmpeg.pipe import FFmpegPipe
from viseron.helpers.recording_timing_contract import permits_timing_capture
from viseron.domains.camera.playlist_timing import capture_playlist_times
from viseron.domains.camera.fragmenter_process import FragmenterWork
from tests.domains.camera.test_backlog_timing import _fragment_bytes
stage=Path(sys.argv[1])/'stage'
marker=multiprocessing.get_context('fork').Array('c',64)
config=CAMERA_SCHEMA({'name':'Camera','host':'camera.example','port':554,'path':'/video'})
command=[sys.executable,'-c','import time;time.sleep(60)',
         str(stage/'__VISERON_RUN__'/'index.m3u8')]
pipe=FFmpegPipe('camera',logging.getLogger('test'),20,decoder_command=command,
                run_marker=marker,timing_capture_safe=permits_timing_capture(config))
try:
    pipe.start()
    run=stage/marker.value.decode('ascii')
    name='000000001-1760000000-0000001000000.m4s'
    payload=_fragment_bytes(b'recovered footage')
    (run/name).write_bytes(payload)
    (run/'init.mp4').write_bytes(b'init')
    playlist=run/'index.m3u8'
    playlist.write_text('#EXTM3U\\n#EXTINF:1.000000,\\n'
                        '#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15.123+0000\\n'+name+'\\n')
    capture_playlist_times(stage)
    playlist.write_text('#EXTM3U\\n')
    destination=Path(sys.argv[1])/'published'
    work=FragmenterWork('camera',str(stage),str(destination),None,None,threading.Event())
    work.output_queue=queue.Queue()
    work.work_input({'cmd':'fragment'})
    assert (destination/run.name/name).exists(), 'stranded after playlist rotation'
    assert (destination/run.name/name).read_bytes()==payload
    result=work.output_queue.get_nowait()
    assert result['orig_ctime'].isoformat()=='2026-10-31T14:32:15.123000+00:00'
    assert result['duration']==1.0
finally:
    pipe.close()
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        env={
            **os.environ,
            "VISERON_VAAPI_SUPPORTED": vaapi,
            "VISERON_CUDA_SUPPORTED": "false",
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
