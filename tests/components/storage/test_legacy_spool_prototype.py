"""Preserve the established staging path for custom commands."""
from viseron.const import TEMP_DIR
from viseron.domains.camera import _recording_spool_path

def test_custom_recorder_and_non_ffmpeg_keep_legacy_staging():
    folder='/segments/camera'
    assert _recording_spool_path('ffmpeg', {'raw_command':'custom'}, folder) == TEMP_DIR + folder
    assert _recording_spool_path('gstreamer', {}, folder) == TEMP_DIR + folder
    assert _recording_spool_path('ffmpeg', {'raw_command':None}, folder) == '/segments/.viseron-spool/camera'
