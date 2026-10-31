"""Isolated first-tier interrupted-publication recovery tests."""
import datetime
import os
from pathlib import Path
from unittest.mock import Mock
import pytest
from viseron.components.storage.source_move_intent import prepare_source_intent, marker_for_source
from viseron.components.storage.pending_journal import marker_for_path
from viseron.domains.camera.fragmenter_process import FragmenterWork, _get_mp4_files_to_fragment, _get_orig_ctime

RUN = '20261031T143215.482000Z-a7c93e12ab34'

def make_work(tmp_path):
    camera = tmp_path/'segments'/'cam'
    spool = camera.parent/'.viseron-spool'/camera.name
    run = spool/RUN; run.mkdir(parents=True)
    (run/'init.mp4').write_bytes(b'init')
    source = run/'1760000000.m4s'; source.write_bytes(b'video')
    work = FragmenterWork('cam', str(spool), str(camera), None, None, None)
    return work, source, camera/RUN/source.name

def test_intent_without_destination_journal_is_discoverable_and_retried(tmp_path):
    work, source, dest = make_work(tmp_path)
    intent = prepare_source_intent(str(source), str(dest), os.stat(source), None)
    assert f'{RUN}/{source.name}' in _get_mp4_files_to_fragment(work.temp_segments_folder)
    work._move_to_segments_folder(f'{RUN}/{source.name}', datetime.datetime.now(datetime.timezone.utc), 1.5)
    assert intent.exists() and Path(marker_for_path(str(dest))).exists()
    assert source.read_bytes() == dest.read_bytes() == b'video'

def test_incomplete_source_intent_fails_closed(tmp_path):
    work, source, dest = make_work(tmp_path)
    intent = marker_for_source(source); intent.write_text('{')
    with pytest.raises(ValueError, match='incomplete source move intent'):
        work._move_to_segments_folder(f'{RUN}/{source.name}', datetime.datetime.now(datetime.timezone.utc), 1.5)
    assert source.read_bytes() == b'video'
    assert intent.read_text() == '{'
    assert not dest.exists() and not os.path.exists(marker_for_path(str(dest)))

def test_epoch_fallback_uses_basename():
    assert _get_orig_ctime(f'{RUN}/1760000000.m4s', None) == datetime.datetime.fromtimestamp(1760000000, tz=datetime.timezone.utc)

def test_sequence_first_epoch_fallback():
    assert _get_orig_ctime(f'{RUN}/000000003-1760000000.m4s', None) == datetime.datetime.fromtimestamp(1760000000, tz=datetime.timezone.utc)

def test_actual_ffmpeg_sequence_names_prevent_same_second_collision(tmp_path):
    import subprocess
    run = tmp_path/RUN; run.mkdir()
    subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-f', 'lavfi', '-i',
                    'testsrc2=size=96x96:rate=10', '-t', '4', '-c:v', 'libx264', '-g', '10',
                    '-f', 'hls', '-hls_time', '1', '-hls_list_size', '0', '-hls_segment_type', 'fmp4',
                    '-hls_flags', 'program_date_time+second_level_segment_index', '-strftime', '1',
                    '-hls_segment_filename', str(run/'%%09d-%s.m4s'), str(run/'index.m3u8')], check=True)
    files = sorted(run.glob('*.m4s'))
    assert len(files) == 4
    assert [p.name.split('-')[0] for p in files] == ['000000000', '000000001', '000000002', '000000003']
    for file in files:
        assert _get_orig_ctime(file.name, None).timestamp() == int(file.stem.split('-')[1])
