"""Actual FFmpeg output remains recoverable after the playlist rolls."""
import datetime
import subprocess
from pathlib import Path
from unittest.mock import Mock
from viseron.domains.camera.fragmenter_process import FragmenterWork, _filename_duration, _get_orig_ctime

RUN='20261031T143215.482000Z-a7c93e12ab34'

def test_expired_playlist_entry_is_preserved_without_fabricating_pdt(tmp_path, caplog, monkeypatch):
    stage=tmp_path/'stage'; run=stage/RUN; run.mkdir(parents=True)
    subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=96x96:rate=10',
                    '-t','15','-c:v','libx264','-g','10','-f','hls','-hls_time','1','-hls_list_size','2',
                    '-hls_segment_type','fmp4','-hls_flags','program_date_time+second_level_segment_index+second_level_segment_duration+temp_file',
                    '-strftime','1','-hls_segment_filename',str(run/'%%09d-%s-%%013t.m4s'),str(run/'index.m3u8')], check=True)
    files=sorted(run.glob('*.m4s'))
    assert len(files)==15
    playlist=(run/'index.m3u8').read_text()
    assert playlist.count('#EXTINF:')==2 and files[0].name not in playlist
    worker=FragmenterWork('cam',str(stage),str(tmp_path/'segments'/'cam'),None,None,None)
    worker._send_files_metadata=Mock()
    now=[100.0]
    monkeypatch.setattr('viseron.domains.camera.fragmenter_process.time.monotonic',lambda:now[0])
    with caplog.at_level('WARNING'):
        worker._handle_m4s(f'{RUN}/{files[0].name}')
        assert not caplog.records
        now[0]=131.0
        worker._handle_m4s(f'{RUN}/{files[0].name}')
    worker._send_files_metadata.assert_not_called()
    assert 'unindexed in staging' in caplog.text
    assert _filename_duration(files[0].name) == 1.0
    assert files[0].exists()
    assert not (tmp_path/'segments'/'cam'/RUN/files[0].name).exists()
    worker._handle_m4s(f'{RUN}/{files[-1].name}')
    assert worker._send_files_metadata.call_args.args[2] == 1.0
    assert (tmp_path/'segments'/'cam'/RUN/files[-1].name).is_file()

def test_incomplete_duration_never_inferred():
    assert _filename_duration('000000001-1760000000-0000000000000.m4s') is None
    assert _filename_duration('1760000000.m4s') is None
    assert _filename_duration('000000001-1760000000-0000001234567.m4s') == 1.234567
    assert _get_orig_ctime('000000001-1760000000-0000001234567.m4s',None) == datetime.datetime.fromtimestamp(1760000000,tz=datetime.timezone.utc)

def test_current_playlist_entry_is_prioritized_over_old_backlog(tmp_path, monkeypatch):
    stage=tmp_path/'stage'; stage.mkdir()
    worker=FragmenterWork('cam',str(stage),str(tmp_path/'segments'/'cam'),None,None,None)
    old=[f'{RUN}/{i:09d}-1760000000-0000001000000.m4s' for i in range(20)]
    current=f'{RUN}/000000020-1760000000-0000001000000.m4s'
    monkeypatch.setattr('viseron.domains.camera.fragmenter_process._get_mp4_files_to_fragment',lambda _:old+[current])
    worker._read_m3u8=lambda _: '#EXTINF:1.000000,\n#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n'+Path(current).name+'\n'
    worker._handle_m4s=Mock()
    worker.work_input({'cmd':'fragment'})
    processed=[c.args[0] for c in worker._handle_m4s.call_args_list]
    assert current in processed
    assert len(processed)==2 and processed[1] in old

def test_failed_recent_entries_do_not_starve_later_recent_entries(tmp_path, monkeypatch):
    stage=tmp_path/'stage'; stage.mkdir()
    worker=FragmenterWork('cam',str(stage),str(tmp_path/'segments'/'cam'),None,None,None)
    recent=[f'{RUN}/{i:09d}-1760000000-0000001000000.m4s' for i in range(9)]
    old=f'{RUN}/000000009-1760000000-0000001000000.m4s'
    monkeypatch.setattr('viseron.domains.camera.fragmenter_process._get_mp4_files_to_fragment',lambda _:recent+[old])
    playlist=''.join('#EXTINF:1.000000,\n#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n'+Path(name).name+'\n' for name in recent)
    worker._read_m3u8=lambda _:playlist
    worker._handle_m4s=Mock()  # Simulate every item failing without disappearing.
    for _ in range(3): worker.work_input({'cmd':'fragment'})
    processed={c.args[0] for c in worker._handle_m4s.call_args_list}
    assert set(recent).issubset(processed)


def test_missing_timestamp_warning_is_coalesced_and_repeats(tmp_path, caplog, monkeypatch):
    stage=tmp_path/'stage'; run=stage/RUN; run.mkdir(parents=True)
    (run/'index.m3u8').write_text('#EXTM3U\n')
    worker=FragmenterWork('cam',str(stage),str(tmp_path/'segments'/'cam'),None,None,None)
    now=[100.0]
    monkeypatch.setattr('viseron.domains.camera.fragmenter_process.time.monotonic',lambda:now[0])
    filename=f'{RUN}/000000000-1760000000-0000001000000.m4s'
    with caplog.at_level('WARNING'):
        worker._handle_m4s(filename); worker._handle_m4s(filename)
        assert len(caplog.records)==0
        now[0]=130.0; worker._handle_m4s(filename)
        assert len(caplog.records)==1
        assert filename in caplog.text and 'configured storage' in caplog.text
        now[0]=190.0; worker._handle_m4s(filename)
        assert len(caplog.records)==2


def test_timing_arriving_within_grace_is_processed_without_warning(tmp_path, caplog, monkeypatch):
    stage=tmp_path/'stage'; run=stage/RUN; run.mkdir(parents=True)
    name='000000000-1760000000-0000001000000.m4s'
    (run/name).write_bytes(b'\x00\x00\x00\x0cmoofdata\x00\x00\x00\x0cmdatdata')
    playlist=run/'index.m3u8'; playlist.write_text('#EXTM3U\n')
    worker=FragmenterWork('cam',str(stage),str(tmp_path/'segments'/'cam'),None,None,None)
    worker._send_files_metadata=Mock()
    worker._move_to_segments_folder=Mock()
    now=[100.0]
    monkeypatch.setattr('viseron.domains.camera.fragmenter_process.time.monotonic',lambda:now[0])
    filename=f'{RUN}/{name}'
    with caplog.at_level('WARNING'):
        worker._handle_m4s(filename)
        playlist.write_text('#EXTM3U\n#EXTINF:1.0,\n#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n'+name+'\n')
        now[0]=105.0
        worker._handle_m4s(filename)
    assert not caplog.records
    assert filename not in worker._first_missing_timing
    worker._send_files_metadata.assert_called_once()
    worker._move_to_segments_folder.assert_called_once()
