"""Event requests reject visible run transitions; timeline remains available."""

import asyncio
import datetime
from http import HTTPStatus
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from viseron.components.webserver.api.v1.hls import (
    EventSpansRunsError,
    HlsAPIHandler,
    _generate_playlist,
    _generate_playlist_time_period,
)

RUN1 = "20261031T143215.482000Z-a7c93e12ab34"
RUN2 = "20261031T143315.482000Z-a7c93e12ab35"
NOW = datetime.datetime(2026, 10, 31, 15, tzinfo=datetime.timezone.utc)


def _file(run, key):
    directory = f"/recordings/test/{run}" if run else "/recordings/test"
    return SimpleNamespace(
        path=f"{directory}/{100 + key}.m4s",
        filename=f"{100 + key}.m4s",
        camera_identifier="test",
        file_key=key,
        duration=2,
        orig_ctime=NOW + datetime.timedelta(seconds=key * 2),
    )


def _session_for(files):
    recording = SimpleNamespace(
        end_time=NOW - datetime.timedelta(minutes=2),
        get_fragments=Mock(return_value=files),
    )
    session = MagicMock()
    session.execute.return_value.scalar.return_value = recording
    manager = MagicMock()
    manager.__enter__.return_value = session
    return lambda: manager


def _camera():
    return SimpleNamespace(identifier="test", recorder=SimpleNamespace(lookback=0))


@pytest.mark.parametrize("runs", [(RUN1, RUN2), (None, RUN1), (RUN2, None)])
def test_event_rejects_multiple_observed_runs_before_updating_client(runs):
    """Reject transitions before mutating client state."""
    files = [_file(runs[0], 1), _file(runs[1], 2)]
    with (
        patch("viseron.components.webserver.api.v1.hls.utcnow", return_value=NOW),
        patch("viseron.components.webserver.api.v1.hls.update_hls_client") as update,
    ):
        with pytest.raises(EventSpansRunsError):
            _generate_playlist(_session_for(files), "test-client", _camera(), 1, "")
    update.assert_not_called()


def test_single_run_event_plays():
    """Allow a single run event."""
    with patch("viseron.components.webserver.api.v1.hls.utcnow", return_value=NOW):
        playlist = _generate_playlist(
            _session_for([_file(RUN1, 1), _file(RUN1, 2)]), None, _camera(), 1, ""
        )
    assert f"/run-init/test/{RUN1}" in playlist
    assert "#EXT-X-DISCONTINUITY" not in playlist


def test_timeline_still_renders_multiple_runs():
    """Keep cross-run timeline playlists available."""
    files = [_file(RUN1, 1), _file(RUN2, 2)]
    with patch(
        "viseron.components.webserver.api.v1.hls.get_time_period_fragments",
        return_value=files,
    ):
        playlist = _generate_playlist_time_period(
            lambda: None, _camera(), None, datetime.timedelta(), 1, 2
        )
    assert f"/run-init/test/{RUN1}" in playlist
    assert f"/run-init/test/{RUN2}" in playlist
    assert "#EXT-X-DISCONTINUITY" in playlist


def test_event_endpoint_returns_distinct_conflict():
    """Return a specific error for unsupported events."""
    handler = SimpleNamespace(
        _get_camera=Mock(return_value=_camera()),
        request=SimpleNamespace(headers={}),
        get_subpath=Mock(return_value=""),
        _get_session=Mock(),
        request_arguments={"_HLS_skip": None},
        run_in_executor=AsyncMock(side_effect=EventSpansRunsError),
        response_error=Mock(),
    )
    asyncio.run(HlsAPIHandler.get_recording_hls_playlist(handler, "test", 1))
    handler.response_error.assert_called_once_with(
        HTTPStatus.CONFLICT, "Event playback across camera runs is not supported"
    )


def test_recent_event_uses_database_time_with_sequence_filename():
    """An ordered run filename is not parsed as a floating timestamp."""
    fragment = _file(RUN1, 1)
    fragment.filename = '000000001-1793450000.m4s'
    fragment.path = f'/recordings/test/{RUN1}/{fragment.filename}'
    fragment.orig_ctime = NOW - datetime.timedelta(seconds=5)
    recording = SimpleNamespace(end_time=NOW - datetime.timedelta(seconds=1), get_fragments=Mock(return_value=[fragment]))
    session = MagicMock(); session.execute.return_value.scalar.return_value = recording
    manager = MagicMock(); manager.__enter__.return_value = session
    with patch('viseron.components.webserver.api.v1.hls.utcnow', return_value=NOW):
        playlist = _generate_playlist(lambda: manager, None, _camera(), 1, '')
    assert '#EXT-X-ENDLIST' not in playlist
