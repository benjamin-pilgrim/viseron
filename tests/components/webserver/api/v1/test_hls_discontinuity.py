"""Regression tests for discontinuities in sliding HLS playlists."""

import datetime
from uuid import uuid4

from viseron.components.webserver.api.v1.hls import _render_client_playlist
from viseron.domains.camera.fragmenter import Fragment

START = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


def fragments(offsets: list[int]) -> list[Fragment]:
    """Create five-second segments at the given offsets."""
    return [
        Fragment(
            f"{offset}.m4s",
            f"/{offset}.m4s",
            5,
            START + datetime.timedelta(seconds=offset),
        )
        for offset in offsets
    ]


def render(client: str, offsets: list[int], *, skip: bool = False) -> str:
    """Render a client's current live playlist."""
    return _render_client_playlist(
        fragments(offsets), "/init.mp4", client, skip_requested=skip, end=False
    )


def test_sliding_contiguous_playlist_does_not_increase_discontinuity() -> None:
    """Removing an ordinary segment changes media sequence only."""
    client = str(uuid4())
    render(client, [0, 5, 10])
    playlist = render(client, [5, 10, 15])
    assert "#EXT-X-MEDIA-SEQUENCE:1\n" in playlist
    assert "#EXT-X-DISCONTINUITY-SEQUENCE:" not in playlist


def test_sliding_playlist_counts_removed_gaps() -> None:
    """A removed gap, not the number of removed segments, advances the counter."""
    client = str(uuid4())
    render(client, [0, 5, 20, 25])
    playlist = render(client, [20, 25, 30])
    assert "#EXT-X-MEDIA-SEQUENCE:2\n" in playlist
    assert "#EXT-X-DISCONTINUITY-SEQUENCE:1\n" in playlist


def test_delta_playlist_keeps_full_window_discontinuity_sequence() -> None:
    """Skipped already-received segments must not advance the window's base."""
    client = str(uuid4())
    render(client, [0, 5, *range(20, 90, 5)])
    offsets = list(range(20, 95, 5))
    playlist = render(client, offsets, skip=True)
    assert "#EXT-X-SKIP:" in playlist
    assert "#EXT-X-DISCONTINUITY-SEQUENCE:1\n" in playlist


def test_disjoint_windows_count_internal_and_boundary_gaps() -> None:
    """Count both gaps when a new window has no segments in common."""
    client = str(uuid4())
    render(client, [0, 5, 20])
    playlist = render(client, [40, 45])
    assert "#EXT-X-MEDIA-SEQUENCE:3\n" in playlist
    assert "#EXT-X-DISCONTINUITY-SEQUENCE:2\n" in playlist
