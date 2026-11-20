"""API handler for vod."""

from __future__ import annotations

import datetime
import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from itertools import pairwise
from math import ceil
from typing import TYPE_CHECKING

import voluptuous as vol
from sqlalchemy import select

from viseron.components.storage.const import (
    TIER_CATEGORY_RECORDER,
    TIER_SUBCATEGORY_SEGMENTS,
)
from viseron.components.storage.files import file_tier_for_path
from viseron.components.storage.models import (
    FileLocations,
    FileLocationState,
    Files,
    Recordings,
)
from viseron.components.storage.queries import get_time_period_fragments
from viseron.components.webserver.api.handlers import BaseAPIHandler
from viseron.components.webserver.api.v1.files import (
    ResolvedFile,
    authorize_file_request,
    serve_resolved_file,
)
from viseron.const import CAMERA_SEGMENT_DURATION, HLS_SKIP_BOUNDARY_TARGET_DURATIONS
from viseron.domains.camera import AbstractCamera
from viseron.domains.camera.fragmenter import (
    Fragment,
    discontinuity_in_fragments,
    generate_playlist,
    get_available_timespans,
)
from viseron.helpers import client_current_datetime, daterange_to_utc, utcnow
from viseron.helpers.fixed_size_dict import FixedSizeDict

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

    from viseron.components.storage import Storage
    from viseron.domains.camera import FailedCamera

LOGGER = logging.getLogger(__name__)
HLS_SEGMENT_RESPONSE_WARN_SECONDS = 0.05
HLS_INIT_HASH_PATTERN = re.compile(r"^[a-f0-9]{64}$")


def count_files_removed(
    previous_list: list[Fragment], current_list: list[Fragment]
) -> int:
    """Count the number of Fragments removed from the previous playlist."""
    if not previous_list:
        return 0
    if not current_list:
        return len(previous_list)

    index = 0
    for index, file in enumerate(previous_list):
        if file.filename == current_list[0].filename:
            return index
    return index + 1


def count_discontinuities(fragments: list[Fragment]) -> int:
    """Count the discontinuities between consecutive Fragments."""
    return sum(
        1
        for previous, fragment in pairwise(fragments)
        if discontinuity_in_fragments(previous, fragment)
    )


def count_matching_fragments(
    previous_list: list[Fragment], current_list: list[Fragment]
) -> int:
    """Count the leading Fragments that are the same in both lists."""
    count = 0
    for previous, current in zip(previous_list, current_list):
        if previous.filename != current.filename:
            break
        count += 1
    return count


@dataclass
class HlsClient:
    """Dataclass for HLS client to keep track of removed files in live playlists."""

    client_id: str
    fragments: list[Fragment]
    media_sequence: int
    discontinuity_sequence: int
    target_duration: int


class HlsAPIHandler(BaseAPIHandler):
    """API handler for HLS."""

    hls_client_ids: FixedSizeDict[str, HlsClient] = FixedSizeDict(maxlen=500)

    routes = [
        {
            "path_pattern": (
                r"/hls/(?P<camera_identifier>[A-Za-z0-9_]+)/"
                r"(?P<recording_id>[0-9]+)/index.m3u8"
            ),
            "supported_methods": ["GET"],
            "method": "get_recording_hls_playlist",
            "allow_token_parameter": True,
            "request_arguments_schema": vol.Schema(
                {
                    vol.Optional("_HLS_skip", default=None): vol.Maybe(
                        vol.In(["YES", "v2"])
                    ),
                }
            ),
        },
        {
            "path_pattern": (r"/hls/(?P<camera_identifier>[A-Za-z0-9_]+)/index.m3u8"),
            "supported_methods": ["GET"],
            "method": "get_hls_playlist_time_period",
            "allow_token_parameter": True,
            "request_arguments_schema": vol.Schema(
                {
                    vol.Required("start_timestamp"): vol.Coerce(int),
                    vol.Optional("end_timestamp", default=None): vol.Maybe(
                        vol.Coerce(int)
                    ),
                    vol.Optional("date", default=None): vol.Maybe(str),
                    vol.Optional("_HLS_skip", default=None): vol.Maybe(
                        vol.In(["YES", "v2"])
                    ),
                }
            ),
        },
        {
            "path_pattern": (
                r"/hls/(?P<camera_identifier>[A-Za-z0-9_]+)/available_timespans"
            ),
            "supported_methods": ["GET"],
            "method": "get_available_timespans",
            "request_arguments_schema": vol.Schema(
                vol.Any(
                    {
                        vol.Required("time_from"): vol.Coerce(int),
                        vol.Optional("time_to", default=None): vol.Maybe(
                            vol.Coerce(int)
                        ),
                    },
                    {
                        vol.Required("date"): str,
                    },
                )
            ),
        },
        {
            "path_pattern": (
                r"/hls/init/(?P<camera_identifier>[A-Za-z0-9_]+)/"
                r"(?P<init_hash>[a-f0-9]{64}).mp4"
            ),
            "supported_methods": ["GET"],
            "method": "get_hls_init_file",
            "allow_token_parameter": True,
            "requires_auth": False,
        },
        {
            "path_pattern": (
                r"/hls/init/(?P<camera_identifier>[A-Za-z0-9_]+)/"
                r"(?P<tier_id>[0-9]+).mp4"
            ),
            "supported_methods": ["GET"],
            "method": "get_legacy_hls_init_file",
            "allow_token_parameter": True,
            "requires_auth": False,
        },
    ]

    async def get_recording_hls_playlist(
        self, camera_identifier: str, recording_id: int
    ):
        """Get the HLS playlist for a recording."""
        camera = self._get_camera(camera_identifier, failed=True)

        if not camera:
            self.response_error(
                HTTPStatus.NOT_FOUND,
                reason=f"Camera {camera_identifier} not found",
            )
            return

        hls_client_id = self.request.headers.get("Hls-Client-Id", None)
        subpath = self.get_subpath()
        playlist = await self.run_in_executor(
            _generate_playlist,
            self._get_session,
            hls_client_id,
            camera,
            recording_id,
            subpath,
            self.request_arguments["_HLS_skip"] is not None,
        )
        if not playlist:
            LOGGER.warning(
                "Returning 404 for HLS playlist (camera=%s, recording_id=%s)",
                camera_identifier,
                recording_id,
            )
            self.response_error(
                HTTPStatus.NOT_FOUND, f"Recording with id {recording_id} not found"
            )
            return

        self.set_header("Content-Type", "application/x-mpegURL")
        self.set_header("Cache-Control", "no-cache")
        await self.response_success(response=playlist)

    async def get_hls_playlist_time_period(
        self,
        camera_identifier: str,
    ):
        """Get the HLS playlist for a time period."""
        camera = self._get_camera(camera_identifier, failed=True)

        if not camera:
            self.response_error(
                HTTPStatus.NOT_FOUND,
                reason=f"Camera {camera_identifier} not found",
            )
            return

        hls_client_id = self.request.headers.get("Hls-Client-Id", None)
        subpath = self.get_subpath()
        playlist = await self.run_in_executor(
            _generate_playlist_time_period,
            self._get_session,
            camera,
            hls_client_id,
            self.utc_offset,
            self.request_arguments["start_timestamp"],
            self.request_arguments["end_timestamp"],
            self.request_arguments["date"],
            subpath,
            self.request_arguments["_HLS_skip"] is not None,
        )
        if not playlist:
            LOGGER.warning(
                "Returning 404 for HLS time-period playlist "
                "(camera=%s, start_timestamp=%s, end_timestamp=%s, date=%s)",
                camera_identifier,
                self.request_arguments["start_timestamp"],
                self.request_arguments["end_timestamp"],
                self.request_arguments["date"],
            )
            self.response_error(
                HTTPStatus.NOT_FOUND, "HLS playlist could not be generated"
            )
            return

        self.set_header("Content-Type", "application/x-mpegURL")
        self.set_header("Cache-control", "no-cache, must-revalidate, max-age=0")
        await self.response_success(response=playlist)

    async def get_available_timespans(
        self,
        camera_identifier: str,
    ):
        """Get the available timespans of HLS fragments for a time period."""
        camera = self._get_camera(camera_identifier, failed=True)

        if not camera:
            self.response_error(
                HTTPStatus.NOT_FOUND,
                reason=f"Camera {camera_identifier} not found",
            )
            return

        # Convert local start of day to UTC
        if "date" in self.request_arguments:
            _time_from, _time_to = daterange_to_utc(
                self.request_arguments["date"], self.utc_offset
            )
            time_from = _time_from.timestamp()
            time_to = _time_to.timestamp()
        else:
            time_from = self.request_arguments["time_from"]
            time_to = self.request_arguments["time_to"]

        timespans = await self.run_in_executor(
            get_available_timespans,
            self._get_session,
            [camera.identifier],
            time_from,
            time_to,
        )
        await self.response_success(response={"timespans": timespans})

    async def get_hls_init_file(self, camera_identifier: str, init_hash: str):
        """Get a hash-addressed HLS init file for a camera."""
        if not await authorize_file_request(self, camera_identifier, failed=True):
            return

        camera = self._get_camera(camera_identifier, failed=True)
        if not camera:
            self.response_error(
                HTTPStatus.NOT_FOUND,
                reason=f"Camera {camera_identifier} not found",
            )
            return

        if not HLS_INIT_HASH_PATTERN.match(init_hash):
            self.response_error(HTTPStatus.BAD_REQUEST, reason="Invalid init hash")
            return

        init_file = await self.run_in_executor(
            _get_hashed_init_file,
            self._get_session,
            self._storage,
            camera_identifier,
            init_hash,
        )
        if init_file is None:
            LOGGER.warning(
                "Returning 404 for missing HLS init file (camera=%s, init_hash=%s)",
                camera_identifier,
                init_hash,
            )
            self.response_error(HTTPStatus.NOT_FOUND, reason="Init file not found")
            return

        await serve_resolved_file(
            self,
            init_file,
            cache_control="public, max-age=31536000, immutable",
        )

    async def get_legacy_hls_init_file(self, camera_identifier: str, tier_id: str):
        """Get a legacy HLS init file by camera and tier id."""
        if not await authorize_file_request(self, camera_identifier, failed=True):
            return

        camera = self._get_camera(camera_identifier, failed=True)
        if not camera:
            self.response_error(
                HTTPStatus.NOT_FOUND,
                reason=f"Camera {camera_identifier} not found",
            )
            return

        init_file = await self.run_in_executor(
            _get_init_file,
            camera,
            int(tier_id),
        )
        if init_file is None:
            LOGGER.warning(
                "Returning 404 for missing HLS init file (camera=%s, tier_id=%s)",
                camera_identifier,
                tier_id,
            )
            self.response_error(HTTPStatus.NOT_FOUND, reason="Init file not found")
            return

        await serve_resolved_file(
            self,
            init_file,
            cache_control="public, max-age=31536000, immutable",
        )


def _get_hashed_init_file(
    get_session: Callable[[], Session],
    storage: Storage,
    camera_identifier: str,
    init_hash: str,
) -> ResolvedFile | None:
    """Get a hash-addressed HLS init sidecar referenced by available segments."""
    init_filename = f"init-{init_hash}.mp4"
    with get_session() as session:
        stmt = (
            select(FileLocations.path)
            .join(Files, Files.id == FileLocations.file_id)
            .where(Files.camera_identifier == camera_identifier)
            .where(Files.category == TIER_CATEGORY_RECORDER)
            .where(Files.subcategory == TIER_SUBCATEGORY_SEGMENTS)
            .where(Files.hls_init_hash == init_hash)
            .where(FileLocations.state == FileLocationState.AVAILABLE.value)
            .order_by(FileLocations.tier_id.asc())
        )
        segment_paths = session.execute(stmt).scalars().all()

    for segment_path in segment_paths:
        init_path = os.path.join(os.path.dirname(segment_path), init_filename)
        if not file_tier_for_path(
            storage,
            camera_identifier,
            TIER_CATEGORY_RECORDER,
            TIER_SUBCATEGORY_SEGMENTS,
            init_path,
        ):
            continue
        try:
            size = os.path.getsize(init_path)
        except OSError:
            continue
        if os.path.isfile(init_path):
            return ResolvedFile(
                file_id=0,
                camera_identifier=camera_identifier,
                category=TIER_CATEGORY_RECORDER,
                subcategory=TIER_SUBCATEGORY_SEGMENTS,
                path=init_path,
                size=size,
            )
    return None


def _get_init_file(
    camera: AbstractCamera | FailedCamera,
    tier_id: int,
) -> ResolvedFile | None:
    """Get the init file for a camera tier."""
    try:
        tier_path = camera.tier_base_path(
            tier_id,
            TIER_CATEGORY_RECORDER,
            TIER_SUBCATEGORY_SEGMENTS,
        )
    except ValueError:
        return None

    init_path = os.path.normpath(
        os.path.join(
            os.path.join(tier_path, TIER_SUBCATEGORY_SEGMENTS, camera.identifier),
            "init.mp4",
        )
    )
    if not os.path.isfile(init_path) or os.path.realpath(init_path) != os.path.join(
        os.path.realpath(os.path.dirname(init_path)), "init.mp4"
    ):
        LOGGER.error("Could not find init.mp4 file for camera %s", camera.identifier)
        return None

    return ResolvedFile(
        file_id=0,
        camera_identifier=camera.identifier,
        category=TIER_CATEGORY_RECORDER,
        subcategory=TIER_SUBCATEGORY_SEGMENTS,
        path=init_path,
        size=os.path.getsize(init_path),
    )


def _legacy_init_file_url(
    camera: AbstractCamera | FailedCamera,
    subpath: str,
    tier_id: int,
) -> str:
    """Return logical legacy HLS init file URL."""
    return f"{subpath}/api/v1/hls/init/{camera.identifier}/{tier_id}.mp4"


def _hashed_init_file_url(
    camera: AbstractCamera | FailedCamera,
    subpath: str,
    init_hash: str,
) -> str:
    """Return logical hash-addressed HLS init file URL."""
    return f"{subpath}/api/v1/hls/init/{camera.identifier}/{init_hash}.mp4"


def _init_file_url(
    camera: AbstractCamera | FailedCamera,
    subpath: str,
    files: list,
) -> str | None:
    """Return logical HLS init file URL for the first fragment."""
    if not files:
        return None
    if getattr(files[0], "hls_init_hash", None):
        return _hashed_init_file_url(camera, subpath, files[0].hls_init_hash)
    return _legacy_init_file_url(camera, subpath, files[0].tier_id)


def _legacy_init_files_available(
    camera: AbstractCamera | FailedCamera,
    files: list,
) -> bool:
    """Return if all legacy fragments have an available init.mp4."""
    legacy_tier_ids = {
        file.tier_id for file in files if not getattr(file, "hls_init_hash", None)
    }
    return all(_get_init_file(camera, tier_id) for tier_id in legacy_tier_ids)


def get_target_duration(fragments: list[Fragment]) -> int:
    """Get the target duration for HLS playlist."""
    target_duration = 0
    if fragments:
        target_duration = ceil(max(f.duration for f in fragments))
    target_duration = max(target_duration, CAMERA_SEGMENT_DURATION)
    return target_duration


def update_hls_client(
    hls_client_id: str,
    fragments: list[Fragment],
) -> tuple[HlsClient, int]:
    """Keep track of HLS client media and discontinuity sequence.

    Only call this for playlists that are returned to the client, since the fragments
    are stored as the client's previous playlist.
    Returns the client and the number of leading fragments it already received.
    """
    hls_client = HlsAPIHandler.hls_client_ids.get(hls_client_id, None)
    if hls_client:
        removed = count_files_removed(hls_client.fragments, fragments)
        received = count_matching_fragments(hls_client.fragments[removed:], fragments)
        # The new first fragment loses its discontinuity too, since a playlist never
        # starts with one
        hls_client.discontinuity_sequence += count_discontinuities(
            hls_client.fragments[:removed] + fragments[:1]
        )
        hls_client.fragments = fragments
        hls_client.media_sequence += removed
        return hls_client, received

    hls_client = HlsClient(
        client_id=hls_client_id,
        fragments=fragments,
        media_sequence=0,
        discontinuity_sequence=0,
        target_duration=get_target_duration(fragments),
    )
    HlsAPIHandler.hls_client_ids[hls_client_id] = hls_client
    return hls_client, 0


def files_to_fragments(subpath: str, files: list) -> list[Fragment]:
    """Create fragments that reference segments by their file key.

    Segment URIs must not change between updates of a playlist, and the file key
    stays the same when a segment moves to another tier.
    """
    return [
        Fragment(
            file.filename,
            # file_key is converted to a hexadecimal string in the URL
            f"{subpath}/file/{file.camera_identifier}/{file.file_key:x}",
            file.duration,
            file.orig_ctime,
            (
                f"{subpath}/api/v1/hls/init/{file.camera_identifier}/"
                f"{file.hls_init_hash}.mp4"
            )
            if getattr(file, "hls_init_hash", None)
            else (
                f"{subpath}/api/v1/hls/init/{file.camera_identifier}/{file.tier_id}.mp4"
            ),
        )
        for file in files
    ]


def _render_client_playlist(
    fragments: list[Fragment],
    init_file: str,
    hls_client_id: str | None,
    *,
    skip_requested: bool,
    end: bool,
) -> str:
    """Render the playlist with the sequence numbers and delta updates of a client."""
    if hls_client_id is None:
        return generate_playlist(fragments, init_file, end=end, file_directive=False)

    hls_client, received = update_hls_client(hls_client_id, fragments)
    return generate_playlist(
        fragments,
        init_file,
        media_sequence=hls_client.media_sequence,
        target_duration=hls_client.target_duration,
        discontinuity_sequence=hls_client.discontinuity_sequence,
        end=end,
        file_directive=False,
        can_skip_until=hls_client.target_duration * HLS_SKIP_BOUNDARY_TARGET_DURATIONS,
        # The client restores skipped segments from its previous playlist, which
        # new or evicted clients lack and stale clients only partially have
        max_skipped_segments=received if skip_requested else 0,
    )


def _generate_playlist(
    get_session: Callable[[], Session],
    hls_client_id: str | None,
    camera: AbstractCamera | FailedCamera,
    recording_id: int,
    subpath: str,
    skip_requested: bool = False,
) -> str | None:
    """Generate the HLS playlist for a recording."""
    now = utcnow()

    with get_session() as session:
        stmt = (
            select(Recordings)
            .where(Recordings.id == recording_id)
            .where(Recordings.camera_identifier == camera.identifier)
        )
        recording = session.execute(stmt).scalar()
        if recording is None:
            return None

    files = recording.get_fragments(
        camera.recorder.lookback,
        get_session,
        now=now,
    )
    fragments = files_to_fragments(subpath, files)

    if not fragments:
        return None

    end: bool = True
    # Recording has not ended yet
    if recording.end_time is None:
        LOGGER.debug("Recording has not ended yet")
        end = False
    # End the playlist if the recording ended more than a minute ago
    # Prevents infinitely waiting for the last file to finish if it is missing
    # for some reason
    elif recording.end_time < now - datetime.timedelta(minutes=1):
        LOGGER.debug("Recording ended more than a minute ago")
        end = True
    # Recording has ended but the last file is not finished yet
    elif len(files) > 0 and recording.end_time.timestamp() > float(
        files[-1].filename.split(".")[0]
    ) + float(files[-1].duration):
        LOGGER.debug("Recording has ended but the last file is not finished yet")
        end = False

    if not _legacy_init_files_available(camera, files):
        return None

    init_file_url = _init_file_url(camera, subpath, files)
    if not init_file_url:
        return None

    return _render_client_playlist(
        fragments,
        init_file_url,
        hls_client_id,
        skip_requested=skip_requested,
        end=end,
    )


def _generate_playlist_time_period(
    get_session: Callable[[], Session],
    camera: AbstractCamera | FailedCamera,
    hls_client_id: str | None,
    utc_offset: datetime.timedelta,
    start_timestamp: int,
    end_timestamp: int | None = None,
    date: str | None = None,
    subpath: str = "",
    skip_requested: bool = False,
) -> str | None:
    """Generate the HLS playlist for a time period."""
    end_playlist = False
    if date and end_timestamp is None:
        # If a date is provided, convert to timestamp range
        _, time_to = daterange_to_utc(date, utc_offset)
        end_timestamp = int(time_to.timestamp())
        # If the date is not today, playlist should end
        if date != client_current_datetime(utc_offset).date().isoformat():
            end_playlist = True
    elif end_timestamp is not None:
        end_playlist = True

    files = get_time_period_fragments(
        [camera.identifier], start_timestamp, end_timestamp, get_session
    )
    fragments = files_to_fragments(subpath, files)

    if not fragments and end_playlist:
        return generate_playlist([], "", end=True)

    if not _legacy_init_files_available(camera, files):
        return None
    init_file_url = _init_file_url(camera, subpath, files)
    if not init_file_url:
        return None

    return _render_client_playlist(
        fragments,
        init_file_url,
        hls_client_id,
        skip_requested=skip_requested,
        end=end_playlist,
    )
