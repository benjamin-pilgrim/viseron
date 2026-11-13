"""Segment fragmenter, executed in the child process of the fragmenter worker.

Deliberately depends on nothing that reaches back into the running Viseron instance,
so that the child process can be created with the forkserver start method to reduce
memory usage.
"""

from __future__ import annotations

import contextlib
import datetime
import errno
import hashlib
import logging
import os
import re
import shutil
import subprocess as sp
import time
import uuid
from typing import TYPE_CHECKING

import psutil
from path import Path

from viseron.components.storage.pending_journal import (
    marker_for_path,
    prepare,
    publish_or_verify,
)
from viseron.components.storage.run_init_prototype import ensure_init
from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    is_run_directory,
    reclaim_spool_run,
)
from viseron.components.storage.source_move_intent import (
    prepare_source_intent,
    source_move_pending,
)
from viseron.components.storage.util import TIER_MOVE_TEMP_PREFIX, require_tier_markers
from viseron.domains.camera.const import MP4BOX_PATH
from viseron.domains.camera.playlist_timing import (
    read_playlist_time,
    read_source_intent_time,
)
from viseron.helpers import get_utc_offset
from viseron.helpers.child_process_worker import ChildProcessWork
from viseron.helpers.logs import LogPipe

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

TIMELAPSE_FFMPEG_TIMEOUT = 10
MISSING_TIMING_GRACE_SECONDS = 30
METADATA_RETRY_INITIAL_SECONDS = 5
METADATA_RETRY_MAX_SECONDS = 60
METADATA_WARNING_INTERVAL_SECONDS = 60


def _get_open_files(path: str, process: psutil.Process) -> list[str]:
    """Get open files for a process."""
    files = []
    try:
        open_files = process.open_files()
        for file in open_files:
            if file.path.startswith(f"{path}/"):
                files.append(os.path.relpath(file.path, path))
    except psutil.Error:
        pass
    return files


def _get_mp4_files_to_fragment(path: str) -> list[str]:
    """Get mp4 files that are not in use by ffmpeg/gstreamer."""
    mp4_files = [
        file for file in Path(path).walkfiles() if file.endswith((".mp4", ".m4s"))
    ]
    files_in_use = ["init.mp4"]
    for process in psutil.process_iter():
        try:
            process_name = process.name()
            if any(pattern in process_name for pattern in ["ffmpeg", "gstreamer"]):
                files_in_use.extend(_get_open_files(path, process))
        except psutil.Error:
            pass
    return [
        os.path.relpath(str(f), path)
        for f in mp4_files
        if f.name != "init.mp4" and os.path.relpath(str(f), path) not in files_in_use
    ]


def _extract_extinf_number(playlist_content: str, file: str) -> float | None:
    """Extract the EXTINF number from a HLS playlist."""
    # Escape special regex characters in the file name
    escaped_file = re.escape(file)

    # Simplified pattern
    pattern = (
        rf"#EXTINF:(\d+(?:\.\d+)?)[^\n]*\n"
        rf"(?:(?!#EXTINF)#[^\n]*\n)*{escaped_file}(?=\r?(?:\n|$))"
    )

    match = re.search(pattern, playlist_content)

    if match:
        # Extract the duration number from the match
        return float(match.group(1))
    return None


def _extract_program_date_time(
    playlist_content: str, file: str
) -> datetime.datetime | None:
    """Extract the EXT-X-PROGRAM-DATE-TIME number from a HLS playlist."""
    escaped_file = re.escape(file)
    pattern = (
        r"#EXT-X-PROGRAM-DATE-TIME:"
        rf"([^\n]+)\n(?:(?!#EXT-X-PROGRAM-DATE-TIME)#[^\n]*\n)*"
        rf"{escaped_file}(?=\r?(?:\n|$))"
    )

    match = re.search(pattern, playlist_content)

    try:
        if match:
            parsed = datetime.datetime.fromisoformat(match.group(1))
            # Preserve the historical local-time interpretation for custom
            # playlists without an offset; FFmpeg normally supplies one.
            if parsed.tzinfo is None:
                return (parsed - get_utc_offset()).replace(tzinfo=datetime.timezone.utc)
            return parsed.astimezone(datetime.timezone.utc)
    except ValueError:
        pass
    return None


_RUN_SEGMENT_NAME = re.compile(
    r"^(?P<sequence>\d{9,})-(?P<epoch>\d+)-(?P<duration>\d{13,})\.m4s$"
)


def _filename_duration(file: str) -> float | None:
    """Read FFmpeg's finalized HLS duration in microseconds, never infer it."""
    match = _RUN_SEGMENT_NAME.fullmatch(os.path.basename(file))
    if match is None:
        return None
    duration = int(match.group("duration"))
    return duration / 1_000_000 if duration > 0 else None


def _get_orig_ctime(
    file: str, program_date_time: datetime.datetime | None
) -> datetime.datetime:
    """Prefer playlist time; filename timestamps may differ from recorded time."""
    if program_date_time:
        return program_date_time
    basename = os.path.basename(file)
    match = _RUN_SEGMENT_NAME.fullmatch(basename)
    epoch = (
        match.group("epoch")
        if match
        else basename.split(".", maxsplit=1)[0].rsplit("-", maxsplit=1)[-1]
    )
    return datetime.datetime.fromtimestamp(int(epoch), tz=datetime.timezone.utc)


def _validate_fragment(path: str, expected_identity: list[int]) -> None:
    """Require finite, complete moof/mdat boxes in a finalized recorder fragment.

    ENOSPC can leave an empty or partial file despite finalized HLS timing.
    Read only box headers and seek over payloads; this checks container
    completeness, not codec correctness. Publication checks the identity again.
    """
    with open(path, "rb") as stream:
        before = os.fstat(stream.fileno())
        if [before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns] != (
            expected_identity
        ):
            raise ValueError("fragment changed before completeness check")
        offset = 0
        pending_moof = False
        has_media = False
        while offset < before.st_size:
            stream.seek(offset)
            header_size = 8
            header = stream.read(header_size)
            if len(header) != header_size:
                raise ValueError("truncated MP4 box header")
            size = int.from_bytes(header[:4], "big")
            kind = header[4:]
            if size == 1:
                extended_size = stream.read(header_size)
                if len(extended_size) != header_size:
                    raise ValueError("truncated extended MP4 box header")
                size = int.from_bytes(extended_size, "big")
                header_size *= 2
            # Our finite FFmpeg/MP4Box fragments have explicit box sizes.
            # A zero-sized box extends to EOF and cannot prove completeness.
            if size < header_size or offset + size > before.st_size:
                raise ValueError("incomplete or unfinalized MP4 box")
            if kind == b"moof":
                if pending_moof or size == header_size:
                    raise ValueError("fragment has no complete movie fragment")
                pending_moof = True
            elif kind == b"mdat":
                if not pending_moof or size == header_size:
                    raise ValueError("fragment has no complete media payload")
                has_media = True
                pending_moof = False
            offset += size
        if not has_media or pending_moof:
            raise ValueError("fragment lacks a complete moof/mdat pair")
        after = os.fstat(stream.fileno())
        if [after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns] != (
            expected_identity
        ):
            raise ValueError("fragment changed during completeness check")


def fragmenter_logger_names(camera_identifier: str) -> tuple[str, str]:
    """Return the names of the loggers the fragmenter child process creates.

    Named after the fragmenter module so that log levels configured for it also apply
    to the child process.
    """
    logger_name = "viseron.domains.camera.fragmenter"
    return (
        f"{logger_name}.subprocess.{camera_identifier}",
        f"{logger_name}.{camera_identifier}.mp4box",
    )


class FragmenterWork(ChildProcessWork):
    """Fragment the segments of a camera inside the child process."""

    def __init__(
        self,
        camera_identifier: str,
        temp_segments_folder: str,
        segments_folder: str,
        timelapse_folder: str | None,
        temp_timelapse_folder: str | None,
        worker_event: Event,
        tier_markers: dict[str, str] | None = None,
    ) -> None:
        self._camera_identifier = camera_identifier
        self.temp_segments_folder = temp_segments_folder
        self.segments_folder = segments_folder
        self._timelapse_folder = timelapse_folder
        self._temp_timelapse_folder = temp_timelapse_folder
        self._worker_event = worker_event
        self._tier_markers = tier_markers or {}
        self._logger = logging.getLogger(fragmenter_logger_names(camera_identifier)[0])
        self._log_pipe: LogPipe
        self._last_fragment: str | None = None
        self._last_recent: str | None = None
        self._last_missing_timestamp_warning: float | None = None
        self._first_missing_timing: dict[str, float] = {}
        self._metadata_retry: dict[str, tuple[float, float]] = {}
        self._last_missing_duration_warning: float | None = None
        self._last_reaped_run: str | None = None
        self._last_invalid_fragment_warning: float | None = None
        self._storage_blocked = False
        self._blocked_sequences: dict[str, int] = {}

    @property
    def logger_names(self) -> tuple[str, ...]:
        """Return the names of the loggers the work uses in the child process."""
        return fragmenter_logger_names(self._camera_identifier)

    def initialize(self) -> None:
        """Create the MP4Box log pipe, which can't be pickled."""
        self._log_pipe = LogPipe(
            logging.getLogger(fragmenter_logger_names(self._camera_identifier)[1]),
            logging.DEBUG,
        )

    def work_input(self, item) -> dict | None:
        """Handle input commands in the child process."""
        cmd = item.get("cmd")
        if cmd not in ("fragment", "drain"):
            return None

        self._logger.debug(
            f"Checking for new segments to fragment in {self.temp_segments_folder}"
        )
        mp4s = sorted(_get_mp4_files_to_fragment(self.temp_segments_folder))
        if cmd == "fragment":
            blocked = item.get("storage_blocked", False)
            if blocked and not self._storage_blocked:
                self._blocked_sequences = {}
                for name in mp4s:
                    match = _RUN_SEGMENT_NAME.fullmatch(os.path.basename(name))
                    if match:
                        run = os.path.dirname(name)
                        self._blocked_sequences[run] = max(
                            self._blocked_sequences.get(run, -1), int(match["sequence"])
                        )
            if not blocked:
                self._blocked_sequences.clear()
            self._storage_blocked = blocked
        # A durable destination journal already owns this source; the watcher
        # reconciler handles it. An intent without a journal is a crashed copy
        # and must remain discoverable for idempotent re-publication.
        mp4s = [
            name
            for name in mp4s
            if not (
                source_move_pending(os.path.join(self.temp_segments_folder, name))
                and os.path.lexists(
                    marker_for_path(os.path.join(self.segments_folder, name))
                )
            )
        ]
        # Forget resolved or removed entries rather than keeping a per-file
        # timer for the lifetime of the camera.
        staged = set(mp4s)
        self._first_missing_timing = {
            name: first_seen
            for name, first_seen in self._first_missing_timing.items()
            if name in staged
        }
        self._metadata_retry = {
            name: retry
            for name, retry in self._metadata_retry.items()
            if name in staged
        }
        if cmd == "fragment":
            now = time.monotonic()
            mp4s = [
                name
                for name in mp4s
                if now >= self._metadata_retry.get(name, (0, 0))[0]
            ]
        if cmd == "fragment" and mp4s:
            # Keep current playlist entries moving even if old unindexed
            # fragments accumulate. Revisit one old item per pass to report it.
            recent: list[str] = []
            retained: list[str] = []
            playlists: dict[str, str] = {}
            for name in mp4s:
                if not name.endswith(".m4s") or not _RUN_SEGMENT_NAME.fullmatch(
                    os.path.basename(name)
                ):
                    recent.append(name)
                    continue
                run = os.path.dirname(name)
                if run not in playlists:
                    try:
                        playlists[run] = self._read_m3u8(name)
                    except OSError:
                        playlists[run] = ""
                data = playlists[run]
                if (
                    _extract_extinf_number(data, os.path.basename(name)) is not None
                    and _extract_program_date_time(data, os.path.basename(name))
                    is not None
                ):
                    recent.append(name)
                else:
                    retained.append(name)
            batch: list[str] = []
            if recent:
                start = next(
                    (
                        i
                        for i, name in enumerate(recent)
                        if self._last_recent is None or name > self._last_recent
                    ),
                    0,
                )
                batch = (recent[start:] + recent[:start])[: 4 if retained else 5]
                self._last_recent = batch[-1]
            if retained:
                start = next(
                    (
                        i
                        for i, name in enumerate(retained)
                        if self._last_fragment is None or name > self._last_fragment
                    ),
                    0,
                )
                old = retained[start]
                self._last_fragment = old
                batch.append(old)
            mp4s = batch
        for mp4 in mp4s:
            self._logger.debug(f"Processing {mp4}")
            if mp4.endswith(".m4s"):
                self._handle_m4s(mp4)
            else:
                self._handle_mp4(mp4)

        # Bound work per pass; rotate so an active or damaged old run cannot
        # starve cleanup of later runs. Missing spool folders fail closed.
        try:
            runs = sorted(
                name
                for name in os.listdir(self.temp_segments_folder)
                if is_run_directory(name)
            )
            if runs:
                start = next(
                    (
                        i
                        for i, name in enumerate(runs)
                        if self._last_reaped_run is None or name > self._last_reaped_run
                    ),
                    0,
                )
                batch = (runs[start:] + runs[:start])[:5]
                self._last_reaped_run = batch[-1]
                for name in batch:
                    reclaim_spool_run(
                        "/tmp/viseron-run-locks",
                        os.path.abspath(self.temp_segments_folder),
                        os.path.join(self.temp_segments_folder, name),
                    )
        except OSError:
            self._logger.exception("Could not inspect spool runs for cleanup")
        if cmd == "drain":
            return {"drained": True}
        return None

    def _mp4box_command(self, file: str) -> bool:
        """Create fragmented fmp4 from mp4 using MP4Box."""
        outdir = os.path.join(self.temp_segments_folder, file.split(".", maxsplit=1)[0])
        os.makedirs(outdir, exist_ok=True)
        try:
            sp.run(  # type: ignore[call-overload]
                [
                    MP4BOX_PATH,
                    "-logs",
                    "dash@error:ncl",
                    "-noprog",
                    "-dash",
                    "10000",
                    "-rap",
                    "-frag-rap",
                    "-segment-name",
                    "clip_",
                    "-out",
                    os.path.join(outdir, "master.m3u8"),
                    os.path.join(self.temp_segments_folder, file),
                ],
                stdout=self._log_pipe,
                stderr=self._log_pipe,
                check=True,
            )
        except sp.CalledProcessError as err:
            self._logger.error("MP4Box command failed", exc_info=err)
            return False
        return True

    def _segment_hook_mp4box(self, file: str, orig_ctime: datetime.datetime) -> None:
        """Perform per fragment tasks before moving fragment to storage.

        Currently only used for extracting timelapse frames from fragments.
        """
        if self._timelapse_folder is None or self._temp_timelapse_folder is None:
            return

        frame_filename = os.path.splitext(os.path.basename(file))[0] + ".jpg"
        tmp_frame_path = os.path.join(self._temp_timelapse_folder, frame_filename)
        frame_path = os.path.join(self._timelapse_folder, frame_filename)
        init_path = os.path.join(
            self.temp_segments_folder, file.split(".", maxsplit=1)[0], "clip_init.mp4"
        )
        segment_path = os.path.join(
            self.temp_segments_folder, file.split(".", maxsplit=1)[0], "clip_1.m4s"
        )

        self._extract_timelapse_frame(
            init_path, segment_path, tmp_frame_path, frame_path, orig_ctime
        )

    def _segment_hook(
        self,
        file: str,
        orig_ctime: datetime.datetime,
        *,
        init_path: str | None = None,
        segment_path: str | None = None,
        pass_fds: tuple[int, ...] = (),
    ) -> None:
        """Perform per fragment tasks before moving fragment to storage.

        Currently only used for extracting timelapse frames from fragments.
        """
        if self._timelapse_folder is None or self._temp_timelapse_folder is None:
            return

        frame_filename = os.path.splitext(os.path.basename(file))[0] + ".jpg"
        tmp_frame_path = os.path.join(self._temp_timelapse_folder, frame_filename)
        frame_path = os.path.join(self._timelapse_folder, frame_filename)
        init_path = init_path or os.path.join(
            self.temp_segments_folder, os.path.dirname(file), "init.mp4"
        )
        segment_path = segment_path or os.path.join(self.temp_segments_folder, file)

        self._extract_timelapse_frame(
            init_path,
            segment_path,
            tmp_frame_path,
            frame_path,
            orig_ctime,
            pass_fds=pass_fds,
        )

    def _extract_timelapse_frame(
        self,
        init_path: str,
        segment_path: str,
        tmp_frame_path: str,
        frame_path: str,
        orig_ctime: datetime.datetime,
        *,
        pass_fds: tuple[int, ...] = (),
    ) -> None:
        """Extract a timelapse frame from segment files."""
        try:
            # Run ffmpeg command to extract first keyframe of the segment
            cmd = [
                "bash",
                "-c",
                (
                    f"cat '{init_path}' '{segment_path}' | "
                    f"ffmpeg -skip_frame nokey -i pipe:0 -frames:v 1 "
                    f"-update true -f mjpeg '{tmp_frame_path}' -y"
                ),
            ]

            result = sp.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=TIMELAPSE_FFMPEG_TIMEOUT,
                check=False,
                pass_fds=pass_fds,
            )
            if result.returncode == 0:
                self._logger.debug(f"Timelapse: Extracted frame {tmp_frame_path}")
                self._send_files_metadata(frame_path, orig_ctime, None)
                shutil.move(tmp_frame_path, frame_path)
            else:
                self._logger.warning(
                    f"Timelapse: Failed to extract frame {frame_path}: {result.stderr}"
                )
                os.remove(tmp_frame_path)

        except FileNotFoundError:
            self._logger.debug(f"{tmp_frame_path} not found")

        except (OSError, sp.TimeoutExpired, sp.SubprocessError) as error:
            self._logger.error(
                f"Timelapse: Error extracting frame {frame_path}: {error}"
            )

        finally:
            try:
                os.remove(tmp_frame_path)
            except FileNotFoundError:
                pass

    def _move_to_segments_folder_mp4box(
        self, file: str, orig_ctime: datetime.datetime
    ) -> bool:
        """Move fragmented mp4 created by mp4box to segments folder."""
        self._segment_hook_mp4box(file, orig_ctime)
        try:
            shutil.move(
                os.path.join(
                    self.temp_segments_folder,
                    file.split(".", maxsplit=1)[0],
                    "clip_1.m4s",
                ),
                os.path.join(
                    self.segments_folder, file.split(".", maxsplit=1)[0] + ".m4s"
                ),
            )
            shutil.move(
                os.path.join(
                    self.temp_segments_folder,
                    file.split(".", maxsplit=1)[0],
                    "clip_init.mp4",
                ),
                os.path.join(self.segments_folder, "init.mp4"),
            )
        except FileNotFoundError:
            self._logger.debug(f"{file} not found")
            return False
        return True

    def _move_to_segments_folder(
        self,
        file: str,
        orig_ctime: datetime.datetime,
        duration: float | None = None,
        expected_identity: list[int] | None = None,
        expected_digest: str | None = None,
    ) -> None:
        """Move fragmented mp4 created by encoder to segments folder."""
        run = os.path.dirname(file)
        is_run = is_run_directory(run)
        if not is_run:
            self._segment_hook(file, orig_ctime)
        timelapse_fds: list[int] = []
        try:
            destination_dir = os.path.join(self.segments_folder, run)
            require_tier_markers(destination_dir, self._tier_markers)
            lock = (
                camera_lock(
                    "/tmp/viseron-run-locks", os.path.abspath(self.segments_folder)
                )
                if is_run
                else contextlib.nullcontext()
            )
            with lock:
                require_tier_markers(destination_dir, self._tier_markers)
                os.makedirs(destination_dir, exist_ok=True)
                init_source = os.path.join(self.temp_segments_folder, run, "init.mp4")
                init_dest = os.path.join(destination_dir, "init.mp4")
                if is_run:
                    ensure_init(init_source, destination_dir)
                else:
                    temp_init = os.path.join(
                        destination_dir, TIER_MOVE_TEMP_PREFIX + uuid.uuid4().hex
                    )
                    try:
                        shutil.copyfile(init_source, temp_init)
                        os.replace(temp_init, init_dest)
                    finally:
                        if os.path.exists(temp_init):
                            os.unlink(temp_init)
                source = os.path.join(self.temp_segments_folder, file)
                destination = os.path.join(self.segments_folder, file)
                if not is_run:
                    shutil.move(source, destination)
                else:
                    require_tier_markers(destination_dir, self._tier_markers)
                    if source_move_pending(destination) or os.path.lexists(destination):
                        raise BlockingIOError("run segment is held or already exists")
                    source_camera = os.path.abspath(self.temp_segments_folder)
                    with camera_lock("/tmp/viseron-run-locks", source_camera):
                        # Idempotent only if the entire previous source snapshot
                        # matches. A partial/conflicting intent fails closed.
                        source_stat = os.stat(source, follow_symlinks=False)
                        source_identity = [
                            source_stat.st_dev,
                            source_stat.st_ino,
                            source_stat.st_size,
                            source_stat.st_mtime_ns,
                        ]
                        if (
                            expected_identity is not None
                            and source_identity != expected_identity
                        ):
                            raise OSError(  # noqa: TRY301
                                "staged fragment changed after timing capture"
                            )
                        source_intent = prepare_source_intent(
                            source,
                            destination,
                            source_stat,
                            None,
                            source_timing={
                                "program_date_time": orig_ctime.isoformat(),
                                "duration": duration,
                                "sha256": expected_digest,
                            },
                        )
                    temporary = os.path.join(
                        destination_dir, TIER_MOVE_TEMP_PREFIX + uuid.uuid4().hex
                    )
                    protected_temp = False
                    try:
                        digest = hashlib.sha256()
                        with open(source, "rb") as inp, open(temporary, "xb") as out:
                            before = os.fstat(inp.fileno())
                            if (
                                expected_identity is not None
                                and [
                                    before.st_dev,
                                    before.st_ino,
                                    before.st_size,
                                    before.st_mtime_ns,
                                ]
                                != expected_identity
                            ):
                                raise OSError("staged fragment changed before copy")
                            while chunk := inp.read(1024 * 1024):
                                out.write(chunk)
                                digest.update(chunk)
                            out.flush()
                            os.fsync(out.fileno())
                            after = os.fstat(inp.fileno())
                            if (
                                before.st_dev,
                                before.st_ino,
                                before.st_size,
                                before.st_mtime_ns,
                            ) != (
                                after.st_dev,
                                after.st_ino,
                                after.st_size,
                                after.st_mtime_ns,
                            ) or os.fstat(out.fileno()).st_size != before.st_size:
                                raise OSError(
                                    "staged source changed during publication"
                                )
                        if (
                            expected_digest is not None
                            and digest.hexdigest() != expected_digest
                        ):
                            raise OSError(
                                "staged fragment content changed before publication"
                            )
                        require_tier_markers(destination, self._tier_markers)
                        marker = prepare(
                            destination_dir,
                            os.path.basename(file),
                            {
                                "orig_ctime": orig_ctime.isoformat(),
                                "duration": duration,
                                "file_key": None,
                            },
                            temp_path=temporary,
                            source_intent=source_intent,
                            copied_digest=digest.hexdigest(),
                        )
                        protected_temp = True
                        publish_or_verify(marker)
                        # Pin the verified published inputs under the lock; do
                        # the potentially slow FFmpeg work after releasing it.
                        if self._timelapse_folder and self._temp_timelapse_folder:
                            try:
                                for path in (init_dest, destination):
                                    timelapse_fds.append(
                                        os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                                    )
                            except OSError:
                                for fd in timelapse_fds:
                                    os.close(fd)
                                timelapse_fds.clear()
                                self._logger.warning(
                                    "Cannot open validated timelapse inputs for %s",
                                    file,
                                    exc_info=True,
                                )
                        # Keep staged source and durable hold until DB acknowledgement.
                    finally:
                        if not protected_temp and os.path.exists(temporary):
                            os.unlink(temporary)
            if timelapse_fds:
                self._segment_hook(
                    file,
                    orig_ctime,
                    init_path=f"/proc/self/fd/{timelapse_fds[0]}",
                    segment_path=f"/proc/self/fd/{timelapse_fds[1]}",
                    pass_fds=tuple(timelapse_fds),
                )
        except FileNotFoundError:
            self._logger.debug(f"{file} not found", exc_info=True)
        except CameraLockBusy:
            self._logger.debug(
                "Publication busy for %s; retaining staging for retry", file
            )
        except OSError as err:
            if err.errno == errno.ENOSPC:
                self._logger.error(
                    "No space left on device, trigger tier check",
                    exc_info=err,
                )
                self._worker_event.clear()
                self.output_queue.put(
                    {
                        "error": "no_space_left",
                    }
                )
                self._worker_event.wait(timeout=1)
            else:
                self._logger.warning(
                    "Cannot publish %s; retaining staging file", file, exc_info=err
                )
        finally:
            for fd in timelapse_fds:
                os.close(fd)

    def _send_files_metadata(
        self, path: str, orig_ctime: datetime.datetime, duration: float | None
    ) -> None:
        """Save temporary metadata which is later used when inserting into the DB."""
        self._worker_event.clear()
        self.output_queue.put(
            {
                "path": path,
                "orig_ctime": orig_ctime,
                "duration": duration,
            }
        )
        self._worker_event.wait(timeout=1)

    def _read_m3u8_mp4box(self, file: str) -> str:
        """Read m3u8 file created by MP4Box."""
        return open(
            os.path.join(
                self.temp_segments_folder,
                file.split(".", maxsplit=1)[0],
                "master_1.m3u8",
            ),
            encoding="utf-8",
        ).read()

    def _read_m3u8(self, file: str) -> str:
        """Read m3u8 file created by encoder."""
        with open(
            os.path.join(
                self.temp_segments_folder,
                os.path.dirname(file),
                "index.m3u8",
            ),
            encoding="utf-8",
        ) as playlist:
            return playlist.read()

    def _handle_mp4(self, file: str) -> None:
        """Handle MP4 while retaining the original if conversion fails."""
        try:
            if not self._mp4box_command(file):
                return
            extinf = _extract_extinf_number(self._read_m3u8_mp4box(file), "clip_1.m4s")
            if not extinf:
                self._logger.error("Failed to get extinf for %s", file)
                return
            orig_ctime = _get_orig_ctime(file, None)
            self._send_files_metadata(
                os.path.join(
                    self.segments_folder,
                    file.split(".", maxsplit=1)[0] + ".m4s",
                ),
                orig_ctime,
                extinf,
            )
            if not self._move_to_segments_folder_mp4box(file, orig_ctime):
                return
        except Exception as err:  # pylint: disable=broad-except
            self._logger.error(
                "Failed to fragment %s; retaining original", file, exc_info=err
            )
            return

        try:
            os.remove(os.path.join(self.temp_segments_folder, file))
            shutil.rmtree(
                os.path.join(self.temp_segments_folder, file.split(".", maxsplit=1)[0]),
            )
        except FileNotFoundError as err:
            self._logger.error("Failed to clean converted fragment", exc_info=err)

    def _defer_metadata_retry(self, file: str, now: float) -> None:
        """Retry uncertain metadata without repeatedly processing old fragments."""
        previous_delay = self._metadata_retry.get(file, (0, 0))[1]
        delay = min(
            METADATA_RETRY_MAX_SECONDS,
            max(METADATA_RETRY_INITIAL_SECONDS, previous_delay * 2),
        )
        self._metadata_retry[file] = (now + delay, delay)

    def _handle_m4s(self, file: str) -> None:
        """Handle completed fragments; preserve media with uncertain timing."""
        try:
            name = os.path.basename(file)
            expected_identity = None
            expected_digest = None
            if _RUN_SEGMENT_NAME.fullmatch(name):
                try:
                    snapshot = os.stat(
                        os.path.join(self.temp_segments_folder, file),
                        follow_symlinks=False,
                    )
                    expected_identity = [
                        snapshot.st_dev,
                        snapshot.st_ino,
                        snapshot.st_size,
                        snapshot.st_mtime_ns,
                    ]
                except FileNotFoundError:
                    pass
            try:
                m3u8 = self._read_m3u8(file)
            except FileNotFoundError:
                m3u8 = ""
            extinf = _extract_extinf_number(m3u8, name)
            program_date_time = _extract_program_date_time(m3u8, name)
            if _RUN_SEGMENT_NAME.fullmatch(name) and program_date_time is None:
                fragment = Path(self.temp_segments_folder) / file
                timing = read_playlist_time(fragment) or read_source_intent_time(
                    fragment, Path(self.segments_folder) / file
                )
                if timing is not None:
                    program_date_time, extinf, expected_identity, expected_digest = (
                        timing
                    )
            if _RUN_SEGMENT_NAME.fullmatch(name) and expected_identity is None:
                program_date_time = None
            match = _RUN_SEGMENT_NAME.fullmatch(name)
            if (
                self._storage_blocked
                and match
                and program_date_time is not None
                and int(match["sequence"])
                > self._blocked_sequences.get(os.path.dirname(file), -1)
            ):
                source = os.path.join(self.temp_segments_folder, file)
                with camera_lock(
                    "/tmp/viseron-run-locks", os.path.abspath(self.temp_segments_folder)
                ):
                    if not source_move_pending(source):
                        info = os.stat(source, follow_symlinks=False)
                        if [
                            info.st_dev,
                            info.st_ino,
                            info.st_size,
                            info.st_mtime_ns,
                        ] == expected_identity:
                            os.unlink(source)
                            return
            if _RUN_SEGMENT_NAME.fullmatch(name) and program_date_time is None:
                now = time.monotonic()
                first_seen = self._first_missing_timing.setdefault(file, now)
                if now - first_seen >= MISSING_TIMING_GRACE_SECONDS and (
                    self._last_missing_timestamp_warning is None
                    or now - self._last_missing_timestamp_warning >= 60  # noqa: PLR2004
                ):
                    self._logger.warning(
                        "Playlist timing remains unavailable for staged fragment %s; "
                        "retaining it and its init unindexed in staging. "
                        "Staging durability depends on its configured storage.",
                        file,
                    )
                    self._last_missing_timestamp_warning = now
                if now - first_seen >= MISSING_TIMING_GRACE_SECONDS:
                    self._defer_metadata_retry(file, now)
                return
            self._first_missing_timing.pop(file, None)
            if extinf is None:
                extinf = _filename_duration(name)
            if extinf:
                if match and expected_identity is not None:
                    try:
                        _validate_fragment(
                            os.path.join(self.temp_segments_folder, file),
                            expected_identity,
                        )
                    except ValueError as error:
                        now = time.monotonic()
                        self._defer_metadata_retry(file, now)
                        if (
                            self._last_invalid_fragment_warning is None
                            or now - self._last_invalid_fragment_warning
                            >= METADATA_WARNING_INTERVAL_SECONDS
                        ):
                            self._logger.warning(
                                "Incomplete staged fragment %s: %s; leaving it "
                                "unindexed in staging. Recording has a gap.",
                                file,
                                error,
                            )
                            self._last_invalid_fragment_warning = now
                        return
                self._metadata_retry.pop(file, None)
                orig_ctime = _get_orig_ctime(file, program_date_time)
                self._send_files_metadata(
                    os.path.join(self.segments_folder, file), orig_ctime, extinf
                )
                self._move_to_segments_folder(
                    file, orig_ctime, extinf, expected_identity, expected_digest
                )
            else:
                now = time.monotonic()
                self._defer_metadata_retry(file, now)
                if (
                    self._last_missing_duration_warning is None
                    or now - self._last_missing_duration_warning
                    >= METADATA_WARNING_INTERVAL_SECONDS
                ):
                    self._logger.warning(
                        "No known duration for %s; retaining fragment and init in "
                        "staging and retrying with backoff. Further duration warnings "
                        "are limited to once a minute.",
                        file,
                    )
                    self._last_missing_duration_warning = now
        except Exception as err:  # pylint: disable=broad-except
            self._logger.error(f"Failed to process m4s file {file}", exc_info=err)
