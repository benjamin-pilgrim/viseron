"""Segment fragmenter, executed in the child process of the fragmenter worker.

Deliberately depends on nothing that reaches back into the running Viseron instance,
so that the child process can be created with the forkserver start method to reduce
memory usage.
"""

from __future__ import annotations

import datetime
import errno
import hashlib
import logging
import os
import re
import shutil
import subprocess as sp
import tempfile
import time
from typing import TYPE_CHECKING

import psutil
from path import Path

from viseron.components.storage.util import copy_file_atomic, move_file_atomic
from viseron.domains.camera.const import MP4BOX_PATH
from viseron.helpers import get_utc_offset
from viseron.helpers.child_process_worker import ChildProcessWork
from viseron.helpers.logs import LogPipe

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

TIMELAPSE_FFMPEG_TIMEOUT = 10


def _get_open_files(path: str, process: psutil.Process) -> list[str]:
    """Get open files for a process."""
    files = []
    try:
        open_files = process.open_files()
        for file in open_files:
            if file.path.startswith(f"{path}/"):
                files.append(file.path.split("/")[-1])
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
    return [str(f.name) for f in mp4_files if str(f.name) not in files_in_use]


def _extract_extinf_number(playlist_content: str, file: str) -> float | None:
    """Extract the EXTINF number from a HLS playlist."""
    # Escape special regex characters in the file name
    escaped_file = re.escape(file)

    # Simplified pattern
    pattern = rf"#EXTINF:(\d+(?:\.\d+)?)[^\n]*\n(?:(?!#EXTINF)[^\n]*\n)*{escaped_file}"

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
        rf"([^\n]+)\n(?:(?!#EXT-X-PROGRAM-DATE-TIME)[^\n]*\n)*{escaped_file}"
    )

    match = re.search(pattern, playlist_content)

    try:
        if match:
            # Remove timezone information since fromisoformat does not support it
            no_tz = re.sub(r"\+[0-9]{4}$", "", match.group(1))
            # Adjust according to local timezone
            return (datetime.datetime.fromisoformat(no_tz) - get_utc_offset()).replace(
                tzinfo=datetime.timezone.utc
            )
    except ValueError:
        pass
    return None


def _get_orig_ctime(
    file: str, program_date_time: datetime.datetime | None
) -> datetime.datetime:
    """Return the creation time of a segment named <epoch>.<ext>."""
    if program_date_time:
        return program_date_time
    return datetime.datetime.fromtimestamp(
        int(file.split(".", maxsplit=1)[0]), tz=datetime.timezone.utc
    )


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
        metadata_ack,
    ) -> None:
        self._camera_identifier = camera_identifier
        self.temp_segments_folder = temp_segments_folder
        self.segments_folder = segments_folder
        self._timelapse_folder = timelapse_folder
        self._temp_timelapse_folder = temp_timelapse_folder
        self._worker_event = worker_event
        self._metadata_ack = metadata_ack
        self._logger = logging.getLogger(fragmenter_logger_names(camera_identifier)[0])
        self._log_pipe: LogPipe

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
            mp4s = mp4s[:5]
        for mp4 in mp4s:
            self._logger.debug(f"Processing {mp4}")
            if mp4.split(".")[1] == "m4s":
                self._handle_m4s(mp4)
            else:
                self._handle_mp4(mp4)

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

    def _segment_hook(self, file: str, orig_ctime: datetime.datetime) -> None:
        """Perform per fragment tasks before moving fragment to storage.

        Currently only used for extracting timelapse frames from fragments.
        """
        if self._timelapse_folder is None or self._temp_timelapse_folder is None:
            return

        frame_filename = os.path.splitext(os.path.basename(file))[0] + ".jpg"
        tmp_frame_path = os.path.join(self._temp_timelapse_folder, frame_filename)
        frame_path = os.path.join(self._timelapse_folder, frame_filename)
        init_path = os.path.join(self.temp_segments_folder, "init.mp4")
        segment_path = os.path.join(self.temp_segments_folder, file)

        self._extract_timelapse_frame(
            init_path, segment_path, tmp_frame_path, frame_path, orig_ctime
        )

    def _extract_timelapse_frame(
        self,
        init_path: str,
        segment_path: str,
        tmp_frame_path: str,
        frame_path: str,
        orig_ctime: datetime.datetime,
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
            )
            if result.returncode == 0:
                self._logger.debug(f"Timelapse: Extracted frame {tmp_frame_path}")
                if self._send_files_metadata(frame_path, orig_ctime, None):
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

    def _send_files_metadata(
        self,
        path: str,
        orig_ctime: datetime.datetime,
        duration: float | None,
        init_hash: str | None = None,
    ) -> bool:
        """Stage metadata in the parent and wait for this request's acknowledgement."""
        request_id = time.monotonic_ns()
        self._worker_event.clear()
        self.output_queue.put(
            {
                "path": path,
                "orig_ctime": orig_ctime,
                "duration": duration,
                "hls_init_hash": init_hash,
                "metadata_request": request_id,
            }
        )
        if (
            self._worker_event.wait(timeout=1)
            and self._metadata_ack.value == request_id
        ):
            return True
        self.output_queue.put({"discard_metadata": path})
        self._logger.warning("Metadata acknowledgement timed out for %s", path)
        return False

    def _publish_segment(
        self,
        init_src: str,
        src: str,
        dst: str,
        orig_ctime: datetime.datetime,
        duration: float,
    ) -> bool:
        """Publish a segment only after its init and metadata are ready."""
        try:
            with open(init_src, "rb") as init:
                init_bytes = init.read()
            init_hash = hashlib.sha256(init_bytes).hexdigest()
            sidecar = os.path.join(self.segments_folder, f"init-{init_hash}.mp4")
            if not os.path.isfile(sidecar):
                # Publish exactly the bytes that were hashed, even if the encoder
                # replaces its mutable init.mp4 while remounting or restarting.
                with tempfile.NamedTemporaryFile(
                    dir=self.temp_segments_folder
                ) as staged:
                    staged.write(init_bytes)
                    staged.flush()
                    copy_file_atomic(staged.name, sidecar)
            copy_file_atomic(sidecar, os.path.join(self.segments_folder, "init.mp4"))

            if not self._send_files_metadata(dst, orig_ctime, duration, init_hash):
                return False
            try:
                result = move_file_atomic(src, dst)
                if not result.source_removed:
                    self._logger.warning(
                        "Published %s but could not remove %s", dst, src
                    )
            except OSError:
                self.output_queue.put({"discard_metadata": dst})
                raise
        except OSError as error:
            self._logger.warning("Failed to publish segment %s: %s", dst, error)
            if error.errno == errno.ENOSPC:
                self.output_queue.put({"error": "no_space_left"})
            return False
        return True

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

    def _read_m3u8(self) -> str:
        """Read m3u8 file created by encoder."""
        return open(
            os.path.join(
                self.temp_segments_folder,
                "index.m3u8",
            ),
            encoding="utf-8",
        ).read()

    def _handle_mp4(self, file: str) -> None:
        """Fragment and publish an MP4, retaining the input on publish failure."""
        stem = file.split(".", maxsplit=1)[0]
        outdir = os.path.join(self.temp_segments_folder, stem)
        try:
            if not self._mp4box_command(file):
                return
            extinf = _extract_extinf_number(self._read_m3u8_mp4box(file), "clip_1.m4s")
            if not extinf:
                self._logger.error("Failed to get extinf for %s", file)
                return
            orig_ctime = _get_orig_ctime(file, None)
            self._segment_hook_mp4box(file, orig_ctime)
            if self._publish_segment(
                os.path.join(outdir, "clip_init.mp4"),
                os.path.join(outdir, "clip_1.m4s"),
                os.path.join(self.segments_folder, stem + ".m4s"),
                orig_ctime,
                extinf,
            ):
                os.remove(os.path.join(self.temp_segments_folder, file))
        except (OSError, ValueError):
            self._logger.exception("Failed to fragment %s", file)
        finally:
            shutil.rmtree(outdir, ignore_errors=True)

    def _handle_m4s(self, file: str) -> None:
        """Publish an encoder fragment with its matching init file."""
        try:
            m3u8 = self._read_m3u8()
            extinf = _extract_extinf_number(m3u8, file)
            if not extinf:
                self._logger.error("Failed to get extinf for %s", file)
                return
            orig_ctime = _get_orig_ctime(file, _extract_program_date_time(m3u8, file))
            self._segment_hook(file, orig_ctime)
            self._publish_segment(
                os.path.join(self.temp_segments_folder, "init.mp4"),
                os.path.join(self.temp_segments_folder, file),
                os.path.join(self.segments_folder, file),
                orig_ctime,
                extinf,
            )
        except (OSError, ValueError):
            self._logger.exception("Failed to process segment %s", file)
