"""Reclaim complete recording files according to a tier's full-storage policy."""

from __future__ import annotations

import datetime
import os
import stat
from pathlib import Path

from sqlalchemy import and_, case as sql_case, func, or_, select, update
from sqlalchemy.orm import sessionmaker

from viseron.components.storage.asset_moves import asset_locks, asset_move_pending
from viseron.components.storage.const import ENGINE
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.pending_journal import protected_by_pending, sync_dir
from viseron.components.storage.run_lifecycle import camera_lock, is_run_directory
from viseron.components.storage.source_move_intent import source_move_pending
from viseron.components.storage.util import require_tier_markers
from viseron.helpers import utcnow

BATCH = 128


def event_fence(session, camera):
    """Serialize new event creation with pressure classification and deletion."""
    session.execute(
        select(func.pg_advisory_xact_lock(func.hashtext("recording-events:" + camera)))
    )


def _events(session, candidate):
    field = {
        "thumbnails": Recordings.thumbnail_path,
        "event_clips": Recordings.clip_path,
    }.get(candidate["kind"])
    coverage = (
        field == candidate["path"]
        if field is not None
        else (
            (Recordings.adjusted_start_time <= candidate["end"])
            & (
                Recordings.end_time.is_(None)
                | (Recordings.end_time > candidate["start"])
            )
            & (
                Recordings.recorder_run_id.is_(None)
                | (Recordings.recorder_run_id == candidate["run"])
            )
        )
    )
    return session.execute(
        select(Recordings.end_time).where(
            Recordings.camera_identifier == candidate["camera"],
            coverage,
        )
    ).all()


def _identity(path):
    info = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or info.st_size <= 0:
        raise OSError("Pressure candidate is not complete regular media")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]


def _usage(config, session):
    from viseron.domains.camera.playlist_timing import (  # noqa: PLC0415
        read_live_playlist_time,
        read_playlist_time,
    )

    size = session.execute(
        select(func.coalesce(func.sum(Files.size), 0)).where(
            Files.category == "recorder",
            Files.tier_path == config["path"],
        )
    ).scalar_one()
    staged = []
    for camera in config["cameras"]:
        spool = camera.get("spool")
        if not spool:
            continue
        try:
            for run in Path(spool).iterdir():
                if not is_run_directory(run.name) or run.is_symlink():
                    continue
                for fragment in run.iterdir():
                    if fragment.is_symlink() or not fragment.is_file():
                        continue
                    size += fragment.stat().st_size
                    if fragment.suffix != ".m4s":
                        continue
                    timing = read_playlist_time(fragment) or read_live_playlist_time(
                        fragment
                    )
                    if timing:
                        start, duration, identity, _digest = timing
                        staged.append(
                            {
                                "path": str(fragment),
                                "camera": camera["identifier"],
                                "root": spool,
                                "kind": "segments",
                                "run": run.name,
                                "start": start,
                                "end": start + datetime.timedelta(seconds=duration),
                                "identity": identity,
                                "id": None,
                            }
                        )
        except FileNotFoundError:
            continue
    return int(size), staged


def _candidates(config, session, staged):
    cameras = {item["identifier"]: item for item in config["cameras"]}
    event_related = (
        select(Recordings.id)
        .where(
            Recordings.camera_identifier == Files.camera_identifier,
            or_(
                Recordings.thumbnail_path == Files.path,
                Recordings.clip_path == Files.path,
                and_(
                    Files.subcategory == "segments",
                    Recordings.adjusted_start_time
                    <= Files.orig_ctime
                    + func.make_interval(
                        0, 0, 0, 0, 0, 0, func.coalesce(Files.duration, 0)
                    ),
                    or_(
                        Recordings.end_time.is_(None),
                        Recordings.end_time > Files.orig_ctime,
                    ),
                    or_(
                        Recordings.recorder_run_id.is_(None),
                        Files.directory.endswith("/" + Recordings.recorder_run_id),
                    ),
                ),
            ),
        )
        .exists()
    )
    priority = (
        sql_case((event_related, 1), else_=0)
        if config["policy"] == "ordinary_first"
        else 0
    )
    rows = (
        session.execute(
            select(Files)
            .where(
                Files.category == "recorder",
                Files.tier_path == config["path"],
                Files.camera_identifier.in_(cameras),
                Files.move_target.is_(None),
                Files.subcategory.in_(("segments", "thumbnails", "event_clips")),
            )
            .order_by(
                *([priority] if config["policy"] == "ordinary_first" else []),
                Files.orig_ctime,
                Files.id,
            )
            .offset(config.get("offset", 0))
            .limit(BATCH)
        )
        .scalars()
        .all()
    )
    candidates = list(staged)
    for row in rows:
        if row.subcategory == "segments" and row.duration is None:
            continue
        try:
            identity = _identity(row.path)
        except OSError:
            continue
        if identity[2] != row.size:
            continue
        candidates.append(
            {
                "path": row.path,
                "camera": row.camera_identifier,
                "root": cameras[row.camera_identifier]["segments"],
                "kind": row.subcategory,
                "run": os.path.basename(row.directory),
                "start": row.orig_ctime,
                "end": row.orig_ctime + datetime.timedelta(seconds=row.duration or 0),
                "identity": identity,
                "id": row.id,
            }
        )
    ranked = []
    for candidate in candidates:
        camera = cameras[candidate["camera"]]
        if candidate["path"] in config.get("reserved", ()):
            continue
        if os.path.basename(candidate["path"]) in config.get("requested", {}).get(
            candidate["camera"], ()
        ):
            continue
        if candidate["end"] >= utcnow() - datetime.timedelta(
            seconds=camera["lookback"]
        ):
            continue
        events = _events(session, candidate)
        if any(end is None for (end,) in events):
            continue
        candidate["event"] = bool(events)
        ranked.append(candidate)
    ranked.sort(
        key=lambda item: (
            item["event"] if config["policy"] == "ordinary_first" else False,
            item["start"],
            item["path"],
        )
    )
    return ranked[:BATCH], len(rows)


def reclaim(config, get_session, *, free_space=None):
    """Inspect and reclaim one bounded batch; callers isolate remote filesystems."""
    probe = config["probe"]
    require_tier_markers(probe, config.get("markers", {}))
    initial = os.statvfs(probe)
    device = os.stat(probe).st_dev
    free_space = free_space or (lambda: os.statvfs(probe).f_bavail * initial.f_frsize)
    capacity = initial.f_blocks * initial.f_frsize
    start_free, target_free = capacity * 0.10, capacity * 0.15

    def free_inodes(fraction):
        return (
            not initial.f_files
            or os.statvfs(probe).f_favail >= initial.f_files * fraction
        )

    maximum = config.get("max_size", 0)
    with get_session() as session:
        used, staged = _usage(config, session)
        trigger_free = target_free if config.get("reclaim") else start_free
        trigger_size = maximum
        if (
            free_space() >= trigger_free
            and free_inodes(0.15 if config.get("reclaim") else 0.10)
            and (not maximum or used < trigger_size)
        ):
            return {
                "state": "normal",
                "deleted": 0,
                "offset": 0,
                "used": used,
                "free": free_space(),
            }
        candidates, scanned = _candidates(config, session, staged)
    if not config.get("reclaim", False):
        return {
            "state": "promoting",
            "used": used,
            "free": free_space(),
            "deleted": 0,
            "offset": config.get("offset", 0),
            "promote": [item for item in candidates if item["id"] is not None][:16],
        }
    deleted = 0
    removed = set()
    for candidate in candidates:
        if (
            free_space() >= target_free
            and free_inodes(0.15)
            and (not maximum or used <= maximum)
        ):
            break
        path = candidate["path"]
        if (
            (free_space() < target_free or not free_inodes(0.15))
            and (not maximum or used <= maximum)
            and candidate["identity"][0] != device
        ):
            continue
        lock = (
            camera_lock("/tmp/viseron-run-locks", os.path.abspath(candidate["root"]))
            if candidate["kind"] == "segments"
            else asset_locks(path)
        )
        try:
            with lock, get_session() as session:
                event_fence(session, candidate["camera"])
                require_tier_markers(path, config.get("markers", {}))
                if (
                    source_move_pending(path)
                    or protected_by_pending(path)
                    or asset_move_pending(session, path)
                ):
                    continue
                if _identity(path) != candidate["identity"]:
                    continue
                events = _events(session, candidate)
                if any(end is None for (end,) in events) or (
                    events and config["policy"] == "preserve_events"
                ):
                    continue
                if (
                    config["policy"] == "ordinary_first"
                    and bool(events) != candidate["event"]
                ):
                    continue
                row = (
                    session.get(Files, candidate["id"])
                    if candidate["id"] is not None
                    else None
                )
                if candidate["id"] is not None and (
                    row is None or row.path != path or row.move_target is not None
                ):
                    continue
                if row is None:
                    # Do not remove a staged source already indexed by another path.
                    if session.execute(
                        select(Files.id).where(Files.path == path)
                    ).first():
                        continue
                os.unlink(path)
                sync_dir(os.path.dirname(path))
                if row is not None:
                    session.delete(row)
                if candidate["kind"] in ("thumbnails", "event_clips"):
                    field = (
                        Recordings.thumbnail_path
                        if candidate["kind"] == "thumbnails"
                        else Recordings.clip_path
                    )
                    session.execute(
                        update(Recordings).where(field == path).values({field: None})
                    )
                session.commit()
                deleted += 1
                removed.add(path)
                used -= candidate["identity"][2]
        except OSError:
            continue
    healthy = (
        free_space() >= target_free
        and free_inodes(0.15)
        and (not maximum or used <= maximum)
    )
    return {
        "state": "normal" if healthy else "reclaiming" if deleted else "blocked",
        "deleted": deleted,
        "used": used,
        "free": free_space(),
        "promote": [
            item
            for item in candidates
            if item["id"] is not None and item["path"] not in removed
        ][:16],
        "offset": 0 if deleted or scanned < BATCH else config.get("offset", 0) + BATCH,
    }


def run_pressure(config, output):
    """One isolated root check: a stalled NAS cannot block local reclamation."""
    try:
        output.send(reclaim(config, sessionmaker(bind=ENGINE)))
    except Exception as error:  # Child reports an unavailable root, never guesses.
        output.send(
            {"state": "unavailable", "deleted": 0, "error": str(error), "offset": 0}
        )
    finally:
        output.close()
