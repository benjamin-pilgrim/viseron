"""Independent, bounded pressure checks for configured recorder storage roots."""

import logging
import threading
import time

from viseron.components.storage.pressure import run_pressure
from viseron.const import CAMERA_SEGMENT_DURATION
from viseron.helpers.child_process_context import get_child_process_context

LOGGER = logging.getLogger(__name__)
PRESSURE_TIMEOUT = 15


class PressureManager:
    """A stalled remote root never occupies another root's pressure worker."""

    def __init__(self, storage):
        self.storage = storage
        self.pending = {}
        self.status = {}
        self.retry = {}
        self.stopped = False
        self.promotions = {}
        self.last_progress = {}
        self.promotion_failed = set()
        self.results = {}

    def configurations(self):
        """Resolve camera tier paths without touching remote filesystems."""
        roots = {}
        for identifier, categories in list(self.storage.camera_tier_handlers.items()):
            tiers = categories.get("recorder", ())
            for index, group in enumerate(tiers):
                handler = group.get("segments")
                if handler is None or "on_full" not in handler.tier:
                    continue
                camera = handler._camera
                spool = (
                    getattr(camera, "temp_segments_folder", None)
                    if index == 0
                    else None
                )
                if index == 0 and not isinstance(spool, str):
                    continue  # Camera construction has not finished yet.
                path = handler.tier["path"]
                policy = handler.tier["on_full"]["policy"]
                maximum = handler.tier.get("retention", {}).get("max_size", 0)
                root = roots.setdefault(
                    path,
                    {
                        "path": path,
                        "probe": handler._path,
                        "policy": policy,
                        "max_size": maximum,
                        "markers": dict(self.storage.config.get("tier_markers", {})),
                        "cameras": [],
                        "requested": {},
                    },
                )
                if root["policy"] != policy or root["max_size"] != maximum:
                    raise ValueError(
                        "A shared tier needs one pressure policy and budget"
                    )
                root["cameras"].append(
                    {
                        "identifier": identifier,
                        "tier_id": index,
                        "segments": handler._path,
                        "spool": spool,
                        "has_next": index + 1 < len(tiers),
                        "lookback": camera.config.get("recorder", {}).get("lookback", 0)
                        + CAMERA_SEGMENT_DURATION,
                    }
                )
                requested = self.storage.camera_requested_files_count.get(identifier)
                if requested:
                    root["requested"][identifier] = tuple(requested.filenames)
        return roots

    def poll(self):
        """Collect finished checks and start at most one child per root."""
        if self.stopped:
            return
        now = time.monotonic()
        roots = self.configurations()
        for path, (process, connection, started, config) in list(self.pending.items()):
            if path not in self.results and connection.poll():
                try:
                    # Drain before joining: a promotion result can exceed the
                    # pipe buffer while its child is still finishing send().
                    self.results[path] = connection.recv()
                except EOFError:
                    self.results[path] = {
                        "state": "unavailable",
                        "deleted": 0,
                        "offset": 0,
                    }
            if (
                process.is_alive()
                and path not in self.results
                and now - started < PRESSURE_TIMEOUT
            ):
                continue
            if process.is_alive() and path not in self.results:
                process.terminate()
            process.join(timeout=0.1)
            if process.is_alive() and path in self.results:
                process.terminate()
                process.join(timeout=0.1)
            if process.is_alive():
                self.status[path] = {"state": "unavailable", "deleted": 0, "offset": 0}
                continue
            result = self.results.pop(
                path, {"state": "unavailable", "deleted": 0, "offset": 0}
            )
            connection.close()
            self.pending.pop(path)
            previous_result = self.status.get(path, {})
            previous = previous_result.get("state")
            if (
                path not in self.last_progress
                or result.get("free", 0) > previous_result.get("free", 0)
                or result.get("used", float("inf"))
                < previous_result.get("used", float("inf"))
            ):
                self.last_progress[path] = now
            if result["state"] == "normal":
                self.promotion_failed.discard(path)
                self.last_progress.pop(path, None)
            self.status[path] = result
            if result["state"] != previous:
                log = (
                    LOGGER.warning
                    if result["state"] in ("blocked", "unavailable")
                    else LOGGER.info
                )
                log(
                    "Recording storage %s: %s (%s)",
                    path,
                    result["state"],
                    config["policy"],
                )
            if result.get("promote"):
                thread = self.promotions.get(path)
                if thread is None or not thread.is_alive():
                    thread = threading.Thread(
                        target=self._promote,
                        args=(config, result.get("promote", ())),
                        daemon=True,
                        name="pressure-promotion",
                    )
                    self.promotions[path] = thread
                    thread.start()
            self.retry[path] = now + 1
        for path, config in roots.items():
            if path in self.pending or now < self.retry.get(path, 0):
                continue
            previous = self.status.get(path, {})
            config["reclaim"] = self._can_reclaim(path, config, now)
            with self.storage.pending_moves_lock:
                config["reserved"] = tuple(self.storage.pending_moves)
            config["offset"] = previous.get("offset", 0)
            receive, send = get_child_process_context().Pipe(duplex=False)
            process = get_child_process_context().Process(
                target=run_pressure, args=(config, send), daemon=True
            )
            try:
                process.start()
            except Exception:
                receive.close()
                send.close()
                self.retry[path] = now + PRESSURE_TIMEOUT
                LOGGER.exception("Could not start pressure check for %s", path)
                continue
            send.close()
            self.pending[path] = (process, receive, now, config)

    def _can_reclaim(self, path, config, now):
        """Prefer healthy promotion; fall back promptly on failure or no progress."""
        return (
            not any(camera.get("has_next", True) for camera in config["cameras"])
            or path in self.promotion_failed
            or now - self.last_progress.get(path, now) >= PRESSURE_TIMEOUT
        )

    def _promote(self, config, candidates):
        # Imports stay here to avoid storage/handler construction cycles.
        from viseron.components.storage.tier_handler import handle_file  # noqa: PLC0415

        for candidate in candidates:
            if self.stopped:
                return
            tiers = self.storage.camera_tier_handlers[candidate["camera"]]["recorder"]
            source = next(
                item
                for item in config["cameras"]
                if item["identifier"] == candidate["camera"]
            )
            index = source["tier_id"]
            if index + 1 >= len(tiers):
                self.promotion_failed.add(config["path"])
                continue
            handler = tiers[index][candidate["kind"]]
            destination = tiers[index + 1][candidate["kind"]].tier
            try:
                if not self.storage.tier_available(destination["path"]):
                    self.promotion_failed.add(config["path"])
                    return
                handle_file(
                    handler._vis,
                    self.storage.get_session,
                    self.storage,
                    candidate["camera"],
                    index,
                    "recorder",
                    candidate["kind"],
                    handler.tier,
                    destination,
                    candidate["path"],
                    config["path"],
                    LOGGER,
                )
            except Exception:  # Failed promotion leaves local reclamation available.
                self.promotion_failed.add(config["path"])
                LOGGER.exception(
                    "Pressure promotion deferred for %s", candidate["path"]
                )

    def accepts_promotions(self, path):
        """Keep footage on its source when the destination cannot accept more."""
        return self.status.get(path, {}).get("state") not in ("blocked", "unavailable")

    def recording_blocked(self, identifier):
        """Stop admission only when the first tier has no eligible space left."""
        tiers = self.storage.camera_tier_handlers.get(identifier, {}).get(
            "recorder", ()
        )
        return bool(
            tiers
            and self.status.get(tiers[0]["segments"].tier["path"], {}).get("state")
            == "blocked"
        )

    def stop(self):
        """Cancel owned pressure children without waiting on another root."""
        self.stopped = True
        for process, connection, _started, _config in self.pending.values():
            if process.is_alive():
                process.terminate()
            process.join(timeout=0.1)
            connection.close()
        self.pending.clear()
        self.results.clear()
