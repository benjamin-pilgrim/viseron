"""Check storage tiers in a subprocess."""

from __future__ import annotations

import argparse
import itertools
import logging
import multiprocessing as mp
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field, replace
from queue import Empty, Queue
from typing import TYPE_CHECKING, Generic, Literal, TypeAlias, TypeVar

import psutil
import setproctitle
from apscheduler.schedulers.background import BackgroundScheduler

from manager import connect
from viseron.components.storage.check_tier import Worker
from viseron.helpers.subprocess_worker import SubProcessWorker
from viseron.watchdog.subprocess_watchdog import RestartablePopen
from viseron.watchdog.thread_watchdog import RestartableThread, ThreadWatchDog

if TYPE_CHECKING:
    import datetime

    import numpy as np

    from viseron import Viseron

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")
K = TypeVar("K", bound=Hashable)

# Upper bound on the parent's pending-callback dict. check_tier callbacks are
# deduped in the parent by throttle_key to mirror DedupCheckQueue, but keep a
# hard cap as a final guard if the subprocess takes longer to reply than callers
# can enqueue callback-backed file operations.
MAX_PENDING_CALLBACKS = 10_000


@dataclass
class DataItem:
    """Data item to be processed by the worker."""

    cmd: Literal["check_tier"]
    camera_identifier: str
    tier_id: int
    category: str
    subcategories: list[str]
    throttle_period: datetime.timedelta
    max_bytes: int
    min_age: datetime.timedelta
    max_age: datetime.timedelta
    min_bytes: int
    drain: bool
    files_enabled: bool = True
    events_enabled: bool = False
    events_max_bytes: int | None = None
    events_min_age: datetime.timedelta | None = None
    events_max_age: datetime.timedelta | None = None
    events_min_bytes: int | None = None
    continuous_schedule: list[dict[str, str]] | None = None
    continuous_schedule_timezone: str | None = None
    continuous_lookback_seconds: int = 0
    callback_id: str | None = None
    data: np.ndarray | None = None
    error: str | None = None
    deferred: bool = False
    force: bool = False

    @property
    def throttle_key(self) -> str:
        """Generate a unique key for throttling."""
        return (
            f"{self.camera_identifier}_"
            f"{self.tier_id}_{self.category}_"
            f"{self.subcategories[0]}"
        )


@dataclass
class DataItemMoveFile:
    """Data item to be processed by the worker for moving files."""

    cmd: Literal["move_file"]
    src: str
    dst: str
    destination_root: str | None = None
    callback_id: str | None = None
    coalesced_callback_ids: list[str] = field(default_factory=list)
    error: str | None = None
    deferred: bool = False
    published: bool = False
    source_missing: bool = False
    size: int | None = None


@dataclass
class DataItemCopyFile:
    """Data item to be processed by the worker for copying files."""

    cmd: Literal["copy_file"]
    src: str
    dst: str
    callback_id: str | None = None
    coalesced_callback_ids: list[str] = field(default_factory=list)
    error: str | None = None
    deferred: bool = False
    copied: bool = False
    published: bool = False
    source_missing: bool = False
    size: int | None = None


@dataclass
class DataItemDeleteFile:
    """Data item to be processed by the worker for deleting files."""

    cmd: Literal["delete_file"]
    src: str
    callback_id: str | None = None
    coalesced_callback_ids: list[str] = field(default_factory=list)
    error: str | None = None
    deferred: bool = False


FileOperation: TypeAlias = DataItemCopyFile | DataItemDeleteFile | DataItemMoveFile
FileOperationKey: TypeAlias = tuple[str, str, str | None, str | None]


def file_operation_key(item: FileOperation) -> FileOperationKey:
    """Return queue dedupe key for a file operation."""
    return (
        item.cmd,
        item.src,
        item.dst if isinstance(item, (DataItemCopyFile, DataItemMoveFile)) else None,
        item.callback_id if isinstance(item, DataItemCopyFile) else None,
    )


class DedupCheckQueue:
    """FIFO queue for check_tier jobs that dedupes by ``throttle_key``.

    Drop-in replacement for ``queue.Queue`` on the check_tier dispatch path:
    same ``.put(item)`` / ``.get(timeout=...)`` signature (``queue.Empty`` is
    raised on timeout), so ``dispatcher_task`` and ``worker_task_mixed`` need no
    other change.

    A check_tier job reads the *current* database and filesystem state, so when
    several jobs for the same ``throttle_key`` are queued only the most recent
    one carries useful information. When the workers are saturated by
    ``file_queue`` (move_file/delete_file take ~100 ms while a tier check can
    take tens of seconds), a plain unbounded ``Queue`` lets pending ``DataItem``
    instances accumulate without limit and the subprocess RSS grows unbounded.
    Deduping on insert bounds the queue to ``N_cameras * N_throttle_keys``
    regardless of how fast jobs are enqueued.

    A superseded job still has a ``callback_id`` registered in the parent's
    ``_callbacks`` dict, so it is returned on ``output_queue`` with ``data``
    cleared instead of being silently discarded. Without that acknowledgement
    the parent would never pop the entry and ``_callbacks`` would grow until it
    hit MAX_PENDING_CALLBACKS. ``data=None`` makes the reply a no-op for
    ``on_check_tier_result``, which returns early on a ``None`` payload.

    File operations use a separate queue and share results across pending duplicates.
    """

    def __init__(
        self,
        output_queue: Queue[DataItem | DataItemDeleteFile | DataItemMoveFile],
    ) -> None:
        self._items: OrderedDict[str, DataItem] = OrderedDict()
        self._cond = threading.Condition()
        self._output_queue = output_queue

    def put(self, item: DataItem) -> None:
        """Insert a job, replacing any queued job with the same throttle_key."""
        with self._cond:
            # Drop the stale entry first so the replacement is appended at the
            # tail and processed in arrival order relative to other keys.
            superseded = self._items.pop(item.throttle_key, None)
            self._items[item.throttle_key] = item
            self._cond.notify()

        # Acknowledge the superseded job outside the lock so a full output queue
        # cannot block new insertions. Only jobs that actually registered a
        # callback need a reply.
        if superseded is not None and superseded.callback_id is not None:
            superseded.data = None
            self._output_queue.put(superseded)

    def get(self, timeout: float | None = None) -> DataItem:
        """Pop the oldest job, blocking up to ``timeout`` seconds.

        Raises ``queue.Empty`` if no job becomes available within ``timeout``.
        """
        with self._cond:
            end_time = None if timeout is None else time.monotonic() + timeout
            while not self._items:
                if end_time is None:
                    self._cond.wait()
                else:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        raise Empty
                    self._cond.wait(remaining)
            _, item = self._items.popitem(last=False)
            return item

    def qsize(self) -> int:
        """Return the number of queued jobs."""
        with self._cond:
            return len(self._items)


class DedupFileQueue(Generic[T, K]):
    """FIFO queue that dedupes queued items by caller-provided key."""

    def __init__(self, key_func: Callable[[T], K]) -> None:
        self._key_func = key_func
        self._items: OrderedDict[K, T] = OrderedDict()
        self._cond = threading.Condition()

    def put(self, item: T) -> None:
        """Coalesce identical pending work and retain each completion."""
        with self._cond:
            key = self._key_func(item)
            existing = self._items.get(key)
            if existing is not None:
                if item.callback_id is not None:
                    existing.coalesced_callback_ids.append(item.callback_id)
                return
            self._items[key] = item
            self._cond.notify()

    def get(self, timeout: float | None = None) -> T:
        """Pop the oldest item, blocking up to timeout seconds."""
        with self._cond:
            end_time = None if timeout is None else time.monotonic() + timeout
            while not self._items:
                if end_time is None:
                    self._cond.wait()
                else:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        raise Empty
                    self._cond.wait(remaining)
            _, item = self._items.popitem(last=False)
            return item

    def get_nowait(self) -> T:
        """Pop the oldest item without blocking."""
        with self._cond:
            if not self._items:
                raise Empty
            _, item = self._items.popitem(last=False)
            return item

    def qsize(self) -> int:
        """Return the number of queued items."""
        with self._cond:
            return len(self._items)


class TierCheckWorker(SubProcessWorker):
    """Check tiers in a separate subprocess."""

    def __init__(self, vis: Viseron, cpulimit: int | None, workers: int) -> None:
        self._cpulimit = cpulimit
        self._workers = workers
        self._callbacks: dict[
            str,
            tuple[
                DataItem | FileOperation,
                Callable[[DataItem | FileOperation], None] | None,
            ],
        ] = {}
        self._callbacks_lock = threading.Lock()
        # Monotonic callback ids. str(id(callback)) is unsafe here: bound
        # methods like self.on_check_tier_result are created lazily on each
        # attribute access, so CPython may reuse a freed method's address and
        # two handlers could collide on the same id, routing a reply to the
        # wrong callback. itertools.count.__next__ is atomic under the GIL.
        self._next_callback_id = itertools.count(1)
        super().__init__(vis, f"{__name__}.tier_check_worker", qsize=0)

    def spawn_subprocess(self) -> RestartablePopen:
        """Spawn subprocess."""
        return RestartablePopen(
            (
                "python3 -u viseron/components/storage/storage_subprocess.py "
                f"--manager-port {self._server_port} "
                f"--manager-authkey {self._authkey_store.authkey} "
                f"--cpulimit {self._cpulimit} "
                f"--workers {self._workers} "
                f"--loglevel DEBUG"
            ).split(" "),
            name=self.subprocess_name,
            stdout=self._log_pipe,
            stderr=self._log_pipe,
            before_restart=self.requeue_pending,
        )

    def forward_input(self, item: DataItem | FileOperation) -> None:
        """Forward only requests that still belong to the current child."""
        with self._callbacks_lock:
            if item.callback_id in self._callbacks:
                self._process_queue.put_nowait(item)

    def requeue_pending(self) -> None:
        """Replay unanswered commands after the old process has exited."""
        with self._callbacks_lock:
            for queue in (self.input_queue, self._process_queue):
                while True:
                    try:
                        queue.get_nowait()
                    except Empty:
                        break
            # A manager get() issued by the dead child can still be waiting.
            # Only the replacement child should be able to consume the replay.
            self._process_queue = Queue()
            self._server.process_queue = self._process_queue
            pending = self._callbacks
            self._callbacks = {}
            for item, callback in pending.values():
                callback_id = str(next(self._next_callback_id))
                request = replace(item, callback_id=callback_id)
                self._callbacks[callback_id] = (request, callback)
                self._process_queue.put_nowait(request)

    def send_command(
        self,
        item: DataItem | FileOperation,
        callback: Callable[[DataItem | FileOperation], None] | None,
    ) -> None:
        """Send command to the subprocess."""
        with self._callbacks_lock:
            if len(self._callbacks) < MAX_PENDING_CALLBACKS:
                item.callback_id = str(next(self._next_callback_id))
                request = replace(item)
                self._callbacks[item.callback_id] = (request, callback)
                self.input_queue.put_nowait(request)
                return
        LOGGER.warning("Storage worker at capacity, deferring %s", item.cmd)
        item.error = "Storage worker at capacity; retry on the next tier check"
        item.deferred = True
        if callback is not None:
            callback(item)

    def work_output(self, item: DataItem | FileOperation) -> None:
        """Release admission and report a result to every requester."""
        callback_ids = [item.callback_id]
        if not isinstance(item, DataItem):
            callback_ids.extend(item.coalesced_callback_ids)
        for callback_id in callback_ids:
            if callback_id is None:
                continue
            with self._callbacks_lock:
                pending = self._callbacks.pop(callback_id, None)
            if pending is not None and (callback := pending[1]) is not None:
                try:
                    callback(replace(item, callback_id=callback_id))
                except Exception:
                    LOGGER.exception("Storage callback failed for %s", item.cmd)


def setup_logger(loglevel: str) -> None:
    """Log to stdout without any formatting.

    Viserons main log formatter takes care of the format in the main process.
    """
    root = logging.getLogger()
    root.setLevel(loglevel)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(loglevel)
    formatter = logging.Formatter("%(levelname)s %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)


def get_parser() -> argparse.ArgumentParser:
    """Get parser for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--manager-port", help="Port for the Manager", required=True)
    parser.add_argument(
        "--manager-authkey", help="Password for the Manager", required=True
    )

    parser.add_argument(
        "--cpulimit",
        help="CPU limit for the subprocess",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--workers",
        help="Number of worker threads",
        type=int,
        default=4,
    )
    parser.add_argument(
        "--loglevel",
        help="Loglevel",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser


def initializer(cpulimit: int | None) -> None:
    """Initialize engine in process."""
    # Limit CPU by spawning cpulimit
    pid = mp.current_process().pid
    if pid:
        ps = psutil.Process(pid)
        ps.nice(20)
    if pid and cpulimit is not None:
        command = ["cpulimit", "-l", str(cpulimit), "-p", str(pid), "-z", "-q"]
        LOGGER.debug(f"Running command: {command}")
        RestartablePopen(
            command,
            register=False,
        )
    # Set process name
    setproctitle.setproctitle(f"viseron_storage_subprocess_{pid}")


def worker_task_files(
    worker: Worker,
    file_queue: DedupFileQueue[FileOperation, FileOperationKey],
    output_queue: Queue[FileOperation],
) -> None:
    """Worker thread that only processes file operation commands."""
    while True:
        try:
            job = file_queue.get(timeout=1)
            worker.work_input(job)
            output_queue.put(job)
        except Empty:
            continue
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Error in file worker thread")


def worker_task_mixed(
    worker: Worker,
    check_queue: DedupCheckQueue,
    file_queue: DedupFileQueue[FileOperation, FileOperationKey],
    output_queue: Queue[DataItem | FileOperation],
    name: str,
) -> None:
    """Worker thread that prioritizes file operations but also handles check_tier.

    This ensures that file operations are not blocked by slow check_tier jobs.
    """
    job: DataItem | FileOperation
    while True:
        try:
            try:
                job = file_queue.get_nowait()
            except Empty:
                job = check_queue.get(timeout=1)
            worker.work_input(job)
            output_queue.put(job)
        except Empty:
            continue
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception(f"Error in mixed worker thread {name}")


def dispatcher_task(
    process_queue: Queue[DataItem | FileOperation],
    check_queue: DedupCheckQueue,
    file_queue: DedupFileQueue[FileOperation, FileOperationKey],
) -> None:
    """Dispatcher thread routing jobs to dedicated queues.

    check_tier commands can be slow. File operations should not be blocked by them,
    so they get their own queue and worker.
    """
    while True:
        try:
            job = process_queue.get(timeout=1)
        except Empty:
            continue

        try:
            if job.cmd == "check_tier":
                check_queue.put(job)
            elif job.cmd in ("copy_file", "move_file", "delete_file"):
                file_queue.put(job)
            else:
                LOGGER.debug("Unknown command %s", job.cmd)
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Dispatcher error routing job")


def main() -> None:
    """Run tier check in a subprocess."""
    parser = get_parser()
    args = parser.parse_args()
    setup_logger(args.loglevel)
    process_queue: Queue[DataItem | FileOperation]
    output_queue: Queue[DataItem | FileOperation]
    process_queue, output_queue = connect(
        "127.0.0.1", int(args.manager_port), args.manager_authkey
    )

    initializer(
        cpulimit=args.cpulimit,
    )

    worker = Worker()
    # Run scheduler and watchdog in the subprocess since the Viseron main process
    # watchdog is not available in the subprocess.
    logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)
    logging.getLogger("apscheduler.executors").setLevel(logging.ERROR)
    background_scheduler = BackgroundScheduler(timezone="UTC", daemon=True)
    background_scheduler.start()
    ThreadWatchDog(background_scheduler)

    LOGGER.debug(f"Starting {args.workers} worker threads")

    check_queue = DedupCheckQueue(output_queue)
    file_queue = DedupFileQueue[FileOperation, FileOperationKey](file_operation_key)

    dispatcher = RestartableThread(
        name="storage_subprocess.dispatcher",
        target=dispatcher_task,
        args=(process_queue, check_queue, file_queue),
        daemon=True,
    )
    dispatcher.start()

    for i in range(args.workers):
        thread = RestartableThread(
            name=f"storage_subprocess.mixed_worker.{i}",
            target=worker_task_mixed,
            args=(
                worker,
                check_queue,
                file_queue,
                output_queue,
                f"mixed_worker.{i}",
            ),
            daemon=True,
        )
        thread.start()

    thread = RestartableThread(
        name="storage_subprocess.file_worker",
        target=worker_task_files,
        args=(worker, file_queue, output_queue),
        daemon=True,
    )
    thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.debug("Storage tier check subprocess interrupted by user")
        sys.exit(0)
