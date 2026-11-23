"""Exercise bounded promotion and emergency eviction through the storage worker."""

import datetime
from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from viseron.components.storage.config import RecorderTier
from viseron.components.storage.check_tier import Worker
from viseron.components.storage.files import upsert_file_at_location
from viseron.components.storage.models import Recordings
from viseron.components.storage.storage_subprocess import (
    DataItem,
    DataItemDeleteFile,
    DataItemMoveFile,
)
from viseron.components.storage.recording_policy import RecordingAction
from viseron.components.storage.tier_handler import SegmentsTierHandler
from viseron.const import VISERON_SIGNAL_STOPPING
from tests.common import MockCamera


def check_item(root, destination):
    """Build a forwarding tier request without enabling normal expiry."""
    item = DataItem(
        cmd="check_tier",
        camera_identifier="camera",
        tier_id=0,
        category="recorder",
        subcategories=["segments"],
        throttle_period=datetime.timedelta(0),
        max_bytes=0,
        min_bytes=0,
        min_age=datetime.timedelta(0),
        max_age=datetime.timedelta(0),
        drain=False,
        files_enabled=False,
    )
    item.size_path = str(root)
    item.promote_after = 0
    item.promotion_cutoff = None
    item.promotion_targets = {
        "continuous": (1, str(destination)),
        "events": (1, str(destination)),
    }
    item.on_full = "preserve_events"
    item.destination_failed = False
    item.requested_paths = []
    return item


def add_segment(get_session, root, name, age=120):
    """Publish one closed segment using the storage metadata interface."""
    path = root / "segments" / "camera" / f"{name}.m4s"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"complete recorded footage")
    upsert_file_at_location(
        get_session,
        "camera",
        "recorder",
        "segments",
        0,
        str(root),
        str(path),
        orig_ctime=datetime.datetime.now(datetime.UTC)
        - datetime.timedelta(seconds=age),
        duration=5,
    )
    return path


def set_space(monkeypatch, root, destination, *, pressure=False, offline=False):
    """Control capacity observations independently of candidate selection."""

    destination.mkdir(parents=True, exist_ok=True)

    def statvfs(path):
        if str(path).startswith(str(destination)) and offline:
            raise OSError("NAS offline")
        free = 0 if pressure and str(path).startswith(str(root)) else 1000000
        return SimpleNamespace(
            f_blocks=2000000, f_frsize=4096, f_bavail=free, f_favail=100, f_files=1000
        )

    monkeypatch.setattr("os.statvfs", statvfs)


def run_check(get_session, item):
    """Drive the worker input interface against the test database."""
    worker = Worker()
    worker._get_session = get_session
    worker.work_input(item)
    assert item.error is None, item.error
    return item.recording_actions


def test_normal_promotion_waits_for_batch(get_db_session, tmp_path, monkeypatch):
    """Safe footage stays local until the configured batch opens."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    path = add_segment(get_db_session, ram, "old")
    set_space(monkeypatch, ram, nas)
    item = check_item(ram, nas)
    assert run_check(get_db_session, item) == []
    assert path.exists()
    item.promotion_cutoff = datetime.datetime.now(datetime.UTC)
    actions = run_check(get_db_session, item)
    assert [(a.operation, a.src) for a in actions] == [("move", str(path))]
    assert path.exists(), "Selection cannot delete an unverified source"


def test_batch_has_a_fixed_cutoff(get_db_session, tmp_path, monkeypatch):
    """Later safe arrivals cannot extend an already open batch."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    old = add_segment(get_db_session, ram, "old", age=180)
    new = add_segment(get_db_session, ram, "new", age=60)
    set_space(monkeypatch, ram, nas)
    item = check_item(ram, nas)
    item.promotion_cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        seconds=60
    )
    assert [a.src for a in run_check(get_db_session, item)] == [str(old)]
    assert new.exists()


def test_ram_pressure_flushes_before_batch_deadline(
    get_db_session, tmp_path, monkeypatch
):
    """Space pressure promotes safe footage even during the quiet interval."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    path = add_segment(get_db_session, ram, "old")
    set_space(monkeypatch, ram, nas, pressure=True)
    item = check_item(ram, nas)
    assert [(a.operation, a.src) for a in run_check(get_db_session, item)] == [
        ("move", str(path))
    ]


@pytest.mark.parametrize(
    "policy,expected",
    [
        ("preserve_events", ["ordinary"]),
        ("ordinary_first", ["ordinary", "event"]),
        ("oldest_first", ["event", "ordinary"]),
    ],
)
def test_nas_outage_uses_configured_oldest_eviction(
    get_db_session, tmp_path, monkeypatch, policy, expected
):
    """Only the configured disposable footage is selected during an outage."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    event = add_segment(get_db_session, ram, "event", age=240)
    ordinary = add_segment(get_db_session, ram, "ordinary", age=120)
    active = add_segment(get_db_session, ram, "active", age=60)
    now = datetime.datetime.now(datetime.UTC)
    with get_db_session() as session:
        session.add_all(
            [
                Recordings(
                    camera_identifier="camera",
                    start_time=now - datetime.timedelta(seconds=241),
                    adjusted_start_time=now - datetime.timedelta(seconds=241),
                    end_time=now - datetime.timedelta(seconds=234),
                ),
                Recordings(
                    camera_identifier="camera",
                    start_time=now - datetime.timedelta(seconds=61),
                    adjusted_start_time=now - datetime.timedelta(seconds=61),
                    end_time=None,
                ),
            ]
        )
        session.commit()
    set_space(monkeypatch, ram, nas, pressure=True, offline=True)
    item = check_item(ram, nas)
    item.on_full = policy
    actions = run_check(get_db_session, item)
    assert [(a.operation, a.src) for a in actions] == [
        ("delete", str(ram / "segments" / "camera" / f"{name}.m4s"))
        for name in expected
    ]
    assert event.exists() and ordinary.exists() and active.exists()


def test_recent_and_requested_footage_remains_protected(
    get_db_session, tmp_path, monkeypatch
):
    """Playback requests and the current safety window cannot be reclaimed."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    requested = add_segment(get_db_session, ram, "requested")
    add_segment(get_db_session, ram, "recent", age=10)
    set_space(monkeypatch, ram, nas, pressure=True, offline=True)
    item = check_item(ram, nas)
    item.on_full = "oldest_first"
    item.requested_paths = [str(requested)]
    assert run_check(get_db_session, item) == []


def test_promotion_result_is_bounded(get_db_session, tmp_path, monkeypatch):
    """A large archive cannot produce an unbounded operation queue."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    for number in range(140):
        add_segment(get_db_session, ram, str(number), age=500 - number)
    set_space(monkeypatch, ram, nas, pressure=True)
    actions = run_check(get_db_session, check_item(ram, nas))
    assert 0 < len(actions) <= 128
    assert all(action.operation == "move" for action in actions)


def test_queued_deletion_rechecks_new_event(get_db_session, tmp_path, monkeypatch):
    """An event created after selection protects a queued pressure candidate."""

    ram, nas = tmp_path / "ram", tmp_path / "nas"
    path = add_segment(get_db_session, ram, "old")
    set_space(monkeypatch, ram, nas, pressure=True, offline=True)
    item = check_item(ram, nas)
    assert run_check(get_db_session, item)[0].operation == "delete"
    now = datetime.datetime.now(datetime.UTC)
    with get_db_session() as session:
        session.add(
            Recordings(
                camera_identifier="camera",
                start_time=now - datetime.timedelta(seconds=130),
                adjusted_start_time=now - datetime.timedelta(seconds=130),
                end_time=None,
            )
        )
        session.commit()
    worker = Worker()
    worker._get_session = get_db_session
    deletion = DataItemDeleteFile(
        cmd="delete_file",
        src=str(path),
        camera_identifier="camera",
        recording_policy="preserve_events",
    )
    worker.work_input(deletion)
    assert deletion.error is None
    assert deletion.deferred
    assert path.exists()


def test_queued_deletion_stops_when_space_recovers(
    get_db_session, tmp_path, monkeypatch
):
    """A previously queued deletion must not continue once pressure is gone."""

    ram, nas = tmp_path / "ram", tmp_path / "nas"
    path = add_segment(get_db_session, ram, "old")
    set_space(monkeypatch, ram, nas, pressure=True, offline=True)
    assert run_check(get_db_session, check_item(ram, nas))
    set_space(monkeypatch, ram, nas, pressure=False)
    worker = Worker()
    worker._get_session = get_db_session
    deletion = DataItemDeleteFile(
        cmd="delete_file",
        src=str(path),
        camera_identifier="camera",
        recording_policy="oldest_first",
    )
    worker.work_input(deletion)
    assert deletion.error is None
    assert deletion.deferred
    assert path.exists()


@pytest.mark.parametrize("value", [-1, True, 0.5, "60"])
def test_invalid_promotion_interval_is_rejected(value):
    """Batching uses explicit nonnegative integer seconds."""

    with pytest.raises(ValidationError):
        RecorderTier(path="/ram", promote_after=0, promotion_batch_interval=value)


def test_batch_requires_prompt_promotion():
    """A batching setting must not silently change a retention-only tier."""

    with pytest.raises(ValidationError, match="requires promote_after"):
        RecorderTier(path="/ram", promotion_batch_interval=60)


def test_policy_poll_does_not_starve_normal_retention():
    """Frequent capacity checks must leave the normal retention clock alone."""

    item = check_item("/ram", "/nas")
    item.on_full = None
    item.promote_after = None
    item.policy_only = True
    item.force = True
    item.throttle_period = datetime.timedelta(minutes=1)
    worker = Worker()
    observed = []
    worker._check_tier = observed.append
    worker.check_tier(item)
    normal = replace(item, policy_only=False, force=False)
    worker.check_tier(normal)
    assert observed == [normal]


def test_handler_passes_percentages_and_applies_verified_batch(
    get_db_session, tmp_path, vis
):
    """Actual tier handlers forward via verified worker results and stable file IDs."""

    ram, nas = tmp_path / "ram", tmp_path / "nas"
    nas.mkdir()
    path = add_segment(get_db_session, ram, "old")
    (path.parent / "init.mp4").write_bytes(b"matching init")
    camera = MockCamera(identifier="camera")
    storage = vis.data["storage"]
    storage.get_session = get_db_session
    storage.promotion_batches = {}
    storage.temporary_moving_files = set()
    storage.temporary_files_meta = {}
    storage.camera_requested_files_count = {"camera": SimpleNamespace(filenames=set())}
    storage.camera_tier_handlers = {"camera": {"recorder": []}}
    worker = Worker()
    worker._get_session = get_db_session

    held = []

    def send(operation, callback=None):
        worker.work_input(operation)
        assert operation.error is None, operation.error
        if isinstance(operation, DataItemMoveFile):
            held.append((operation, callback))
        elif callback:
            callback(operation)

    storage.tier_check_worker_send_command.side_effect = send
    try:
        first = SegmentsTierHandler(
            vis,
            camera,
            0,
            "recorder",
            "segments",
            RecorderTier(
                path=str(ram),
                promote_after=0,
                promotion_batch_interval=0,
                continuous={"max_size": {"percent": 80}},
                events={"max_size": {"percent": 20}},
            ),
            None,
        )
        second = SegmentsTierHandler(
            vis,
            camera,
            1,
            "recorder",
            "segments",
            RecorderTier(
                path=str(nas),
                continuous={"max_age": {"days": 1}},
                events={"max_age": {"days": 7}},
            ),
            None,
        )
        storage.camera_tier_handlers["camera"]["recorder"] = [
            {"segments": first},
            {"segments": second},
        ]
        item = first._create_dataitem(force=True)
        assert item.size_percentages == {"max_bytes": 80, "events_max_bytes": 20}
        item.policy_only = True
        worker.work_input(item)
        assert item.error is None
        assert [action.src for action in item.recording_actions] == [str(path)]
        first._handle_recording_result(item)
        assert path.exists(), "An uncommitted destination cannot release its source"
        pressure = first._create_dataitem(force=True)
        pressure.on_full = "oldest_first"
        pressure.recording_actions = [RecordingAction("delete", str(path), str(ram))]
        first._handle_recording_result(pressure)
        assert path.exists(), "Pressure must not delete a source awaiting publication"
        assert len(held) == 1
        operation, callback = held.pop()
        callback(operation)
        assert not path.exists()
        assert (
            nas / "segments" / "camera" / path.name
        ).read_bytes() == b"complete recorded footage"
        assert (
            nas / "segments" / "camera" / "init.mp4"
        ).read_bytes() == b"matching init"
    finally:
        for call in vis.register_signal_handler.call_args_list:
            if call.args[0] == VISERON_SIGNAL_STOPPING:
                call.args[1]()


@pytest.mark.parametrize("offline", [False, True])
def test_budget_crossing_never_authorizes_emergency_deletion(
    get_db_session, tmp_path, monkeypatch, offline
):
    """Budgets flush to healthy storage; outages preserve input with headroom."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    path = add_segment(get_db_session, ram, "old")
    set_space(monkeypatch, ram, nas, offline=offline)
    item = check_item(ram, nas)
    item.max_bytes = 1
    actions = run_check(get_db_session, item)
    assert [(a.operation, a.src) for a in actions] == (
        [] if offline else [("move", str(path))]
    )
    assert path.exists()


def test_pressure_overrides_cache_delay_but_not_preroll(
    get_db_session, tmp_path, monkeypatch
):
    """A long optional cache delay must not strand safe footage in full RAM."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    path = add_segment(get_db_session, ram, "old")
    set_space(monkeypatch, ram, nas, pressure=True)
    item = check_item(ram, nas)
    item.promote_after = 600
    assert [(a.operation, a.src) for a in run_check(get_db_session, item)] == [
        ("move", str(path))
    ]
    item.continuous_lookback_seconds = 600
    assert run_check(get_db_session, item) == []


def test_prompt_promotion_includes_closed_event_assets(
    get_db_session, tmp_path, monkeypatch
):
    """Moving footage early cannot strand a closed event's thumbnail and clip in RAM."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    nas.mkdir()
    now = datetime.datetime.now(datetime.UTC)
    assets = []
    for kind, name, age in (
        ("thumbnails", "1.jpg", 120),
        ("event_clips", "date/1.mp4", 60),
    ):
        path = ram / kind / "camera" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"closed event asset")
        published = upsert_file_at_location(
            get_db_session,
            "camera",
            "recorder",
            kind,
            0,
            str(ram),
            str(path),
            orig_ctime=now - datetime.timedelta(seconds=age),
        )
        assets.append((path, published.file_id))
    with get_db_session() as session:
        session.add(
            Recordings(
                camera_identifier="camera",
                start_time=now - datetime.timedelta(seconds=125),
                adjusted_start_time=now - datetime.timedelta(seconds=130),
                end_time=now - datetime.timedelta(seconds=100),
                thumbnail_path=str(assets[0][0]),
                thumbnail_file_id=assets[0][1],
                clip_path=str(assets[1][0]),
                clip_file_id=assets[1][1],
            )
        )
        session.commit()
    set_space(monkeypatch, ram, nas)
    item = check_item(ram, nas)
    item.promotion_cutoff = now
    result = run_check(get_db_session, item)
    assert [(action.src, action.subcategory) for action in result] == [
        (str(assets[0][0]), "thumbnails"),
        (str(assets[1][0]), "event_clips"),
    ]
