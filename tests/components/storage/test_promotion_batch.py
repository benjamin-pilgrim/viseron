"""Root batching follows verified completion and all participating cameras."""

from viseron.components.storage.promotion_batch import PromotionBatch


def test_root_quiet_interval_starts_after_last_verified_transfer():
    """Two cameras share a cutoff and cannot start the next batch too early."""
    clock = [0.0]
    batch = PromotionBatch(clock=lambda: clock[0])
    batch.register("front", 60)
    batch.register("driveway", 60)
    assert batch.request()[0] is None
    clock[0] = 60
    cutoff = batch.request()[0]
    assert cutoff is not None
    assert batch.reserve("front.m4s")
    assert batch.reserve("driveway.m4s")
    assert not batch.reserve("front.m4s")
    batch.checked("front", True)
    batch.checked("driveway", True)
    clock[0] = 70
    batch.finish("front.m4s", True)
    assert batch.request()[0] == cutoff
    clock[0] = 100
    batch.finish("driveway.m4s", True)
    assert batch.request()[0] is None
    clock[0] = 159
    assert batch.request()[0] is None
    clock[0] = 160
    assert batch.request()[0] is not None


def test_late_drain_check_does_not_restart_the_quiet_clock():
    """An empty follow-up check keeps the interval anchored to transfer finish."""
    clock = [0.0]
    batch = PromotionBatch(clock=lambda: clock[0])
    batch.register("camera", 60)
    clock[0] = 60
    assert batch.request()[0] is not None
    batch.reserve("segment")
    clock[0] = 70
    batch.finish("segment", True)
    clock[0] = 90
    batch.checked("camera", True)
    clock[0] = 129
    assert batch.request()[0] is None
    clock[0] = 130
    assert batch.request()[0] is not None


def test_each_camera_must_finish_the_fixed_batch():
    """One empty camera cannot close a batch still needed by its neighbour."""
    clock = [0.0]
    batch = PromotionBatch(clock=lambda: clock[0])
    batch.register("front", 60)
    batch.register("driveway", 60)
    clock[0] = 60
    cutoff = batch.request()[0]
    batch.checked("front", True)
    assert batch.request()[0] == cutoff
    batch.checked("driveway", False)
    assert batch.request()[0] == cutoff
    batch.checked("driveway", True)
    assert batch.request()[0] is None
