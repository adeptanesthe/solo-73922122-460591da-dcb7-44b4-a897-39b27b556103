"""Governor tests: cross-connection pacing, cooldown and restart survival."""

from __future__ import annotations

import pytest

from app.governor import OriginGovernor


ORIGIN = "http://demo-a:8001"


@pytest.fixture()
def db(tmp_path):
    return str(tmp_path / "gov.db")


def test_second_slot_is_refused_within_interval(db):
    gov = OriginGovernor(db, owner="w1")
    slot = gov.try_acquire(ORIGIN, "token-1", now=100.0)
    assert slot is not None
    # Still leased: a second process may not start a request.
    assert gov.try_acquire(ORIGIN, "token-2", now=100.2) is None
    gov.release(slot, now=100.5)
    # Lease released but the one-second spacing still applies.
    assert gov.try_acquire(ORIGIN, "token-3", now=100.9) is None
    slot2 = gov.try_acquire(ORIGIN, "token-4", now=101.05)
    assert slot2 is not None
    gov.release(slot2, now=101.1)


def test_dispatch_anchor_moves_spacing_forward(db):
    gov = OriginGovernor(db, owner="w1")
    slot = gov.try_acquire(ORIGIN, "t1", now=10.0)
    # The request actually leaves 0.4 s after the slot was claimed.
    assert gov.mark_dispatched(slot, now=10.4) is True
    gov.release(slot, now=10.5)
    assert gov.try_acquire(ORIGIN, "t2", now=11.2) is None
    assert gov.try_acquire(ORIGIN, "t3", now=11.45) is not None


def test_mark_dispatched_fails_after_lease_loss(db):
    gov = OriginGovernor(db, owner="w1", lease_seconds=1)
    slot = gov.try_acquire(ORIGIN, "t1", now=10.0)
    assert gov.try_acquire(ORIGIN, "t2", now=11.5) is not None  # takes over
    assert gov.mark_dispatched(slot, now=11.6) is False


def test_unsent_slot_does_not_delay_the_next_request(db):
    gov = OriginGovernor(db, owner="w1")
    slot = gov.try_acquire(ORIGIN, "t1", now=10.0)
    gov.release(slot, sent=False, now=10.05)
    assert gov.try_acquire(ORIGIN, "t2", now=10.06) is not None


def test_origins_are_independent(db):
    gov = OriginGovernor(db, owner="w1")
    a = gov.try_acquire("http://a:80", "ta", now=10.0)
    b = gov.try_acquire("http://b:80", "tb", now=10.0)
    assert a is not None and b is not None


def test_two_governor_instances_share_state(db):
    gov1 = OriginGovernor(db, owner="w1")
    gov2 = OriginGovernor(db, owner="w2")
    assert gov1.try_acquire(ORIGIN, "t1", now=20.0) is not None
    assert gov2.try_acquire(ORIGIN, "t2", now=20.5) is None
    # Another origin is unaffected.
    assert gov2.try_acquire("http://other:80", "t3", now=20.5) is not None


def test_cooldown_blocks_everyone_and_survives_release(db):
    gov1 = OriginGovernor(db, owner="w1")
    slot = gov1.try_acquire(ORIGIN, "t1", now=30.0)
    gov1.release(slot, now=30.1)
    deadline = gov1.start_cooldown(ORIGIN, 5, now=30.2)
    assert deadline == pytest.approx(35.2)
    # A brand-new governor (e.g. restarted process) sees the cooldown.
    gov3 = OriginGovernor(db, owner="w3")
    assert gov3.try_acquire(ORIGIN, "t2", now=33.0) is None
    assert gov3.cooldown_remaining(ORIGIN, now=33.0) == pytest.approx(2.2)
    assert gov3.try_acquire(ORIGIN, "t3", now=35.3) is not None


def test_cooldown_extends_but_never_shortens(db):
    gov = OriginGovernor(db, owner="w1")
    gov.start_cooldown(ORIGIN, 10, now=40.0)
    gov.start_cooldown(ORIGIN, 2, now=41.0)
    assert gov.state(ORIGIN).cooldown_until == pytest.approx(50.0)


def test_retry_after_requires_integer(db):
    gov = OriginGovernor(db, owner="w1")
    with pytest.raises(TypeError):
        gov.start_cooldown(ORIGIN, "5")  # type: ignore[arg-type]


def test_stale_lease_does_not_block_forever(db):
    gov = OriginGovernor(db, owner="w1", lease_seconds=5)
    assert gov.try_acquire(ORIGIN, "t1", now=50.0) is not None
    # Worker died without releasing; after the lease expires another worker
    # may proceed.
    assert gov.try_acquire(ORIGIN, "t2", now=54.0) is None
    slot = gov.try_acquire(ORIGIN, "t3", now=55.5)
    assert slot is not None


def test_readiness_reflects_lease_and_cooldown(db):
    gov = OriginGovernor(db, owner="w1")
    slot = gov.try_acquire(ORIGIN, "t1", now=60.0)
    assert gov.readiness(ORIGIN, now=60.1) >= 60.0
    gov.release(slot, now=60.2)
    gov.start_cooldown(ORIGIN, 7, now=60.3)
    assert gov.readiness(ORIGIN, now=60.4) == pytest.approx(67.3)
