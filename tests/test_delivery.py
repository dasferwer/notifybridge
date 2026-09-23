import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from conftest import event, recipient
from sqlalchemy import func, select, update

from notifybridge.models import Attempt, Delivery, Event, Outbox, Receipt
from notifybridge.service import Conflict, claim, finish, retry_dead
from notifybridge.worker import deliver_once


async def expire(sessions, job):
    async with sessions.begin() as db:
        await db.execute(
            update(Delivery)
            .where(Delivery.id == job["id"])
            .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )


async def ready(sessions):
    async with sessions.begin() as db:
        await db.execute(
            update(Delivery)
            .where(Delivery.status == "pending")
            .values(due_at=datetime.now(UTC) - timedelta(seconds=1))
        )


async def test_concurrent_event_deduplication(sessions):
    user = await recipient(sessions)
    key = uuid4()
    results = await asyncio.gather(*(event(sessions, user, event_id=key) for _ in range(25)))
    assert sum(created for _, created in results) == 1
    async with sessions() as db:
        assert await db.scalar(select(func.count()).select_from(Event)) == 1
        assert await db.scalar(select(func.count()).select_from(Delivery)) == 1
        assert await db.scalar(select(func.count()).select_from(Outbox)) == 1
    with pytest.raises(Conflict):
        await event(sessions, user, event_id=key, body="Другой текст")


async def test_channel_fanout_and_disabled_preferences(sessions):
    user = await recipient(
        sessions,
        channels={
            "email": {"destination": "email"},
            "push": {"destination": "push"},
            "webhook": {"destination": "hook", "enabled": False},
        },
    )
    row, _ = await event(
        sessions, user, channels=["email", "push", "webhook"], priority="transactional"
    )
    assert len(row.delivery_ids) == 2
    assert row.suppressed == ["webhook"]


async def test_all_suppressed_has_no_outbox(sessions):
    user = await recipient(sessions, channels={"email": {"destination": "email", "enabled": False}})
    row, _ = await event(sessions, user)
    assert row.delivery_ids == [] and row.suppressed == ["email"]
    async with sessions() as db:
        assert await db.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_digest_coalesces_and_seals(sessions, settings):
    user = await recipient(
        sessions,
        channels={"email": {"destination": "email", "mode": "digest", "digest_seconds": 86400}},
    )
    rows = await asyncio.gather(*(event(sessions, user) for _ in range(20)))
    assert len({row.delivery_ids[0] for row, _ in rows}) == 1
    assert await claim(sessions, settings) is None
    await ready(sessions)
    first = await claim(sessions, settings)
    assert len(first["envelope"]["event_ids"]) == 20
    later, _ = await event(sessions, user)
    assert later.delivery_ids[0] != str(first["id"])
    await expire(sessions, first)
    second = await claim(sessions, settings)
    assert second["envelope"] == first["envelope"]
    assert second["id"] == first["id"]


async def test_digest_chunks_at_100(sessions):
    user = await recipient(
        sessions,
        channels={"email": {"destination": "email", "mode": "digest", "digest_seconds": 86400}},
    )
    for _ in range(101):
        await event(sessions, user)
    async with sessions() as db:
        rows = (await db.scalars(select(Delivery))).all()
        assert sorted(len(d.items) for d in rows) == [1, 100]


async def test_quiet_hours_and_transactional_bypass(sessions, settings):
    now = datetime.now(UTC)
    start, end = (
        (now - timedelta(hours=1)).strftime("%H:%M"),
        (now + timedelta(hours=1)).strftime("%H:%M"),
    )
    user = await recipient(
        sessions,
        quiet_start=start,
        quiet_end=end,
        channels={"email": {"destination": "email", "mode": "digest"}},
    )
    normal, _ = await event(sessions, user)
    urgent, _ = await event(sessions, user, priority="transactional")
    job = await claim(sessions, settings)
    assert str(job["id"]) == urgent.delivery_ids[0]
    assert len(job["envelope"]["event_ids"]) == 1
    assert await claim(sessions, settings) is None
    async with sessions() as db:
        assert (await db.get(Delivery, UUID(normal.delivery_ids[0]))).due_at > now


async def test_claim_rechecks_quiet_hours(sessions, settings):
    now = datetime.now(UTC)
    user = await recipient(
        sessions,
        quiet_start=(now - timedelta(hours=1)).strftime("%H:%M"),
        quiet_end=(now + timedelta(hours=1)).strftime("%H:%M"),
    )
    await event(sessions, user)
    await ready(sessions)
    assert await claim(sessions, settings) is None


async def test_competing_workers_and_stale_fencing(sessions, settings):
    user = await recipient(sessions)
    await event(sessions, user)
    jobs = await asyncio.gather(*(claim(sessions, settings) for _ in range(15)))
    claimed = [job for job in jobs if job]
    assert len(claimed) == 1
    first = claimed[0]
    await expire(sessions, first)
    second = await claim(sessions, settings)
    assert second["token"] != first["token"]
    assert await finish(sessions, settings, first, receipt="stale") is False
    assert await finish(sessions, settings, second, receipt="current") is True


async def test_priority_lane_and_order(sessions, settings):
    user = await recipient(sessions)
    normal, _ = await event(sessions, user)
    urgent, _ = await event(sessions, user, priority="transactional")
    settings.worker_lane = "transactional"
    assert str((await claim(sessions, settings))["id"]) == urgent.delivery_ids[0]
    assert await claim(sessions, settings) is None
    settings.worker_lane = "all"
    assert str((await claim(sessions, settings))["id"]) == normal.delivery_ids[0]


async def test_provider_timeout_after_acceptance_is_idempotent(sessions, settings, provider):
    user = await recipient(sessions)
    await event(sessions, user)
    headers = {"Authorization": "Bearer " + settings.admin_token.get_secret_value()}
    await provider.put("/admin/modes/email", headers=headers, json={"mode": "accept_then_timeout"})
    assert await deliver_once(sessions, settings, provider)
    await ready(sessions)
    assert await deliver_once(sessions, settings, provider)
    async with sessions() as db:
        delivery = await db.scalar(select(Delivery))
        assert delivery.status == "sent" and delivery.attempts == 2
        assert await db.scalar(select(func.count()).select_from(Receipt)) == 1
        assert await db.scalar(select(func.count()).select_from(Attempt)) == 2


async def test_worker_crash_after_provider_accepts(sessions, settings, provider):
    user = await recipient(sessions)
    await event(sessions, user)
    job = await claim(sessions, settings)
    response = await provider.post(
        "/send/email",
        headers={
            "Authorization": "Bearer " + settings.admin_token.get_secret_value(),
            "Idempotency-Key": str(job["id"]),
        },
        json=job["envelope"],
    )
    assert response.status_code == 200
    await expire(sessions, job)
    assert await deliver_once(sessions, settings, provider)
    async with sessions() as db:
        assert (await db.get(Delivery, job["id"])).receipt_id == response.json()["receipt_id"]
        assert await db.scalar(select(func.count()).select_from(Receipt)) == 1


@pytest.mark.parametrize("code,permanent", [(422, True), (503, False), (429, False), (302, False)])
async def test_retry_and_dead_letter(sessions, settings, code, permanent):
    user = await recipient(sessions)
    await event(sessions, user)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(code))
    ) as client:
        await deliver_once(sessions, settings, client)
    async with sessions() as db:
        row = await db.scalar(select(Delivery))
        assert row.status == ("dead" if permanent else "pending")
        assert row.attempts == 1
        if not permanent:
            assert row.due_at > datetime.now(UTC)


async def test_exhaustion_and_manual_retry_preserve_payload(sessions, settings):
    settings.max_attempts = 1
    user = await recipient(sessions)
    await event(sessions, user)
    job = await claim(sessions, settings)
    await finish(sessions, settings, job, error="provider_unavailable")
    async with sessions.begin() as db:
        assert await retry_dead(db, job["id"])
        assert not await retry_dead(db, job["id"])
    retried = await claim(sessions, settings)
    assert retried["id"] == job["id"] and retried["envelope"] == job["envelope"]


async def test_crash_attempt_limit(sessions, settings):
    settings.max_attempts = 1
    user = await recipient(sessions)
    await event(sessions, user)
    job = await claim(sessions, settings)
    await expire(sessions, job)
    assert await claim(sessions, settings) is None
    async with sessions() as db:
        assert (await db.get(Delivery, job["id"])).status == "dead"
