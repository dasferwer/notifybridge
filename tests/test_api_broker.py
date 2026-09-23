import asyncio
import contextlib
from uuid import uuid4

import aio_pika
from conftest import event, recipient
from sqlalchemy import func, select

from notifybridge.dispatcher import publish_once
from notifybridge.models import Outbox
from notifybridge.worker import QUEUE, listen


async def test_api_auth_validation_and_event_replay(api):
    client, headers = api
    assert (await client.post("/v1/events", json={})).status_code == 401
    assert (await client.get("/metrics")).status_code == 401
    config = {
        "name": "Тест",
        "timezone": "UTC",
        "channels": {"email": {"destination": "test@example.test"}},
    }
    user = (await client.post("/v1/recipients", headers=headers, json=config)).json()
    payload = {
        "event_id": str(uuid4()),
        "recipient_id": user["id"],
        "subject": "Тема",
        "body": "Текст",
    }
    first = await client.post("/v1/events", headers=headers, json=payload)
    assert first.status_code == 202
    second = await client.post("/v1/events", headers=headers, json=payload)
    assert second.status_code == 200 and second.json() == first.json()
    assert (
        await client.post("/v1/events", headers=headers, json={**payload, "body": "Другой"})
    ).status_code == 409
    status = await client.get(f"/v1/events/{payload['event_id']}", headers=headers)
    assert status.json()["deliveries"][0]["status"] == "pending"
    assert (await client.get("/health/ready")).status_code == 200
    assert (await client.get("/health/live")).status_code == 200
    assert (
        'notifybridge_deliveries{status="pending"} 1'
        in (await client.get("/metrics", headers=headers)).text
    )
    assert (await client.get(f"/v1/events/{uuid4()}", headers=headers)).status_code == 404
    assert (
        await client.post(f"/v1/deliveries/{uuid4()}/retry", headers=headers)
    ).status_code == 409
    assert (
        await client.post("/v1/recipients", headers=headers, json={**config, "timezone": "Invalid"})
    ).status_code == 422


async def test_preferences_version_splits_digest(api):
    client, headers = api
    config = {
        "name": "Тест",
        "channels": {"email": {"destination": "old", "mode": "digest", "digest_seconds": 86400}},
    }
    user = (await client.post("/v1/recipients", headers=headers, json=config)).json()
    payload = {"recipient_id": user["id"], "subject": "Тема", "body": "Текст"}
    first = (
        await client.post("/v1/events", headers=headers, json={**payload, "event_id": str(uuid4())})
    ).json()
    config["channels"]["email"]["destination"] = "new"
    changed = await client.put("/v1/recipients/" + user["id"], headers=headers, json=config)
    assert changed.json()["version"] == 2
    second = (
        await client.post("/v1/events", headers=headers, json={**payload, "event_id": str(uuid4())})
    ).json()
    assert first["delivery_ids"] != second["delivery_ids"]


async def test_provider_key_conflict_and_auth(provider, settings):
    headers = {
        "Authorization": "Bearer " + settings.admin_token.get_secret_value(),
        "Idempotency-Key": str(uuid4()),
    }
    body = {
        "channel": "email",
        "destination": "x",
        "subject": "x",
        "text": "x",
        "html": "x",
        "event_ids": [str(uuid4())],
    }
    assert (
        await provider.post(
            "/send/email", json=body, headers={"Idempotency-Key": headers["Idempotency-Key"]}
        )
    ).status_code == 401
    first = await provider.post("/send/email", json=body, headers=headers)
    second = await provider.post("/send/email", json=body, headers=headers)
    assert first.json()["receipt_id"] == second.json()["receipt_id"]
    assert second.json()["duplicate"] is True
    assert (
        await provider.post("/send/email", json={**body, "text": "changed"}, headers=headers)
    ).status_code == 409


async def test_real_broker_outbox_confirmation_and_duplicate_signals(sessions, settings):
    user = await recipient(sessions)
    await event(sessions, user)
    connection = await aio_pika.connect(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        queue = await channel.declare_queue(QUEUE, durable=True)
        await queue.purge()
        assert await publish_once(sessions, channel.default_exchange)
        assert not await publish_once(sessions, channel.default_exchange)
        message = await queue.get(timeout=2)
        await message.nack(requeue=True)
        duplicate = await queue.get(timeout=2)
        assert duplicate.redelivered
        await duplicate.ack()
        async with sessions() as db:
            assert (
                await db.scalar(
                    select(func.count()).select_from(Outbox).where(Outbox.published_at.is_not(None))
                )
                == 1
            )
        wake, stop = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(listen(settings, wake, stop))
        try:
            await channel.default_exchange.publish(
                aio_pika.Message(b"duplicate"), routing_key=QUEUE
            )
            await asyncio.wait_for(wake.wait(), timeout=3)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_failed_publish_keeps_outbox(sessions):
    user = await recipient(sessions)
    await event(sessions, user)

    class FailedExchange:
        async def publish(self, *args, **kwargs):
            raise ConnectionError("Соединение потеряно до подтверждения")

    with contextlib.suppress(ConnectionError):
        await publish_once(sessions, FailedExchange())
    async with sessions() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
            )
            == 1
        )
