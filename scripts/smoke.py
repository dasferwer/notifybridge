"""Проверяет доставку по трём каналам, повтор события и объединение дайджеста."""

import asyncio
import json
import os
import time
from uuid import uuid4

import httpx


async def wait_sent(client, event_id, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = await client.get(f"/v1/events/{event_id}")
        response.raise_for_status()
        data = response.json()
        if data["deliveries"] and all(d["status"] == "sent" for d in data["deliveries"]):
            return data
        if any(d["status"] == "dead" for d in data["deliveries"]):
            raise AssertionError(data)
        await asyncio.sleep(0.2)
    raise AssertionError(f"Доставка {event_id} не завершилась за {timeout} секунд")


async def main():
    headers = {"Authorization": "Bearer " + os.environ["ADMIN_TOKEN"]}
    async with httpx.AsyncClient(base_url="http://api:8000", headers=headers, timeout=10) as client:
        response = await client.post(
            "/v1/recipients",
            json={
                "name": "Проверка каналов",
                "channels": {
                    channel: {"destination": "local-demo"}
                    for channel in ("email", "push", "webhook")
                },
            },
        )
        response.raise_for_status()
        payload = {
            "event_id": str(uuid4()),
            "recipient_id": response.json()["id"],
            "subject": "Проверка",
            "body": "Тестовое уведомление",
            "channels": ["email", "push", "webhook"],
        }
        first = await client.post("/v1/events", json=payload)
        assert first.status_code == 202
        duplicates = await asyncio.gather(
            *(client.post("/v1/events", json=payload) for _ in range(10))
        )
        assert all(r.status_code == 200 and r.json() == first.json() for r in duplicates)
        delivered = await wait_sent(client, payload["event_id"])
        assert len(delivered["deliveries"]) == 3
        response = await client.post(
            "/v1/recipients",
            json={
                "name": "Проверка дайджеста",
                "timezone": "UTC",
                "channels": {
                    "email": {"destination": "digest-demo", "mode": "digest", "digest_seconds": 10}
                },
            },
        )
        response.raise_for_status()
        user = response.json()["id"]
        # Оставляем запас до границы окна, чтобы проверять один дайджест, а не два соседних.
        remaining = 10 - time.time() % 10
        if remaining < 2:
            await asyncio.sleep(remaining + 0.1)
        events = [
            {
                "event_id": str(uuid4()),
                "recipient_id": user,
                "subject": f"Событие {i}",
                "body": "Запись дайджеста",
            }
            for i in range(5)
        ]
        results = await asyncio.gather(*(client.post("/v1/events", json=body) for body in events))
        assert all(r.status_code == 202 for r in results)
        ids = {r.json()["delivery_ids"][0] for r in results}
        assert len(ids) == 1
        await wait_sent(client, events[0]["event_id"])
    async with httpx.AsyncClient(base_url="http://provider:8000", headers=headers) as provider:
        receipts = (await provider.get("/admin/receipts")).json()
        digest = [r for r in receipts if r["idempotency_key"] in ids]
        assert len(digest) == 1 and len(digest[0]["envelope"]["event_ids"]) == 5
        receipt_keys = {d["id"] for d in delivered["deliveries"]}
        assert len([r for r in receipts if r["idempotency_key"] in receipt_keys]) == 3
    print(
        json.dumps(
            {
                "channels": 3,
                "duplicate_events": 10,
                "provider_receipts": 3,
                "digest_events": 5,
                "digest_receipts": 1,
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
