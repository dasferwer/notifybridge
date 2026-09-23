"""Проверяет сбои провайдера, аварийную остановку воркера и работу без RabbitMQ."""

import asyncio
import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import dotenv_values
from smoke import wait_sent

ROOT = Path(__file__).resolve().parents[1]


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)


async def main():
    headers = {"Authorization": "Bearer " + dotenv_values(ROOT / ".env")["ADMIN_TOKEN"]}
    async with (
        httpx.AsyncClient(base_url="http://127.0.0.1:8250", headers=headers, timeout=10) as api,
        httpx.AsyncClient(
            base_url="http://127.0.0.1:8251", headers=headers, timeout=10
        ) as provider,
    ):
        response = await api.post(
            "/v1/recipients",
            json={
                "name": "Проверка восстановления",
                "timezone": "UTC",
                "channels": {"email": {"destination": "recovery-demo"}},
            },
        )
        response.raise_for_status()
        user = response.json()["id"]

        async def mode(value):
            response = await provider.put("/admin/modes/email", json={"mode": value})
            response.raise_for_status()

        async def submit():
            body = {
                "event_id": str(uuid4()),
                "recipient_id": user,
                "subject": "Проверка сбоя",
                "body": "Локальная проверка",
            }
            response = await api.post("/v1/events", json=body)
            response.raise_for_status()
            return body["event_id"], response.json()["delivery_ids"][0]

        try:
            await mode("unavailable")
            first, _ = await submit()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = (await api.get(f"/v1/events/{first}")).json()["deliveries"][0]
                if state["attempts"] >= 1 and state["last_error"] == "provider_http_503":
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("Не зафиксирован отказ провайдера")
            await mode("healthy")
            await wait_sent(api, first)

            await mode("accept_then_timeout")
            second, delivery_id = await submit()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                receipts = (await provider.get("/admin/receipts")).json()
                if any(r["idempotency_key"] == delivery_id for r in receipts):
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("Провайдер не сохранил результат")
            state = (await api.get(f"/v1/events/{second}")).json()["deliveries"][0]
            assert state["status"] == "sending", state
            compose("kill", "-s", "SIGKILL", "worker")
            await mode("healthy")
            compose("start", "worker")
            recovered = await wait_sent(api, second)
            assert recovered["deliveries"][0]["attempts"] >= 2
            receipts = (await provider.get("/admin/receipts")).json()
            assert sum(r["idempotency_key"] == delivery_id for r in receipts) == 1

            compose("stop", "rabbitmq")
            third, _ = await submit()
            await wait_sent(api, third)
            metrics = (await api.get("/metrics")).text
            assert any(
                line.startswith("notifybridge_outbox_pending ") and float(line.split()[-1]) >= 1
                for line in metrics.splitlines()
            )
            compose("start", "rabbitmq")
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if "notifybridge_outbox_pending 0\n" in (await api.get("/metrics")).text:
                    break
                await asyncio.sleep(0.5)
            else:
                raise AssertionError("Outbox не опустел после восстановления RabbitMQ")
        finally:
            await mode("healthy")
            compose("start", "worker", "rabbitmq")
    print(
        json.dumps(
            {
                "provider_retry": "passed",
                "worker_killed_after_acceptance": "passed",
                "single_provider_receipt": True,
                "broker_outage_fallback": "passed",
                "outbox_recovered": True,
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
