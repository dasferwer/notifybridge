import asyncio
import contextlib
import logging
import time
from pathlib import Path

import aio_pika
import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from notifybridge.config import Settings
from notifybridge.service import claim, finish

log = logging.getLogger("notifybridge.worker")
QUEUE = "notifybridge.wakeup"


async def deliver_once(sessions, settings, client):
    job = await claim(sessions, settings)
    if not job:
        return False
    receipt, error, permanent = None, None, False
    try:
        # Таймаут запроса короче срока аренды. При повторе сохраняем прежний ключ и содержимое.
        async with asyncio.timeout(settings.provider_timeout):
            response = await client.post(
                str(settings.provider_url).rstrip("/") + "/send/" + job["envelope"]["channel"],
                headers={
                    "Idempotency-Key": str(job["id"]),
                    "Authorization": "Bearer " + settings.admin_token.get_secret_value(),
                },
                json=job["envelope"],
            )
        if response.status_code == 200:
            data = response.json()
            receipt = data.get("receipt_id")
            if not isinstance(receipt, str) or not 1 <= len(receipt) <= 80:
                receipt, error = None, "invalid_receipt"
        else:
            error = f"provider_http_{response.status_code}"
            permanent = 400 <= response.status_code < 500 and response.status_code not in (
                408,
                425,
                429,
            )
    except (httpx.HTTPError, TimeoutError):
        error = "provider_unavailable"
    except (ValueError, TypeError, AttributeError):
        error = "invalid_response"
    accepted = await finish(
        sessions, settings, job, receipt=receipt, error=error, permanent=permanent
    )
    log.info(
        "delivery=%s outcome=%s lease_valid=%s", job["id"], "sent" if receipt else error, accepted
    )
    return True


async def listen(settings, wake, stop):
    while not stop.is_set():
        try:
            connection = await aio_pika.connect(settings.rabbitmq_url, timeout=3)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                queue = await channel.declare_queue(QUEUE, durable=True)
                async with queue.iterator() as messages:
                    async for message in messages:
                        # Сообщение лишь будит воркер. Источник заданий — транзакционная таблица.
                        async with message.process():
                            wake.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("broker_listener_error=%s", type(exc).__name__)
            await asyncio.sleep(1)


async def main():
    settings = Settings()
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    wake, stop = asyncio.Event(), asyncio.Event()
    listener = asyncio.create_task(listen(settings, wake, stop))
    try:
        async with httpx.AsyncClient(
            timeout=settings.provider_timeout, follow_redirects=False, trust_env=False
        ) as client:
            while True:
                Path("/tmp/notifybridge-heartbeat").write_text(str(time.time()))
                try:
                    worked = await deliver_once(sessions, settings, client)
                except Exception as exc:
                    log.error("worker_error=%s", type(exc).__name__)
                    worked = False
                if not worked:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(wake.wait(), timeout=settings.poll_seconds)
                    wake.clear()
    finally:
        stop.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
