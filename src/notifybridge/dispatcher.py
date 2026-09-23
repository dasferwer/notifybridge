import asyncio
import logging
import time
from pathlib import Path

import aio_pika
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from notifybridge.config import Settings
from notifybridge.models import Outbox
from notifybridge.worker import QUEUE

log = logging.getLogger("notifybridge.dispatcher")


async def publish_once(sessions, exchange):
    async with sessions.begin() as db:
        item = await db.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if item is None:
            return False
        # Отмечаем публикацию только после подтверждения брокера. Повторный сигнал безопасен.
        await exchange.publish(
            aio_pika.Message(
                body=str(item.id).encode(),
                message_id=str(item.id),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=QUEUE,
            mandatory=True,
            timeout=3,
        )
        item.published_at = await db.scalar(select(func.clock_timestamp()))
        return True


async def main():
    settings = Settings()
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        while True:
            Path("/tmp/notifybridge-heartbeat").write_text(str(time.time()))
            try:
                connection = await aio_pika.connect(settings.rabbitmq_url, timeout=3)
                async with connection:
                    channel = await connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    await channel.declare_queue(QUEUE, durable=True)
                    while True:
                        Path("/tmp/notifybridge-heartbeat").write_text(str(time.time()))
                        if not await publish_once(sessions, channel.default_exchange):
                            await asyncio.sleep(settings.poll_seconds)
            except Exception as exc:
                log.warning("dispatcher_error=%s", type(exc).__name__)
                await asyncio.sleep(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
