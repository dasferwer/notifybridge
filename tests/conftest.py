# ruff: noqa: E402
from safety import ensure_test_environment

# До импорта модулей с engine/settings проверяем все ресурсы тестового профиля.
ensure_test_environment()

import os
import subprocess
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from notifybridge.app import create_app
from notifybridge.config import Settings
from notifybridge.models import Recipient
from notifybridge.provider import create_app as create_provider
from notifybridge.schemas import EventInput, RecipientInput
from notifybridge.service import ingest


@pytest.fixture(scope="session", autouse=True)
def migrate():
    subprocess.run(["alembic", "upgrade", "head"], check=True)


@pytest.fixture
def settings():
    return Settings(provider_timeout=0.15, lease_seconds=2, retry_base=0.1)


@pytest_asyncio.fixture
async def sessions():
    engine = create_async_engine(os.environ["DATABASE_URL"])
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "TRUNCATE recipients, events, deliveries, outbox, attempts, provider_receipts, provider_modes CASCADE"
            )
        )
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@asynccontextmanager
async def application_client(app):
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


@pytest_asyncio.fixture
async def api(sessions, settings):
    async with application_client(create_app(settings)) as client:
        yield client, {"Authorization": "Bearer " + settings.admin_token.get_secret_value()}


@pytest_asyncio.fixture
async def provider(sessions, settings):
    async with application_client(create_provider(settings)) as client:
        yield client


async def recipient(sessions, **changes):
    data = {
        "name": "Получатель",
        "timezone": "UTC",
        "channels": {"email": {"destination": "person@example.test"}},
    }
    data.update(changes)
    async with sessions.begin() as db:
        row = Recipient(config=RecipientInput.model_validate(data).model_dump(mode="json"))
        db.add(row)
        await db.flush()
        return row.id


async def event(sessions, recipient_id, **changes):
    data = {"event_id": uuid4(), "recipient_id": recipient_id, "subject": "Тема", "body": "Текст"}
    data.update(changes)
    request = EventInput.model_validate(data)
    async with sessions.begin() as db:
        row, created = await ingest(db, request)
        return row, created
