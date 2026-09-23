"""Локальный эмулятор провайдера. Не отправляет сообщения во внешние системы."""

import asyncio
import secrets
from contextlib import asynccontextmanager
from typing import Literal
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from notifybridge.config import Settings
from notifybridge.models import ProviderMode, Receipt
from notifybridge.schemas import Channel
from notifybridge.service import fingerprint


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel: Channel
    destination: str = Field(min_length=1, max_length=200)
    subject: str = Field(min_length=1, max_length=200)
    text: str = Field(max_length=500_000)
    html: str = Field(max_length=3_000_000)
    event_ids: list[UUID] = Field(min_length=1, max_length=100)


class ModeInput(BaseModel):
    mode: Literal["healthy", "unavailable", "accept_then_timeout", "reject"]


def create_app(settings=None):
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app):
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            yield
        finally:
            await engine.dispose()

    async def admin(authorization: str = Header(default="")):
        if not secrets.compare_digest(
            authorization.encode(), ("Bearer " + settings.admin_token.get_secret_value()).encode()
        ):
            raise HTTPException(401, "Неверный токен")

    app = FastAPI(title="NotifyBridge: эмулятор провайдера", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.put("/admin/modes/{channel}", dependencies=[Depends(admin)])
    async def mode(channel: Channel, body: ModeInput):
        async with app.state.sessions.begin() as db:
            await db.execute(
                insert(ProviderMode)
                .values(channel=channel, mode=body.mode)
                .on_conflict_do_update(
                    index_elements=[ProviderMode.channel], set_={"mode": body.mode}
                )
            )
        return {"channel": channel, "mode": body.mode}

    @app.get("/admin/receipts", dependencies=[Depends(admin)])
    async def receipts():
        async with app.state.sessions() as db:
            rows = (
                await db.scalars(select(Receipt).order_by(Receipt.created_at.desc()).limit(100))
            ).all()
            return [
                {"idempotency_key": r.key, "receipt_id": r.receipt_id, "envelope": r.envelope}
                for r in rows
            ]

    @app.post("/send/{channel}", dependencies=[Depends(admin)])
    async def send(channel: Channel, body: Envelope, idempotency_key: UUID = Header()):
        if channel != body.channel:
            raise HTTPException(422, "Канал в пути не совпадает с телом запроса")
        payload = body.model_dump(mode="json")
        digest = fingerprint(payload)
        inserted = None
        async with app.state.sessions.begin() as db:
            mode = await db.get(ProviderMode, channel)
            current_mode = mode.mode if mode else "healthy"
            if current_mode == "unavailable":
                return JSONResponse({"detail": "Провайдер временно недоступен"}, status_code=503)
            if current_mode == "reject":
                return JSONResponse({"detail": "Адрес отклонён"}, status_code=422)
            receipt_id = str(uuid4())
            inserted = await db.scalar(
                insert(Receipt)
                .values(
                    key=idempotency_key, fingerprint=digest, envelope=payload, receipt_id=receipt_id
                )
                .on_conflict_do_nothing()
                .returning(Receipt.key)
            )
            receipt = await db.get(Receipt, idempotency_key)
            if receipt.fingerprint != digest:
                raise HTTPException(409, "Тот же ключ использован с другим содержимым")
            receipt_id = receipt.receipt_id
        # Сначала сохраняем результат, затем теряем ответ: это воспроизводит неоднозначный сбой.
        if current_mode == "accept_then_timeout" and inserted:
            await asyncio.sleep(settings.provider_timeout + 2)
        return {"receipt_id": receipt_id, "duplicate": inserted is None}

    return app
