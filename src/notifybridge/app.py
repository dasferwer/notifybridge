import secrets
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, Response
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from notifybridge.config import Settings
from notifybridge.models import Attempt, Delivery, Event, Outbox, Recipient
from notifybridge.schemas import EventInput, RecipientInput
from notifybridge.service import Conflict, NotFound, ingest, retry_dead


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

    app = FastAPI(title="NotifyBridge", version="0.1.0", lifespan=lifespan)

    async def admin(authorization: str = Header(default="")):
        if not secrets.compare_digest(
            authorization.encode(), ("Bearer " + settings.admin_token.get_secret_value()).encode()
        ):
            raise HTTPException(401, "Неверный токен")

    async def session():
        async with app.state.sessions() as db:
            yield db

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, exc):
        return JSONResponse({"detail": "База данных недоступна"}, status_code=503)

    @app.exception_handler(Conflict)
    async def conflict(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(NotFound)
    async def not_found(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(db=Depends(session)):
        await db.execute(text("SELECT 1"))
        return {"status": "ready", "queue_fallback": "database_polling"}

    router = APIRouter(prefix="/v1", dependencies=[Depends(admin)])

    @router.post("/recipients", status_code=201)
    async def create_recipient(body: RecipientInput, db=Depends(session)):
        recipient = Recipient(config=body.model_dump(mode="json"))
        db.add(recipient)
        await db.commit()
        return {"id": recipient.id, "version": recipient.version, **recipient.config}

    @router.put("/recipients/{recipient_id}")
    async def update_recipient(recipient_id: UUID, body: RecipientInput, db=Depends(session)):
        recipient = await db.scalar(
            select(Recipient).where(Recipient.id == recipient_id).with_for_update()
        )
        if recipient is None:
            raise NotFound("Получатель не найден")
        recipient.config, recipient.version = body.model_dump(mode="json"), recipient.version + 1
        await db.commit()
        return {"id": recipient.id, "version": recipient.version, **recipient.config}

    @router.post("/events")
    async def create_event(body: EventInput, db=Depends(session)):
        event, created = await ingest(db, body)
        result = {
            "event_id": str(event.id),
            "delivery_ids": event.delivery_ids,
            "suppressed": event.suppressed,
        }
        await db.commit()
        return JSONResponse(result, status_code=202 if created else 200)

    @router.get("/events/{event_id}")
    async def get_event(event_id: UUID, db=Depends(session)):
        event = await db.get(Event, event_id)
        if not event:
            raise NotFound("Событие не найдено")
        rows = (
            await db.scalars(
                select(Delivery).where(Delivery.id.in_([UUID(v) for v in event.delivery_ids]))
            )
        ).all()
        return {
            "event_id": event.id,
            "suppressed": event.suppressed,
            "deliveries": [
                {
                    "id": d.id,
                    "channel": d.channel,
                    "status": d.status,
                    "due_at": d.due_at,
                    "attempts": d.attempts,
                    "receipt_id": d.receipt_id,
                    "last_error": d.last_error,
                }
                for d in rows
            ],
        }

    @router.get("/deliveries")
    async def deliveries(status: str = "dead", db=Depends(session)):
        if status not in ("pending", "sending", "sent", "dead"):
            raise HTTPException(422, "Неизвестный статус")
        rows = (
            await db.scalars(
                select(Delivery)
                .where(Delivery.status == status)
                .order_by(Delivery.created_at)
                .limit(100)
            )
        ).all()
        return [
            {
                "id": d.id,
                "status": d.status,
                "channel": d.channel,
                "due_at": d.due_at,
                "last_error": d.last_error,
            }
            for d in rows
        ]

    @router.get("/deliveries/{delivery_id}/attempts")
    async def attempts(delivery_id: UUID, db=Depends(session)):
        if not await db.get(Delivery, delivery_id):
            raise NotFound("Доставка не найдена")
        rows = (
            await db.scalars(
                select(Attempt)
                .where(Attempt.delivery_id == delivery_id)
                .order_by(Attempt.created_at)
                .limit(100)
            )
        ).all()
        return [
            {"number": a.number, "outcome": a.outcome, "created_at": a.created_at} for a in rows
        ]

    @router.post("/deliveries/{delivery_id}/retry")
    async def retry(delivery_id: UUID, db=Depends(session)):
        if not await retry_dead(db, delivery_id):
            raise Conflict("Повторно запустить можно только доставку в статусе dead")
        await db.commit()
        return {"status": "pending", "idempotency_key": str(delivery_id)}

    @app.get("/metrics", dependencies=[Depends(admin)])
    async def metrics(db=Depends(session)):
        counts = dict(
            (
                await db.execute(select(Delivery.status, func.count()).group_by(Delivery.status))
            ).all()
        )
        backlog = await db.scalar(
            select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
        )
        lines = [
            "# HELP notifybridge_deliveries Количество доставок по текущему состоянию.",
            "# TYPE notifybridge_deliveries gauge",
        ]
        lines += [
            f'notifybridge_deliveries{{status="{state}"}} {counts.get(state, 0)}'
            for state in ("pending", "sending", "sent", "dead")
        ]
        lines += [
            "# HELP notifybridge_outbox_pending Количество неопубликованных сигналов.",
            "# TYPE notifybridge_outbox_pending gauge",
            f"notifybridge_outbox_pending {backlog}",
        ]
        return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    app.include_router(router)
    return app
