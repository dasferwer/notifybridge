import hashlib
import json
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert

from notifybridge.models import Attempt, Delivery, Event, Outbox, Recipient
from notifybridge.rendering import render
from notifybridge.scheduling import digest_boundary, next_allowed


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass


def fingerprint(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


async def ingest(db, request):
    content_hash = fingerprint(request.model_dump(mode="json"))
    # Блокировка получателя согласует обновление настроек, дедупликацию и сборку дайджеста.
    recipient = await db.scalar(
        select(Recipient).where(Recipient.id == request.recipient_id).with_for_update()
    )
    if recipient is None:
        raise NotFound("Получатель не найден")
    inserted = await db.scalar(
        insert(Event)
        .values(
            id=request.event_id,
            recipient_id=recipient.id,
            fingerprint=content_hash,
            delivery_ids=[],
            suppressed=[],
        )
        .on_conflict_do_nothing()
        .returning(Event.id)
    )
    event = await db.get(Event, request.event_id)
    if not inserted:
        if event.fingerprint != content_hash:
            raise Conflict("Этот event_id уже использован для другого события")
        return event, False
    now = await db.scalar(select(func.clock_timestamp()))
    ids, suppressed = [], []
    for channel in request.channels:
        pref = recipient.config["channels"].get(channel)
        if not pref or not pref["enabled"]:
            suppressed.append(channel)
            continue
        priority = 10 if request.priority == "transactional" else 0
        due = now if priority else next_allowed(now, recipient.config)
        group, delivery = None, None
        if not priority and pref["mode"] == "digest":
            bucket, boundary = digest_boundary(now, pref["digest_seconds"])
            group = f"{recipient.id}:{recipient.version}:{channel}:{bucket}"
            due = next_allowed(max(due, boundary), recipient.config)
            delivery = await db.scalar(
                select(Delivery)
                .where(
                    Delivery.group_key == group,
                    Delivery.sealed.is_(False),
                    Delivery.status == "pending",
                )
                .order_by(Delivery.created_at.desc(), Delivery.id)
                .with_for_update()
                .limit(1)
            )
            if delivery and len(delivery.items) >= 100:
                delivery = None
        item = {"event_id": str(request.event_id), "subject": request.subject, "body": request.body}
        if delivery:
            delivery.items = [*delivery.items, item]
        else:
            delivery = Delivery(
                recipient_id=recipient.id,
                channel=channel,
                destination=pref["destination"],
                priority=priority,
                due_at=due,
                group_key=group,
                items=[item],
                preferences=recipient.config,
            )
            db.add(delivery)
            await db.flush()
        ids.append(str(delivery.id))
    event.delivery_ids, event.suppressed = ids, suppressed
    if ids:
        db.add(Outbox())
    await db.flush()
    return event, True


async def claim(sessions, settings):
    async with sessions.begin() as db:
        now = await db.scalar(select(func.clock_timestamp()))
        ready = or_(
            and_(Delivery.status == "pending", Delivery.due_at <= now),
            and_(Delivery.status == "sending", Delivery.lease_until <= now),
        )
        query = select(Delivery).where(ready)
        if settings.worker_lane == "transactional":
            query = query.where(Delivery.priority == 10)
        # Транзакционные сообщения забираются первыми; отдельный воркер обслуживает только их.
        delivery = await db.scalar(
            query.order_by(Delivery.priority.desc(), Delivery.due_at, Delivery.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if delivery is None:
            return None
        if delivery.status == "sending":
            db.add(
                Attempt(delivery_id=delivery.id, number=delivery.attempts, outcome="lease_expired")
            )
        if delivery.attempts >= settings.max_attempts:
            delivery.status, delivery.last_error = "dead", "attempts_exhausted"
            delivery.lease_token, delivery.lease_until = None, None
            return None
        if not delivery.priority:
            allowed = next_allowed(now, delivery.preferences)
            if allowed > now:
                delivery.status, delivery.due_at = "pending", allowed
                delivery.lease_token, delivery.lease_until = None, None
                return None
        if not delivery.sealed:
            delivery.envelope = render(delivery)
            delivery.sealed = True
        delivery.status = "sending"
        delivery.attempts += 1
        delivery.lease_token = uuid4()
        delivery.lease_until = now + timedelta(seconds=settings.lease_seconds)
        await db.flush()
        return {
            "id": delivery.id,
            "token": delivery.lease_token,
            "envelope": delivery.envelope,
            "attempts": delivery.attempts,
            "preferences": delivery.preferences,
            "priority": delivery.priority,
        }


async def finish(sessions, settings, job, receipt=None, error=None, permanent=False):
    async with sessions.begin() as db:
        now = await db.scalar(select(func.clock_timestamp()))
        delivery = await db.scalar(
            select(Delivery)
            .where(
                Delivery.id == job["id"],
                Delivery.status == "sending",
                Delivery.lease_token == job["token"],
                Delivery.lease_until > now,
            )
            .with_for_update()
        )
        if delivery is None:
            return False
        outcome = "sent" if receipt else error
        db.add(Attempt(delivery_id=delivery.id, number=delivery.attempts, outcome=outcome))
        if receipt:
            delivery.status, delivery.receipt_id, delivery.sent_at = "sent", receipt, now
            delivery.last_error = None
        elif permanent or delivery.attempts >= settings.max_attempts:
            delivery.status, delivery.last_error = "dead", error
        else:
            delivery.status, delivery.last_error = "pending", error
            # Небольшой детерминированный разброс не даёт всем повторам начаться одновременно.
            jitter = (delivery.id.int % 100) / 100
            delay = min(300, settings.retry_base * 2 ** (delivery.attempts - 1)) + jitter
            delivery.due_at = now + timedelta(seconds=delay)
            if not delivery.priority:
                delivery.due_at = next_allowed(delivery.due_at, delivery.preferences)
        delivery.lease_token, delivery.lease_until = None, None
        return True


async def retry_dead(db, delivery_id: UUID):
    now = await db.scalar(select(func.clock_timestamp()))
    result = await db.execute(
        update(Delivery)
        .where(Delivery.id == delivery_id, Delivery.status == "dead")
        .values(status="pending", attempts=0, due_at=now, last_error=None)
    )
    if result.rowcount:
        db.add(Outbox())
    return bool(result.rowcount)
