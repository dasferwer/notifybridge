from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo


def next_allowed(now: datetime, config: dict) -> datetime:
    if not config.get("quiet_start"):
        return now
    start, end = config["quiet_start"], config["quiet_end"]
    zone = ZoneInfo(config["timezone"])

    def quiet(instant):
        local = instant.astimezone(zone).strftime("%H:%M")
        return start <= local < end if start < end else local >= start or local < end

    if not quiet(now):
        return now
    # Перебор минут по UTC корректно проходит пропущенные и повторяющиеся часы при смене DST.
    candidate = now.astimezone(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 50):
        if not quiet(candidate):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError("Не удалось найти конец тихих часов за 50 часов")


def digest_boundary(now: datetime, seconds: int) -> tuple[int, datetime]:
    bucket = int(now.timestamp()) // seconds
    return bucket, datetime.fromtimestamp((bucket + 1) * seconds, UTC)
