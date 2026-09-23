from datetime import time
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Channel = Literal["email", "push", "webhook"]


class ChannelPreference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    destination: str = Field(min_length=1, max_length=200)
    enabled: bool = True
    mode: Literal["immediate", "digest"] = "immediate"
    digest_seconds: int = Field(default=300, ge=5, le=86400)


class RecipientInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=100)
    timezone: str = "Europe/Moscow"
    quiet_start: str | None = None
    quiet_end: str | None = None
    channels: dict[Channel, ChannelPreference] = Field(min_length=1, max_length=3)

    @field_validator("timezone")
    @classmethod
    def known_zone(cls, value):
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("Неизвестный часовой пояс") from exc
        return value

    @field_validator("quiet_start", "quiet_end")
    @classmethod
    def valid_clock(cls, value):
        if value is not None:
            try:
                parsed = time.fromisoformat(value)
            except ValueError as exc:
                raise ValueError("Время должно быть записано в формате HH:MM") from exc
            if len(value) != 5 or parsed.second or parsed.tzinfo:
                raise ValueError("Время должно быть записано в формате HH:MM")
        return value

    @model_validator(mode="after")
    def complete_interval(self):
        if (self.quiet_start is None) != (self.quiet_end is None):
            raise ValueError("Укажите обе границы тихих часов")
        if self.quiet_start is not None and self.quiet_start == self.quiet_end:
            raise ValueError("Границы тихих часов должны различаться")
        return self


class EventInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    recipient_id: UUID
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=4000)
    priority: Literal["normal", "transactional"] = "normal"
    channels: list[Channel] = Field(default_factory=lambda: ["email"], min_length=1, max_length=3)

    @field_validator("channels")
    @classmethod
    def distinct_channels(cls, value):
        if len(set(value)) != len(value):
            raise ValueError("Каналы не должны повторяться")
        return sorted(value)
