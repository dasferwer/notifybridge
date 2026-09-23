from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from notifybridge.rendering import render
from notifybridge.scheduling import digest_boundary, next_allowed
from notifybridge.schemas import EventInput, RecipientInput


def instant(value):
    return datetime.fromisoformat(value).astimezone(UTC)


@pytest.mark.parametrize(
    "now,start,end,expected",
    [
        ("2030-01-01T23:15:00+00:00", "22:00", "08:00", "2030-01-02T08:00:00+00:00"),
        ("2030-01-01T08:00:00+00:00", "22:00", "08:00", "2030-01-01T08:00:00+00:00"),
        ("2030-01-01T12:30:00+00:00", "12:00", "13:00", "2030-01-01T13:00:00+00:00"),
        ("2030-01-01T22:00:00+00:00", "22:00", "08:00", "2030-01-02T08:00:00+00:00"),
    ],
)
def test_quiet_boundaries(now, start, end, expected):
    assert next_allowed(
        instant(now), {"timezone": "UTC", "quiet_start": start, "quiet_end": end}
    ) == instant(expected)


def test_dst_missing_hour():
    config = {"timezone": "America/New_York", "quiet_start": "01:00", "quiet_end": "02:30"}
    assert next_allowed(instant("2030-03-10T06:30:00+00:00"), config) == instant(
        "2030-03-10T07:00:00+00:00"
    )


def test_dst_repeated_hour():
    config = {"timezone": "America/New_York", "quiet_start": "00:00", "quiet_end": "02:30"}
    assert next_allowed(instant("2030-11-03T05:30:00+00:00"), config) == instant(
        "2030-11-03T07:30:00+00:00"
    )


def test_no_quiet_hours():
    now = datetime.now(UTC)
    assert next_allowed(now, {}) == now


def test_digest_fixed_boundary():
    bucket, boundary = digest_boundary(instant("2030-01-01T12:04:59+00:00"), 300)
    assert boundary == instant("2030-01-01T12:05:00+00:00")
    assert digest_boundary(boundary, 300)[0] == bucket + 1


@pytest.mark.parametrize(
    "changes",
    [
        {"timezone": "Unknown/Zone"},
        {"quiet_start": "22:00"},
        {"quiet_start": "22:00", "quiet_end": "22:00"},
        {"quiet_start": "25:00", "quiet_end": "08:00"},
        {"quiet_start": "08:00:00", "quiet_end": "09:00"},
    ],
)
def test_bad_preferences(changes):
    with pytest.raises(ValidationError):
        RecipientInput.model_validate(
            {"name": "Тест", "channels": {"email": {"destination": "x"}}, **changes}
        )


def test_template_escapes_html():
    value = render(
        SimpleNamespace(
            channel="email",
            destination="x",
            items=[{"subject": "<script>", "body": "{{ 7*7 }} & <img>", "event_id": "id"}],
        )
    )
    assert "<script>" not in value["html"]
    assert "&lt;script&gt;" in value["html"]
    assert "{{ 7*7 }}" in value["html"]
    assert "<img>" in value["text"]


def test_repeated_channels_rejected():
    from uuid import uuid4

    with pytest.raises(ValidationError):
        EventInput(
            event_id=uuid4(),
            recipient_id=uuid4(),
            subject="x",
            body="x",
            channels=["email", "email"],
        )
