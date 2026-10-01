"""Защита ресурсов тестового профиля до миграций и очистки данных."""

import os
from urllib.parse import unquote, urlsplit

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

DATABASE_NAME = "notify_test"
ENDPOINTS = {"RABBITMQ_URL": ("amqp", "test-rabbitmq", 5672, ("/",))}


def refuse(variable):
    # URL не выводится: он может содержать пароль из ошибочно выбранного окружения.
    raise pytest.UsageError(
        f"Небезопасное тестовое окружение: проверьте {variable}. "
        "Запускайте тесты через make test в отдельном профиле Compose."
    )


def ensure_test_environment():
    if os.environ.get("TESTING") != "1":
        refuse("TESTING=1")
    raw = os.environ.get("DATABASE_URL", "")
    try:
        url = make_url(raw)
        valid = (
            url.drivername == "postgresql+asyncpg"
            and url.host == "test-db"
            and url.port == 5432
            and url.database == DATABASE_NAME
            and not url.query
            and not any(ord(char) < 32 for char in raw)
        )
    except (ArgumentError, ValueError):
        valid = False
    if not valid:
        refuse("DATABASE_URL")
    for variable, (scheme, host, port, paths) in ENDPOINTS.items():
        raw = os.environ.get(variable, "")
        try:
            url = urlsplit(raw)
            valid = (
                url.scheme == scheme
                and url.hostname == host
                and url.port == port
                and unquote(url.path) in paths
                and not url.query
                and not url.fragment
                and not any(ord(char) < 32 for char in raw)
            )
            if variable == "S3_ENDPOINT":
                valid = valid and url.username is None and url.password is None
        except ValueError:
            valid = False
        if not valid:
            refuse(variable)
