"""Создаёт локальный токен администратора, сохраняя существующие настройки."""

import os
import secrets
from pathlib import Path

path = Path(__file__).resolve().parents[1] / ".env"
try:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    print("Файл .env уже существует")
else:
    with os.fdopen(descriptor, "w") as stream:
        stream.write(f"ADMIN_TOKEN={secrets.token_urlsafe(36)}\n")
    print("Создан файл .env с правами 0600")
