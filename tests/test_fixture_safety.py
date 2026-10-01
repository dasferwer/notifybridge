import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SAFE_ENV = {
    "TESTING": "1",
    "ADMIN_TOKEN": "test-admin-token-for-safety-at-least-24",
    "S3_SECRET_KEY": "test-docroute-secret-key-long-enough",
    "PYTHONDONTWRITEBYTECODE": "1",
    "DATABASE_URL": "postgresql+asyncpg://notify:notify@test-db:5432/notify_test",
    "RABBITMQ_URL": "amqp://notify:notify@test-rabbitmq:5672/",
}

# Отказ от проверки, неверный разбор URL или поздний вызов защиты нарушают этот контракт.
INVALID_ENVIRONMENTS = [
    {"TESTING": None},
    {"TESTING": "0"},
    {"TESTING": "true"},
    {"DATABASE_URL": None},
    {"DATABASE_URL": "postgresql+asyncpg://notify:notify@database:5432/notify_test"},
    {"DATABASE_URL": "postgresql+asyncpg://notify:notify@test-db:5432/notify"},
    {"DATABASE_URL": "postgresql+asyncpg://notify:notify@test-db.example.com:5432/notify_test"},
    {"DATABASE_URL": "postgresql+asyncpg://notify:notify@test-db:5433/notify_test"},
    {"DATABASE_URL": "postgresql+asyncpg://notify:notify@test-db:5432/notify_test?host=database"},
    {"DATABASE_URL": "postgresql+asyncpg://notify:notify@test-db:5432/notify_test#database"},
    {"DATABASE_URL": "postgresql://notify:notify@test-db:5432/notify_test"},
    {"DATABASE_URL": "not-a-url"},
    {"RABBITMQ_URL": None},
    {"RABBITMQ_URL": "amqp://notify:notify@rabbitmq:5672/"},
    {"RABBITMQ_URL": "amqp://notify:notify@test-rabbitmq:5672/production"},
    {"RABBITMQ_URL": "amqp://notify:notify@test-rabbitmq:5672/?host=rabbitmq"},
]

PROBE = """
import json, subprocess, sys
from pathlib import Path
import pytest

actions = []
def forbidden_process(*args, **kwargs):
    actions.append('process')
    raise RuntimeError('Побочный процесс запрещён в проверке защиты')
subprocess.run = forbidden_process
def audit(event, args):
    if event == 'socket.connect':
        actions.append('network')
        raise RuntimeError('Сеть запрещена в проверке защиты')
sys.addaudithook(audit)
try:
    code = pytest.main(['-q', '-p', 'no:cacheprovider',
                       'tests/test_fixture_safety.py',
                       '-k', 'test_isolated_environment_can_be_collected', *sys.argv[2:]])
finally:
    Path(sys.argv[1]).write_text(json.dumps(actions))
raise SystemExit(code)
"""


def run_probe(tmp_path, overrides, *, collect_only=False):
    environment = os.environ.copy()
    environment.update(SAFE_ENV)
    for key, value in overrides.items():
        if value is None:
            environment.pop(key, None)
        else:
            environment[key] = value
    marker = tmp_path / "actions.json"
    command = [sys.executable, "-B", "-c", PROBE, str(marker)]
    if collect_only:
        command.append("--collect-only")
    result = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result, json.loads(marker.read_text())


@pytest.mark.parametrize("overrides", INVALID_ENVIRONMENTS)
def test_unsafe_environment_is_rejected_before_side_effects(tmp_path, overrides):
    result, actions = run_probe(tmp_path, overrides)
    assert result.returncode == 4, result.stdout + result.stderr
    assert "Небезопасное тестовое окружение" in result.stdout + result.stderr
    assert actions == []


def test_isolated_environment_can_be_collected(tmp_path):
    result, actions = run_probe(tmp_path, {}, collect_only=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert actions == []
