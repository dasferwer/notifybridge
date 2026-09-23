"""Проверяет, что основной цикл процесса недавно обновлял отметку времени."""

import time
from pathlib import Path

assert time.time() - float(Path("/tmp/notifybridge-heartbeat").read_text()) < 20
