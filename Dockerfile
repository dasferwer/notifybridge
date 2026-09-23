FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable
ENV PATH="/app/.venv/bin:$PATH"
COPY alembic.ini ./
COPY migrations ./migrations
COPY scripts ./scripts
RUN useradd --uid 10001 --create-home app
USER app
FROM base AS runtime
CMD ["uvicorn", "notifybridge.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
FROM base AS test
USER root
RUN uv sync --frozen --extra dev --no-editable
COPY tests ./tests
USER app
CMD ["pytest", "-p", "no:cacheprovider"]
