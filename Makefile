.PHONY: init up down check test smoke recovery logs
init:
	python3 scripts/init_env.py
up: init
	docker compose up --build -d --wait --wait-timeout 180
check:
	uv sync --frozen --extra dev
	uv run ruff check .
	uv run ruff format --check .
test: init
	docker compose --profile test build test
	docker compose --profile test run --rm test
smoke:
	docker compose exec -T api python scripts/smoke.py
recovery:
	uv run python scripts/recovery.py
logs:
	docker compose logs --tail=100 -f worker priority-worker dispatcher
down:
	docker compose --profile test down
