.PHONY: fix check lint fmt type test precommit doctor doctor-fix

fix:
	uv run ruff check . --fix
	uv run ruff format .

lint:
	uv run ruff check .

fmt:
	uv run ruff format --check .

type:
	uv run mypy src

test:
	uv run pytest -q

check: lint fmt type test

precommit:
	uv run pre-commit run --all-files

bootstrap:
	uv sync --dev
	uv pip install -e .
	uv run pre-commit install
	./scripts/doctor.sh

doctor:
	./scripts/doctor.sh

doctor-fix:
	./scripts/doctor.sh --fix
