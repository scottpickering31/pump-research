PYTHON ?= .venv/bin/python

.PHONY: install db-up db-down db-status db-health test lint typecheck check

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e '.[dev]'

db-up:
	docker compose up -d

db-down:
	docker compose down

db-status:
	docker compose ps

db-health:
	$(PYTHON) -m pump_research database health

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy

check: test lint typecheck
