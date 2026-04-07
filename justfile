check: format-check lint test

lint:
    ruff check src tests
    basedpyright

format:
    ruff format src tests

format-check:
    ruff format --check src tests

test:
    uv run pytest

setup:
    uv sync
    uv tool install ruff basedpyright 
    uv run pre-commit install
