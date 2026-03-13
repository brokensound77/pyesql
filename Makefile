.PHONY: install test coverage lint format fix ci clean

install:
	uv sync --extra dev

test:
	uv run pytest

coverage:
	uv run pytest --cov=pyesql --cov-report=term-missing

lint:
	uv run ruff check .

format:
	uv run ruff format .

fix:
	uv run ruff format .
	uv run ruff check --fix .

ci:
	uv run ruff format --check .
	uv run ruff check .
	uv run pytest

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache .coverage htmlcov .ruff_cache