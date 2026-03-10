.PHONY: install test coverage lint format clean

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

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache .coverage htmlcov .ruff_cache