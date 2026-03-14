.PHONY: install test coverage lint format fix ci clean release

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
	rm -rf .pytest_cache .coverage htmlcov .ruff_cache dist build

release:
	@test -n "$(VERSION)" || (echo "Usage: make release VERSION=x.y.z" && exit 1)
	@grep -q 'version = "$(VERSION)"' pyproject.toml || \
		(echo "pyproject.toml version != $(VERSION) — bump it first" && exit 1)
	@grep -q '__version__ = "$(VERSION)"' pyesql/__init__.py || \
		(echo "pyesql/__init__.py version != $(VERSION) — bump it first" && exit 1)
	git tag v$(VERSION)
	git push origin v$(VERSION)