.PHONY: help sync lint format typecheck imports history test coverage check build verify-package clean \
	docs docs-serve

UV ?= uv

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

sync: ## Install the package and every dependency group into .venv
	$(UV) sync --all-groups

lint: ## Lint and check formatting
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format: ## Apply lint fixes and formatting
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

typecheck: ## Type-check the package and scripts (mypy --strict)
	$(UV) run mypy

imports: ## Check the layer contracts
	$(UV) run lint-imports

history: ## Reject change-history commentary in tracked files
	$(UV) run python scripts/check_no_history.py

test: ## Run the test suite
	$(UV) run pytest -q

coverage: ## Run the test suite with the coverage gate
	$(UV) run pytest -q --cov --cov-report=term-missing

check: lint typecheck imports history coverage ## Everything CI runs on every pull request

build: ## Build the wheel and sdist into dist/
	$(UV) build

verify-package: ## Build, then install with pip and uv into clean environments
	./scripts/verify_package.sh

docs: ## Build the documentation site into site/ (strict: any warning fails)
	$(UV) run mkdocs build --strict --site-dir site

docs-serve: ## Serve the documentation site with live reload
	$(UV) run mkdocs serve

clean: ## Remove build output and tool caches
	rm -rf dist build site htmlcov .coverage .coverage.* .pytest_cache .mypy_cache .ruff_cache
