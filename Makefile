.PHONY: help sync lint format typecheck imports history test coverage check build verify-package clean \
	certs services-up services-down services-reset test-harness suite release-gate \
	docs docs-serve docs-site

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

certs: ## Generate the throwaway TLS certificates the local services use (.certs/)
	./scripts/make_test_certs.sh

services-up: certs ## Start the local test services and wait until they are healthy
	docker compose up -d --wait

services-down: ## Stop the local test services and delete their data
	docker compose down -v

services-reset: services-down services-up ## Restart the local test services from empty

test-harness: ## Check that every local test service works
	$(UV) run pytest -q -m harness

suite: ## Run one release suite and record its evidence: make suite SUITE=unit [WHEEL=path]
	$(UV) run python scripts/run_suite.py $(SUITE) $(if $(WHEEL),--wheel $(WHEEL))

acceptance-cloud: ## Run the Databricks and Snowflake suites locally: make acceptance-cloud [ENV_FILE=.env.acceptance]
	$(UV) run python scripts/acceptance_cloud.py $(if $(ENV_FILE),--env-file $(ENV_FILE))

release-gate: ## Check the release evidence of every required suite
	$(UV) run python scripts/release_gate.py

docs: ## Build the documentation site into site/ (strict: any warning fails)
	$(UV) run mkdocs build --strict --site-dir site

docs-serve: ## Serve the documentation site with live reload
	$(UV) run mkdocs serve

docs-site: ## Build the versioned site as GitHub Pages serves it into _site/
	rm -rf _site
	$(UV) run python scripts/build_docs_site.py _site

clean: ## Remove build output and tool caches
	rm -rf dist build site _site htmlcov .coverage .coverage.* .pytest_cache .mypy_cache .ruff_cache
