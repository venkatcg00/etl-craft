.PHONY: help sync lint format typecheck imports history test test-changed coverage check static build verify-package clean regressions \
	quality quality-baseline \
	certs services-up services-down services-reset test-harness suite release-gate \
	docs docs-serve docs-site

UV ?= uv
PYTEST_WORKERS ?= auto
SERVICE_WORKERS = $(if $(filter auto,$(PYTEST_WORKERS)),4,$(PYTEST_WORKERS))

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

test coverage test-changed: export TMPDIR = $(HOME)/.cache/etl-craft/tests/tmp

test: ## Run the test suite in parallel, capping service workers
	mkdir -p "$(TMPDIR)"
	$(UV) run pytest -q -m unit -n $(PYTEST_WORKERS) --dist worksteal
	$(UV) run pytest -q -m "not unit" -n $(SERVICE_WORKERS) --dist worksteal

test-changed: ## Run affected tests locally, including full suites for changed data files
	mkdir -p "$(TMPDIR)"
	$(UV) run python scripts/test_changed.py --workers $(PYTEST_WORKERS)

coverage: ## Run the test suite with the coverage gate
	mkdir -p "$(TMPDIR)"
	$(UV) run pytest -q -m unit --cov --cov-report= --cov-fail-under=0 -n $(PYTEST_WORKERS) --dist worksteal
	$(UV) run pytest -q -m "not unit" --cov --cov-append --cov-report=term-missing -n $(SERVICE_WORKERS) --dist worksteal

regressions: ## Check named regression coverage for stabilization defects
	$(UV) run python scripts/check_regressions.py

quality: ## Hold src/etl_craft to scripts/quality-baseline.json: nothing new over a limit, nothing worse
	$(UV) run python scripts/check_quality.py

quality-baseline: ## Rewrite scripts/quality-baseline.json from the code, after an improvement
	$(UV) run python scripts/check_quality.py --update

check: lint typecheck imports history regressions quality coverage ## Everything CI runs on every pull request

static: lint typecheck imports history regressions quality ## The checks of make check without the test suite

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
	$(UV) run python scripts/check_docs_content.py site

docs-serve: ## Serve the documentation site with live reload
	$(UV) run mkdocs serve

docs-site: ## Build the versioned site as GitHub Pages serves it into _site/
	rm -rf _site
	$(UV) run python scripts/build_docs_site.py _site

clean: ## Remove build output and tool caches
	rm -rf dist build site _site htmlcov .coverage .coverage.* .pytest_cache .mypy_cache .ruff_cache
