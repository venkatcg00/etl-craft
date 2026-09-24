.PHONY: db-up db-down db-reset db-schema-test test test-sqlite-engine coverage typecheck wheel-smoke check

# Brings up everything the test suite can use:
#   * Postgres 16 (the Engine DB) with src/etl_craft/sql/schema.sql applied
#     — docker-entrypoint-initdb.d only runs on a fresh volume, so this is a
#     no-op on an already-initialized one; use db-reset to force a clean slate.
#   * A complete local Iceberg warehouse — MinIO, an Iceberg REST catalog and
#     Trino — which is what exercises the Iceberg code path for real. That is
#     the warehouse shape this project supports for everything except
#     Postgres, and no cloud account is needed for it.
# DuckDB needs no service at all: it is embedded and its tests use a tmp_path
# file (see tests/conftest.py). Every one of these skips cleanly if it is not
# running, so plain `pytest -q` never requires Docker.
db-up:
	docker compose up -d --wait

db-down:
	docker compose down

# Tear down including the data volume, then bring up fresh so schema.sql
# re-applies. schema.sql isn't written to be idempotent (plain CREATE TABLE,
# not IF NOT EXISTS) by design — it's meant to run once against an empty DB.
db-reset:
	docker compose down -v
	$(MAKE) db-up

# Runs src/etl_craft/sql/schema_test.sql's own EXPECT-FAIL/EXPECT-SUCCEED smoke test.
# [DEVIATION, 2026-09-20, E2-26] Now under -v ON_ERROR_STOP=1: the file is
# self-asserting, so psql's exit code is the result. It previously ran
# without it here and in CI, so the step always exited 0 and nobody was
# reading the output.
#
# Runs against a genuinely disposable database created/dropped inside the same
# running container — never the persistent `etl_craft` dev database that
# `make test`/`postgres_engine` use, matching schema_test.sql's own header
# ("Run against a disposable database... never against a real Engine DB").
# [Bug found and fixed]: this target originally ran straight against
# `etl_craft`, leaving schema_test.sql's own rows (PL_A, PL_B, ...)
# permanently in the dev database — invisible to per-pipeline-scoped test
# queries, but picked up for real by a genuinely global one
# (cfg.fetch_all_pipeline_dependency_edges), causing real, reproduced test
# failures until `make db-reset` cleared them.
db-schema-test: db-up
	docker compose exec -T postgres psql -U etl_craft -d etl_craft -c "DROP DATABASE IF EXISTS etl_craft_schema_test"
	docker compose exec -T postgres psql -U etl_craft -d etl_craft -c "CREATE DATABASE etl_craft_schema_test"
	docker compose exec -T postgres psql -U etl_craft -d etl_craft_schema_test -v ON_ERROR_STOP=1 -f /sql/schema.sql
	docker compose exec -T postgres psql -U etl_craft -d etl_craft_schema_test -v ON_ERROR_STOP=1 -f /sql/schema_test.sql
	docker compose exec -T postgres psql -U etl_craft -d etl_craft -c "DROP DATABASE etl_craft_schema_test"

# Full test suite, including the Postgres-backed integration tests in
# tests/test_integration.py — those skip themselves with a clear message
# if db-up hasn't been run (see tests/conftest.py).
test: db-up
	uv run pytest -q

# [ADDITION, 2026-09-24] The same suite with a SQLite Engine DB -- the default
# one -- instead of Postgres (tests/conftest.py's ETL_CRAFT_TEST_ENGINE). The
# warehouses tests configure are unchanged. Tests built on a Postgres-only
# premise are skipped with their reason (conftest.POSTGRES_ENGINE_ONLY).
test-sqlite-engine: db-up
	ETL_CRAFT_TEST_ENGINE=sqlite uv run pytest -q

# Coverage is only meaningful against the full suite (unit + integration) —
# most of the source is exercised through the Postgres-backed tests, so
# without db-up this reports large, misleading gaps rather than the real
# ones. fail_under=80 lives in pyproject.toml's [tool.coverage.report].
coverage: db-up
	uv run pytest -q --cov=etl_craft --cov-report=term-missing

# mypy over the source. Every module uses `from __future__ import annotations`
# and complete signatures, and nothing checked them until now (E2-29).
typecheck:
	uv run mypy src/etl_craft

# [ADDITION, 2026-09-20, E2-28] Build the wheel, install it into a clean venv
# *outside* the checkout, and drive it against a disposable database. This is
# the class of bug a test suite that always runs from the checkout cannot see
# — it is how E2-13 (a wheel shipping no sql/ at all) went unnoticed.
wheel-smoke: db-up
	./scripts/wheel-smoke.sh

# [DEVIATION, 2026-09-20, E2-28] Now genuinely the same checks CI runs — it
# previously omitted both psql schema steps and had no typecheck, so the two
# could disagree about a known hazard.
check: db-up db-schema-test typecheck
	uv run black --check .
	uv run ruff check .
	uv run pydocstyle .
	uv run pytest -q --cov=etl_craft --cov-report=term-missing
	ETL_CRAFT_TEST_ENGINE=sqlite uv run pytest -q
