.PHONY: db-up db-down db-reset db-schema-test test coverage check

# Bring up a local Postgres 16 with src/etl_craft/sql/schema.sql already applied
# (docker-entrypoint-initdb.d only runs on a fresh volume, so this is a
# no-op on an already-initialized one — use db-reset to force a clean slate)
# plus a local ClickHouse, which stands in as a genuinely different
# SQLAlchemy dialect for warehouse.py's own tests (see docker-compose.yml).
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

# Runs src/etl_craft/sql/schema_test.sql's own EXPECT-FAIL/EXPECT-SUCCEED smoke test,
# against a genuinely disposable database created/dropped inside the same
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
	docker compose exec -T postgres psql -U etl_craft -d etl_craft_schema_test -f /sql/schema.sql
	docker compose exec -T postgres psql -U etl_craft -d etl_craft_schema_test -f /sql/schema_test.sql
	docker compose exec -T postgres psql -U etl_craft -d etl_craft -c "DROP DATABASE etl_craft_schema_test"

# Full test suite, including the Postgres-backed integration tests in
# tests/test_integration.py — those skip themselves with a clear message
# if db-up hasn't been run (see tests/conftest.py).
test: db-up
	uv run pytest -q

# Coverage is only meaningful against the full suite (unit + integration) —
# most of the source is exercised through the Postgres-backed tests, so
# without db-up this reports large, misleading gaps rather than the real
# ones. fail_under=80 lives in pyproject.toml's [tool.coverage.report].
coverage: db-up
	uv run pytest -q --cov=etl_craft --cov-report=term-missing

# Same checks CI runs on a PR into main.
check: db-up
	uv run black --check .
	uv run ruff check .
	uv run pydocstyle .
	uv run pytest -q --cov=etl_craft --cov-report=term-missing
