.PHONY: db-up db-down db-reset db-schema-test test check

# Bring up a local Postgres 16 with sql/schema.sql already applied
# (docker-entrypoint-initdb.d only runs on a fresh volume, so this is a
# no-op on an already-initialized one — use db-reset to force a clean slate).
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

# Runs sql/schema_test.sql's own EXPECT-FAIL/EXPECT-SUCCEED smoke test
# inside the running container. Read the \echo lines in the output by eye —
# there's no pass/fail summary line (see the file's own header).
db-schema-test: db-up
	docker compose exec -T postgres psql -U etl_craft -d etl_craft -f /sql/schema_test.sql

# Full test suite, including the Postgres-backed integration tests in
# tests/test_runlog_postgres.py — those skip themselves with a clear message
# if db-up hasn't been run (see tests/conftest.py).
test: db-up
	uv run pytest -q

# Same checks CI runs on a PR into main.
check: db-up
	uv run black --check .
	uv run ruff check .
	uv run pydocstyle .
	uv run pytest -q
