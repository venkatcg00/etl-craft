#!/usr/bin/env bash
# Build the wheel, install it into a clean venv outside the checkout, and drive
# it end to end against a disposable database.
#
# [ADDITION, 2026-09-20, E2-28] This exists because a test suite that always
# runs from the git checkout cannot see packaging bugs at all. E2-13 — a wheel
# shipping no sql/, so there was no way to create the Engine DB from an
# installed package and `migrate` silently no-opped — survived 368 passing
# tests. Everything below runs against the *installed* package only.
set -euo pipefail

WORK="$(mktemp -d)"
DB_NAME="${ETL_CRAFT_SMOKE_DB:-etl_craft_wheel_smoke}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-55432}"
PGUSER="${PGUSER:-etl_craft}"
PGPASSWORD="${PGPASSWORD:-etl_craft}"
export PGPASSWORD
trap 'rm -rf "$WORK"' EXIT

# CI runs on a host with psql installed; a dev machine usually only has it
# inside the compose container. Either way the *installed package under test*
# always connects from outside, over the host-mapped port.
if command -v psql >/dev/null 2>&1; then
    psql_admin() {
        psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 "$@"
    }
else
    COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    psql_admin() {
        (cd "$COMPOSE_DIR" && docker compose exec -T postgres \
            psql -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 "$@")
    }
fi

echo "==> building the wheel"
uv build --out-dir "$WORK/dist" >/dev/null

echo "==> the wheel must ship each Engine DB dialect's SQL, a type marker and a license"
python - "$WORK/dist" <<'PY'
import sys, zipfile, pathlib
wheel = next(pathlib.Path(sys.argv[1]).glob("*.whl"))
names = zipfile.ZipFile(wheel).namelist()
engine = "etl_craft/dialects/engine_dialects"
required = [
    f"{engine}/postgres/schema.sql",
    f"{engine}/sqlite/schema.sql",
    "etl_craft/dialects/warehouse_dialects/base.py",
    "etl_craft/py.typed",
]
missing = [r for r in required if r not in names]
if missing:
    sys.exit(f"wheel is missing {missing}")
if not any(n.endswith("LICENSE") for n in names):
    sys.exit("wheel ships no LICENSE")
if not any(n.startswith(f"{engine}/postgres/migrations/") and n.endswith(".sql") for n in names):
    sys.exit("wheel ships no PostgreSQL migrations")
print(f"    {wheel.name}: dialect SQL, py.typed and LICENSE all present")
PY

echo "==> installing into a clean venv outside the checkout"
uv venv "$WORK/venv" -q
uv pip install -q --python "$WORK/venv/bin/python" "$WORK"/dist/*.whl
EC="$WORK/venv/bin/etl-craft"

echo "==> setup refuses to run without a craft-connector.yml, and never writes one"
mkdir -p "$WORK/no-config"
(
    cd "$WORK/no-config"
    if "$EC" setup >/dev/null 2>&1; then
        echo "FAIL: setup succeeded with no craft-connector.yml" >&2
        exit 1
    fi
    test ! -e craft-connector.yml || { echo "FAIL: setup wrote craft-connector.yml" >&2; exit 1; }
)

echo "==> the smallest user-written config: SQLite Engine DB and DuckDB, nothing set"
mkdir -p "$WORK/zero-config"
(
    cd "$WORK/zero-config"
    cat > craft-connector.yml <<'YML'
Secrets:
  Source_type: environment
  Profile: dev

Orchestration:
  Mode: local

Engine:
  dev:
    jdbc_url: jdbc:sqlite:etl-craft-engine.db

Warehouse:
  Name: DuckDB
  dev:
    jdbc_url: jdbc:duckdb:warehouse.duckdb
YML
    env -u ETL_CRAFT_PROFILE "$EC" setup >/dev/null
    test -f etl-craft-engine.db || { echo "setup created no SQLite Engine DB"; exit 1; }
    "$EC" setup | grep -qi "already up to date"
    "$EC" doctor | grep -q "SQLite Engine DB"
    "$EC" list >/dev/null
    "$EC" validate >/dev/null
)

echo "==> preparing a disposable database"
psql_admin -q -c "DROP DATABASE IF EXISTS $DB_NAME" -c "CREATE DATABASE $DB_NAME"

# [DEVIATION, 2026-09-24] The team writes craft-connector.yml; etl-craft only
# reads it. So this writes one the way an adopter would -- secrets in a
# .env-style file beside it -- and checks `setup` leaves it byte-for-byte alone.
mkdir -p "$WORK/adopter"
cat > "$WORK/adopter/secrets.env" <<ENV
ENGINE_JDBC_URL=jdbc:postgresql://$PGHOST:$PGPORT/$DB_NAME
ENGINE_USER=$PGUSER
ENGINE_AUTH_MODE=password
ENGINE_DEV_SECRET=$PGPASSWORD
WAREHOUSE_JDBC_URL=jdbc:postgresql://$PGHOST:$PGPORT/$DB_NAME
WAREHOUSE_USER=$PGUSER
WAREHOUSE_AUTH_MODE=password
WAREHOUSE_DEV_SECRET=$PGPASSWORD
ENV
cat > "$WORK/adopter/craft-connector.yml" <<'YML'
Secrets:
  Source_type: file
  Path: secrets.env
  Profile: dev

Orchestration:
  Mode: local

Engine:
  dev:
    jdbc_url: ENGINE_JDBC_URL
    user: ENGINE_USER
    auth_mode: ENGINE_AUTH_MODE
    secret: ENGINE_SECRET

Warehouse:
  Name: Postgres
  dev:
    jdbc_url: WAREHOUSE_JDBC_URL
    user: WAREHOUSE_USER
    auth_mode: WAREHOUSE_AUTH_MODE
    secret: WAREHOUSE_SECRET
YML
cd "$WORK/adopter"
BEFORE="$(sha256sum craft-connector.yml)"

echo "==> --help"
"$EC" --help >/dev/null

echo "==> setup: an empty database to a working Engine DB in one command"
env -u ETL_CRAFT_PROFILE "$EC" setup

echo "==> setup left the user's craft-connector.yml untouched"
test "$BEFORE" = "$(sha256sum craft-connector.yml)" \
    || { echo "FAIL: setup modified craft-connector.yml" >&2; exit 1; }

echo "==> setup again must be idempotent"
"$EC" setup | grep -qi "already up to date"

echo "==> the read verbs work against the set-up deployment"
"$EC" list >/dev/null
"$EC" doctor >/dev/null

echo "==> and init-db/migrate still work as standalone verbs"
psql_admin -q -c "DROP DATABASE IF EXISTS $DB_NAME" -c "CREATE DATABASE $DB_NAME"

echo "==> init-db against an empty database"
"$EC" init-db

echo "==> init-db again must refuse (schema.sql is not idempotent)"
if "$EC" init-db >/dev/null 2>&1; then
    echo "FAIL: init-db re-applied the schema to a populated database" >&2
    exit 1
fi

echo "==> migrate reports nothing pending: init-db records the packaged ones"
"$EC" migrate | grep -q "up to date"

echo "==> the read verbs work against the installed package"
"$EC" list >/dev/null
"$EC" validate >/dev/null
"$EC" doctor >/dev/null

cd /
psql_admin -q -c "DROP DATABASE IF EXISTS $DB_NAME"
echo "==> wheel smoke test passed"
