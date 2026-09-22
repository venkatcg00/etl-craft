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

echo "==> the wheel must ship its SQL, type marker and license"
python - "$WORK/dist" <<'PY'
import sys, zipfile, pathlib
wheel = next(pathlib.Path(sys.argv[1]).glob("*.whl"))
names = zipfile.ZipFile(wheel).namelist()
required = ["etl_craft/sql/schema.sql", "etl_craft/py.typed"]
missing = [r for r in required if r not in names]
if missing:
    sys.exit(f"wheel is missing {missing}")
if not any(n.endswith("LICENSE") for n in names):
    sys.exit("wheel ships no LICENSE")
if not any(n.startswith("etl_craft/sql/migrations/") and n.endswith(".sql") for n in names):
    sys.exit("wheel ships no migrations")
print(f"    {wheel.name}: sql/, py.typed and LICENSE all present")
PY

echo "==> installing into a clean venv outside the checkout"
uv venv "$WORK/venv" -q
uv pip install -q --python "$WORK/venv/bin/python" "$WORK"/dist/*.whl
EC="$WORK/venv/bin/etl-craft"

echo "==> preparing a disposable database"
psql_admin -q -c "DROP DATABASE IF EXISTS $DB_NAME" -c "CREATE DATABASE $DB_NAME"

# [ADDITION, 2026-09-22, E2-88] The config is written by `setup`, not by a
# heredoc here. CLAUDE.md's CLI table names `setup` as the one command that
# takes a team from nothing to a working deployment, and it was the only verb
# in that table this install-path test did not exercise -- while writing the
# config by hand is exactly the step `setup` exists to replace. Not
# speculative: `setup` shipped without writing a [Warehouse] section at all, so
# every SQL/BUSINESS_RULES task on a setup-produced deployment failed with "no
# [Warehouse] section configured". That bug lived in the one command this test
# skipped, and was found by hand months later.
mkdir -p "$WORK/adopter"
cat > "$WORK/adopter/.env" <<ENV
ETL_CRAFT_MODE=local
ETL_CRAFT_SOURCE_TYPE=environment
ETL_CRAFT_POSTGRES_PROFILE=dev
ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://$PGHOST:$PGPORT/$DB_NAME
ETL_CRAFT_POSTGRES_USER=$PGUSER
ETL_CRAFT_POSTGRES_AUTH_MODE=password
ETL_CRAFT_WAREHOUSE_JDBC_URL=jdbc:postgresql://$PGHOST:$PGPORT/$DB_NAME
ETL_CRAFT_WAREHOUSE_USER=$PGUSER
ETL_CRAFT_WAREHOUSE_AUTH_MODE=password
ENV
export ETL_CRAFT_POSTGRES_DEV_SECRET="$PGPASSWORD"
export ETL_CRAFT_WAREHOUSE_DEV_SECRET="$PGPASSWORD"
cd "$WORK/adopter"

echo "==> --help"
"$EC" --help >/dev/null

echo "==> setup: from nothing to a working deployment in one command"
"$EC" setup

echo "==> the config setup wrote must carry both sections"
"$WORK/venv/bin/python" - "$WORK/adopter/craft-connector.yml" <<'PYEOF'
import sys, pathlib, re
text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
for section in ("Postgres:", "Warehouse:"):
    if not re.search(rf"^{re.escape(section)}", text, re.MULTILINE):
        sys.exit(f"setup wrote no {section} section:\n{text}")
print("    Postgres and Warehouse both written")
PYEOF

echo "==> setup again must be idempotent"
"$EC" setup | grep -qi "already current"

echo "==> the read verbs work against a setup-produced deployment"
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
