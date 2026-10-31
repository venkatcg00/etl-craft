#!/usr/bin/env bash
# Build the wheel and sdist, then install each into a clean virtual environment with pip and
# with uv, outside the source tree, and check the installed package.
#
# Usage: scripts/verify_package.sh            (uses python3; override with PYTHON=...)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON:-python3}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

expected="$("$python_bin" - "$root/pyproject.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    print(tomllib.load(handle)["project"]["version"])
PY
)"

echo "==> building etl-craft $expected"
uv build --quiet --out-dir "$work/dist" "$root"
wheel="$(ls "$work"/dist/*.whl)"
sdist="$(ls "$work"/dist/*.tar.gz)"

echo "==> checking wheel contents"
"$python_bin" - "$wheel" <<'PY'
import sys
import zipfile

names = zipfile.ZipFile(sys.argv[1]).namelist()
required = [
    "etl_craft/__init__.py",
    "etl_craft/py.typed",
    "etl_craft/cli/__init__.py",
    "etl_craft/dialects/engine/postgres/schema.sql",
    "etl_craft/dialects/engine/sqlite/schema.sql",
    "etl_craft/dialects/engine/queries/applied_migrations.sql",
    "etl_craft/core/static/catalog.css",
    "etl_craft/core/static/catalog.js",
    "etl_craft/core/static/mark.html",
    "etl_craft/core/static/favicon.svg",
    "etl_craft/core/static/theme.js",
    "etl_craft/api/templates/base.html",
    "etl_craft/api/templates/signin.html",
    "etl_craft/api/static/shell.js",
    "etl_craft/api/templates/monitor/run.html",
    "etl_craft/api/templates/monitor/search.html",
    "etl_craft/api/templates/monitor/_live_state.html",
    "etl_craft/api/static/monitor.js",
    "etl_craft/api/static/monitor.css",
    "etl_craft/api/static/attempt.js",
    "etl_craft/api/templates/monitor/attempt.html",
    "etl_craft/api/templates/insights.html",
    "etl_craft/api/templates/console.html",
    "etl_craft/api/templates/admin.html",
    "etl_craft/api/templates/operations/system.html",
    "etl_craft/api/templates/operations/agents.html",
    "etl_craft/api/templates/operations/scheduled.html",
    "etl_craft/api/templates/operations/maintenance.html",
    "etl_craft/api/templates/monitor/items.html",
    "etl_craft/api/static/console.js",
    "etl_craft/api/static/admin.js",
    "etl_craft/api/static/htmx.min.js",
    "etl_craft/api/static/htmx.LICENSE",
]
missing = [name for name in required if name not in names]
if not any(name.endswith("licenses/LICENSE") for name in names):
    missing.append("LICENSE")
if missing:
    sys.exit(f"wheel is missing: {', '.join(missing)}")
PY

check_install() {
    local venv="$1" label="$2"
    (
        cd "$work"
        [[ "$("$venv/bin/etl-craft" --version)" == "etl-craft $expected" ]] \
            || { echo "FAIL ($label): etl-craft --version" >&2; exit 1; }
        [[ "$("$venv/bin/python" -m etl_craft --version)" == "etl-craft $expected" ]] \
            || { echo "FAIL ($label): python -m etl_craft --version" >&2; exit 1; }
        "$venv/bin/python" -c "import etl_craft, sys; assert etl_craft.__version__ == sys.argv[1]" \
            "$expected"
        # The Engine DB schemas and query catalog ship inside the package.
        "$venv/bin/python" -c "
from etl_craft.dialects.engine import all_dialects
for dialect in all_dialects():
    assert 'CREATE TABLE CFG_PIPELINES' in dialect.schema_path().read_text(encoding='utf-8')
    assert dialect.query('existing_tables')
" || { echo "FAIL ($label): Engine DB SQL files" >&2; exit 1; }
    )
    echo "    ok: $label"
}

echo "==> installing"
"$python_bin" -m venv "$work/pip-wheel"
"$work/pip-wheel/bin/python" -m pip install --quiet --disable-pip-version-check "$wheel"
check_install "$work/pip-wheel" "pip install <wheel>"

uv venv --quiet --python "$python_bin" "$work/uv-wheel"
uv pip install --quiet --python "$work/uv-wheel/bin/python" "$wheel"
check_install "$work/uv-wheel" "uv pip install <wheel>"

"$python_bin" -m venv "$work/pip-sdist"
"$work/pip-sdist/bin/python" -m pip install --quiet --disable-pip-version-check "$sdist"
check_install "$work/pip-sdist" "pip install <sdist>"

uv venv --quiet --python "$python_bin" "$work/uv-sdist"
uv pip install --quiet --python "$work/uv-sdist/bin/python" "$sdist"
check_install "$work/uv-sdist" "uv pip install <sdist>"

echo "==> installing all extras at direct lower bounds and highest versions below caps"
"$python_bin" "$root/scripts/dependency_bounds.py" "$root/pyproject.toml" > "$work/lower.txt"
for bounds in lower caps; do
    uv venv --quiet --python "$python_bin" "$work/$bounds"
    constraints=()
    if [[ "$bounds" == lower ]]; then constraints=(--requirement "$work/lower.txt" --resolution lowest-direct); fi
    uv pip install --quiet --python "$work/$bounds/bin/python" "${constraints[@]}" "$wheel[all,publish]"
    uv pip check --python "$work/$bounds/bin/python"
    check_install "$work/$bounds" "all extras: $bounds"
done

echo "==> etl-craft $expected: wheel, sdist and dependency bounds verified"
