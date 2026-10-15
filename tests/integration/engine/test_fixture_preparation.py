"""Schema preparation is reused while each test receives an isolated database."""

import runpy
from pathlib import Path

import pytest

from fixtures.services import require


@pytest.mark.parametrize(
    "marker",
    [
        pytest.param("engine_sqlite", marks=pytest.mark.engine_sqlite),
        pytest.param("engine_postgres", marks=pytest.mark.engine_postgres),
    ],
)
def test_packaged_schema_is_built_once_for_independent_fixture_databases(
    pytester, monkeypatch, marker
):
    if marker == "engine_postgres":
        require("postgres")
    root = Path(__file__).resolve().parents[2]
    plugins = runpy.run_path(str(root / "conftest.py"))["pytest_plugins"]
    monkeypatch.setenv("PYTHONPATH", str(root))
    counter = pytester.path / "schema-calls.txt"
    pytester.makeconftest(
        f"pytest_plugins = {plugins!r}\n"
        "from pathlib import Path\nfrom fixtures import engine_db as databases\n"
        "original = databases.apply_schema\n"
        "def counted(engine):\n"
        f"    with Path({str(counter)!r}).open('a') as stream: stream.write('schema' + chr(10))\n"
        "    return original(engine)\n"
        "databases.apply_schema = counted\n"
    )
    pytester.makepyfile(
        "from sqlalchemy import text\n"
        "def test_first(engine_db):\n"
        "    with engine_db.engine.begin() as conn:\n"
        "        conn.execute(text('CREATE TABLE ONLY_FIRST (id INT)'))\n"
        "def test_second(engine_db):\n"
        "    with engine_db.engine.connect() as conn:\n"
        "        tables = engine_db.dialect.existing_tables(conn.engine, ['ONLY_FIRST'])\n"
        "        assert 'only_first' not in tables\n"
    )
    result = pytester.runpytest_subprocess("-q", "-m", marker)
    result.assert_outcomes(passed=2, deselected=2)
    assert counter.read_text().splitlines() == ["schema"]
