"""Canonical hashes and explicit upgrades on every local warehouse."""

import hashlib

import pytest
from sqlalchemy import text

from etl_craft.core.errors import HandlerError
from etl_craft.engine.repository.hash_versions import clear_hash_version, fetch_hash_version
from etl_craft.services.rehash import rehash
from etl_craft.warehouse.connection import warehouse_dialect
from fixtures.hash_contract import golden_sql


def test_golden_canonical_hash_and_session_timezone(sql_world):
    w = sql_world
    dialect = warehouse_dialect(w.config)
    expression = golden_sql(dialect)
    payload = (
        "NV0:V5:a|b:cV2:é😀V26:2020-01-02T03:04:05.123456"
        "V26:2020-01-02T03:04:05.123456V7:12.3400V5:falseV10:2020-01-02"
    )
    expected = hashlib.md5(payload.encode()).hexdigest()
    with w.warehouse.connect() as conn:
        assert conn.execute(text(f"SELECT {expression}")).scalar_one() == expected
        conn.execute(
            text(
                "SET TIME ZONE 'Asia/Kolkata'"
                if w.kind == "trino_iceberg"
                else "SET TimeZone = 'Asia/Kolkata'"
            )
        )
        assert conn.execute(text(f"SELECT {expression}")).scalar_one() == expected


def test_null_empty_and_separator_values_have_distinct_hashes(sql_world):
    w = sql_world
    dialect = warehouse_dialect(w.config)
    values = [["NULL", "'x'"], ["''", "'x'"], ["'a|b'", "'c'"], ["'a'", "'b|c'"]]
    hashes = [w.rows(f"SELECT {dialect.hash_expression(row)}")[0][0] for row in values]
    assert len(set(hashes)) == len(hashes)


@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
def test_rehash_upgrades_every_row_then_unchanged_merge_does_nothing(sql_world, kind):
    w = sql_world
    source = "SELECT 1 AS id, CAST('old' AS VARCHAR(20)) AS name"
    w.setup("people", source, kind)
    params = {
        "SQL_ACTION": kind,
        "TARGET_OBJECT": "people",
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
        "SOURCE_SQL": source,
    }
    w.run("merge", **params)
    if kind == "SCD2_MERGE":
        w.run("merge", **{**params, "SOURCE_SQL": source.replace("'old'", "'new'")})
    w.execute(f"UPDATE {w.name('people')} SET HASH_KEY = 'legacy'")
    with w.engine_db.begin() as conn:
        clear_hash_version(conn, w.name("people"))
    before = w.rows(f"SELECT * FROM {w.name('people')} ORDER BY ROW_ID")
    with pytest.raises(HandlerError, match="rehash --target"):
        w.run("merge", **params)
    dry = rehash(w.engine_db, w.config, f"{w.schema}.people", dry_run=True)
    assert dry.rows == len(before) and dry.sql.startswith("UPDATE ")
    assert w.rows(f"SELECT * FROM {w.name('people')} ORDER BY ROW_ID") == before
    with w.engine_db.connect() as conn:
        assert fetch_hash_version(conn, w.name("people")) is None
    result = rehash(w.engine_db, w.config, f"{w.schema}.people")
    assert result.rows == len(before)
    with w.engine_db.connect() as conn:
        assert fetch_hash_version(conn, w.name("people")) == 2
    after = w.rows(f"SELECT * FROM {w.name('people')} ORDER BY ROW_ID")
    index = w.columns("people").index("hash_key")
    assert [row[:index] + row[index + 1 :] for row in before] == [
        row[:index] + row[index + 1 :] for row in after
    ]
    assert all(row[index] != "legacy" for row in after)
    unchanged = w.run(
        "merge",
        **{
            **params,
            "SOURCE_SQL": source.replace("'old'", "'new'") if kind == "SCD2_MERGE" else source,
        },
    )
    assert unchanged.insert_count == 0 and unchanged.update_count == 0


def test_floating_compare_columns_are_refused_before_target_writes(sql_world):
    w = sql_world
    source = "SELECT 1 AS id, CAST(1.25 AS DOUBLE PRECISION) AS amount"
    w.setup("floating", source, "SCD1_MERGE")
    with pytest.raises(HandlerError, match="floats have no stable text form; cast to DECIMAL"):
        w.run(
            "merge_float",
            SQL_ACTION="SCD1_MERGE",
            TARGET_OBJECT="floating",
            SOURCE_SQL=source,
            MERGE_KEY="id",
            MERGE_COMPARE_COLUMNS="amount",
        )
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('floating')}") == [(0,)]


def test_missing_or_ambiguous_rehash_contract_is_refused(sql_world):
    w = sql_world
    with pytest.raises(HandlerError, match="one common ordered"):
        rehash(w.engine_db, w.config, f"{w.schema}.absent", dry_run=True)
    w.setup("ambiguous", "SELECT 1 AS id, 'x' AS name, 'y' AS city", "SCD1_MERGE")
    for code, compare in [("first", "name"), ("second", "city")]:
        w.task(
            code,
            SQL_ACTION="SCD1_MERGE",
            TARGET_OBJECT="ambiguous",
            SOURCE_SQL="SELECT 1 AS id, 'x' AS name, 'y' AS city",
            MERGE_KEY="id",
            MERGE_COMPARE_COLUMNS=compare,
        )
    with pytest.raises(HandlerError, match="one common ordered"):
        rehash(w.engine_db, w.config, f"{w.schema}.ambiguous")


def test_rehash_command_validates_dry_run_then_upgrades(sql_world, capsys):
    import yaml

    from etl_craft.cli import main

    w = sql_world
    source = "SELECT 1 AS id, 'x' AS name"
    w.setup("cli_hash", source, "SCD1_MERGE")
    w.run(
        "merge_cli",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT="cli_hash",
        SOURCE_SQL=source,
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
    )
    profile = w.config.warehouse
    fields = {
        "jdbc_url": profile.jdbc_url,
        "schema": profile.schema,
        "user": profile.user,
        "auth_mode": str(profile.auth_mode),
        **profile.extra,
    }
    if name := fields.pop("secret_var", None):
        fields["secret"] = name
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {
            "dev": {"jdbc_url": f"jdbc:sqlite:{w.engine_db.url.database}", "schema": "main"}
        },
        "Warehouse": {
            "Name": {
                "duckdb": "DuckDB",
                "duckdb_iceberg": "DuckDB",
                "postgres": "Postgres",
                "trino_iceberg": "Trino",
            }[w.kind],
            "Table_format": str(w.config.warehouse_table_format),
            "dev": fields,
        },
    }
    path = w.config.config_path
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    args = ["rehash", "--config", str(path), "--target", f"{w.schema}.cli_hash"]
    assert main([*args, "--dry-run"]) == 0
    assert "would rehash 1 row(s)" in capsys.readouterr().out
    assert main(args) == 0
    assert "rehashed 1 row(s)" in capsys.readouterr().out
