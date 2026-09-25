import hashlib

import pytest

from etl_craft.core import text
from etl_craft.core.enums import RefreshType
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.core.text import JdbcUrl, split_statements

pytestmark = pytest.mark.unit


# JDBC URLs


def test_parse_jdbc_url_with_a_port_and_query():
    assert text.parse_jdbc_url("jdbc:mysql://myhost:3306/mydb?useSSL=true") == JdbcUrl(
        scheme="mysql", host="myhost", port=3306, database="mydb", query={"useSSL": "true"}
    )


def test_parse_jdbc_url_keeps_the_query_string_after_the_database():
    # sslmode=require must reach the driver, or the connection silently runs without TLS.
    url = text.parse_jdbc_url("jdbc:postgresql://myhost/mydb?sslmode=require&application_name=etl")
    assert url.database == "mydb"
    assert url.query == {"sslmode": "require", "application_name": "etl"}


def test_parse_jdbc_url_port_is_none_unless_a_default_is_given():
    assert text.parse_jdbc_url("jdbc:postgresql://myhost/mydb").port is None
    assert text.parse_jdbc_url("jdbc:postgresql://myhost/mydb", default_port=5432).port == 5432
    assert text.parse_jdbc_url("jdbc:postgresql://h:6543/db", default_port=5432).port == 6543


def test_a_catalog_schema_path_has_the_catalog_first():
    url = text.parse_jdbc_url("jdbc:trino://trino.internal:8080/iceberg/analytics")
    assert url.database == "iceberg/analytics"
    assert url.catalog == "iceberg"
    assert text.parse_jdbc_url("jdbc:postgresql://h/db").catalog == "db"


def test_parse_jdbc_url_allows_an_empty_database():
    url = text.parse_jdbc_url("jdbc:snowflake://acct.snowflakecomputing.com/?db=A")
    assert (url.database, url.catalog, url.query) == ("", "", {"db": "A"})


@pytest.mark.parametrize(
    "jdbc_url",
    ["postgresql://h/db", "not-a-jdbc-url", "jdbc:duckdb:/data/w.duckdb", "jdbc:postgresql://h"],
)
def test_parse_jdbc_url_rejects_another_shape(jdbc_url):
    with pytest.raises(ConfigurationError, match="not a recognized JDBC URL"):
        text.parse_jdbc_url(jdbc_url)


@pytest.mark.parametrize(
    ("jdbc_url", "scheme"),
    [
        ("jdbc:postgresql://h/db", "postgresql"),
        ("jdbc:DuckDB:/data/w.duckdb", "duckdb"),
        ("jdbc:duckdb:", "duckdb"),
        ("jdbc:sqlite:engine.db", "sqlite"),
    ],
)
def test_jdbc_scheme(jdbc_url, scheme):
    assert text.jdbc_scheme(jdbc_url) == scheme


def test_jdbc_scheme_rejects_a_non_jdbc_url():
    with pytest.raises(ConfigurationError, match="expected jdbc:<vendor>"):
        text.jdbc_scheme("postgresql://h/db")


# Secrets files


def test_parse_env_file_skips_blanks_comments_and_lines_without_equals():
    contents = "# secrets\n\nENGINE_SECRET = s3cret \nnot a setting\nEMPTY=\nURL=a=b\n"
    assert text.parse_env_file(contents) == {"ENGINE_SECRET": "s3cret", "EMPTY": "", "URL": "a=b"}


def test_parse_env_file_keeps_a_quote_that_is_part_of_the_secret():
    # Stripping every quote character would truncate a generated password that ends in one,
    # and the failure would read as a plain authentication error.
    contents = (
        'TRAILING=p@ssw0rd"\n'
        'LEADING="p@ssw0rd\n'
        "BOTH_REAL='\"p@ssw0rd\"'\n"
        'WRAPPED="p@ssw0rd"\n'
        "SINGLE='p@ssw0rd'\n"
        "MISMATCHED=\"p@ssw0rd'\n"
        "PLAIN=p@ssw0rd\n"
    )
    values = text.parse_env_file(contents)
    assert values["TRAILING"] == 'p@ssw0rd"'
    assert values["LEADING"] == '"p@ssw0rd'
    assert values["BOTH_REAL"] == '"p@ssw0rd"'
    assert values["WRAPPED"] == "p@ssw0rd"
    assert values["SINGLE"] == "p@ssw0rd"
    assert values["MISMATCHED"] == "\"p@ssw0rd'"
    assert values["PLAIN"] == "p@ssw0rd"


@pytest.mark.parametrize("value", ['"', "'", "", "x"])
def test_unquote_leaves_values_too_short_to_be_quoted(value):
    assert text.unquote(value) == value


@pytest.mark.parametrize(
    ("name", "valid"),
    [("ENGINE_SECRET", True), ("_x1", True), ("lower_ok", True), ("1ABC", False), ("A-B", False)],
)
def test_is_env_name(name, valid):
    assert text.is_env_name(name) is valid


# SQL statements


def test_split_statements_keeps_dollar_quoted_bodies_whole():
    sql = (
        "CREATE FUNCTION f() RETURNS TRIGGER AS $$ BEGIN "
        "NEW.a := 1; NEW.b := 2; RETURN NEW; END; $$ LANGUAGE plpgsql;\n"
        "SELECT 1;"
    )
    statements = split_statements(sql)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE FUNCTION")
    assert statements[0].count(";") == 4
    assert statements[1] == "SELECT 1"


def test_split_statements_ignores_semicolons_in_literals_and_comments():
    sql = (
        "INSERT INTO t VALUES ('a;b', 'it''s; fine');\n"
        "-- a trailing comment; with a semicolon\n"
        "/* and a block; comment */\n"
        "SELECT 2;"
    )
    statements = split_statements(sql)
    assert len(statements) == 2
    assert "'a;b'" in statements[0]
    assert "'it''s; fine'" in statements[0]
    assert statements[1].endswith("SELECT 2")


def test_split_statements_handles_tagged_dollar_quotes():
    sql = "DO $fn$ BEGIN RAISE NOTICE 'x;y'; END $fn$;\nSELECT 3;"
    assert split_statements(sql) == ["DO $fn$ BEGIN RAISE NOTICE 'x;y'; END $fn$", "SELECT 3"]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT $",  # a lone $ is not a dollar quote
        "SELECT $1 + $2",  # $1 is a placeholder, not a tag
        "SELECT $a-b$ x $a-b$",  # not a valid tag either
    ],
)
def test_split_statements_does_not_mistake_other_dollars_for_quotes(sql):
    assert split_statements(sql) == [sql]


def test_split_statements_ignores_blank_and_whitespace_only_segments():
    sql_text = "ALTER TABLE t ADD COLUMN c int;  \n\n  UPDATE t SET c = 1; \n ;"
    assert split_statements(sql_text) == ["ALTER TABLE t ADD COLUMN c int", "UPDATE t SET c = 1"]


def test_split_statements_empty_input_returns_empty_list():
    assert split_statements("") == []
    assert split_statements("   \n  ") == []


def test_split_statements_drops_comment_only_statements():
    assert split_statements("-- header\n/* note */;\nSELECT 1;\n-- trailer") == ["SELECT 1"]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'unterminated;",
        "SELECT 1 /* unterminated; comment",
        "SELECT $$ unterminated; body",
        "SELECT 1 -- comment at the very end",
    ],
)
def test_split_statements_keeps_an_unterminated_construct_as_one_statement(sql):
    assert split_statements(sql) == [sql]


def test_is_only_comments():
    assert text.is_only_comments("-- a\n/* b\n c */  \n")
    assert not text.is_only_comments("/* a */ SELECT 1")


# $$pipeline_id


@pytest.mark.parametrize(
    ("refresh_type", "force_all", "expected"),
    [
        ("INCREMENTAL", False, "pipeline_run_id = 42"),
        ("FULL", False, "1=1"),
        (RefreshType.FULL, False, "1=1"),
        # --force scans everything, whatever the refresh type.
        ("INCREMENTAL", True, "1=1"),
    ],
)
def test_substitute_pipeline_id(refresh_type, force_all, expected):
    result = text.substitute_pipeline_id(
        "SELECT 1 WHERE $$pipeline_id",
        refresh_type=refresh_type,
        pipeline_run_id=42,
        force_all=force_all,
    )
    assert result == f"SELECT 1 WHERE {expected}"


def test_substitute_pipeline_id_replaces_every_token():
    sql = "SELECT * FROM a WHERE $$pipeline_id UNION ALL SELECT * FROM b WHERE $$pipeline_id"
    result = text.substitute_pipeline_id(sql, refresh_type="INCREMENTAL", pipeline_run_id=7)
    assert result.count("pipeline_run_id = 7") == 2


@pytest.mark.parametrize(
    "sql",
    ["SELECT * FROM some_table", "SELECT * FROM reference_table WHERE active = true"],
)
def test_substitute_pipeline_id_leaves_sql_without_the_token_untouched(sql):
    # Nothing is appended: a missing token is a definition mistake for review to catch.
    assert text.substitute_pipeline_id(sql, refresh_type="INCREMENTAL", pipeline_run_id=42) == sql


# Read-only lint


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t",
        "  (SELECT a FROM t)",
        "with x as (select 1) select * from x",
        "TABLE t",
        "VALUES (1)",
        "SELECT update_date, created_by FROM t",  # write keywords only as whole words
        "SELECT 'DROP TABLE t; DELETE' AS note FROM t",  # inside a literal
        "-- DELETE old rows first\nSELECT a FROM t /* not an UPDATE */",  # inside comments
    ],
)
def test_read_only_sql_passes(sql):
    assert text.read_only_problem(sql) is None


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("", "is empty"),
        ("  -- only a comment\n", "is empty"),
        ("DELETE FROM t", "starts with 'DELETE', not SELECT/WITH"),
        ("(((", "starts with '((('"),
        (
            "WITH x AS (DELETE FROM other RETURNING *) SELECT * FROM x",
            "contains ['DELETE'] — a read-only SELECT should not",
        ),
        ("SELECT * INTO new_t FROM t; insert into t values (1)", "contains ['INSERT']"),
        ("select 1; drop table t; truncate u", "contains ['TRUNCATE', 'DROP']"),
    ],
)
def test_read_only_problem_explains_what_is_wrong(sql, reason):
    problem = text.read_only_problem(sql)
    assert problem is not None
    assert problem.startswith(reason)


def test_strip_comments_and_literals():
    assert text.strip_comments_and_literals("SELECT 'a''b' -- x\nFROM t /* y */") == (
        "SELECT ''  \nFROM t  "
    )


# Identifiers


@pytest.mark.parametrize(
    ("name", "safe"),
    [
        ("customer_id", True),
        ("_x", True),
        ("P1", True),
        ("customer id", False),
        ("1abc", False),
        ("../escape", False),
        ("a-b", False),
        ("", False),
    ],
)
def test_is_safe_identifier(name, safe):
    assert text.is_safe_identifier(name) is safe


@pytest.mark.parametrize(
    ("ref", "safe"),
    [("public.t", True), ("t", False), ("db.public.t", False), ("public.my table", False)],
)
def test_is_safe_object_ref(ref, safe):
    assert text.is_safe_object_ref(ref) is safe


@pytest.mark.parametrize(
    ("term", "safe"),
    [
        ("updated_at", True),
        ("updated_at DESC", True),
        (" updated_at asc nulls last ", True),
        ("updated_at NULLS FIRST", True),
        ("updated_at DESC NULLS", False),
        ("coalesce(a, b)", False),
        ("a, b", False),
    ],
)
def test_is_safe_order_term(term, safe):
    assert text.is_safe_order_term(term) is safe


def test_split_object_ref():
    assert text.split_object_ref(" public . t ") == ("public", "t")


@pytest.mark.parametrize("ref", ["t", "db.public.t", ".t", "public."])
def test_split_object_ref_rejects_anything_but_schema_dot_table(ref):
    with pytest.raises(HandlerError, match=r"CFG_TASK_PARAMETERS.TARGET_TABLE=.* must be exactly"):
        text.split_object_ref(ref, param_name="TARGET_TABLE")


def test_qualify_prepends_the_catalog():
    assert text.qualify("public.some_table", "etl_craft") == "etl_craft.public.some_table"


def test_split_pipe_list():
    assert text.split_pipe_list(" id | region ||", param_name="MERGE_KEY") == ["id", "region"]


@pytest.mark.parametrize("value", [None, ""])
def test_split_pipe_list_requires_a_value(value):
    with pytest.raises(HandlerError, match=r"CFG_TASK_PARAMETERS\.MERGE_KEY is required"):
        text.split_pipe_list(value, param_name="MERGE_KEY")


# Checksums


def test_sha256_hex():
    assert text.sha256_hex(b"SELECT 1;") == hashlib.sha256(b"SELECT 1;").hexdigest()


def test_fingerprint_of_one_part_is_its_md5():
    # Documentation versions store this value, so it must not change.
    assert text.fingerprint("docs") == hashlib.md5(b"docs").hexdigest()


def test_fingerprint_joins_parts_with_nul_so_they_cannot_collide():
    assert text.fingerprint("ab", "c") != text.fingerprint("a", "bc")
    assert text.fingerprint("SELECT 1", "public.t", "") == (
        hashlib.md5(b"SELECT 1\0public.t\0").hexdigest()
    )


# Suggestions


@pytest.mark.parametrize(
    ("unknown", "expected"),
    [
        ("pl_alpa", ["PL_ALPHA"]),
        ("LOAD", ["load_dim", "load_fact"]),
        ("zzz", []),
    ],
)
def test_suggest(unknown, expected):
    assert text.suggest(unknown, ["PL_ALPHA", "load_dim", "load_fact", "extract"]) == expected


def test_suggest_limits_the_list():
    assert len(text.suggest("t", [f"t{i}" for i in range(10)], limit=3)) == 3
