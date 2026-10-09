"""Warehouse dialects and connections without a warehouse: registry, SQL fragments, auth."""

from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy.engine import URL

from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    parse_config,
)
from etl_craft.config.targets import parse_warehouse_url
from etl_craft.core.enums import TableFormat
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.dialects import credentials
from etl_craft.dialects.warehouse import all_dialects, for_key, resolve
from etl_craft.warehouse import connection

pytestmark = pytest.mark.unit

POSTGRES = "jdbc:postgresql://wh:5432/analytics?sslmode=require"
TRINO = "jdbc:trino://trino.internal:8443/iceberg/analytics"
DATABRICKS = "jdbc:databricks://adb-1.azuredatabricks.net:443/default;httpPath=/sql/1.0/w/1"
SNOWFLAKE = "jdbc:snowflake://org-acct.snowflakecomputing.com/?db=ANALYTICS&schema=PUBLIC"


def profile(auth_mode, jdbc_url, user="etl", **extra):
    return ConnectionProfile("WAREHOUSE", "dev", jdbc_url, user, auth_mode, extra)


def config_for(
    warehouse_profile, table_format=TableFormat.NATIVE, *, path=Path("/tmp/craft-connector.yml")
):
    return parse_config(
        {
            "Secrets": {"Source_type": "environment"},
            "Orchestration": {"Mode": "local"},
            "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:e.db", "schema": "main"}},
            "Warehouse": {
                **(
                    {"Name": "DuckDB"}
                    if warehouse_profile.jdbc_url.startswith("jdbc:duckdb:")
                    else {}
                ),
                "Table_format": table_format,
                "dev": {
                    "jdbc_url": warehouse_profile.jdbc_url,
                    "user": warehouse_profile.user,
                    "auth_mode": warehouse_profile.auth_mode,
                    "secret": warehouse_profile.extra.get(
                        "secret_var", "ETL_CRAFT_DIALECT_TEST_SECRET"
                    ),
                    **(
                        {"catalog": "iceberg"}
                        if table_format == TableFormat.ICEBERG
                        and warehouse_profile.jdbc_url.startswith("jdbc:duckdb:")
                        else {}
                    ),
                    "schema": "analytics" if warehouse_profile.jdbc_url == TRINO else "main",
                    **{
                        key: value
                        for key, value in warehouse_profile.extra.items()
                        if key != "secret_var"
                    },
                },
            },
        },
        path,
    )


@pytest.fixture(autouse=True)
def configured_secret(monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_DIALECT_TEST_SECRET", "test-secret")


@pytest.fixture
def captured(monkeypatch):
    """Record what each connection hands the driver, instead of connecting."""
    seen = {}

    def fake_connect(url, extra=None):
        seen["url"] = url
        seen["args"] = extra or {}
        return object()

    monkeypatch.setattr(connection, "dbapi_connect", fake_connect)
    return seen


@pytest.fixture
def minted(monkeypatch):
    """Replace the OAuth exchange with a recorder returning numbered tokens."""
    calls = []

    def fake_token(token_url, client_id, client_secret, scope=None):
        calls.append((token_url, client_id, client_secret, scope))
        return f"access-token-{len(calls)}"

    monkeypatch.setattr(credentials, "client_credentials_token", fake_token)
    return calls


def connect(auth_mode, jdbc_url, secret="", table_format=TableFormat.NATIVE, **kwargs):
    warehouse = profile(auth_mode, jdbc_url, **kwargs)
    url = parse_warehouse_url(jdbc_url)
    dialect = resolve(url.dialect, table_format)
    return connection.warehouse_creator(dialect, warehouse, secret, url)()


# The registry


@pytest.mark.parametrize(
    ("name", "table_format", "key"),
    [
        ("postgresql+psycopg", "native", "postgres"),
        ("duckdb", "native", "duckdb"),
        ("duckdb", "iceberg", "duckdb_iceberg"),
        ("trino", "native", "trino_iceberg"),
        ("databricks", "native", "databricks"),
        ("databricks", "iceberg", "databricks_iceberg"),
        ("snowflake", "native", "snowflake"),
        ("snowflake", "iceberg", "snowflake_iceberg"),
    ],
)
def test_resolve(name, table_format, key):
    assert resolve(name, table_format).spec.key == key


def test_a_database_without_a_dialect_is_refused_naming_the_supported_ones():
    with pytest.raises(
        ConfigurationError, match=r"'oracle' is not a supported warehouse; .*DuckDB"
    ):
        resolve("oracle", "native")


def test_postgres_has_no_iceberg_dialect():
    with pytest.raises(ConfigurationError, match="always native"):
        resolve("postgresql", "iceberg")


def test_every_dialect_is_registered_once_with_its_spec():
    keys = [dialect.spec.key for dialect in all_dialects()]
    assert len(keys) == len(set(keys)) == 8
    for dialect in all_dialects():
        assert for_key(dialect.spec.key) is dialect
    with pytest.raises(LookupError):
        for_key("oracle")


@pytest.mark.parametrize(
    ("key", "single_writer", "surrogate_key", "primary_keys", "temporary"),
    [
        ("postgres", False, "identity", True, True),
        ("duckdb", True, "sequence", True, True),
        ("duckdb_iceberg", False, "computed", False, True),
        ("trino_iceberg", False, "computed", False, False),
        ("databricks", False, "identity", False, False),
        ("databricks_iceberg", False, "identity", False, False),
        ("snowflake", False, "identity", False, True),
        ("snowflake_iceberg", False, "computed", False, True),
    ],
)
def test_what_each_warehouse_supports(key, single_writer, surrogate_key, primary_keys, temporary):
    dialect = for_key(key)
    assert dialect.single_writer is single_writer
    assert dialect.surrogate_key == surrogate_key
    assert dialect.enforces_primary_keys is primary_keys
    assert dialect.temporary_tables is temporary
    assert dialect.scratch_table_keyword() == ("TEMPORARY TABLE" if temporary else "TABLE")


# SQL fragments


def test_hash_expressions():
    encoded = (
        "CASE WHEN a IS NULL THEN 'N' ELSE 'V' || "
        "CAST(LENGTH(CAST(a AS VARCHAR)) AS VARCHAR) || ':' || CAST(a AS VARCHAR) END"
    )
    assert for_key("postgres").hash_expression(["a"]) == f"MD5({encoded})"
    assert (
        for_key("databricks").hash_expression(["a"])
        == f"MD5({encoded.replace('VARCHAR', 'STRING')})"
    )
    assert (
        for_key("trino_iceberg").hash_expression(["a"]) == f"lower(to_hex(md5(to_utf8({encoded}))))"
    )


@pytest.mark.parametrize("key", ["postgres", "duckdb", "snowflake", "databricks", "trino_iceberg"])
def test_connections_pin_utc_and_close_the_setup_cursor(key):
    from unittest.mock import Mock

    conn = Mock()
    for_key(key).on_connect(conn, None, "")
    assert "UTC" in conn.cursor.return_value.execute.call_args.args[0]
    conn.cursor.return_value.close.assert_called_once()
    if key == "postgres":
        conn.commit.assert_called_once()


@pytest.mark.parametrize("key", ["postgres", "duckdb", "snowflake", "databricks", "trino_iceberg"])
@pytest.mark.parametrize("kind", ["DOUBLE", "REAL", "FLOAT", "ARRAY", "NUMERIC"])
def test_unsafe_hash_types_are_refused(key, kind):
    from etl_craft.core.errors import HandlerError

    with pytest.raises(HandlerError, match=r"cast to|without a declared scale") as failure:
        for_key(key).hash_expression(["amount"], [kind])
    assert not failure.value.retryable


def test_audit_column_types():
    assert for_key("postgres").audit_column_type("CREATE_DATE") == "TIMESTAMP WITH TIME ZONE"
    assert for_key("databricks").audit_column_type("UPDATE_DATE") == "TIMESTAMP"
    assert for_key("snowflake_iceberg").audit_column_type("CREATE_DATE") == "TIMESTAMP_NTZ(6)"
    assert for_key("snowflake_iceberg").audit_column_type("HASH_KEY") == "VARCHAR(32)"


def test_alter_keywords():
    assert for_key("snowflake_iceberg").alter_table_keyword() == "ALTER ICEBERG TABLE"
    assert for_key("snowflake").alter_table_keyword() == "ALTER TABLE"


class RecordingConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement, *args):
        self.statements.append(str(statement))


def created(key, params=None):
    conn = RecordingConnection()
    for_key(key).create_table_as(conn, "cat.s.t", "SELECT 1 AS id", params or {})
    return conn.statements[0]


def test_create_table_as_in_each_format():
    assert created("postgres") == "CREATE TABLE cat.s.t AS SELECT 1 AS id"
    assert created("databricks") == "CREATE TABLE cat.s.t USING DELTA AS SELECT 1 AS id"
    assert created("databricks_iceberg") == (
        "CREATE TABLE cat.s.t USING DELTA TBLPROPERTIES ('delta.enableIcebergCompatV2' = "
        "'true', 'delta.universalFormat.enabledFormats' = 'iceberg') AS SELECT 1 AS id"
    )
    assert created("snowflake_iceberg") == (
        "CREATE ICEBERG TABLE cat.s.t EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED' ICEBERG_VERSION = 2 "
        "CATALOG = 'SNOWFLAKE' AS SELECT 1 AS id"
    )


def test_databricks_external_location():
    params = {"EXTERNAL_LOCATION": "abfss://lake@acct.dfs.core.windows.net/sales/t"}
    assert created("databricks", params) == (
        "CREATE TABLE cat.s.t USING DELTA LOCATION "
        "'abfss://lake@acct.dfs.core.windows.net/sales/t' AS SELECT 1 AS id"
    )
    # LOCATION comes before the UniForm properties.
    assert created("databricks_iceberg", {"EXTERNAL_LOCATION": "s3://lake/t"}).startswith(
        "CREATE TABLE cat.s.t USING DELTA LOCATION 's3://lake/t' TBLPROPERTIES ("
    )
    with pytest.raises(HandlerError, match="EXTERNAL_LOCATION must not contain a quote"):
        created("databricks", {"EXTERNAL_LOCATION": "s3://it's"})


def test_databricks_mirrors_are_external_under_the_cloning_base_location():
    dialect = for_key("databricks_iceberg")
    ddl = dialect.mirror_table_ddl("m", "id INT", CloningConfig(base_location="s3://lake/clones/"))
    assert ddl.startswith("CREATE TABLE m (id INT) USING DELTA LOCATION 's3://lake/clones/m'")
    assert for_key("databricks").mirror_table_ddl("m", "id INT", CloningConfig()) == (
        "CREATE TABLE m (id INT) USING DELTA"
    )
    with pytest.raises(ConfigurationError, match="must not contain a quote"):
        dialect.mirror_table_ddl("m", "id INT", CloningConfig(base_location="s3://'"))
    assert for_key("postgres").mirror_table_ddl("m", "id INT", CloningConfig()) is None


def test_snowflake_iceberg_on_a_customer_volume_and_an_external_catalog():
    volume = {"EXTERNAL_VOLUME": "LAKE_VOL", "BASE_LOCATION": "sales/t"}
    assert created("snowflake_iceberg", volume) == (
        "CREATE ICEBERG TABLE cat.s.t EXTERNAL_VOLUME = 'LAKE_VOL' ICEBERG_VERSION = 2 "
        "CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'sales/t' AS SELECT 1 AS id"
    )
    external = {**volume, "CATALOG": "GLUE_CATALOG"}
    assert "CATALOG = 'GLUE_CATALOG'" in created("snowflake_iceberg", external)


@pytest.mark.parametrize(
    ("params", "problem"),
    [
        ({"EXTERNAL_VOLUME": "LAKE_VOL"}, "needs BASE_LOCATION"),
        ({"CATALOG": "GLUE_CATALOG"}, "needs an EXTERNAL_VOLUME"),
        ({"CATALOG": "glue-catalog", "EXTERNAL_VOLUME": "V", "BASE_LOCATION": "p"}, "identifier"),
        ({"EXTERNAL_VOLUME": "V'", "BASE_LOCATION": "p"}, "must not contain a quote"),
    ],
)
def test_snowflake_iceberg_storage_problems(params, problem):
    with pytest.raises(HandlerError, match=problem):
        created("snowflake_iceberg", params)


def test_task_storage_problem_reports_without_raising():
    dialect = for_key("snowflake_iceberg")
    assert dialect.task_storage_problem({}) is None
    assert "BASE_LOCATION" in dialect.task_storage_problem({"EXTERNAL_VOLUME": "V"})
    assert for_key("postgres").task_storage_problem({"EXTERNAL_VOLUME": "V"}) is None


def test_snowflake_iceberg_mirrors_need_both_cloning_settings():
    dialect = for_key("snowflake_iceberg")
    cloning = CloningConfig(external_volume="VOL", base_location="clones")
    assert dialect.mirror_table_ddl("m", "id INT", cloning) == (
        "CREATE ICEBERG TABLE m (id INT) EXTERNAL_VOLUME = 'VOL' CATALOG = 'SNOWFLAKE' "
        "BASE_LOCATION = 'clones/m'"
    )
    assert dialect.cloning_storage_problem(CloningConfig()) is not None
    with pytest.raises(ConfigurationError, match="Cloning External_volume, Cloning Base_location"):
        dialect.mirror_table_ddl("m", "id INT", CloningConfig())
    with pytest.raises(ConfigurationError, match="must not contain a quote"):
        dialect.mirror_table_ddl("m", "id INT", replace(cloning, base_location="c'"))


# Profile validation belongs to configuration parsing.


@pytest.mark.parametrize(
    ("warehouse", "error"),
    [
        (profile("key_file", "jdbc:duckdb:w.duckdb", key_file="k"), "DuckDB warehouse takes"),
        (profile("key_file", SNOWFLAKE), "needs key_file"),
        (profile("sts", POSTGRES), "needs region"),
        (profile("token", POSTGRES, user=""), "needs user"),
    ],
)
def test_config_refuses_modes_and_missing_auth_fields(tmp_path, warehouse, error):
    with pytest.raises(ConfigurationError, match=error):
        config_for(warehouse, path=tmp_path / "craft-connector.yml")


def test_an_unsupported_mode_is_refused_when_presenting():
    duckdb = "jdbc:duckdb:w.duckdb"
    with pytest.raises(ConfigurationError, match="not available for a DuckDB warehouse"):
        resolve("duckdb", "native").present(profile("sso", duckdb), "", parse_warehouse_url(duckdb))
    with pytest.raises(ConfigurationError, match="needs a `user`"):
        resolve("postgresql", "native").present_bearer("t", None)


# What each connection hands the driver


def test_a_databricks_token_is_sent_as_the_literal_user_token(captured):
    connect("token", DATABRICKS + ";ConnCatalog=main", "dapi-secret", user="")
    url = captured["url"]
    assert (url.drivername, url.username, url.password) == ("databricks", "token", "dapi-secret")
    assert url.query["http_path"] == "/sql/1.0/w/1"


def test_the_postgres_warehouse_authenticates_like_the_engine_db(captured, minted):
    connect("oauth", POSTGRES, "csecret", client_id="cid", token_url="https://idp/token")
    assert captured["args"] == {"password": "access-token-1"}
    assert captured["url"].password is None
    connect("key_file", POSTGRES, "pp", key_file="/k.pem", cert_file="/c.pem")
    assert captured["args"] == {"sslkey": "/k.pem", "sslpassword": "pp", "sslcert": "/c.pem"}
    # sslmode is in the URL's query, so an sts token's default does not override it.
    connect("password", POSTGRES, "pw")
    assert captured["args"] == {"password": "pw"}


def trino_auth(url: URL):
    from trino.sqlalchemy.dialect import TrinoDialect

    _, kwargs = TrinoDialect().create_connect_args(url)
    return kwargs.get("auth")


def test_trino_token_and_oauth_are_jwts_without_a_user(captured, minted):
    from trino.auth import JWTAuthentication

    connect("token", TRINO, "jwt", user="")
    assert isinstance(trino_auth(captured["url"]), JWTAuthentication)
    connect("oauth", TRINO, "cs", user="", client_id="cid", token_url="https://idp/token")
    assert isinstance(trino_auth(captured["url"]), JWTAuthentication)
    assert captured["url"].query["access_token"] == "access-token-1"


def test_trino_sso_and_key_file_select_the_clients_own_classes(captured):
    from trino.auth import CertificateAuthentication, OAuth2Authentication

    connect("sso", TRINO)
    assert isinstance(trino_auth(captured["url"]), OAuth2Authentication)
    connect("key_file", TRINO, key_file="/k.pem", cert_file="/c.pem")
    assert isinstance(trino_auth(captured["url"]), CertificateAuthentication)


def test_databricks_oauth_uses_the_workspace_token_endpoint_by_default(captured, minted):
    from databricks.sqlalchemy import DatabricksDialect

    connect("oauth", DATABRICKS, "s", user="", client_id="sp")
    assert minted == [("https://adb-1.azuredatabricks.net/oidc/v1/token", "sp", "s", "all-apis")]
    _, kwargs = DatabricksDialect().create_connect_args(captured["url"])
    assert kwargs["access_token"] == "access-token-1"


def test_databricks_sso_asks_the_connector_for_its_browser_login(captured):
    connect("sso", DATABRICKS, user="", client_id="app")
    assert captured["args"] == {"auth_type": "databricks-oauth", "oauth_client_id": "app"}
    assert captured["url"].password is None


def test_snowflake_oauth_is_the_connectors_own_flow(captured, minted):
    connect(
        "oauth", SNOWFLAKE, "csecret", client_id="cid", token_url="https://idp/token", scope="r"
    )
    assert captured["args"] == {
        "authenticator": "OAUTH_CLIENT_CREDENTIALS",
        "oauth_client_id": "cid",
        "oauth_client_secret": "csecret",
        "oauth_token_request_url": "https://idp/token",
        "oauth_scope": "r",
    }
    assert minted == []


def test_snowflake_sso_sts_and_key_pair(captured):
    connect("sso", SNOWFLAKE)
    assert captured["args"] == {"authenticator": "externalbrowser"}
    connect("sts", SNOWFLAKE)
    assert captured["args"] == {
        "authenticator": "WORKLOAD_IDENTITY",
        "workload_identity_provider": "AWS",
    }
    connect("key_file", SNOWFLAKE, "passphrase", key_file="/keys/rsa_key.p8")
    assert captured["args"] == {
        "private_key_file": "/keys/rsa_key.p8",
        "private_key_file_pwd": "passphrase",
    }
    assert captured["url"].password is None
    connect("key_file", SNOWFLAKE, key_file="/keys/rsa_key.p8")
    assert captured["args"] == {"private_key_file": "/keys/rsa_key.p8"}


def test_the_snowflake_connector_accepts_every_argument_the_dialect_sends():
    from snowflake.connector.connection import DEFAULT_CONFIGURATION

    dialect = for_key("snowflake")
    url = parse_warehouse_url(SNOWFLAKE)
    for mode in ("oauth", "sso", "sts", "key_file"):
        warehouse = profile(
            mode, SNOWFLAKE, client_id="c", token_url="https://idp/t", scope="s", key_file="/k"
        )
        presented = dialect.present(warehouse, "secret", url)
        assert set(presented.connect_args) <= set(DEFAULT_CONFIGURATION), mode


def test_a_duckdb_file_is_addressed_by_its_path(captured):
    connect("none", "jdbc:duckdb:/data/warehouse.duckdb")
    assert captured["url"].render_as_string() == "duckdb:////data/warehouse.duckdb"


# Engines


def test_the_engine_url_never_carries_the_secret(monkeypatch):
    monkeypatch.setenv("WH_SECRET", "s3cr3t")
    warehouse = profile("password", POSTGRES, secret_var="WH_SECRET")
    engine = connection.build_warehouse_engine(config_for(warehouse))
    try:
        assert engine.url.password is None
        assert "s3cr3t" not in engine.url.render_as_string(hide_password=False)
        assert engine.url.query == {"sslmode": "require"}
    finally:
        engine.dispose()


def test_a_minted_credential_recycles_pooled_connections(monkeypatch):
    monkeypatch.setenv("WH_SECRET", "s")
    warehouse = profile(
        "oauth", POSTGRES, client_id="c", token_url="https://idp/t", secret_var="WH_SECRET"
    )
    engine = connection.build_warehouse_engine(config_for(warehouse))
    try:
        assert engine.pool._recycle == credentials.MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS
    finally:
        engine.dispose()


def test_parsing_checks_the_profile_first():
    with pytest.raises(ConfigurationError, match="needs region"):
        connection.build_warehouse_engine(config_for(profile("sts", POSTGRES)))


def test_no_warehouse_section():
    config = replace(config_for(profile("none", "jdbc:duckdb:w.duckdb")), warehouse=None)
    with pytest.raises(ConfigurationError, match="no Warehouse section"):
        connection.build_warehouse_engine(config)
    assert connection.is_single_writer(config) is False
    assert connection.is_in_memory(config) is False


@pytest.mark.parametrize(
    ("jdbc_url", "table_format", "single_writer", "in_memory"),
    [
        (POSTGRES, TableFormat.NATIVE, False, False),
        ("jdbc:duckdb:/data/warehouse.duckdb", TableFormat.NATIVE, True, False),
        ("jdbc:duckdb:", TableFormat.NATIVE, True, True),
        ("jdbc:duckdb:", TableFormat.ICEBERG, False, False),
        ("nonsense", TableFormat.NATIVE, False, False),
    ],
)
def test_single_writer_and_in_memory(jdbc_url, table_format, single_writer, in_memory):
    mode = "password" if jdbc_url == POSTGRES else "none"
    config = config_for(
        profile(mode, "jdbc:duckdb:" if jdbc_url == "nonsense" else jdbc_url), table_format
    )
    if jdbc_url == "nonsense":
        config = replace(config, warehouse=profile("none", jdbc_url))
    assert connection.is_single_writer(config) is single_writer
    assert connection.is_in_memory(config) is in_memory


def test_only_trino_has_a_catalog_to_verify():
    class Engine:
        class dialect:  # noqa: N801 - mimics Engine.dialect
            name = "postgresql"

    config = config_for(profile("password", POSTGRES))
    assert connection.verify_iceberg_catalog(config, Engine()) is None


def test_dialect_names_and_defaults():
    postgres, duckdb = for_key("postgres"), for_key("duckdb")
    assert (postgres.spec.sqlalchemy_name, postgres.spec.per_task_format) == ("postgresql", True)
    assert duckdb.spec.per_task_format is False
    assert postgres.load_table_metadata(object(), "s", "t") is None
    assert postgres.cloning_storage_problem(CloningConfig()) is None
    databricks = for_key("databricks")
    assert databricks.create_table_clause() == "USING DELTA"
    assert databricks.audit_column_type("HASH_KEY") == "VARCHAR(32)"


def test_databricks_sso_without_a_client_and_token_through_the_base(captured):
    connect("sso", DATABRICKS, user="")
    assert captured["args"] == {"auth_type": "databricks-oauth"}
    connect("password", "jdbc:snowflake://a.snowflakecomputing.com/?db=D", "pw")
    assert captured["url"].password == "pw"


def test_duckdb_iceberg_secrets():
    from etl_craft.dialects.warehouse.duckdb_iceberg import _catalog_secret, _storage_secret

    # Without a key pair, object storage uses the AWS credential chain.
    assert _storage_secret({}) == ["TYPE S3", "PROVIDER credential_chain"]
    assert _storage_secret({"s3_use_ssl": "yes", "s3_key_id": "k"}) == [
        "TYPE S3",
        "PROVIDER credential_chain",
        "USE_SSL true",
    ]
    assert _catalog_secret("token", {}, "t0k") == ["TOKEN 't0k'"]
    assert _catalog_secret("oauth", {"client_id": "c", "token_url": "u"}, "s") == [
        "CLIENT_ID 'c'",
        "CLIENT_SECRET 's'",
        "OAUTH2_SERVER_URI 'u'",
    ]


def test_duckdb_iceberg_attaches_with_a_token():
    from etl_craft.dialects.warehouse.duckdb_iceberg import DuckDBIcebergWarehouse

    class Cursor:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.cursor_ = Cursor()

        def cursor(self):
            return self.cursor_

    conn = Connection()
    warehouse = profile(
        "token", "jdbc:duckdb:", catalog="lake", catalog_uri="http://c", iceberg_warehouse="s3://w/"
    )
    DuckDBIcebergWarehouse().on_connect(conn, warehouse, "t0k")
    statements = conn.cursor_.statements
    assert "CREATE OR REPLACE SECRET etl_craft_iceberg (TYPE ICEBERG, TOKEN 't0k')" in statements
    assert statements[-1] == (
        "ATTACH IF NOT EXISTS 's3://w/' AS lake (TYPE ICEBERG, ENDPOINT 'http://c', "
        "SECRET etl_craft_iceberg, ACCESS_DELEGATION_MODE 'none', READ_ONLY false)"
    )
    assert conn.begin() is None


def test_verify_iceberg_catalog_without_a_catalog_or_when_the_check_fails():
    from sqlalchemy.exc import OperationalError

    class Trino:
        class dialect:  # noqa: N801 - mimics Engine.dialect
            name = "trino"

        def connect(self):
            raise OperationalError("SELECT", {}, Exception("unreachable"))

    no_catalog = replace(
        config_for(profile("sso", TRINO)),
        warehouse=profile("sso", "jdbc:trino://t:8080/"),
    )
    assert connection.verify_iceberg_catalog(no_catalog, Trino()) is None
    problem = connection.verify_iceberg_catalog(config_for(profile("sso", TRINO)), Trino())
    assert problem.startswith("could not check whether catalog 'iceberg' is an Iceberg catalog")


@pytest.mark.parametrize(
    ("key", "params", "message"),
    [
        ("postgres", {"EXTERNAL_LOCATION": "s3://x"}, "EXTERNAL_LOCATION does not apply to"),
        ("duckdb_iceberg", {"EXTERNAL_LOCATION": "s3://x"}, "storage parameters here: none"),
        (
            "snowflake",
            {"EXTERNAL_VOLUME": "v", "BASE_LOCATION": "b"},
            "EXTERNAL_VOLUME, BASE_LOCATION",
        ),
        (
            "snowflake_iceberg",
            {"EXTERNAL_LOCATION": "s3://x"},
            "here: BASE_LOCATION, CATALOG, EXTERNAL_VOLUME",
        ),
        ("databricks", {"EXTERNAL_LOCATION": "s3://x", "CATALOG": "c"}, "CATALOG does not apply"),
    ],
)
def test_storage_parameters_a_warehouse_would_ignore_are_refused(key, params, message):
    problem = for_key(key).unsupported_storage_problem(params)
    assert problem is not None and message in problem


@pytest.mark.parametrize(
    ("key", "params"),
    [
        ("postgres", {"EXTERNAL_LOCATION": "  "}),
        ("databricks_iceberg", {"EXTERNAL_LOCATION": "s3://x"}),
        ("trino_iceberg", {"EXTERNAL_LOCATION": "s3://x"}),
        ("snowflake_iceberg", {"EXTERNAL_VOLUME": "v", "BASE_LOCATION": "b"}),
    ],
)
def test_storage_parameters_that_apply(key, params):
    assert for_key(key).unsupported_storage_problem(params) is None


def test_a_trino_location_with_a_quote_is_refused():
    problem = for_key("trino_iceberg").task_storage_problem({"EXTERNAL_LOCATION": "s3://a'b"})
    assert problem == 'EXTERNAL_LOCATION must not contain a quote: "s3://a\'b"'


@pytest.mark.parametrize("kind", [d.spec.key for d in all_dialects() if d.spec.key != "generic"])
def test_joined_updates_use_the_warehouse_write_strategy(kind):
    dialect = for_key(kind)
    assignments = {"name": "COALESCE(s.name, t.name)", "UPDATED_BY": ":user"}
    sql = dialect.update_from_stage(
        "db.s.people", "stage", ("id", "region"), assignments, "t.live = 'Y'"
    )
    match = "t.id = s.id AND t.region = s.region"
    values = "name = COALESCE(s.name, t.name), UPDATED_BY = :user"
    if kind.startswith("databricks") or kind == "trino_iceberg":
        assert sql == (
            f"MERGE INTO db.s.people t USING stage s ON {match} "
            f"WHEN MATCHED AND (t.live = 'Y') THEN UPDATE SET {values}"
        )
    else:
        assert (
            sql
            == f"UPDATE db.s.people t SET {values} FROM stage s WHERE {match} AND (t.live = 'Y')"
        )


def test_postgres_prepares_composite_merge_keys_and_stage_statistics():
    assert for_key("postgres").prepare_update_stage("stage", ("id", "region")) == (
        "CREATE INDEX ON stage (id, region)",
        "ANALYZE stage",
    )
    assert for_key("duckdb").prepare_update_stage("stage", ("id",)) == ()


@pytest.mark.parametrize(
    ("source", "target", "same"),
    [
        ("VARCHAR(20)", "VARCHAR(134217728)", True),
        ("ARRAY(VARCHAR(20))", "ARRAY(VARCHAR(134217728))", True),
        ("NUMBER(12,2)", "NUMBER(12,3)", False),
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(9)", False),
        ("ARRAY(VARCHAR(20))", "ARRAY(NUMBER(12,2))", False),
    ],
)
def test_snowflake_iceberg_type_comparison_ignores_only_string_bounds(source, target, same):
    assert for_key("snowflake_iceberg").same_column_type(source, target) == same
    assert for_key("snowflake").same_column_type(source, target) == (source == target)


@pytest.mark.parametrize(
    "column,generated",
    [
        ("row_id BIGINT", False),
        ("`ROW_ID` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)", True),
        ("row_id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY", True),
        ("row_id BIGINT GENERATED ALWAYS AS (id + 1)", False),
    ],
)
def test_databricks_reads_the_existing_identity_instead_of_assuming_one(column, generated):
    from types import SimpleNamespace

    conn = SimpleNamespace(
        execute=lambda statement: SimpleNamespace(
            scalar_one=lambda: f"CREATE TABLE c.s.t ({column})"
        )
    )
    assert for_key("databricks").row_id_generated(conn, "c.s.t") is generated


@pytest.mark.parametrize(
    "default,generated", [(None, False), ("IDENTITY START 1 INCREMENT 1", True)]
)
def test_snowflake_reads_existing_identity_defaults(default, generated):
    from types import SimpleNamespace

    conn = SimpleNamespace(
        execute=lambda statement: SimpleNamespace(
            mappings=lambda: [{"name": "ROW_ID", "default": default}]
        )
    )
    assert for_key("snowflake").row_id_generated(conn, "c.s.t") is generated


@pytest.mark.parametrize(
    "key, data_type, expected",
    [
        ("postgres", "BIGINT", True),
        ("duckdb", "BIGINT", True),
        ("databricks", "bigint", True),
        ("trino_iceberg", "bigint", True),
        ("snowflake", "NUMBER(38,0)", True),
        ("snowflake_iceberg", "NUMBER(19, 0)", True),
        ("snowflake", "NUMBER(38,2)", False),
        ("postgres", "INTEGER", False),
        ("snowflake_iceberg", "VARCHAR", False),
    ],
)
def test_task_run_id_accepts_signed_bigint_representations(key, data_type, expected):
    assert for_key(key).is_bigint_type(data_type) is expected


@pytest.mark.parametrize("key", ["postgres", "duckdb", "snowflake", "databricks", "trino_iceberg"])
def test_failed_session_setup_closes_the_cursor_without_committing(key):
    from unittest.mock import Mock

    conn = Mock()
    conn.cursor.return_value.execute.side_effect = RuntimeError("setup failed")
    with pytest.raises(RuntimeError, match="setup failed"):
        for_key(key).on_connect(conn, None, "")
    conn.cursor.return_value.close.assert_called_once()
    conn.commit.assert_not_called()
