"""Warehouse dialects and connections without a warehouse: registry, SQL fragments, auth."""

from dataclasses import replace

import pytest
from sqlalchemy.engine import URL

from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    SourceConfig,
)
from etl_craft.config.targets import parse_warehouse_url
from etl_craft.core.enums import Mode, TableFormat
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.dialects import credentials
from etl_craft.dialects.warehouse import all_dialects, for_key, resolve
from etl_craft.dialects.warehouse.registry import GenericWarehouse
from etl_craft.warehouse import connection

pytestmark = pytest.mark.unit

POSTGRES = "jdbc:postgresql://wh:5432/analytics?sslmode=require"
TRINO = "jdbc:trino://trino.internal:8443/iceberg/analytics"
DATABRICKS = "jdbc:databricks://adb-1.azuredatabricks.net:443/default;httpPath=/sql/1.0/w/1"
SNOWFLAKE = "jdbc:snowflake://org-acct.snowflakecomputing.com/?db=ANALYTICS&schema=PUBLIC"


def profile(auth_mode, jdbc_url, user="etl", **extra):
    return ConnectionProfile("WAREHOUSE", "dev", jdbc_url, user, auth_mode, extra)


def config_for(warehouse_profile, table_format=TableFormat.NATIVE):
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "none")
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        warehouse=ConnectionSection("dev", {"dev": warehouse_profile}),
        warehouse_table_format=table_format,
    )


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
    dialect.check_profile(warehouse)
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
    assert resolve(name, table_format).key == key


def test_a_database_without_a_dialect_is_generic_ansi():
    dialect = resolve("oracle", "native")
    assert isinstance(dialect, GenericWarehouse)
    assert (dialect.key, dialect.surrogate_key, dialect.enforces_primary_keys) == (
        "oracle",
        "computed",
        False,
    )


def test_postgres_has_no_iceberg_dialect():
    with pytest.raises(ConfigurationError, match="always native"):
        resolve("postgresql", "iceberg")


def test_every_dialect_is_registered_once_with_its_spec():
    keys = [dialect.key for dialect in all_dialects()]
    assert len(keys) == len(set(keys)) == 8
    for dialect in all_dialects():
        assert for_key(dialect.key) is dialect
        assert dialect.table_format == dialect.spec.table_format
    with pytest.raises(LookupError):
        for_key("oracle")


@pytest.mark.parametrize(
    ("key", "single_writer", "surrogate_key", "primary_keys", "temporary"),
    [
        ("postgres", False, "identity", True, True),
        ("duckdb", True, "sequence", True, True),
        ("duckdb_iceberg", False, "computed", False, True),
        ("trino_iceberg", False, "computed", False, False),
        ("databricks", False, "computed", False, False),
        ("databricks_iceberg", False, "computed", False, False),
        ("snowflake", False, "computed", False, True),
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
    assert for_key("postgres").hash_expression(["a", "b"]) == (
        "MD5(COALESCE(CAST(a AS VARCHAR), '') || '|' || COALESCE(CAST(b AS VARCHAR), ''))"
    )
    assert for_key("databricks").hash_expression(["a"]) == "MD5(COALESCE(CAST(a AS STRING), ''))"
    assert for_key("trino_iceberg").hash_expression(["a"]) == (
        "lower(to_hex(md5(to_utf8(COALESCE(CAST(a AS VARCHAR), '')))))"
    )


def test_audit_column_types():
    assert for_key("postgres").audit_column_type("CREATE_DATE") == "TIMESTAMP WITH TIME ZONE"
    assert for_key("databricks").audit_column_type("UPDATE_DATE") == "TIMESTAMP"
    assert for_key("snowflake_iceberg").audit_column_type("CREATE_DATE") == "TIMESTAMP_NTZ(6)"
    assert for_key("snowflake_iceberg").audit_column_type("HASH_KEY") == "VARCHAR(32)"


def test_alter_keywords_and_scalar_values():
    assert for_key("snowflake_iceberg").alter_table_keyword() == "ALTER ICEBERG TABLE"
    assert for_key("snowflake").alter_table_keyword() == "ALTER TABLE"
    assert for_key("databricks").scalar_source_value("x") == "FIRST(x)"
    assert for_key("postgres").scalar_source_value("x") == "x"
    assert for_key("snowflake_iceberg").scalar_source_value("x") == "ANY_VALUE(x)"


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


# Checking a profile before connecting


def test_check_profile_refuses_modes_and_missing_fields():
    with pytest.raises(ConfigurationError, match="not available for a DuckDB warehouse"):
        for_key("duckdb").check_profile(profile("key_file", "jdbc:duckdb:w.duckdb", key_file="k"))
    with pytest.raises(ConfigurationError, match="requires a `key_file:` path"):
        for_key("snowflake").check_profile(profile("key_file", SNOWFLAKE))
    with pytest.raises(ConfigurationError, match="requires a `region:` value"):
        for_key("postgres").check_profile(profile("sts", POSTGRES))
    with pytest.raises(ConfigurationError, match="needs a `user`"):
        resolve("mysql", "native").check_profile(profile("token", "jdbc:mysql://h/db", user=""))


def test_an_unsupported_mode_is_refused_when_presenting():
    with pytest.raises(ConfigurationError, match="not available for a mysql warehouse"):
        resolve("mysql", "native").present(
            profile("sso", "jdbc:mysql://h/db"), "", parse_warehouse_url("jdbc:mysql://h/db")
        )
    with pytest.raises(ConfigurationError, match="needs a `user`"):
        resolve("mysql", "native").present_bearer("t", None)


# What each connection hands the driver


def test_password_goes_into_the_connection_url_only(captured):
    connect("password", "jdbc:mysql://myhost:3306/mydb", "s3cr3t")
    url = captured["url"]
    assert (url.drivername, url.username, url.password) == ("mysql+pymysql", "etl", "s3cr3t")
    assert (url.host, url.port, url.database) == ("myhost", 3306, "mydb")


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


def test_building_checks_the_profile_first():
    with pytest.raises(ConfigurationError, match="requires a `region:`"):
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
    config = config_for(profile("none", jdbc_url), table_format)
    assert connection.is_single_writer(config) is single_writer
    assert connection.is_in_memory(config) is in_memory


def test_only_trino_has_a_catalog_to_verify():
    class Engine:
        class dialect:  # noqa: N801 - mimics Engine.dialect
            name = "postgresql"

    config = config_for(profile("none", POSTGRES))
    assert connection.verify_iceberg_catalog(config, Engine()) is None


def test_dialect_names_and_defaults():
    postgres, duckdb = for_key("postgres"), for_key("duckdb")
    assert (postgres.sqlalchemy_name, postgres.per_task_format) == ("postgresql", True)
    assert duckdb.per_task_format is False
    assert postgres.load_table_metadata(object(), "s", "t") is None
    assert postgres.on_connect(object(), profile("none", POSTGRES), "") is None
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

    no_catalog = config_for(profile("none", "jdbc:trino://t:8080/"))
    assert connection.verify_iceberg_catalog(no_catalog, Trino()) is None
    problem = connection.verify_iceberg_catalog(config_for(profile("none", TRINO)), Trino())
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
