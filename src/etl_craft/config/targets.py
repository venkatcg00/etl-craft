"""What a warehouse connection points at: its JDBC URL's parts, its dialect and its catalog.

Most warehouses use the ``jdbc:<scheme>://host[:port]/database[?query]`` form. DuckDB names a
file, Databricks puts semicolon parameters after the path, and Snowflake keeps the database in
the query string, so each of those has its own parser here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from etl_craft.config.auth import WAREHOUSE_NAMES, WarehouseSpec, warehouse_spec
from etl_craft.config.model import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import is_safe_identifier, jdbc_scheme, parse_jdbc_url

# SQLAlchemy dialect names, with their driver, for schemes that differ from the scheme itself.
_SQLALCHEMY_DIALECTS = {"postgresql": "postgresql+psycopg", "mysql": "mysql+pymysql"}

_DUCKDB_URL = re.compile(r"^jdbc:duckdb:(?P<path>.*)$")
_DATABRICKS_URL = re.compile(
    r"^jdbc:databricks://(?P<host>[^:/;]+)(:(?P<port>\d+))?"
    r"(/(?P<schema>[^;]*))?(;(?P<params>.*))?$"
)
_SNOWFLAKE_URL = re.compile(
    r"^jdbc:snowflake://(?P<host>[^:/?]+)(:(?P<port>\d+))?/?(\?(?P<query>.*))?$"
)
_SNOWFLAKE_ACCOUNT = re.compile(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*")
# The Databricks parameters that name what to connect to; every other one (AuthMech, UID, PWD)
# is a credential or a JDBC-driver setting, and is dropped.
_DATABRICKS_PUBLIC_PARAMS = frozenset(
    {"httppath", "transportmode", "ssl", "conncatalog", "connschema", "catalog", "schema"}
)


@dataclass(frozen=True)
class WarehouseUrl:
    """The parts of a warehouse JDBC URL.

    ``dialect`` is the SQLAlchemy dialect name, with its driver where one is implied.
    ``database`` is what the SQLAlchemy URL's database becomes: ``catalog/schema`` for Trino and
    Snowflake. ``catalog`` is the first part of every ``catalog.schema.table`` name. ``path`` is
    a DuckDB file, or ``:memory:``.
    """

    dialect: str
    host: str | None
    port: int | None
    database: str
    catalog: str
    query: dict[str, str] = field(default_factory=dict)
    path: str | None = None


def parse_warehouse_url(jdbc_url: str) -> WarehouseUrl:
    """Split a warehouse JDBC URL into its parts; ``ConfigurationError`` if it is malformed."""
    scheme = jdbc_scheme(jdbc_url)
    if scheme == "duckdb":
        return _parse_duckdb(jdbc_url)
    if scheme == "databricks":
        return _parse_databricks(jdbc_url)
    if scheme == "snowflake":
        return _parse_snowflake(jdbc_url)
    url = parse_jdbc_url(jdbc_url)
    return WarehouseUrl(
        dialect=_SQLALCHEMY_DIALECTS.get(url.scheme, url.scheme),
        host=url.host,
        port=url.port,
        database=url.database,
        catalog=url.catalog,
        query=url.query,
    )


def _parse_duckdb(jdbc_url: str) -> WarehouseUrl:
    """Parse ``jdbc:duckdb:<path>``, or bare ``jdbc:duckdb:`` for an in-memory database.

    The catalog is the file's stem (``memory`` in memory), and it is written unquoted into
    ``catalog.schema.table``, so a stem that is not a plain identifier is refused here, where
    it is derived.
    """
    match = _DUCKDB_URL.match(jdbc_url)
    if match is None:
        raise ConfigurationError(f"not a recognized DuckDB JDBC URL: {jdbc_url!r}")
    path = match["path"]
    if not path:
        return WarehouseUrl(
            dialect="duckdb",
            host=None,
            port=None,
            database="memory",
            catalog="memory",
            path=":memory:",
        )
    stem = Path(path).stem
    if not is_safe_identifier(stem):
        raise ConfigurationError(
            f"DuckDB warehouse file {path!r} gives the catalog name {stem!r}, which is not "
            "a usable SQL identifier — it is interpolated unquoted into "
            "database.schema.table. Rename the file to use only letters, digits and "
            "underscores, starting with a letter or underscore."
        )
    return WarehouseUrl(
        dialect="duckdb", host=None, port=None, database=stem, catalog=stem, path=path
    )


def _parse_databricks(jdbc_url: str) -> WarehouseUrl:
    """Parse ``jdbc:databricks://<host>:443/<schema>;httpPath=...;ConnCatalog=...``.

    Only what the SQLAlchemy dialect uses is kept (``http_path``, ``catalog``, ``schema``);
    driver and credential parameters are dropped, so a token in ``PWD`` never travels on.
    """
    match = _DATABRICKS_URL.match(jdbc_url)
    if match is None:
        raise ConfigurationError(
            f"not a recognized Databricks JDBC URL: {jdbc_url!r} — expected "
            "jdbc:databricks://<host>:443/<schema>;httpPath=/sql/1.0/warehouses/<id>"
        )
    params: dict[str, str] = {}
    for chunk in (match["params"] or "").split(";"):
        if "=" in chunk:
            key, _, value = chunk.partition("=")
            params[key.strip().lower()] = value.strip()
    http_path = params.get("httppath")
    if not http_path:
        raise ConfigurationError(
            f"Databricks JDBC URL {jdbc_url!r} has no httpPath — it names the SQL warehouse "
            "or cluster to run against (e.g. httpPath=/sql/1.0/warehouses/<id>)"
        )
    query = {"http_path": http_path}
    catalog = params.get("conncatalog") or params.get("catalog")
    schema = params.get("connschema") or params.get("schema") or match["schema"]
    if catalog:
        query["catalog"] = catalog
    if schema and schema != "default":
        query["schema"] = schema
    return WarehouseUrl(
        dialect="databricks",
        host=match["host"],
        port=int(match["port"]) if match["port"] else None,
        database=catalog or "",
        catalog=catalog or "",
        query=query,
    )


def _parse_snowflake(jdbc_url: str) -> WarehouseUrl:
    """Parse ``jdbc:snowflake://<account>.snowflakecomputing.com/?db=<db>&schema=<schema>``.

    The SQLAlchemy dialect takes ``database/schema`` as one path and splits it itself; the
    account is the first label of the host unless the query names it.
    """
    match = _SNOWFLAKE_URL.match(jdbc_url)
    if match is None:
        raise ConfigurationError(
            f"not a recognized Snowflake JDBC URL: {jdbc_url!r} — expected "
            "jdbc:snowflake://<account>.snowflakecomputing.com/?db=<db>&schema=<schema>"
        )
    query = dict(parse_qsl(match["query"] or ""))
    database = query.pop("db", "") or query.pop("database", "")
    schema = query.pop("schema", "")
    query.setdefault("account", match["host"].split(".", 1)[0])
    if not database:
        raise ConfigurationError(
            f"Snowflake JDBC URL {jdbc_url!r} has no db= parameter — it names the database "
            "every schema.table resolves against"
        )
    return WarehouseUrl(
        dialect="snowflake",
        host=match["host"],
        port=int(match["port"]) if match["port"] else None,
        database=f"{database}/{schema}" if schema else database,
        catalog=database,
        query=query,
    )


def strip_databricks_credentials(jdbc_url: str) -> str:
    """Drop credential and driver parameters from a Databricks JDBC URL.

    The URL Databricks' "Connection Details" page offers includes ``AuthMech``, ``UID`` and a
    real token in ``PWD``; what is left is safe to keep.
    """
    prefix, sep, params_blob = jdbc_url.partition(";")
    if not sep:
        return jdbc_url
    kept = [
        chunk
        for chunk in params_blob.split(";")
        if chunk.partition("=")[0].strip().lower() in _DATABRICKS_PUBLIC_PARAMS
    ]
    return prefix + (";" + ";".join(kept) if kept else "")


def preferred_connection_url(name: str, fields: Mapping[str, str]) -> str:
    """Build a credential-free JDBC URL from Databricks' or Snowflake's separate fields.

    ``name`` is ``Warehouse.Name``. Every preferred field except the token must be present.
    """
    dialect = WAREHOUSE_NAMES.get(name.lower())
    if dialect == "databricks":
        _require(fields, ("jdbc_url", "catalog", "schema"), "Databricks")
        for key in ("catalog", "schema"):
            if not is_safe_identifier(fields[key]):
                raise ConfigurationError(f"Databricks {key} must be an unquoted SQL identifier")
        if not fields["jdbc_url"].startswith("jdbc:databricks://"):
            raise ConfigurationError("Databricks jdbc_url must start with jdbc:databricks://")
        # The separate fields come last, so they win over any defaults in the copied URL.
        return (
            strip_databricks_credentials(fields["jdbc_url"]).rstrip(";")
            + f";ConnCatalog={fields['catalog']};ConnSchema={fields['schema']}"
        )
    if dialect == "snowflake":
        _require(
            fields, ("user", "account", "database", "schema", "warehouse", "role"), "Snowflake"
        )
        account = fields["account"]
        if not _SNOWFLAKE_ACCOUNT.fullmatch(account):
            raise ConfigurationError("Snowflake account must be an account identifier, not a URL")
        account = account.removesuffix(".snowflakecomputing.com")
        query = {
            "db": fields["database"],
            "schema": fields["schema"],
            "warehouse": fields["warehouse"],
            "role": fields["role"],
        }
        return f"jdbc:snowflake://{account}.snowflakecomputing.com/?{urlencode(query)}"
    raise ConfigurationError("separate connection fields require Databricks or Snowflake")


def _require(fields: Mapping[str, str], keys: tuple[str, ...], label: str) -> None:
    for key in keys:
        if not fields.get(key):
            raise ConfigurationError(f"{label} connection requires {key}")


def active_warehouse(config: ConnectorConfig) -> WarehouseSpec:
    """Return the dialect the active warehouse profile and the default table format select."""
    if config.warehouse is None:
        raise ConfigurationError("no Warehouse section configured in craft-connector.yml")
    url = parse_warehouse_url(config.warehouse.active.jdbc_url)
    return warehouse_spec(url.dialect, config.warehouse_table_format)


def active_catalog(config: ConnectorConfig) -> str:
    """Return the catalog the active warehouse profile writes into.

    It is the first part of every ``catalog.schema.table`` name the engine builds; the schema
    and table come from ``CFG_`` rows. DuckDB over Iceberg names it in the profile's
    ``catalog``; every other warehouse names it in its URL.
    """
    spec = active_warehouse(config)
    assert config.warehouse is not None
    profile = config.warehouse.active
    if spec.key == "duckdb_iceberg":
        catalog = str(profile.extra.get("catalog") or "").strip()
        if not is_safe_identifier(catalog):
            raise ConfigurationError(
                "a DuckDB Iceberg warehouse needs `catalog` — the name the Iceberg catalog is "
                f"attached as, a plain SQL identifier — got {catalog!r}"
            )
        return catalog
    catalog = parse_warehouse_url(profile.jdbc_url).catalog
    if not catalog:
        raise ConfigurationError(
            "the active warehouse profile's jdbc_url names no catalog/database, so "
            "TARGET_OBJECT's schema.table cannot be resolved to a full name — add one "
            "(e.g. ConnCatalog=<catalog> for Databricks, db=<database> for Snowflake)"
        )
    return catalog
