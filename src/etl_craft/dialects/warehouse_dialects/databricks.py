"""Databricks SQL, native (Delta) tables.

Every difference below was found against a real Databricks SQL warehouse:
temporary tables collide with DROP on the same name, the session has no
default schema, window functions need an ordering, audit timestamps are plain
TIMESTAMP, text casts to STRING, and a correlated scalar subquery must be
provably single-valued (FIRST) even after the source is deduplicated.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from etl_craft.db import ConnectionError_
from etl_craft.dialects.warehouse_dialects.base import SAFE_IDENTIFIER, WarehouseDialect

_DATABRICKS_URL_RE = re.compile(
    r"^jdbc:databricks://(?P<host>[^:/;]+)(:(?P<port>\d+))?"
    r"(/(?P<schema>[^;]*))?(;(?P<params>.*))?$"
)
_SAFE_CATALOG = SAFE_IDENTIFIER


class DatabricksWarehouse(WarehouseDialect):
    """Databricks, Delta tables."""

    key = "databricks"
    display_name = "Databricks"
    sqlalchemy_name = "databricks"
    temporary_tables = False
    default_schema = False
    qualified_rename = True
    surrogate_key = "computed"
    enforces_primary_keys = False
    string_type = "STRING"
    token_username = "token"
    #: Separate connection fields -- the tested and recommended connection shape.
    preferred_fields = ("jdbc_url", "catalog", "schema", "token")

    def preferred_connection_url(self, fields: Mapping[str, str]) -> str:
        """Build a credential-free JDBC URL from the separate connection fields."""
        from etl_craft.db import ConnectionError_

        self.require_preferred_fields(fields)
        for key in ("catalog", "schema"):
            if not _SAFE_CATALOG.fullmatch(fields[key]):
                raise ConnectionError_(f"Databricks {key} must be an unquoted SQL identifier")
        if not fields["jdbc_url"].startswith("jdbc:databricks://"):
            raise ConnectionError_("Databricks jdbc_url must start with jdbc:databricks://")
        # [ADDITION, 2026-09-23, E3-08] Databricks' own "Connection Details"
        # JDBC tab hands out a URL that already contains AuthMech/UID/PWD -- a
        # real personal access token, in cleartext. Stripped here, where the
        # URL is built, so the result is credential-free whatever was pasted.
        # The real token reaches the connection through the separate `token`
        # field, never through this URL.
        sanitized_url = _strip_databricks_credentials(fields["jdbc_url"])
        # The separate fields take precedence over defaults in the copied URL.
        return (
            sanitized_url.rstrip(";")
            + f";ConnCatalog={fields['catalog']};ConnSchema={fields['schema']}"
        )

    def parse_jdbc(self, jdbc_url: str) -> tuple[str, dict[str, Any]]:
        """Parse Databricks' semicolon-parameter JDBC form."""
        return _parse_databricks(jdbc_url)

    def create_table_clause(self) -> str:
        """Name Delta explicitly rather than relying on the workspace default."""
        return "USING DELTA"

    def audit_column_type(self, column: str) -> str:
        """Use Databricks' TIMESTAMP (always UTC-normalized) for audit instants."""
        if column in {"CREATE_DATE", "UPDATE_DATE"}:
            return "TIMESTAMP"
        return super().audit_column_type(column)

    def scalar_source_value(self, expression: str) -> str:
        """Satisfy Spark's scalar-subquery cardinality rule, after source-key dedupe."""
        return f"FIRST({expression})"


_DATABRICKS_PUBLIC_PARAMS = frozenset(
    {"httppath", "transportmode", "ssl", "conncatalog", "connschema", "catalog", "schema"}
)


def _strip_databricks_credentials(jdbc_url: str) -> str:
    """Remove credential-bearing JDBC parameters from a Databricks connection string.

    Databricks' own "Connection Details" UI presents a JDBC URL that already
    includes `AuthMech`/`UID`/`PWD` — a real personal access token in
    cleartext — as *the* string to copy. This makes any URL built from one
    of those genuinely safe to persist, regardless of what a caller pasted.
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


def _parse_databricks(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse Databricks' semicolon-parameter JDBC form.

    [ADDITION, 2026-09-22] `jdbc:databricks://<host>:443/<schema>;httpPath=...;
    ConnCatalog=...` — semicolon-separated parameters after the path, not a
    query string, so the generic parser cannot read it.

    Only the parameters the SQLAlchemy dialect actually consumes are carried
    over (`http_path`, `catalog`, `schema`, verified against
    `create_connect_args`). Transport/auth parameters a JDBC driver needs and
    this one does not — `AuthMech`, `transportMode`, `ssl`, `UID`, `PWD` — are
    dropped rather than passed through, since `PWD` in particular would put
    the token in the URL, which this module goes out of its way to avoid.
    """
    match = _DATABRICKS_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
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
        raise ConnectionError_(
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
    return "databricks", {
        "host": match["host"],
        "port": int(match["port"]) if match["port"] else None,
        # qualify()'s three-part name needs the Unity Catalog catalog here.
        "database": catalog or "",
        "query": query,
    }
