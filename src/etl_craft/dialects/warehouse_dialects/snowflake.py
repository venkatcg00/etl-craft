"""Snowflake, native (ordinary Snowflake) tables.

Verified live through the preferred connection shape (a Programmatic Access
Token). Iceberg tables are a different statement entirely and live in
snowflake_iceberg.py.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl, urlencode

from etl_craft.db import ConnectionError_
from etl_craft.dialects.warehouse_dialects.base import WarehouseDialect

_SNOWFLAKE_URL_RE = re.compile(
    r"^jdbc:snowflake://(?P<host>[^:/?]+)(:(?P<port>\d+))?/?(\?(?P<query>.*))?$"
)


class SnowflakeWarehouse(WarehouseDialect):
    """Snowflake, ordinary tables."""

    key = "snowflake"
    display_name = "Snowflake"
    sqlalchemy_name = "snowflake"
    surrogate_key = "computed"
    enforces_primary_keys = False
    key_file_connect_args = ("private_key_file", "private_key_file_pwd")
    #: Separate connection fields -- the tested and recommended connection shape.
    preferred_fields = ("user", "account", "database", "schema", "warehouse", "role", "token")

    def preferred_connection_url(self, fields: Mapping[str, str]) -> str:
        """Build a credential-free JDBC URL from the separate connection fields."""
        from etl_craft.db import ConnectionError_

        self.require_preferred_fields(fields)
        account = fields["account"]
        if not re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*", account):
            raise ConnectionError_("Snowflake account must be an account identifier, not a URL")
        account = account.removesuffix(".snowflakecomputing.com")
        query = {
            "db": fields["database"],
            "schema": fields["schema"],
            "warehouse": fields["warehouse"],
            "role": fields["role"],
        }
        return f"jdbc:snowflake://{account}.snowflakecomputing.com/?{urlencode(query)}"

    def parse_jdbc(self, jdbc_url: str) -> tuple[str, dict[str, Any]]:
        """Parse Snowflake's account-host JDBC form."""
        return _parse_snowflake(jdbc_url)


def _parse_snowflake(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse Snowflake's account-host JDBC form.

    [ADDITION, 2026-09-22] `jdbc:snowflake://<account>.snowflakecomputing.com/
    ?db=<db>&schema=<schema>&warehouse=<wh>&role=<role>` — the path is empty
    and the database lives in the query string, which the generic parser (it
    requires a non-empty path segment) cannot read.

    The SQLAlchemy dialect takes database and schema as a two-segment
    `database/schema` path and splits them itself — verified against
    `create_connect_args`, which produced `database='MYDB', schema='PUBLIC'`.
    """
    match = _SNOWFLAKE_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized Snowflake JDBC URL: {jdbc_url!r} — expected "
            "jdbc:snowflake://<account>.snowflakecomputing.com/?db=<db>&schema=<schema>"
        )
    query = dict(parse_qsl(match["query"] or ""))
    database = query.pop("db", "") or query.pop("database", "")
    schema = query.pop("schema", "")
    # With a fully qualified host the Snowflake dialect does not derive the
    # required account argument. Preserve the endpoint and supply it explicitly.
    query.setdefault("account", match["host"].split(".", 1)[0])
    if not database:
        raise ConnectionError_(
            f"Snowflake JDBC URL {jdbc_url!r} has no db= parameter — it names the database "
            "qualify() resolves schema.table against"
        )
    return "snowflake", {
        "host": match["host"],
        "port": int(match["port"]) if match["port"] else None,
        "database": f"{database}/{schema}" if schema else database,
        # The catalog half of qualify()'s three-part name.
        "catalog": database,
        "query": query,
    }
