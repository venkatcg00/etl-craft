"""Snowflake warehouse, ordinary tables."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import text
from sqlalchemy.dialects import registry
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import AuthMode, FailureClass, TableFormat
from etl_craft.core.errors import SqlGuardError
from etl_craft.core.failures import Failure
from etl_craft.dialects.failure_evidence import driver_error, sqlstate
from etl_craft.dialects.warehouse.base import (
    Presented,
    ReplaceStrategy,
    SurrogateKey,
    WarehouseDialect,
)
from etl_craft.dialects.warehouse.metadata import TableMetadata, describe_result
from etl_craft.dialects.warehouse.replacement import identity_replacement, replacement_ddl

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl

registry.register("snowflake.etl_craft", "etl_craft.dialects.warehouse.snowflake_driver", "dialect")


class SnowflakeWarehouse(WarehouseDialect):
    """Snowflake, ordinary tables."""

    session_fencing = True

    def archive_type_matches(self, actual: str, expected: str) -> bool:
        """Compare Snowflake's physical spellings for the archive's native DDL aliases."""
        aliases = {
            "BIGINT": "NUMBER(38,0)",
            "VARCHAR": "VARCHAR(16777216)",
            "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ(9)",
        }
        return self.same_column_type(actual, aliases.get(expected.upper(), expected))

    def session_tag_args(self, tag: str) -> Mapping[str, Any]:
        """Label every query of the session through its QUERY_TAG session parameter."""
        return {"session_parameters": {"QUERY_TAG": tag}}

    _UNFINISHED = (
        "SELECT query_id AS query_id FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY("
        "RESULT_LIMIT => 10000)) WHERE query_tag = :tag AND execution_status NOT IN "
        "('SUCCESS', 'FAILED_WITH_ERROR', 'FAILED_WITH_INCIDENT', 'ABORTED')"
    )

    def stop_tagged(self, conn: Connection, tag: str) -> None:
        """Cancel each unfinished query labelled ``tag``."""
        for row in conn.execute(text(self._UNFINISHED), {"tag": tag}).all():
            conn.execute(text("SELECT SYSTEM$CANCEL_QUERY(:id) AS cancelled"), {"id": row.query_id})

    def running_tagged(self, conn: Connection, tag: str) -> int:
        """Count unfinished queries; a status this code does not know counts as running."""
        return len(conn.execute(text(self._UNFINISHED), {"tag": tag}).all())

    def classify_failure(self, error: BaseException) -> Failure:
        """SQLSTATE and compile errors precede Snowflake's general network classes."""
        error = driver_error(error)
        failure = super().classify_failure(error)
        if getattr(error, "errno", None) in {1003, 2003, 2043}:
            return replace(failure, kind=FailureClass.PERMANENT)
        return failure

    timestamp_default_precision = 9
    hash_metadata_columns = (
        "column_name, CASE WHEN data_type LIKE 'TIMESTAMP%' THEN "
        "data_type || '(' || datetime_precision || ')' ELSE data_type END AS data_type, "
        "numeric_precision, numeric_scale"
    )

    spec = warehouse_by_key("snowflake")
    sqlalchemy_driver = "snowflake+etl_craft"
    catalog_columns = False
    metadata_types_complete = True
    dml_rowcounts = True

    def describe_select(self, conn: Connection, sql: str) -> list[tuple[str, str]] | None:
        """Read scalar source types from a zero-row result without creating a stage."""
        if self.surrogate_key == "computed":
            return None
        return describe_result(conn, self.event_probe_sql(sql, 0), self.result_type)

    def result_type(self, column: object) -> str | None:
        """Keep driver-declared sizes and scales; structured types need materialized metadata."""
        if getattr(column, "fields", None):
            return None
        code = getattr(column, "type_code", None)
        if code == 0:
            precision, scale = getattr(column, "precision", None), getattr(column, "scale", None)
            return (
                f"NUMBER({precision},{scale})"
                if precision is not None and scale is not None
                else None
            )
        if code == 2:
            size = getattr(column, "internal_size", None)
            return f"VARCHAR({size})" if size is not None else None
        if not isinstance(code, int):
            return None
        return {
            1: "FLOAT",
            3: "DATE",
            4: "TIMESTAMP",
            5: "VARIANT",
            6: "TIMESTAMP_LTZ",
            7: "TIMESTAMP_TZ",
            8: "TIMESTAMP_NTZ",
            9: "OBJECT",
            10: "ARRAY",
            11: "BINARY",
            12: "TIME",
            13: "BOOLEAN",
            14: "GEOGRAPHY",
            15: "GEOMETRY",
        }.get(code)

    def relation_types_query(self, tables: tuple[str, ...]) -> tuple[str, dict[str, str | None]]:
        """Read relations sharing one schema in a single information_schema scan.

        Snowflake narrows its metadata lookup for ``ILIKE`` filters but not for ``lower()``
        comparisons; escaped patterns keep the case-insensitive exact match of the base query.
        """
        split = [table.split(".") for table in tables]
        if len(split[0]) < 2 or len({tuple(parts[:-1]) for parts in split}) != 1:
            return super().relation_types_query(tables)
        namespace = split[0]
        parameters: dict[str, str | None] = {"schema": _like_literal(namespace[-2])}
        where = "table_schema ILIKE :schema ESCAPE '!'"
        if len(namespace) == 3:
            parameters["catalog"] = _like_literal(namespace[0])
            where += " AND table_catalog ILIKE :catalog ESCAPE '!'"
        matches, positions = [], []
        for index, parts in enumerate(split):
            parameters[f"table_{index}"] = _like_literal(parts[-1])
            parameters[f"exact_{index}"] = parts[-1]
            matches.append(f"table_name ILIKE :table_{index} ESCAPE '!'")
            positions.append(f"WHEN lower(table_name) = lower(:exact_{index}) THEN {index}")
        query = (
            f"SELECT position, table_type, CASE WHEN is_iceberg='YES' THEN 'ICEBERG' "
            f"WHEN is_iceberg='NO' THEN 'NATIVE' END AS table_format "
            f"FROM (SELECT CASE {' '.join(positions)} END AS position, "
            f"table_type, is_iceberg FROM information_schema.tables WHERE {where} "
            f"AND ({' OR '.join(matches)})) relation WHERE position IS NOT NULL"
        )
        return query, parameters

    def read_metadata(self, conn: Connection, table: str) -> TableMetadata:
        """DESCRIBE supplies complete types, nullability and identity defaults together."""
        try:
            rows = conn.execute(text(f"DESCRIBE TABLE {table}")).mappings().all()
        except SQLAlchemyError as error:
            if sqlstate(driver_error(error)) == "42S02":
                return TableMetadata(())
            raise
        return TableMetadata(
            tuple(
                (
                    str(row["name"]),
                    str(row["type"]),
                    None,
                    None,
                    "NO" if row["null?"] == "N" else "YES",
                )
                for row in rows
            ),
            generated=any(
                str(row["name"]).lower() == "row_id"
                and bool(re.search(r"AUTOINCREMENT|IDENTITY", str(row["default"]), re.I))
                for row in rows
            ),
        )

    sqlglot_dialect = "snowflake"
    replace_strategy: ReplaceStrategy = "create_or_replace"
    identifier_case = "upper"
    qualified_rename = True
    surrogate_key: SurrogateKey = "identity"
    identity_in_create = True
    enforces_primary_keys = False
    key_file_connect_args = ("private_key_file", "private_key_file_pwd")
    secure_view = "keyword"
    view_replacement = "copy_grants"

    def reject_checks(self, value: str, kind: str) -> list[tuple[str, str]]:
        """Cast the value to text first: Snowflake's TRY_CAST reads text only."""
        return super().reject_checks(f"CAST({value} AS VARCHAR)", kind)

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Use an ordered native identity; inserts omit the managed key."""
        return (
            f"CREATE TABLE {target} ({columns}, "
            "ROW_ID BIGINT AUTOINCREMENT START 1 INCREMENT 1 ORDER)"
        )

    def identity_replacement_ddl(
        self,
        conn: Connection,
        target: str,
        candidate: str,
        columns: str,
        params: Mapping[str, str],
        *,
        existing: bool,
    ) -> tuple[str, str]:
        """Clone a fully populated identity candidate, preserving the target's grants."""
        ddl = (
            str(
                conn.execute(
                    text("SELECT GET_DDL('TABLE', :target)"), {"target": target}
                ).scalar_one()
            )
            if existing
            else self.identity_table_ddl(target, columns, params)
        )
        return identity_replacement(
            ddl,
            target,
            candidate,
            f"{columns}, ROW_ID BIGINT AUTOINCREMENT START 1 INCREMENT 1 ORDER",
            "snowflake",
        )

    def is_bigint_type(self, data_type: str) -> bool:
        """Recognize native and Iceberg BIGINT's Snowflake numeric representations."""
        return data_type.upper().replace(" ", "") in {"BIGINT", "NUMBER(38,0)", "NUMBER(19,0)"}

    def existing_table_format(self, conn: Connection, target: str) -> TableFormat:
        """Read Snowflake's native/Iceberg flag in the target's own database."""
        catalog, schema, table = target.split(".")
        flag = str(
            conn.execute(
                text(
                    f"SELECT is_iceberg FROM {catalog}.information_schema.tables "
                    "WHERE lower(table_schema) = lower(:schema) "
                    "AND lower(table_name) = lower(:table)"
                ),
                {"schema": schema, "table": table},
            ).scalar_one()
        ).upper()
        if flag not in {"YES", "NO"}:
            raise SqlGuardError(f"{target}: cannot determine table format from IS_ICEBERG={flag!r}")
        return TableFormat.ICEBERG if flag == "YES" else TableFormat.NATIVE

    def row_id_generated(self, conn: Connection, target: str) -> bool:
        """Recognize actual native identity metadata, including targets created before this run."""
        rows = conn.execute(text(f"DESCRIBE TABLE {target}")).mappings()
        return any(
            str(row["name"]).lower() == "row_id"
            and bool(re.search(r"AUTOINCREMENT|IDENTITY", str(row["default"]), re.I))
            for row in rows
        )

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Name the connector's own authenticator for oauth, sso and sts."""
        user = profile.user or None
        mode = profile.auth_mode
        if mode == AuthMode.OAUTH:
            connect_args: dict[str, Any] = {
                "authenticator": "OAUTH_CLIENT_CREDENTIALS",
                "oauth_client_id": str(profile.extra["client_id"]),
                "oauth_client_secret": secret,
                "oauth_token_request_url": str(profile.extra["token_url"]),
            }
            if profile.extra.get("scope"):
                connect_args["oauth_scope"] = str(profile.extra["scope"])
            return Presented(username=user, connect_args=connect_args)
        if mode == AuthMode.SSO:
            return Presented(username=user, connect_args={"authenticator": "externalbrowser"})
        if mode == AuthMode.STS:
            return Presented(
                username=user,
                connect_args={
                    "authenticator": "WORKLOAD_IDENTITY",
                    "workload_identity_provider": "AWS",
                },
            )
        return super().present(profile, secret, url)

    def timestamp_text(self, value: str, kind: str) -> str:
        """Format UTC instants and naive UTC values without session format defaults."""
        utc = (
            f"CONVERT_TIMEZONE('UTC', {value})"
            if kind.startswith(("TIMESTAMP_TZ", "TIMESTAMP_LTZ"))
            else value
        )
        return f"TO_CHAR(CAST({utc} AS TIMESTAMP_NTZ), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')"

    def date_text(self, value: str) -> str:
        """Ignore DATE_OUTPUT_FORMAT."""
        return f"TO_CHAR({value}, 'YYYY-MM-DD')"

    def decimal_text(self, value: str, kind: str) -> str:
        """Keep every declared fractional digit without integer padding."""
        precision, scale = [int(n) for n in kind[kind.index("(") + 1 : -1].split(",")]
        model = "FM" + "9" * max(precision - scale - 1, 0) + "0"
        if scale:
            model += "." + "0" * scale
        return f"TO_CHAR(CAST({value} AS {kind}), '{model}')"

    session_connect_args: ClassVar[Mapping[str, Any]] = {
        "timezone": "UTC",
        "session_parameters": {"TIMESTAMP_OUTPUT_FORMAT": 'YYYY-MM-DD"T"HH24:MI:SS.FF6'},
    }

    def replacement_ddl(
        self,
        conn: Connection,
        target: str,
        select_sql: str,
        params: Mapping[str, str],
        *,
        existing: bool,
    ) -> str:
        """Preserve the existing table's declared properties in one CTAS replacement."""
        if existing:
            ddl = str(
                conn.execute(
                    text("SELECT GET_DDL('TABLE', :target)"), {"target": target}
                ).scalar_one()
            )
            return replacement_ddl(ddl, target, select_sql, "snowflake")
        return f"CREATE OR REPLACE TABLE {target} AS {select_sql}"

    def overwrite_statement(self, target: str, columns: str, select_sql: str) -> str:
        """Commit deletion and insertion together, retaining the table definition."""
        return f"INSERT OVERWRITE INTO {target} ({columns}) {select_sql}"


def _like_literal(name: str) -> str:
    """Escape ``name`` for ``ILIKE ... ESCAPE '!'`` so it matches only itself."""
    return re.sub(r"([!%_])", r"!\1", name)
