"""Config-row validation shared by file loading and Engine DB application."""

import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation

from etl_craft.core.enums import RunCondition
from etl_craft.core.errors import MetadataFileError
from etl_craft.core.text import is_metadata_code, is_safe_identifier
from etl_craft.engine.repository.config_rows import (
    BUSINESS_RULES,
    OBJECT_TABLES,
    PIPELINE_DEPENDENCIES,
    SOURCE_COLUMNS,
    TASK_DEPENDENCIES,
    TASK_PARAMETERS,
    TASKS,
    Column,
    ConfigTable,
    Value,
)


def row_problem(table: ConfigTable, values: dict[str, Value]) -> str | None:
    """Return the remedy for an invalid row, before writing any config rows."""
    if table is OBJECT_TABLES:
        return _table_problem(values)
    if table in (SOURCE_COLUMNS, BUSINESS_RULES, TASKS) and (
        problem := _identifier_problem(table, values)
    ):
        return problem
    if (
        table is TASK_PARAMETERS
        and str(values["PARAMETER_NAME"]).strip().upper() == "TARGET_OBJECT"
    ):
        return (
            "TARGET_OBJECT is derived; set TARGET_CATALOG_NAME, TARGET_SCHEMA_NAME and"
            " TARGET_TABLE_NAME in tasks.csv"
        )
    if table is TASKS:
        schema, target = values["TARGET_SCHEMA_NAME"], values["TARGET_TABLE_NAME"]
        if (schema is None) != (target is None) or (
            schema is None and values["TARGET_CATALOG_NAME"] is not None
        ):
            return (
                "a target needs both TARGET_SCHEMA_NAME and TARGET_TABLE_NAME; leave "
                "all target components empty for a task without one"
            )
        condition, count = values["RUN_CONDITION"], values["RUN_CONDITION_COUNT"]
        if condition == RunCondition.N and count is None:
            return "RUN_CONDITION N needs a RUN_CONDITION_COUNT"
        if condition != RunCondition.N and count is not None:
            return "RUN_CONDITION_COUNT goes with RUN_CONDITION N only; leave it empty"
    if (
        table is TASK_DEPENDENCIES
        and not values["DEPENDS_ON_PROJECT_CODE"]
        and (values["PIPELINE_CODE"], values["TASK_CODE"])
        == (
            values["DEPENDS_ON_PIPELINE_CODE"],
            values["DEPENDS_ON_TASK_CODE"],
        )
    ):
        return f"{values['PIPELINE_CODE']}.{values['TASK_CODE']} depends on itself"
    if (
        table is PIPELINE_DEPENDENCIES
        and not values["DEPENDS_ON_PROJECT_CODE"]
        and values["PIPELINE_CODE"] == values["DEPENDS_ON_PIPELINE_CODE"]
    ):
        return f"pipeline {values['PIPELINE_CODE']} depends on itself"
    return None


def _table_problem(values: dict[str, Value]) -> str | None:
    for name in ("REJECT_RETENTION_DAYS", "SNAPSHOT_RETENTION_DAYS", "ORPHAN_RETENTION_DAYS"):
        days = values.get(name)
        if days is not None and (int(str(days)) > 36500 or values.get("OBJECT_KIND") == "VIEW"):
            return f"{name}={days!r}: expected at most 36500 days on a TABLE; correct tables.csv"
    return _identifier_problem(OBJECT_TABLES, values)


def _identifier_problem(table: ConfigTable, values: dict[str, Value]) -> str | None:
    names = (
        ("TARGET_CATALOG_NAME", "TARGET_SCHEMA_NAME", "TARGET_TABLE_NAME")
        if table is TASKS
        else ("CATALOG_NAME", "SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME")
    )
    for name in names:
        value = values.get(name)
        if value is not None and not is_safe_identifier(str(value)):
            return f"{name}={str(value)!r}: expected a separate unquoted SQL identifier"
    return None


def parse_cell(column: Column, cell: str) -> Value:
    """Return a cell's value in its column's type; ``MetadataFileError`` says what was expected."""
    if cell == "":
        if column.required:
            raise MetadataFileError("is empty; this column needs a value")
        return column.default
    if column.kind == "code":
        if not is_metadata_code(cell):
            raise MetadataFileError(
                f"{cell!r} is not a code: start with an ASCII letter, then use letters, digits "
                "or _, at most 128 characters"
            )
        return cell
    if column.kind == "choice":
        if cell not in column.choices:
            raise MetadataFileError(f"{cell!r} is not one of {', '.join(column.choices)}")
        return cell
    if column.kind == "int":
        if not re.fullmatch(r"-?[0-9]+", cell):
            raise MetadataFileError(f"{cell!r} is not a whole number")
        number = int(cell)
        if column.minimum is not None and number < column.minimum:
            raise MetadataFileError(f"{number} is less than {column.minimum}")
        return number
    if column.kind == "float":
        return parse_decimal(cell)
    if column.kind == "date":
        return parse_date(cell)
    if column.kind == "json":
        return parse_object(cell)
    return cell


def parse_decimal(cell: str) -> float:
    """Read a finite numeric config cell."""
    try:
        decimal = Decimal(cell)
    except InvalidOperation:
        raise MetadataFileError(f"{cell!r} is not a number") from None
    if not decimal.is_finite():
        raise MetadataFileError(f"{cell!r} is not a finite number")
    return float(decimal)


def parse_date(cell: str) -> date:
    """Read a date with the config file's exact ISO representation."""
    try:
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", cell):
            return date.fromisoformat(cell)
    except ValueError:
        pass
    raise MetadataFileError(f"{cell!r} is not a date written YYYY-MM-DD")


def parse_object(cell: str) -> dict[str, object]:
    """Read a JSON object rather than another JSON value."""
    try:
        parsed = json.loads(cell)
    except ValueError as error:
        raise MetadataFileError(f"is not JSON: {error}") from None
    if not isinstance(parsed, dict):
        raise MetadataFileError("is not a JSON object; write it as {...}")
    return parsed
