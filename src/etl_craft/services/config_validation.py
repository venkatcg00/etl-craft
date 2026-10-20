"""Config-row validation shared by file loading and Engine DB application."""

from etl_craft.core.enums import RunCondition
from etl_craft.core.tags import valid_tag
from etl_craft.core.text import is_safe_identifier
from etl_craft.engine.repository.config_rows import (
    BUSINESS_RULES,
    OBJECT_TABLES,
    PIPELINE_DEPENDENCIES,
    PIPELINE_GRANTS,
    SOURCE_COLUMNS,
    TASK_DEPENDENCIES,
    TASK_PARAMETERS,
    TASKS,
    ConfigTable,
    Value,
)


def row_problem(table: ConfigTable, values: dict[str, Value]) -> str | None:
    """Return the remedy for an invalid row, before writing any config rows."""
    if table is PIPELINE_GRANTS:
        return _grant_problem(values)
    if table in (OBJECT_TABLES, SOURCE_COLUMNS, BUSINESS_RULES, TASKS):
        names = (
            ("TARGET_CATALOG_NAME", "TARGET_SCHEMA_NAME", "TARGET_TABLE_NAME")
            if table is TASKS
            else ("CATALOG_NAME", "SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME")
        )
        for name in names:
            if name not in values or values[name] is None:
                continue
            value = str(values[name])
            if not is_safe_identifier(value):
                return f"{name}={value!r}: expected a separate unquoted SQL identifier"
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
    if table is TASK_DEPENDENCIES and (values["PIPELINE_CODE"], values["TASK_CODE"]) == (
        values["DEPENDS_ON_PIPELINE_CODE"],
        values["DEPENDS_ON_TASK_CODE"],
    ):
        return f"{values['PIPELINE_CODE']}.{values['TASK_CODE']} depends on itself"
    if (
        table is PIPELINE_DEPENDENCIES
        and values["PIPELINE_CODE"] == values["DEPENDS_ON_PIPELINE_CODE"]
    ):
        return f"pipeline {values['PIPELINE_CODE']} depends on itself"
    return None


def _grant_problem(values: dict[str, Value]) -> str | None:
    if (values["PIPELINE_CODE"] is None) == (values["TAG"] is None):
        return "set exactly one of PIPELINE_CODE or TAG"
    if values["TAG"] is not None and not valid_tag(values["TAG"]):
        return f"TAG={values['TAG']!r}: expected a valid pipeline tag"
    return None
