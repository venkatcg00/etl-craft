"""The ``CFG_`` tables the project's config files hold, one file per table.

Rows are identified as the files identify them, by codes and never by ids: a pipeline by its
code, a task by its pipeline's code and its own, a parameter by its task and name, a dependency
by both of its ends and its type, and a business rule by its task and name. Each key is one of
the Engine DB's unique indexes on active rows. Every file also has an ``ACTIVE_FLAG`` column.

``read_rows`` returns every row of a table, active or not, with the ids of the rows it hangs
off; ``insert_row``, ``update_row`` and ``retire_row`` change one row each, and refuse to change
any other number of rows.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from sqlalchemy import JSON, Date, Float, Integer, String, TextClause, bindparam, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError
from sqlalchemy.types import TypeEngine

from etl_craft.core.enums import DependencyType, Handler, RefreshType, RunCondition
from etl_craft.core.errors import MetadataFileError

Kind = Literal["code", "text", "choice", "int", "float", "date", "json"]
Value = str | int | float | date | dict[str, object] | None
Key = tuple[Value, ...]

YES_NO = ("Y", "N")
DEPENDENCY_TYPES = tuple(kind.value for kind in DependencyType)

_BIND_TYPES: dict[Kind, TypeEngine[Any]] = {
    "code": String(),
    "text": String(),
    "choice": String(),
    "int": Integer(),
    "float": Float(),
    "date": Date(),
    "json": JSON(none_as_null=True),
}


@dataclass(frozen=True)
class Column:
    """A column of a config file, named as its ``CFG_`` column.

    ``required`` columns need a value. An empty cell in any other column is NULL, or ``default``
    where the table gives the column one. ``minimum`` bounds a whole number.
    """

    name: str
    kind: Kind = "text"
    required: bool = False
    default: Value = None
    choices: tuple[str, ...] = ()
    minimum: int | None = None


ACTIVE_FLAG = Column("ACTIVE_FLAG", "choice", default="Y", choices=YES_NO)


@dataclass(frozen=True)
class ConfigTable:
    """A config file and the ``CFG_`` table its rows are.

    ``stored`` names the file's columns the table itself holds; the others are the codes of the
    rows it hangs off. ``parents`` pairs each id ``read_sql`` returns for those rows with the
    file that holds them, and ``links`` names the id columns an insert sets.
    """

    file: str
    table: str
    id_column: str
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    stored: tuple[str, ...]
    parents: tuple[tuple[str, str], ...]
    links: tuple[str, ...]
    label_format: str
    read_sql: str

    @property
    def updated(self) -> tuple[str, ...]:
        """The stored columns outside the key, which an update sets."""
        return tuple(name for name in self.stored if name not in self.key)

    def column(self, name: str) -> Column:
        """Return the column ``name``."""
        return next(column for column in self.columns if column.name == name)

    def label(self, values: Mapping[str, Value]) -> str:
        """Name a row by its key, such as ``SALES.load_orders`` for a task."""
        return self.label_format.format(**{name: values[name] for name in self.key})


@dataclass(frozen=True)
class StoredRow:
    """A ``CFG_`` row as a config file would hold it.

    ``parents`` are the ids of the rows it hangs off, in ``ConfigTable.parents`` order;
    ``well_formed`` is false when its own ids disagree with those rows, such as a business rule
    whose ``PIPELINE_ID`` is not its task's.
    """

    row_id: int
    active: bool
    parents: tuple[int, ...]
    well_formed: bool
    values: dict[str, Value]


PIPELINES = ConfigTable(
    file="pipelines.csv",
    table="CFG_PIPELINES",
    id_column="PIPELINE_ID",
    key=("PIPELINE_CODE",),
    columns=(
        Column("PIPELINE_CODE", "code", required=True),
        Column("PIPELINE_NAME", required=True),
        Column("DESCRIPTION"),
        Column(
            "REFRESH_TYPE",
            "choice",
            required=True,
            choices=tuple(kind.value for kind in RefreshType),
        ),
        Column("RUN_SCHEDULE"),
        Column("SCHEDULE_TIMEZONE"),
        Column("SCHEDULE_START_DATE", "date"),
        Column("CATCHUP", "choice", default="N", choices=YES_NO),
        Column("MAX_CATCHUP_RUNS", "int", default=1, minimum=1),
        Column("OVERLAP_POLICY", "choice", default="SKIP", choices=("SKIP", "QUEUE")),
        Column("SLA_IN_HOURS", "float"),
        Column("PIPELINE_PARAMETERS", "json"),
        ACTIVE_FLAG,
    ),
    stored=(
        "PIPELINE_CODE",
        "PIPELINE_NAME",
        "DESCRIPTION",
        "REFRESH_TYPE",
        "RUN_SCHEDULE",
        "SCHEDULE_TIMEZONE",
        "SCHEDULE_START_DATE",
        "CATCHUP",
        "MAX_CATCHUP_RUNS",
        "OVERLAP_POLICY",
        "SLA_IN_HOURS",
        "PIPELINE_PARAMETERS",
    ),
    parents=(),
    links=(),
    label_format="{PIPELINE_CODE}",
    read_sql=(
        "SELECT PIPELINE_ID AS row_id, ACTIVE_FLAG AS active_flag, 'Y' AS well_formed, "
        "PIPELINE_CODE AS pipeline_code, PIPELINE_NAME AS pipeline_name, "
        "DESCRIPTION AS description, REFRESH_TYPE AS refresh_type, "
        "RUN_SCHEDULE AS run_schedule, SCHEDULE_TIMEZONE AS schedule_timezone, "
        "SCHEDULE_START_DATE AS schedule_start_date, CATCHUP AS catchup, "
        "MAX_CATCHUP_RUNS AS max_catchup_runs, OVERLAP_POLICY AS overlap_policy, "
        "SLA_IN_HOURS AS sla_in_hours, PIPELINE_PARAMETERS AS pipeline_parameters "
        "FROM CFG_PIPELINES"
    ),
)

TASKS = ConfigTable(
    file="tasks.csv",
    table="CFG_TASKS",
    id_column="TASK_ID",
    key=("PIPELINE_CODE", "TASK_CODE"),
    columns=(
        Column("PIPELINE_CODE", "code", required=True),
        Column("TASK_CODE", "code", required=True),
        Column("TASK_TYPE", "choice", required=True, choices=("INGESTION", "ETL")),
        Column("HANDLER", "choice", required=True, choices=tuple(kind.value for kind in Handler)),
        Column("RUN_CONDITION", "choice", choices=tuple(kind.value for kind in RunCondition)),
        Column("RUN_CONDITION_COUNT", "int", minimum=1),
        ACTIVE_FLAG,
    ),
    stored=("TASK_CODE", "TASK_TYPE", "HANDLER", "RUN_CONDITION", "RUN_CONDITION_COUNT"),
    parents=(("pipeline_id", "pipelines.csv"),),
    links=("PIPELINE_ID",),
    label_format="{PIPELINE_CODE}.{TASK_CODE}",
    read_sql=(
        "SELECT t.TASK_ID AS row_id, t.ACTIVE_FLAG AS active_flag, 'Y' AS well_formed, "
        "t.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code, "
        "t.TASK_CODE AS task_code, t.TASK_TYPE AS task_type, t.HANDLER AS handler, "
        "t.RUN_CONDITION AS run_condition, t.RUN_CONDITION_COUNT AS run_condition_count "
        "FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID"
    ),
)

TASK_PARAMETERS = ConfigTable(
    file="task_parameters.csv",
    table="CFG_TASK_PARAMETERS",
    id_column="TASK_PARAMETER_ID",
    key=("PIPELINE_CODE", "TASK_CODE", "PARAMETER_NAME"),
    columns=(
        Column("PIPELINE_CODE", "code", required=True),
        Column("TASK_CODE", "code", required=True),
        Column("PARAMETER_NAME", required=True),
        Column("PARAMETER_VALUE"),
        ACTIVE_FLAG,
    ),
    stored=("PARAMETER_NAME", "PARAMETER_VALUE"),
    parents=(("task_id", "tasks.csv"),),
    links=("TASK_ID",),
    label_format="{PIPELINE_CODE}.{TASK_CODE} {PARAMETER_NAME}",
    read_sql=(
        "SELECT x.TASK_PARAMETER_ID AS row_id, x.ACTIVE_FLAG AS active_flag, "
        "'Y' AS well_formed, x.TASK_ID AS task_id, p.PIPELINE_CODE AS pipeline_code, "
        "t.TASK_CODE AS task_code, x.PARAMETER_NAME AS parameter_name, "
        "x.PARAMETER_VALUE AS parameter_value "
        "FROM CFG_TASK_PARAMETERS x JOIN CFG_TASKS t ON t.TASK_ID = x.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID"
    ),
)

TASK_DEPENDENCIES = ConfigTable(
    file="task_dependencies.csv",
    table="CFG_TASK_DEPENDENCY",
    id_column="TASK_DEPENDENCY_ID",
    key=(
        "PIPELINE_CODE",
        "TASK_CODE",
        "DEPENDS_ON_PIPELINE_CODE",
        "DEPENDS_ON_TASK_CODE",
        "DEPENDENCY_TYPE",
    ),
    columns=(
        Column("PIPELINE_CODE", "code", required=True),
        Column("TASK_CODE", "code", required=True),
        Column("DEPENDS_ON_PIPELINE_CODE", "code", required=True),
        Column("DEPENDS_ON_TASK_CODE", "code", required=True),
        Column("DEPENDENCY_TYPE", "choice", required=True, choices=DEPENDENCY_TYPES),
        Column("CONSUME_REPAIRS", "choice", default="Y", choices=YES_NO),
        ACTIVE_FLAG,
    ),
    stored=("DEPENDENCY_TYPE", "CONSUME_REPAIRS"),
    parents=(("task_id", "tasks.csv"), ("depends_on_task_id", "tasks.csv")),
    links=("PIPELINE_ID", "TASK_ID", "DEPENDS_ON_PIPELINE_ID", "DEPENDS_ON_TASK_ID"),
    label_format=(
        "{PIPELINE_CODE}.{TASK_CODE} on {DEPENDS_ON_PIPELINE_CODE}.{DEPENDS_ON_TASK_CODE} "
        "{DEPENDENCY_TYPE}"
    ),
    read_sql=(
        "SELECT d.TASK_DEPENDENCY_ID AS row_id, d.ACTIVE_FLAG AS active_flag, "
        "CASE WHEN d.PIPELINE_ID = t.PIPELINE_ID "
        "AND COALESCE(d.DEPENDS_ON_PIPELINE_ID, u.PIPELINE_ID) = u.PIPELINE_ID "
        "THEN 'Y' ELSE 'N' END AS well_formed, "
        "d.TASK_ID AS task_id, d.DEPENDS_ON_TASK_ID AS depends_on_task_id, "
        "p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, "
        "up.PIPELINE_CODE AS depends_on_pipeline_code, u.TASK_CODE AS depends_on_task_code, "
        "d.DEPENDENCY_TYPE AS dependency_type, d.CONSUME_REPAIRS AS consume_repairs "
        "FROM CFG_TASK_DEPENDENCY d JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
        "JOIN CFG_TASKS u ON u.TASK_ID = d.DEPENDS_ON_TASK_ID "
        "JOIN CFG_PIPELINES up ON up.PIPELINE_ID = u.PIPELINE_ID"
    ),
)

PIPELINE_DEPENDENCIES = ConfigTable(
    file="pipeline_dependencies.csv",
    table="CFG_PIPELINE_DEPENDENCY",
    id_column="PIPELINE_DEPENDENCY_ID",
    key=("PIPELINE_CODE", "DEPENDS_ON_PIPELINE_CODE", "DEPENDENCY_TYPE"),
    columns=(
        Column("PIPELINE_CODE", "code", required=True),
        Column("DEPENDS_ON_PIPELINE_CODE", "code", required=True),
        Column("DEPENDENCY_TYPE", "choice", required=True, choices=DEPENDENCY_TYPES),
        Column("CONSUME_REPAIRS", "choice", default="Y", choices=YES_NO),
        ACTIVE_FLAG,
    ),
    stored=("DEPENDENCY_TYPE", "CONSUME_REPAIRS"),
    parents=(("pipeline_id", "pipelines.csv"), ("depends_on_pipeline_id", "pipelines.csv")),
    links=("PIPELINE_ID", "DEPENDS_ON_PIPELINE_ID"),
    label_format="{PIPELINE_CODE} on {DEPENDS_ON_PIPELINE_CODE} {DEPENDENCY_TYPE}",
    read_sql=(
        "SELECT d.PIPELINE_DEPENDENCY_ID AS row_id, d.ACTIVE_FLAG AS active_flag, "
        "'Y' AS well_formed, d.PIPELINE_ID AS pipeline_id, "
        "d.DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, p.PIPELINE_CODE AS pipeline_code, "
        "up.PIPELINE_CODE AS depends_on_pipeline_code, d.DEPENDENCY_TYPE AS dependency_type, "
        "d.CONSUME_REPAIRS AS consume_repairs "
        "FROM CFG_PIPELINE_DEPENDENCY d JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.PIPELINE_ID "
        "JOIN CFG_PIPELINES up ON up.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID"
    ),
)

BUSINESS_RULES = ConfigTable(
    file="business_rules.csv",
    table="CFG_BUSINESS_RULES",
    id_column="BUSINESS_RULE_ID",
    key=("PIPELINE_CODE", "TASK_CODE", "BUSINESS_RULE_NAME"),
    columns=(
        Column("PIPELINE_CODE", "code", required=True),
        Column("TASK_CODE", "code", required=True),
        Column("BUSINESS_RULE_NAME", required=True),
        Column("SEQUENCE_NUMBER", "int", required=True),
        Column(
            "BUSINESS_RULE_TYPE",
            "choice",
            required=True,
            choices=("INCOMPLETE", "REJECT", "REPORT"),
        ),
        Column("BUSINESS_RULE_KEY_COLUMN", required=True),
        Column("TARGET_TABLE", required=True),
        Column("BUSINESS_RULE_SQL", required=True),
        ACTIVE_FLAG,
    ),
    stored=(
        "BUSINESS_RULE_NAME",
        "SEQUENCE_NUMBER",
        "BUSINESS_RULE_TYPE",
        "BUSINESS_RULE_KEY_COLUMN",
        "TARGET_TABLE",
        "BUSINESS_RULE_SQL",
    ),
    parents=(("task_id", "tasks.csv"),),
    links=("PIPELINE_ID", "TASK_ID"),
    label_format="{PIPELINE_CODE}.{TASK_CODE} rule {BUSINESS_RULE_NAME!r}",
    read_sql=(
        "SELECT r.BUSINESS_RULE_ID AS row_id, r.ACTIVE_FLAG AS active_flag, "
        "CASE WHEN r.PIPELINE_ID = t.PIPELINE_ID THEN 'Y' ELSE 'N' END AS well_formed, "
        "r.TASK_ID AS task_id, p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, "
        "r.BUSINESS_RULE_NAME AS business_rule_name, r.SEQUENCE_NUMBER AS sequence_number, "
        "r.BUSINESS_RULE_TYPE AS business_rule_type, "
        "r.BUSINESS_RULE_KEY_COLUMN AS business_rule_key_column, "
        "r.TARGET_TABLE AS target_table, r.BUSINESS_RULE_SQL AS business_rule_sql "
        "FROM CFG_BUSINESS_RULES r JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID"
    ),
)

TABLES = (
    PIPELINES,
    TASKS,
    TASK_PARAMETERS,
    TASK_DEPENDENCIES,
    PIPELINE_DEPENDENCIES,
    BUSINESS_RULES,
)
"""Every config file's table, parents before the rows that hang off them."""


def stored_value(column: Column, raw: object) -> Value:
    """Return a stored value as the file's column holds it, so the two compare equal."""
    if raw is None:
        return None
    if column.kind == "int":
        return int(str(raw))
    if column.kind == "float":
        return float(raw) if isinstance(raw, (int, float, Decimal)) else float(str(raw))
    if column.kind == "date":
        if isinstance(raw, datetime):
            return raw.date()
        return raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
    if column.kind == "json":
        if isinstance(raw, dict):
            return raw
        try:
            parsed = json.loads(str(raw))
        except ValueError:
            return str(raw)
        return parsed if isinstance(parsed, dict) else str(raw)
    return str(raw)


def read_rows(conn: Connection, table: ConfigTable) -> list[StoredRow]:
    """Return every row of ``table``, active or not, in id order."""
    rows = []
    for row in conn.execute(text(f"{table.read_sql} ORDER BY 1")).mappings():
        values = {
            column.name: stored_value(column, row[column.name.lower()])
            for column in table.columns
            if column is not ACTIVE_FLAG
        }
        rows.append(
            StoredRow(
                int(row["row_id"]),
                row["active_flag"] == "Y",
                tuple(int(row[name]) for name, _ in table.parents),
                row["well_formed"] == "Y",
                values,
            )
        )
    return rows


def _bound(table: ConfigTable, sql: str, names: tuple[str, ...]) -> TextClause:
    types = [
        _BIND_TYPES[table.column(name).kind] if name in table.stored else Integer()
        for name in names
    ]
    return text(sql).bindparams(
        *(bindparam(name, type_=kind) for name, kind in zip(names, types, strict=True))
    )


def _write(
    conn: Connection, statement: TextClause, params: Mapping[str, object], what: str
) -> None:
    try:
        changed = conn.execute(statement, dict(params)).rowcount
    except DBAPIError as error:
        raise MetadataFileError(f"{what} failed: {error.orig}") from error
    if changed != 1:
        raise MetadataFileError(f"{what} changed {changed} rows instead of one")


def insert_row(
    conn: Connection, table: ConfigTable, values: Mapping[str, Value], links: Mapping[str, int]
) -> None:
    """Insert one active row: its stored values, and the ids of the rows it hangs off."""
    names = (*table.links, *table.stored)
    statement = _bound(
        table,
        f"INSERT INTO {table.table} ({', '.join(names)}) "
        f"VALUES ({', '.join(f':{name}' for name in names)})",
        names,
    )
    params = {name: links[name] for name in table.links} | {
        name: values[name] for name in table.stored
    }
    _write(conn, statement, params, f"inserting {table.label(values)} into {table.table}")


def update_row(
    conn: Connection, table: ConfigTable, row_id: int, values: Mapping[str, Value]
) -> None:
    """Set the stored columns outside the key of row ``row_id``, and make it active."""
    names = table.updated
    assignments = ", ".join([*(f"{name} = :{name}" for name in names), "ACTIVE_FLAG = 'Y'"])
    statement = _bound(
        table,
        f"UPDATE {table.table} SET {assignments} WHERE {table.id_column} = :row_id",
        (*names, "row_id"),
    )
    params = {name: values[name] for name in names} | {"row_id": row_id}
    _write(conn, statement, params, f"updating {table.label(values)} in {table.table}")


def retire_row(conn: Connection, table: ConfigTable, row_id: int, label: str) -> None:
    """Set ``ACTIVE_FLAG = 'N'`` on the active row ``row_id``."""
    statement = text(
        f"UPDATE {table.table} SET ACTIVE_FLAG = 'N' "
        f"WHERE {table.id_column} = :row_id AND ACTIVE_FLAG = 'Y'"
    )
    _write(conn, statement, {"row_id": row_id}, f"retiring {label} in {table.table}")
