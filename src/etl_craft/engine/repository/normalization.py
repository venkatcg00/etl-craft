"""Read-only checks for converting legacy configuration to relational object contracts."""

from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

ObjectIdentity = tuple[str | None, str, str]


def object_identity(value: str, catalog: str | None, dialect: str | None) -> ObjectIdentity:
    """Resolve exactly two or three identifier components using warehouse case rules."""
    try:
        table = parse_one(value, into=exp.Table, read=dialect)
        if (
            not isinstance(table, exp.Table)
            or not table.db
            or len(table.parts) not in (2, 3)
            or not all(isinstance(part, exp.Identifier) and part.name for part in table.parts)
            or any(node.comments for node in table.walk())
        ):
            raise ValueError
        if not table.catalog and catalog is not None:
            table.set("catalog", exp.to_identifier(catalog))
        normalized = normalize_identifiers(table, dialect=dialect)
        return normalized.catalog or None, normalized.db, normalized.name
    except (SqlglotError, ValueError):
        raise ValueError(
            f"object reference {value!r}: expected schema.table or catalog.schema.table "
            "with exactly two or three SQL identifiers; separate catalog, schema and table "
            "components and quote identifiers containing dots; remove SQL or comments"
        ) from None


def normalization_problems(
    conn: Connection, *, catalog: str | None, dialect: str | None
) -> list[str]:
    """Report ambiguous legacy rows without changing their data or history."""
    problems = config_identity_problems(conn)
    keys: dict[tuple[object, ...], list[str]] = defaultdict(list)
    positions: dict[tuple[object, ...], list[str]] = defaultdict(list)
    rows = conn.execute(
        text(
            "SELECT SOURCE_COLUMN_ID AS row_id, ACTIVE_FLAG AS active_flag, "
            "TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
            "COLUMN_POSITION AS column_position FROM CFG_SOURCE_COLUMNS"
        )
    ).mappings()
    for row in rows:
        label = f"SOURCE_COLUMN_ID={row['row_id']} ACTIVE_FLAG={row['active_flag']}"
        try:
            identity = object_identity(str(row["table_name"]), catalog, dialect)
        except ValueError as error:
            problems.append(f"CFG_SOURCE_COLUMNS {label}: {error}")
            continue
        column = normalize_identifiers(
            exp.to_identifier(str(row["column_name"])), dialect=dialect
        ).name
        keys[(*identity, column)].append(label)
        positions[(*identity, row["column_position"])].append(label)
    for kind, groups in (("identity", keys), ("column position", positions)):
        for key, labels in groups.items():
            if len(labels) > 1:
                problems.append(
                    f"CFG_SOURCE_COLUMNS {kind} {key!r} has multiple rows: {', '.join(labels)}; "
                    "assign distinct identities and repair dependent references before "
                    "normalization; inactive rows also reserve their identity, so "
                    "deactivating a duplicate does not resolve it"
                )
    references = conn.execute(
        text(
            "SELECT 'CFG_TASK_PARAMETERS' AS table_name, TASK_PARAMETER_ID AS row_id, "
            "PARAMETER_NAME AS field_name, PARAMETER_VALUE AS object_name, "
            "ACTIVE_FLAG AS active_flag "
            "FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='TARGET_OBJECT' "
            "UNION ALL SELECT 'CFG_BUSINESS_RULES' AS table_name, BUSINESS_RULE_ID AS row_id, "
            "'TARGET_TABLE' AS field_name, TARGET_TABLE AS object_name, ACTIVE_FLAG AS active_flag "
            "FROM CFG_BUSINESS_RULES"
        )
    )
    for reference in references:
        try:
            object_identity(str(reference.object_name or ""), catalog, dialect)
        except ValueError as error:
            problems.append(
                f"{reference.table_name} id={reference.row_id} ACTIVE_FLAG={reference.active_flag} "
                f"{reference.field_name}: {error}"
            )
    return problems


def config_identity_problems(conn: Connection) -> list[str]:
    """Report duplicate config keys that do not depend on warehouse identifier rules."""
    problems: list[str] = []
    # These physical keys exist before any pending migration, unlike current CSV projections.
    identities = (
        ("CFG_PIPELINES", "PIPELINE_ID", ("PIPELINE_CODE",)),
        ("CFG_TASKS", "TASK_ID", ("PIPELINE_ID", "TASK_CODE")),
        ("CFG_TASK_PARAMETERS", "TASK_PARAMETER_ID", ("TASK_ID", "PARAMETER_NAME")),
        (
            "CFG_PIPELINE_DEPENDENCY",
            "PIPELINE_DEPENDENCY_ID",
            ("PIPELINE_ID", "DEPENDS_ON_PIPELINE_ID", "DEPENDENCY_TYPE"),
        ),
        (
            "CFG_TASK_DEPENDENCY",
            "TASK_DEPENDENCY_ID",
            ("TASK_ID", "DEPENDS_ON_TASK_ID", "DEPENDENCY_TYPE"),
        ),
        ("CFG_BUSINESS_RULES", "BUSINESS_RULE_ID", ("TASK_ID", "BUSINESS_RULE_NAME")),
    )
    for table, id_column, columns in identities:
        groups: dict[tuple[object, ...], list[str]] = defaultdict(list)
        selected = (
            f"{id_column} AS row_id",
            "ACTIVE_FLAG AS active_flag",
            *(f"{name} AS {name.lower()}" for name in columns),
        )
        for row in conn.execute(text(f"SELECT {', '.join(selected)} FROM {table}")).mappings():
            key = tuple(row[name.lower()] for name in columns)
            groups[key].append(f"{id_column}={row['row_id']} ACTIVE_FLAG={row['active_flag']}")
        for key, labels in groups.items():
            if len(labels) > 1:
                problems.append(
                    f"{table} identity {key!r} has multiple rows: {', '.join(labels)}; "
                    "assign distinct identities and repair dependent references before "
                    "normalization; inactive rows also reserve their identity, so "
                    "deactivating a duplicate does not resolve it"
                )
    return problems
