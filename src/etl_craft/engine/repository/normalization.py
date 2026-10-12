"""Read-only checks for converting legacy configuration to relational object contracts."""

from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from etl_craft.engine.repository.config_rows import SOURCE_COLUMNS, TABLES

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
    problems: list[str] = []
    for table in TABLES:
        keys: dict[tuple[object, ...], list[str]] = defaultdict(list)
        positions: dict[tuple[object, ...], list[str]] = defaultdict(list)
        parents = tuple(name for name, _ in table.parents)
        stored_keys = tuple(name.lower() for name in table.key if name in table.stored)
        names = ("row_id", "active_flag", *parents, *stored_keys)
        if table is SOURCE_COLUMNS:
            names += ("column_position",)
        # Do not decode unrelated settings: even an inactive row can contain an invalid date.
        rows = conn.execute(
            text(f"SELECT {', '.join(names)} FROM ({table.read_sql}) AS config_identity")
        ).mappings()
        for row in rows:
            label = f"{table.id_column}={row['row_id']} ACTIVE_FLAG={row['active_flag']}"
            key: tuple[object, ...] = (
                *(row[name] for name in parents),
                *(row[name] for name in stored_keys),
            )
            if table is SOURCE_COLUMNS:
                try:
                    identity = object_identity(str(row["table_name"]), catalog, dialect)
                except ValueError as error:
                    problems.append(f"{table.table} {label}: {error}")
                    continue
                column = normalize_identifiers(
                    exp.to_identifier(str(row["column_name"])), dialect=dialect
                ).name
                key = (*identity, column)
                positions[(*identity, row["column_position"])].append(label)
            keys[key].append(label)
        for kind, groups in (("identity", keys), ("column position", positions)):
            for key, labels in groups.items():
                if len(labels) > 1:
                    problems.append(
                        f"{table.table} {kind} {key!r} has multiple rows: {', '.join(labels)}; "
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
