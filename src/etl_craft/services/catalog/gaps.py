"""Structured schema and lineage outcomes shared by documentation and HTTP views."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar

from sqlglot.errors import SqlglotError

from etl_craft.core.enums import GapAttribute, GapState, Handler, SqlAction
from etl_craft.core.errors import SqlGuardError
from etl_craft.handlers.sql.spec import read_action
from etl_craft.services.source_columns import same_type

if TYPE_CHECKING:
    from etl_craft.services.catalog.collect import Catalog, Column, TableAsset, TaskAsset


@dataclass(frozen=True)
class GapRecord:
    """One checked attribute, including its evidence and affected tasks."""

    table: str
    column: str | None
    attribute: GapAttribute
    state: GapState
    declared: str | None
    deployed: str | None
    reason: str
    tasks: tuple[str, ...]
    engine_managed: bool = False


@dataclass(frozen=True)
class GapSnapshot:
    """A dated observation, never an editable copy of warehouse metadata."""

    SCHEMA: ClassVar[str] = "etl-craft/catalog-gaps/1"
    generated_at: datetime
    compared_at: datetime | None
    warehouse: str | None
    contract_version: str
    records: tuple[GapRecord, ...]


def gap_snapshot(catalog: Catalog) -> GapSnapshot:
    """Expose every object's coverage, including offline and unsuccessful checks."""
    records = []
    for table in catalog.tables.values():
        if table.external:
            continue
        records.extend(table_records(catalog, table))
        records.extend(lineage_records(catalog, table))
    return GapSnapshot(
        catalog.generated_at,
        catalog.compared_at,
        catalog.warehouse_identity,
        catalog.contract_version,
        tuple(records),
    )


def _record(
    table: TableAsset,
    column: Column | None,
    attribute: GapAttribute,
    state: GapState,
    values: tuple[str | None, str | None],
    reason: str,
) -> GapRecord:
    return GapRecord(
        table.name,
        column.name if column else None,
        attribute,
        state,
        *values,
        reason,
        tuple(sorted(set(table.writers + table.readers))),
        bool(column and column.engine_managed),
    )


def table_records(catalog: Catalog, table: TableAsset) -> list[GapRecord]:
    """Expose checked attributes with every reading, writing and rule task affected."""
    tasks = affected_tasks(catalog, table)
    return [replace(record, tasks=tasks) for record in _table_records(catalog, table)]


def affected_tasks(catalog: Catalog, table: TableAsset) -> tuple[str, ...]:
    """Include rule checks even when their task does not declare a table target."""
    rules = [catalog.rules[rule].task for rule in table.rules if rule in catalog.rules]
    return tuple(sorted(set(table.writers + table.readers + rules) & catalog.tasks.keys()))


def _table_records(catalog: Catalog, table: TableAsset) -> list[GapRecord]:
    """Compare presence separately from declared types and nullability."""
    unavailable = _availability(catalog, table)
    if unavailable:
        return unavailable
    result = [
        _record(
            table,
            None,
            GapAttribute.OBJECT,
            GapState.MATCH,
            ("Referenced object", table.warehouse_kind),
            "Object exists in the warehouse.",
        )
    ]
    if table.declared_kind:
        result.append(
            _record(
                table,
                None,
                GapAttribute.KIND,
                GapState.MATCH
                if table.declared_kind == table.warehouse_kind
                else GapState.UNAVAILABLE
                if table.warehouse_kind is None
                else GapState.KIND_MISMATCH,
                (table.declared_kind, table.warehouse_kind),
                "Compared the declared and deployed object kinds.",
            )
        )
    for column in table.columns.values():
        result.extend(_column_records(catalog, table, column))
    return result


def _availability(catalog: Catalog, table: TableAsset) -> list[GapRecord]:
    unavailable = [
        message
        for message in table.warehouse_findings
        if message.startswith("comparison unavailable:")
    ]
    if unavailable or not table.warehouse_checked:
        state = GapState.UNAVAILABLE if unavailable else GapState.UNCHECKED
        reason = (
            "; ".join(unavailable)
            or "Warehouse metadata was not requested; generate with --with-warehouse."
        )
        return [
            _record(table, None, GapAttribute.OBJECT, state, (table.declared_kind, None), reason)
        ]
    if not table.in_warehouse:
        expected = _expected_before_deployment(catalog, table)
        return [
            _record(
                table,
                None,
                GapAttribute.OBJECT,
                GapState.EXPECTED if expected else GapState.CONFIG_ONLY,
                (table.declared_kind or "Referenced object", None),
                "Creation task has no successful deployment recorded."
                if expected
                else "Referenced table/view is missing; deploy it or correct the object reference.",
            )
        ]
    if catalog.compared_at and datetime.now(UTC) - catalog.compared_at > timedelta(hours=26):
        return [
            _record(
                table,
                None,
                GapAttribute.OBJECT,
                GapState.STALE,
                (table.declared_kind, None),
                f"Comparison from {catalog.compared_at.isoformat()} is stale; "
                "regenerate with --with-warehouse.",
            )
        ]
    return []


def _expected_before_deployment(catalog: Catalog, table: TableAsset) -> bool:
    creators = [
        catalog.tasks[name]
        for name in table.writers
        if catalog.tasks[name].row.handler == Handler.SQL
        and read_action(catalog.tasks[name].params)
        in (SqlAction.CREATE_TABLE, SqlAction.SETUP_TABLE, SqlAction.CREATE_VIEW)
    ]
    return bool(creators) and not any(
        catalog.tasks[name].row.has_successful_run for name in table.writers
    )


def infers_output_columns(task: TaskAsset) -> bool:
    """Only column-producing SQL actions define a target's inferred schema."""
    if task.row.handler != Handler.SQL:
        return False
    try:
        action = read_action(task.params)
    except SqlGuardError:
        return False
    return action not in (SqlAction.DELETE_ROWS, SqlAction.DROP_TABLE)


def _column_records(catalog: Catalog, table: TableAsset, column: Column) -> list[GapRecord]:
    managed_output = any(infers_output_columns(catalog.tasks[name]) for name in table.writers)
    unknown = [
        name
        for name in table.writers
        if infers_output_columns(catalog.tasks[name]) and catalog.tasks[name].lineage_error
    ]
    if unknown and not column.declared:
        return [
            _record(
                table,
                column,
                GapAttribute.COLUMN,
                GapState.UNAVAILABLE,
                (None, column.warehouse_type),
                f"SQL output columns unavailable for {', '.join(unknown)}; "
                "correct task SQL/source contracts and recompute lineage before comparing.",
            )
        ]

    if not column.declared:
        return [
            _record(
                table,
                column,
                GapAttribute.COLUMN,
                GapState.DEPLOYED_ONLY,
                (None, column.warehouse_type),
                "Present in warehouse; missing from contract: warehouse adds "
                f"{'undocumented ' if managed_output else ''}column {column.name}; "
                "update the contract or deployment.",
            )
        ]
    if not column.warehouse_present:
        return [
            _record(
                table,
                column,
                GapAttribute.COLUMN,
                GapState.CONFIG_ONLY,
                (column.declared_type or "Inferred output", None),
                "Declared; missing from deployed table: warehouse lacks "
                f"{'documented ' if managed_output else ''}column {column.name}; "
                "restore it or correct the contract.",
            )
        ]
    result = [
        _record(
            table,
            column,
            GapAttribute.COLUMN,
            GapState.MATCH,
            (column.name, column.name),
            "Column names compared successfully.",
        )
    ]
    result.append(_type_record(catalog, table, column))
    if column.declared_nullable is not None:
        deployed = column.warehouse_nullable
        result.append(
            _record(
                table,
                column,
                GapAttribute.NULLABILITY,
                GapState.UNAVAILABLE
                if deployed is None
                else GapState.MATCH
                if deployed == column.declared_nullable
                else GapState.NULLABILITY_MISMATCH,
                (column.declared_nullable, deployed),
                "Warehouse nullability is unavailable."
                if deployed is None
                else "Compared the declared and deployed nullability.",
            )
        )
    return result


def _type_record(catalog: Catalog, table: TableAsset, column: Column) -> GapRecord:
    values = column.declared_type, column.warehouse_type
    if column.declared_type is None:
        return _record(
            table,
            column,
            GapAttribute.TYPE,
            GapState.NOT_DECLARED,
            values,
            "Output name is inferred; no type contract was declared.",
        )
    if column.warehouse_type is None:
        return _record(
            table,
            column,
            GapAttribute.TYPE,
            GapState.UNAVAILABLE,
            values,
            "Warehouse type is unavailable; retry the metadata comparison.",
        )
    try:
        matched = same_type(column.declared_type, column.warehouse_type, catalog.sql_dialect)
    except (SqlglotError, ValueError) as error:
        return _record(
            table,
            column,
            GapAttribute.TYPE,
            GapState.UNAVAILABLE,
            values,
            f"Type comparison unavailable: {error}; correct the contract or retry.",
        )
    return _record(
        table,
        column,
        GapAttribute.TYPE,
        GapState.MATCH if matched else GapState.TYPE_MISMATCH,
        values,
        "Types compared successfully."
        if matched
        else "Declared and deployed types differ; restore the type or update the contract.",
    )


def lineage_records(catalog: Catalog, table: TableAsset) -> list[GapRecord]:
    """Report failed or unknown writers without treating constant expressions as failures."""
    records = []
    for name in table.writers:
        task = catalog.tasks[name]
        reason = task.lineage_error
        if task.row.handler == Handler.PYTHON:
            reason = "Script column lineage is unknown; a target name does not declare its sources."
        if reason:
            records.append(
                GapRecord(
                    table.name,
                    None,
                    GapAttribute.LINEAGE,
                    GapState.LINEAGE_GAP,
                    None,
                    None,
                    reason,
                    (name,),
                )
            )
    return records
