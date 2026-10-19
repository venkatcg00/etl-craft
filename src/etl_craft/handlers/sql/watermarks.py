"""Typed SQL high-water marks, using the same offset store as ingestion scripts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy.engine import Engine
from sqlglot import exp

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import OffsetType, SqlAction
from etl_craft.core.errors import HandlerError, SqlGuardError
from etl_craft.core.text import is_safe_identifier
from etl_craft.engine import transitions
from etl_craft.engine.repository.offsets import StoredOffset
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.session import Session
from etl_craft.scripting import Offset
from etl_craft.warehouse.connection import warehouse_dialect


@dataclass(frozen=True)
class Watermark:
    """The SELECT column and the typed lower bound it reads after."""

    column: str
    start: Offset

    def literal(self, config: ConnectorConfig) -> str:
        """Render a checked value as a literal in the warehouse's SQL dialect."""
        value = self.start.value
        if self.start.datatype == "NUMBER":
            return format(value, "f") if isinstance(value, Decimal) else str(value)
        dialect = warehouse_dialect(config).sqlglot_dialect if config.warehouse else None
        if isinstance(value, datetime):
            return f"TIMESTAMP {exp.Literal.string(value.isoformat(sep=' ')).sql(dialect=dialect)}"
        # SQLAlchemy text() reads even a quoted :name as a bind without this escape.
        return exp.Literal.string(str(value)).sql(dialect=dialect).replace(":", "\\:")


def read_watermark(
    params: Mapping[str, str], stored: StoredOffset | None = None
) -> Watermark | None:
    """Require a complete watermark definition and a stored offset of the same type."""
    names = ("WATERMARK_COLUMN", "WATERMARK_TYPE", "WATERMARK_START")
    if not any(name in params for name in names):
        return None
    missing = [
        name
        for name in names
        if params.get(name) is None or (name != "WATERMARK_START" and not params[name].strip())
    ]
    if missing:
        raise SqlGuardError(
            f"SQL watermark lacks {', '.join(missing)}; set all of {', '.join(names)}"
        )
    action = (params.get("SQL_ACTION") or "").strip().upper()
    if action not in {
        SqlAction.APPEND_TABLE,
        SqlAction.SCD1_MERGE,
        SqlAction.SCD2_MERGE,
        SqlAction.DELETE_ROWS,
    }:
        raise SqlGuardError(
            f"WATERMARK_COLUMN does not apply to SQL_ACTION={action}; use "
            "APPEND_TABLE, SCD1_MERGE, SCD2_MERGE or DELETE_ROWS"
        )
    column = params["WATERMARK_COLUMN"].strip()
    if not is_safe_identifier(column):
        raise SqlGuardError(
            f"WATERMARK_COLUMN={column!r} is not a plain SELECT column; alias it to "
            "letters, digits and underscores"
        )
    try:
        start = Offset(
            params["WATERMARK_START"], OffsetType(params["WATERMARK_TYPE"].strip().upper())
        )
        if stored is not None:
            if stored.offset_type != start.datatype or stored.offset_value is None:
                raise SqlGuardError(
                    f"WATERMARK_TYPE={start.datatype}, but the stored offset is "
                    f"{stored.offset_type} {stored.offset_value!r}; keep the stored type "
                    "and set a value with `etl-craft mark --watermark`"
                )
            start = Offset.from_stored(stored.offset_type, stored.offset_value)
    except (HandlerError, ValueError) as error:
        raise SqlGuardError(
            f"SQL watermark {params['WATERMARK_TYPE']!r} "
            f"{params['WATERMARK_START']!r}: {error}; set WATERMARK_TYPE to NUMBER, TEXT "
            "or TIMESTAMP and WATERMARK_START to a value of that type"
        ) from error
    return Watermark(column, start)


def record_range(
    session: Session, stage: str, watermark: Watermark, context: TaskContext, engine_db: Engine
) -> None:
    """Record the staged range before target writes; only success advances the offset."""
    columns = {name.lower(): name for name, _ in session.columns(stage)}
    column = columns.get(watermark.column.lower())
    if column is None:
        raise SqlGuardError(
            f"WATERMARK_COLUMN={watermark.column!r} is absent from the SELECT "
            f"({', '.join(columns)}); return it from the SELECT"
        )
    if session.count(
        f"SELECT COUNT(*) FROM {stage} WHERE {column} IS NULL", step="NULL watermarks"
    ):
        raise SqlGuardError(
            f"WATERMARK_COLUMN={column} contains NULL; return a non-NULL NUMBER, "
            "TEXT or TIMESTAMP so every row has a position"
        )
    value = session.run(
        f"SELECT MAX({column}) AS watermark_to FROM {stage}",
        step="watermark upper bound",
    ).scalar_one()
    end = watermark.start
    if value is not None:
        expected = {"NUMBER": (int, Decimal), "TEXT": (str,), "TIMESTAMP": (datetime,)}[
            end.datatype
        ]
        if (
            isinstance(value, bool)
            or not isinstance(value, expected)
            or (isinstance(value, Decimal) and not value.is_finite())
        ):
            raise SqlGuardError(
                f"WATERMARK_COLUMN={column} returned {value!r} "
                f"({type(value).__name__}), expected {end.datatype}; "
                "CAST the column to that type in the SELECT"
            )
        candidate = Offset(value, end.datatype)
        advances = session.run(
            f"SELECT MAX({column}) > {watermark.literal(context.config)} AS advances FROM {stage}",
            step="compare the watermark in the source column's ordering",
        ).scalar_one()
        if advances:
            end = candidate
    with engine_db.begin() as conn:
        transitions.record_watermark_range(
            conn,
            context.task_run_id,
            watermark.start.stored(),
            end.stored(),
            attempt_id=context.attempt_id,
            owner=context.owner,
        )
    session.watermark_to = (
        StoredOffset(end.datatype, end.stored())
        if value is not None and not context.backfill
        else None
    )
