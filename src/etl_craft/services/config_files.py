"""The project's config files: one CSV file per ``CFG_`` table, loaded into the Engine DB.

The files in ``config/`` are the whole configuration, inactive rows included: each row has an
``ACTIVE_FLAG``. ``read_config_files`` reads and checks all six. ``sync_config`` merges them by
key, in one transaction:

- a row the Engine DB lacks, flagged ``Y``, is inserted;
- a row whose values changed is updated in place, so its id and the history that refers to it
  stay; a row flagged ``Y`` whose row is retired is made active again, the same way;
- a row flagged ``N``, or an active row the files no longer hold, is retired.

Each key has one current row: the active one, or else the last one retired. Rows from before a
pipeline or task was replaced under the same code are history, and are left as they are. Every
change is recorded with its actor and the files' revision, and ``validate`` checks the result
inside the same transaction, so a configuration it fails is never committed. ``export_config``
writes the current rows out as files.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import ClassVar, Literal

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import migration as current_migration
from etl_craft.core.errors import EngineDbError, MetadataFileError
from etl_craft.core.text import is_metadata_code
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.repository.config_rows import (
    ACTIVE_FLAG,
    BUSINESS_RULES,
    PIPELINE_DEPENDENCIES,
    PIPELINES,
    TABLES,
    TASK_DEPENDENCIES,
    TASK_PARAMETERS,
    TASKS,
    Column,
    ConfigTable,
    Key,
    StoredRow,
    Value,
    insert_row,
    read_rows,
    retire_row,
    update_row,
)
from etl_craft.services.doctor import Status
from etl_craft.services.validate import Finding, validate_on

logger = logging.getLogger(__name__)

CONFIG_DIRNAME = "config"


@dataclass(frozen=True)
class FileRow:
    """One row of a config file: the line it ends on, and its value in every column."""

    line: int
    values: dict[str, Value]

    @property
    def active(self) -> bool:
        """Whether the row's ``ACTIVE_FLAG`` is ``Y``."""
        return self.values[ACTIVE_FLAG.name] == "Y"


@dataclass(frozen=True)
class ConfigFiles:
    """The six config files, read and checked, with the revision of their content."""

    directory: Path
    revision: str
    rows: dict[str, dict[Key, FileRow]]


@dataclass(frozen=True)
class ColumnChange:
    """A column a change sets, as the files write its value before and after."""

    column: str
    before: str
    after: str


@dataclass(frozen=True)
class Change:
    """One row ``config apply`` inserts, updates, makes active again or retires, and why."""

    file: str
    table: str
    operation: Literal["insert", "update", "reactivate", "retire"]
    row: str
    columns: tuple[ColumnChange, ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class ConfigSync:
    """What loading the config files changed, or would change, and what ``validate`` found."""

    SCHEMA: ClassVar[str] = "etl-craft/config-sync/1"
    directory: str
    revision: str
    applied: bool
    failed: bool
    """Whether ``validate`` failed the resulting rows, so nothing was applied."""
    changes: tuple[Change, ...]
    findings: tuple[Finding, ...] = ()


@dataclass(frozen=True)
class ConfigExport:
    """The config files written from the current ``CFG_`` rows, with each file's row count."""

    SCHEMA: ClassVar[str] = "etl-craft/config-export/1"
    directory: str
    rows: dict[str, int] = field(default_factory=dict)


def config_directory(config: ConnectorConfig, directory: Path | None = None) -> Path:
    """Return ``directory``, or ``config/`` in the project directory."""
    return directory if directory is not None else config.project_dir / CONFIG_DIRNAME


def read_config_files(directory: Path) -> ConfigFiles:
    """Read and check the six config files; raise ``MetadataFileError`` naming every problem.

    Every file must exist; a file with only its header row holds no rows.
    """
    if not directory.is_dir():
        raise MetadataFileError(
            f"no config folder at {directory}: create it with `etl-craft config export`, or "
            f"write {', '.join(table.file for table in TABLES)} there"
        )
    digest = hashlib.sha256()
    rows: dict[str, dict[Key, FileRow]] = {}
    problems: list[str] = []
    for table in TABLES:
        path = directory / table.file
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            problems.append(
                f"{path} is missing: every config file must exist, even with only its header "
                f"row ({','.join(column.name for column in table.columns)})"
            )
            continue
        except OSError as error:
            problems.append(f"{path} cannot be read: {error}")
            continue
        digest.update(table.file.encode() + b"\0" + payload + b"\0")
        rows[table.file] = _read_table(table, path, payload, problems)
    if not problems:
        _check_references(rows, problems)
    if problems:
        raise MetadataFileError(
            f"{len(problems)} problem(s) in {directory}:\n  " + "\n  ".join(problems)
        )
    return ConfigFiles(directory, digest.hexdigest()[:12], rows)


def _read_table(
    table: ConfigTable, path: Path, payload: bytes, problems: list[str]
) -> dict[Key, FileRow]:
    try:
        content = payload.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        problems.append(f"{path} is not UTF-8 text: {error}")
        return {}
    reader = csv.reader(io.StringIO(content, newline=""))
    header = next(reader, None)
    columns = ", ".join(column.name for column in table.columns)
    if header is None:
        problems.append(f"{path} is empty: write its header row ({columns})")
        return {}
    known = {column.name for column in table.columns}
    header_problems = [
        f"{path} line 1: unknown column {name!r}; its columns are {columns}"
        for name in header
        if name not in known
    ]
    header_problems += [
        f"{path} line 1: column {name} appears more than once"
        for name in sorted({name for name in header if header.count(name) > 1})
    ]
    header_problems += [
        f"{path} line 1: column {column.name} is missing; it needs a value in every row"
        for column in table.columns
        if column.required and column.name not in header
    ]
    if header_problems:
        problems.extend(header_problems)
        return {}
    rows: dict[Key, FileRow] = {}
    for cells in reader:
        line = reader.line_num
        if not any(cell.strip() for cell in cells):
            continue
        if len(cells) != len(header):
            problems.append(
                f"{path} line {line}: {len(cells)} values for {len(header)} columns; quote a "
                "value that holds a comma"
            )
            continue
        given = dict(zip(header, cells, strict=True))
        values: dict[str, Value] = {}
        for column in table.columns:
            try:
                values[column.name] = _parse(column, given.get(column.name, ""))
            except ValueError as error:
                problems.append(f"{path} line {line}, column {column.name}: {error}")
        if len(values) != len(table.columns):
            continue
        problem = _row_problem(table, values)
        if problem is not None:
            problems.append(f"{path} line {line}: {problem}")
            continue
        key = tuple(values[name] for name in table.key)
        if key in rows:
            problems.append(
                f"{path} lines {rows[key].line} and {line} both hold {table.label(values)}; "
                "keep one of them"
            )
            continue
        rows[key] = FileRow(line, values)
    return rows


def _parse(column: Column, cell: str) -> Value:
    """Return a cell's value in its column's type; raise ``ValueError`` saying what was expected."""
    if cell == "":
        if column.required:
            raise ValueError("is empty; this column needs a value")
        return column.default
    if column.kind == "code":
        if not is_metadata_code(cell):
            raise ValueError(
                f"{cell!r} is not a code: start with an ASCII letter, then use letters, digits "
                "or _, at most 128 characters"
            )
        return cell
    if column.kind == "choice":
        if cell not in column.choices:
            raise ValueError(f"{cell!r} is not one of {', '.join(column.choices)}")
        return cell
    if column.kind == "int":
        if not re.fullmatch(r"-?[0-9]+", cell):
            raise ValueError(f"{cell!r} is not a whole number")
        number = int(cell)
        if column.minimum is not None and number < column.minimum:
            raise ValueError(f"{number} is less than {column.minimum}")
        return number
    if column.kind == "float":
        try:
            decimal = Decimal(cell)
        except InvalidOperation:
            raise ValueError(f"{cell!r} is not a number") from None
        if not decimal.is_finite():
            raise ValueError(f"{cell!r} is not a finite number")
        return float(decimal)
    if column.kind == "date":
        try:
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", cell):
                raise ValueError
            return date.fromisoformat(cell)
        except ValueError:
            raise ValueError(f"{cell!r} is not a date written YYYY-MM-DD") from None
    if column.kind == "json":
        try:
            parsed = json.loads(cell)
        except ValueError as error:
            raise ValueError(f"is not JSON: {error}") from None
        if not isinstance(parsed, dict):
            raise ValueError("is not a JSON object; write it as {...}")
        return parsed
    return cell


def _row_problem(table: ConfigTable, values: dict[str, Value]) -> str | None:
    if table is TASKS:
        condition, count = values["RUN_CONDITION"], values["RUN_CONDITION_COUNT"]
        if condition == "N" and count is None:
            return "RUN_CONDITION N needs a RUN_CONDITION_COUNT"
        if condition != "N" and count is not None:
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


def _check_references(rows: dict[str, dict[Key, FileRow]], problems: list[str]) -> None:
    """Report every row that names a pipeline or task the files do not hold."""
    pipelines = set(rows[PIPELINES.file])
    tasks = set(rows[TASKS.file])
    for table in (TASKS, TASK_PARAMETERS, TASK_DEPENDENCIES, PIPELINE_DEPENDENCIES, BUSINESS_RULES):
        for row in rows[table.file].values():
            for kind, end in _ends(table, row.values):
                holder = pipelines if kind == "pipeline" else tasks
                if end not in holder:
                    named = ".".join(str(code) for code in end)
                    file = PIPELINES.file if kind == "pipeline" else TASKS.file
                    problems.append(
                        f"{table.file} line {row.line}: {kind} {named} is not in {file}"
                    )


def _ends(table: ConfigTable, values: Mapping[str, Value]) -> list[tuple[str, Key]]:
    """Return the pipelines and tasks a row names, by code."""
    if table is PIPELINES:
        return []
    if table is TASKS:
        return [("pipeline", (values["PIPELINE_CODE"],))]
    if table is PIPELINE_DEPENDENCIES:
        return [
            ("pipeline", (values["PIPELINE_CODE"],)),
            ("pipeline", (values["DEPENDS_ON_PIPELINE_CODE"],)),
        ]
    ends: list[tuple[str, Key]] = [("task", (values["PIPELINE_CODE"], values["TASK_CODE"]))]
    if table is TASK_DEPENDENCIES:
        ends.append(("task", (values["DEPENDS_ON_PIPELINE_CODE"], values["DEPENDS_ON_TASK_CODE"])))
    return ends


def cell(value: Value) -> str:
    """Write a value as a config file holds it: empty for NULL, JSON objects as JSON."""
    if value is None:
        return ""
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else format(Decimal(repr(value)), "f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _current(
    table: ConfigTable, rows: list[StoredRow], current_ids: Mapping[str, set[int]]
) -> tuple[dict[Key, StoredRow], list[StoredRow]]:
    """Return each key's current row, and the active rows whose ids disagree with each other.

    The Engine DB's unique indexes keep at most one active row per key. A row hanging off a
    pipeline or task that is not current is history, and is left out.
    """
    groups: dict[Key, list[StoredRow]] = {}
    malformed: list[StoredRow] = []
    for row in rows:
        if any(
            parent not in current_ids[file]
            for parent, (_, file) in zip(row.parents, table.parents, strict=True)
        ):
            continue
        if not row.well_formed:
            if row.active:
                malformed.append(row)
            continue
        groups.setdefault(tuple(row.values[name] for name in table.key), []).append(row)
    current = {
        key: next((row for row in group if row.active), group[-1]) for key, group in groups.items()
    }
    return current, malformed


def _links(
    table: ConfigTable,
    file_row: FileRow,
    current: Mapping[str, Mapping[Key, StoredRow]],
) -> dict[str, int]:
    """Return the ids an insert of ``file_row`` sets, from the current pipelines and tasks."""
    found: list[StoredRow] = []
    for kind, end in _ends(table, file_row.values):
        file = PIPELINES.file if kind == "pipeline" else TASKS.file
        parent = current[file].get(end)
        if parent is None:
            named = ".".join(str(code) for code in end)
            raise MetadataFileError(
                f"{table.file} line {file_row.line}: {table.label(file_row.values)} is active, "
                f"but {kind} {named} has no row in the Engine DB and is inactive in {file}; "
                f"set the {kind}'s ACTIVE_FLAG to Y, or this row's to N"
            )
        found.append(parent)
    if table is PIPELINES:
        return {}
    if table is TASKS:
        return {"PIPELINE_ID": found[0].row_id}
    if table is TASK_PARAMETERS:
        return {"TASK_ID": found[0].row_id}
    if table is PIPELINE_DEPENDENCIES:
        return {"PIPELINE_ID": found[0].row_id, "DEPENDS_ON_PIPELINE_ID": found[1].row_id}
    if table is TASK_DEPENDENCIES:
        return {
            "PIPELINE_ID": found[0].parents[0],
            "TASK_ID": found[0].row_id,
            "DEPENDS_ON_PIPELINE_ID": found[1].parents[0],
            "DEPENDS_ON_TASK_ID": found[1].row_id,
        }
    return {"PIPELINE_ID": found[0].parents[0], "TASK_ID": found[0].row_id}


def _require_tables(engine: Engine) -> None:
    """Raise ``EngineDbError`` naming the remedy when the ``CFG_`` tables do not exist yet."""
    names = tuple(table.table for table in TABLES)
    found = set(for_engine(engine).existing_tables(engine, names))
    missing = [name for name in names if name.lower() not in found]
    if missing:
        raise EngineDbError(
            f"the Engine DB has no {', '.join(missing)} table(s) yet: run `etl-craft setup` "
            "to create its tables, then run the command again"
        )


class _RollBackError(Exception):
    """Rolls back a configuration that is only planned, or that ``validate`` failed."""


@contextmanager
def _labelled(revision: str) -> Iterator[None]:
    token = current_migration.set(f"config@{revision}")
    try:
        yield
    finally:
        current_migration.reset(token)


def _merge(conn: Connection, files: ConfigFiles) -> list[Change]:
    """Write the changes that make the current rows match ``files``, parents first."""
    changes: list[Change] = []
    current: dict[str, dict[Key, StoredRow]] = {}
    current_ids: dict[str, set[int]] = {}
    for table in TABLES:
        stored, malformed = _current(table, read_rows(conn, table), current_ids)
        wanted = files.rows[table.file]

        def retire(row: StoredRow, reason: str, table: ConfigTable = table) -> None:
            label = table.label(row.values)
            retire_row(conn, table, row.row_id, label)
            flag = ColumnChange(ACTIVE_FLAG.name, "Y", "N")
            changes.append(Change(table.file, table.table, "retire", label, (flag,), reason))

        for row in malformed:
            retire(row, "its ids disagree with its pipeline or task")
        for key, row in stored.items():
            if key not in wanted and row.active:
                retire(row, "not in the file")
        for key, file_row in wanted.items():
            existing = stored.get(key)
            label = table.label(file_row.values)
            if not file_row.active:
                if existing is not None and existing.active:
                    retire(existing, "ACTIVE_FLAG is N")
                continue
            if existing is None:
                insert_row(conn, table, file_row.values, _links(table, file_row, current))
                changes.append(Change(table.file, table.table, "insert", label))
                continue
            differ = tuple(
                ColumnChange(name, cell(existing.values[name]), cell(file_row.values[name]))
                for name in table.updated
                if existing.values[name] != file_row.values[name]
            )
            if not existing.active:
                update_row(conn, table, existing.row_id, file_row.values)
                flag = ColumnChange(ACTIVE_FLAG.name, "N", "Y")
                changes.append(
                    Change(table.file, table.table, "reactivate", label, (flag, *differ))
                )
            elif differ:
                update_row(conn, table, existing.row_id, file_row.values)
                changes.append(Change(table.file, table.table, "update", label, differ))
        current[table.file], _ = _current(table, read_rows(conn, table), current_ids)
        current_ids[table.file] = {row.row_id for row in current[table.file].values()}
    return changes


def sync_config(
    engine: Engine, config: ConnectorConfig, files: ConfigFiles, *, apply: bool
) -> ConfigSync:
    """Merge ``files`` into the ``CFG_`` tables; with ``apply`` false, roll it all back.

    The result is validated in the same transaction; when ``validate`` fails it, nothing is
    committed.
    """
    _require_tables(engine)
    changes: list[Change] = []
    findings: tuple[Finding, ...] = ()
    applied = False
    with locks.MIGRATE.hold(engine), _labelled(files.revision):
        try:
            with engine.begin() as conn:
                changes = _merge(conn, files)
                findings = tuple(validate_on(conn, config).findings)
                if not apply or any(f.status is Status.FAIL for f in findings):
                    raise _RollBackError
                applied = True
        except _RollBackError:
            pass
    failed = any(finding.status is Status.FAIL for finding in findings)
    if applied:
        logger.info(
            "config %s from %s: %d change(s) applied", files.revision, files.directory, len(changes)
        )
    return ConfigSync(
        str(files.directory), files.revision, applied, failed, tuple(changes), findings
    )


def export_config(engine: Engine, directory: Path, *, overwrite: bool = False) -> ConfigExport:
    """Write each key's current ``CFG_`` row, active or not, as the six config files.

    Rows are sorted by key. Existing files are kept unless ``overwrite`` is set.
    """
    _require_tables(engine)
    existing = [table.file for table in TABLES if (directory / table.file).exists()]
    if existing and not overwrite:
        raise MetadataFileError(
            f"{directory} already holds {', '.join(existing)}; move them away, or pass "
            "--force to overwrite them with the Engine DB's rows"
        )
    current: dict[str, dict[Key, StoredRow]] = {}
    current_ids: dict[str, set[int]] = {}
    with engine.connect() as conn:
        for table in TABLES:
            current[table.file], _ = _current(table, read_rows(conn, table), current_ids)
            current_ids[table.file] = {row.row_id for row in current[table.file].values()}
    directory.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for table in TABLES:
        rows = sorted(
            current[table.file].values(),
            key=lambda row: tuple(cell(row.values[name]) for name in table.key),
        )
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow([column.name for column in table.columns])
        for row in rows:
            values = row.values | {ACTIVE_FLAG.name: "Y" if row.active else "N"}
            writer.writerow([cell(values[column.name]) for column in table.columns])
        (directory / table.file).write_text(buffer.getvalue(), encoding="utf-8")
        counts[table.file] = len(rows)
    return ConfigExport(str(directory), counts)
