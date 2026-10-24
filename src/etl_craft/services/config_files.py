"""Merge package-owned configuration from the project's CSV files into the Engine DB.

The files in ``config/`` are the whole configuration, inactive rows included: each row has an
``ACTIVE_FLAG``. ``read_config_files`` reads and checks every file. ``sync_config`` merges them by
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
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import ClassVar, Literal

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import migration as current_migration
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import EngineDbError, MetadataFileError, PermissionDeniedError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.config_rows import (
    ACTIVE_FLAG,
    BUSINESS_RULES,
    OBJECT_TABLES,
    PIPELINE_DEPENDENCIES,
    PIPELINE_GRANTS,
    PIPELINES,
    POOLS,
    PROJECTS,
    SOURCE_COLUMNS,
    TABLES,
    TASK_DEPENDENCIES,
    TASK_PARAMETERS,
    TASKS,
    ConfigTable,
    Key,
    StoredRow,
    Value,
    insert_row,
    read_rows,
    retire_row,
    update_row,
)
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.config_export import export_config as export_config
from etl_craft.services.config_validation import parse_cell, row_problem
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
        return self.values[ACTIVE_FLAG.name] == YesNo.YES


@dataclass(frozen=True)
class ConfigFiles:
    """The config files, read and checked, with the revision of their content."""

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
    """Config files written from the ``CFG_`` rows, rebuilt ``as_of`` a moment or now (``None``)."""

    SCHEMA: ClassVar[str] = "etl-craft/config-export/1"
    directory: str
    rows: dict[str, int] = field(default_factory=dict)
    as_of: str | None = None


def config_directory(config: ConnectorConfig, directory: Path | None = None) -> Path:
    """Return ``directory``, or ``config/`` in the project directory."""
    return directory if directory is not None else config.project_dir / CONFIG_DIRNAME


def read_config_files(directory: Path) -> ConfigFiles:
    """Read and check the config files; raise ``MetadataFileError`` naming every problem.

    The pipeline/task files and grants.csv must exist; tables.csv and columns.csv may be omitted
    together to leave their contracts unchanged. A file with only its header row holds no rows.
    """
    if not directory.is_dir():
        raise MetadataFileError(
            f"no config folder at {directory}: create it with `etl-craft config export`, or "
            f"write {', '.join(table.file for table in TABLES)} there"
        )
    digest = hashlib.sha256()
    rows: dict[str, dict[Key, FileRow]] = {}
    problems: list[str] = []
    if (directory / "source_columns.csv").exists():
        problems.append(
            "source_columns.csv uses the retired source layout; export the upgraded "
            "Engine DB to tables.csv and columns.csv and apply edits to those files"
        )
    for table in TABLES:
        path = directory / table.file
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            if table in (SOURCE_COLUMNS, OBJECT_TABLES):
                continue
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
    if (
        SOURCE_COLUMNS.file in rows
        or rows.get(BUSINESS_RULES.file)
        or any(
            row.values["TARGET_TABLE_NAME"] is not None for row in rows.get(TASKS.file, {}).values()
        )
    ) and OBJECT_TABLES.file not in rows:
        problems.append(
            "columns.csv and task/rule targets require tables.csv; export the contract files"
        )
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
    header, columns = next(reader, None), ", ".join(column.name for column in table.columns)
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
                values[column.name] = parse_cell(column, given.get(column.name, ""))
            except MetadataFileError as error:
                problems.append(f"{path} line {line}, column {column.name}: {error}")
        if len(values) != len(table.columns):
            continue
        problem = row_problem(table, values)
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
    if table is SOURCE_COLUMNS:
        positions: dict[Key, FileRow] = {}
        for row in rows.values():
            if not row.active:
                continue
            key = tuple(
                row.values[name]
                for name in ("CATALOG_NAME", "SCHEMA_NAME", "TABLE_NAME", "COLUMN_POSITION")
            )
            if key in positions:
                problems.append(
                    f"{path} lines {positions[key].line} and {row.line}: "
                    f"{'.'.join(str(part) for part in key[:-1] if part is not None)} has "
                    f"COLUMN_POSITION={key[-1]} twice; give each active column a distinct position"
                )
            positions[key] = row
    return rows


def _check_references(rows: dict[str, dict[Key, FileRow]], problems: list[str]) -> None:
    """Report every row that names a pipeline or task the files do not hold."""
    pipelines = set(rows[PIPELINES.file])
    tasks = set(rows[TASKS.file])
    tables = set(rows.get(OBJECT_TABLES.file, {}))
    for table in (
        TASKS,
        TASK_PARAMETERS,
        TASK_DEPENDENCIES,
        PIPELINE_DEPENDENCIES,
        BUSINESS_RULES,
        SOURCE_COLUMNS,
    ):
        for row in rows.get(table.file, {}).values():
            for kind, end in _ends(table, row.values):
                holder = {"pipeline": pipelines, "task": tasks, "table": tables}[kind]
                if end not in holder:
                    named = ".".join(str(code) for code in end if code is not None)
                    file = {
                        "pipeline": PIPELINES.file,
                        "task": TASKS.file,
                        "table": OBJECT_TABLES.file,
                    }[kind]
                    problems.append(
                        f"{table.file} line {row.line}: {kind} {named} is not in {file}"
                    )


def _ends(table: ConfigTable, values: Mapping[str, Value]) -> list[tuple[str, Key]]:
    """Return the pipelines and tasks a row names, by code."""
    if table in (POOLS, PROJECTS, PIPELINES, OBJECT_TABLES, PIPELINE_GRANTS):
        return []
    if table is SOURCE_COLUMNS:
        return [("table", tuple(values[name] for name in OBJECT_TABLES.key))]
    if table is TASKS:
        return [("pipeline", (values["PIPELINE_CODE"],))] + (
            [("table", tuple(values["TARGET_" + name] for name in OBJECT_TABLES.key))]
            if values["TARGET_TABLE_NAME"] is not None
            else []
        )
    if table is PIPELINE_DEPENDENCIES:
        return [
            ("pipeline", (values["PIPELINE_CODE"],)),
            *(
                []
                if values["DEPENDS_ON_PROJECT_CODE"]
                else [("pipeline", (values["DEPENDS_ON_PIPELINE_CODE"],))]
            ),
        ]
    ends: list[tuple[str, Key]] = [("task", (values["PIPELINE_CODE"], values["TASK_CODE"]))]
    if table is TASK_DEPENDENCIES and not values["DEPENDS_ON_PROJECT_CODE"]:
        ends.append(("task", (values["DEPENDS_ON_PIPELINE_CODE"], values["DEPENDS_ON_TASK_CODE"])))
    if table is BUSINESS_RULES:
        ends.append(("table", tuple(values[name] for name in OBJECT_TABLES.key)))
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


def current_rows(
    table: ConfigTable, rows: list[StoredRow], current_ids: Mapping[str, set[int | str]]
) -> tuple[dict[Key, StoredRow], list[StoredRow]]:
    """Return each key's current row, and the active rows whose ids disagree with each other.

    The Engine DB's unique indexes keep at most one active row per key. A row hanging off a
    pipeline or task that is not current is history, and is left out.
    """
    groups: dict[Key, list[StoredRow]] = {}
    malformed: list[StoredRow] = []
    for row in rows:
        if any(
            parent is not None and parent not in current_ids[file]
            for index, (parent, (_, file)) in enumerate(
                zip(row.parents, table.parents, strict=True)
            )
            if index != 1 or not row.values.get("DEPENDS_ON_PROJECT_CODE")
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
    conn: Connection,
    table: ConfigTable,
    file_row: FileRow,
    current: Mapping[str, Mapping[Key, StoredRow]],
) -> dict[str, int | None]:
    """Return the ids an insert of ``file_row`` sets, from the current pipelines and tasks."""
    if table is PIPELINE_GRANTS:
        from etl_craft.engine.queries import statement

        group_id = conn.execute(
            statement(conn, "group_identity"), {"name": file_row.values["GROUP_NAME"]}
        ).scalar_one_or_none()
        if group_id is None:
            raise MetadataFileError(
                f"grants.csv line {file_row.line}: group {file_row.values['GROUP_NAME']!r} "
                "does not exist; ask an administrator to use `etl-craft group add`"
            )
        return {
            "PROJECT_ID": int(next(iter(current[PROJECTS.file].values())).row_id),
            "GROUP_ID": group_id,
        }
    found: list[StoredRow] = []
    for kind, end in _ends(table, file_row.values):
        file = {"pipeline": PIPELINES.file, "task": TASKS.file, "table": OBJECT_TABLES.file}[kind]
        parent = current[file].get(end)
        if parent is None:
            named = ".".join(str(code) for code in end if code is not None)
            raise MetadataFileError(
                f"{table.file} line {file_row.line}: {table.label(file_row.values)} is active, "
                f"but {kind} {named} has no row in the Engine DB and is inactive in {file}; "
                f"set the {kind}'s ACTIVE_FLAG to Y, or this row's to N"
            )
        found.append(parent)
    if (
        table in (PIPELINE_DEPENDENCIES, TASK_DEPENDENCIES)
        and file_row.values["DEPENDS_ON_PROJECT_CODE"]
    ):
        found.append(external_dependency(conn, table, file_row))
    if table in (PIPELINES, OBJECT_TABLES):
        return {"PROJECT_ID": int(next(iter(current[PROJECTS.file].values())).row_id)}
    if table in (POOLS, PROJECTS, PIPELINE_GRANTS):
        return {}
    if table is SOURCE_COLUMNS:
        return {"TABLE_ID": int(found[0].row_id)}
    if table is TASKS:
        return {
            "PIPELINE_ID": int(found[0].row_id),
            "TARGET_TABLE_ID": int(found[1].row_id) if len(found) > 1 else None,
        }
    if table is TASK_PARAMETERS:
        return {"TASK_ID": int(found[0].row_id)}
    if table is PIPELINE_DEPENDENCIES:
        return {"PIPELINE_ID": int(found[0].row_id), "DEPENDS_ON_PIPELINE_ID": int(found[1].row_id)}
    if table is TASK_DEPENDENCIES:
        return {
            "PIPELINE_ID": found[0].parents[0],
            "TASK_ID": int(found[0].row_id),
            "DEPENDS_ON_PIPELINE_ID": found[1].parents[0],
            "DEPENDS_ON_TASK_ID": int(found[1].row_id),
        }
    return {
        "PIPELINE_ID": found[0].parents[0],
        "TASK_ID": int(found[0].row_id),
        "TARGET_TABLE_ID": int(found[1].row_id),
    }


def external_dependency(conn: Connection, table: ConfigTable, file_row: FileRow) -> StoredRow:
    """Resolve a named upstream project without consulting local file keys."""
    from etl_craft.engine.queries import statement

    upstream = conn.execute(
        statement(conn, "config_dependency_target"),
        {
            "project_code": file_row.values["DEPENDS_ON_PROJECT_CODE"],
            "pipeline_code": file_row.values["DEPENDS_ON_PIPELINE_CODE"],
            "task_code": file_row.values.get("DEPENDS_ON_TASK_CODE"),
        },
    ).one_or_none()
    if upstream is None:
        raise MetadataFileError(
            f"{table.file} line {file_row.line}: upstream project/pipeline/task "
            f"{file_row.values['DEPENDS_ON_PROJECT_CODE']}."
            f"{file_row.values['DEPENDS_ON_PIPELINE_CODE']} is missing or inactive; "
            "apply the upstream project's active definitions first"
        )
    return StoredRow(
        int(upstream.task_id if table is TASK_DEPENDENCIES else upstream.pipeline_id),
        True,
        (int(upstream.pipeline_id),),
        True,
        {},
    )


def require_tables(engine: Engine) -> None:
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


def _merge(conn: Connection, files: ConfigFiles, project_code: str) -> list[Change]:
    """Write the changes that make the current rows match ``files``, parents first."""
    changes: list[Change] = []
    current: dict[str, dict[Key, StoredRow]] = {}
    current_ids: dict[str, set[int | str]] = {}
    for table in TABLES:
        project_id = next(
            (int(row.row_id) for row in current.get(PROJECTS.file, {}).values()), None
        )
        if table.file not in files.rows:
            if table is OBJECT_TABLES:
                current[table.file], _ = current_rows(
                    table,
                    read_rows(conn, table, project_code=project_code, project_id=project_id),
                    current_ids,
                )
                current_ids[table.file] = {row.row_id for row in current[table.file].values()}
            continue
        stored, malformed = current_rows(
            table,
            read_rows(conn, table, project_code=project_code, project_id=project_id),
            current_ids,
        )
        wanted = files.rows[table.file]
        if table is SOURCE_COLUMNS:
            retire_reordered_columns(conn, stored, wanted)

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
                insert_row(conn, table, file_row.values, _links(conn, table, file_row, current))
                changes.append(Change(table.file, table.table, "insert", label))
                continue
            differ = row_changes(table, existing, file_row)
            if not existing.active:
                update_row(
                    conn,
                    table,
                    existing.row_id,
                    file_row.values,
                    _links(conn, table, file_row, current),
                )
                flag = ColumnChange(ACTIVE_FLAG.name, "N", "Y")
                changes.append(
                    Change(table.file, table.table, "reactivate", label, (flag, *differ))
                )
            elif differ:
                update_row(
                    conn,
                    table,
                    existing.row_id,
                    file_row.values,
                    _links(conn, table, file_row, current),
                )
                changes.append(Change(table.file, table.table, "update", label, differ))
        current[table.file], _ = current_rows(
            table,
            read_rows(conn, table, project_code=project_code, project_id=project_id),
            current_ids,
        )
        current_ids[table.file] = {row.row_id for row in current[table.file].values()}
    return changes


def row_changes(
    table: ConfigTable, existing: StoredRow, wanted: FileRow
) -> tuple[ColumnChange, ...]:
    """Describe changed stored values for updates and reactivations."""
    return tuple(
        ColumnChange(name, cell(existing.values[name]), cell(wanted.values[name]))
        for name in table.updated
        if existing.values[name] != wanted.values[name]
    )


def retire_reordered_columns(
    conn: Connection, stored: Mapping[Key, StoredRow], wanted: Mapping[Key, FileRow]
) -> None:
    """Free active column positions before applying a reordered contract."""
    for key, row in stored.items():
        desired = wanted.get(key)
        if (
            row.active
            and desired is not None
            and desired.active
            and row.values["COLUMN_POSITION"] != desired.values["COLUMN_POSITION"]
        ):
            retire_row(conn, SOURCE_COLUMNS, row.row_id, SOURCE_COLUMNS.label(row.values))


def resolved_files(files: ConfigFiles, config: ConnectorConfig) -> ConfigFiles:
    """Resolve warehouse identities consistently for both merging and exporting files."""
    from etl_craft.engine.repository.source_contracts import resolve_source_identity
    from etl_craft.services.lineage import dialect_and_catalog

    dialect, catalog = dialect_and_catalog(config)
    resolved = dict(files.rows)
    for table in (
        OBJECT_TABLES,
        SOURCE_COLUMNS,
        BUSINESS_RULES,
        TASKS,
        PIPELINE_DEPENDENCIES,
        TASK_DEPENDENCIES,
    ):
        if table.file not in resolved:
            continue
        rows: dict[Key, FileRow] = {}
        for row in resolved[table.file].values():
            names = (
                tuple("TARGET_" + name for name in OBJECT_TABLES.key)
                if table is TASKS
                else OBJECT_TABLES.key
            )
            values = row.values
            if table in (PIPELINE_DEPENDENCIES, TASK_DEPENDENCIES):
                upstream = values["DEPENDS_ON_PROJECT_CODE"]
                values = values | {
                    "DEPENDS_ON_PROJECT_CODE": None if upstream == config.project else upstream
                }
            elif table is not TASKS or values["TARGET_TABLE_NAME"] is not None:
                identity = resolve_source_identity(
                    str(values[names[0]]) if values[names[0]] is not None else None,
                    str(values[names[1]]),
                    str(values[names[2]]),
                    catalog,
                    dialect,
                )
                values = values | dict(zip(names, identity, strict=True))
            if table is SOURCE_COLUMNS:
                values["COLUMN_NAME"] = str(values["COLUMN_NAME"]).lower()
            key = tuple(values[name] for name in table.key)
            if key in rows:
                raise MetadataFileError(
                    f"{table.file} lines {rows[key].line} and {row.line} resolve to "
                    f"{table.label(values)}; keep one identity, including inactive rows"
                )
            rows[key] = FileRow(row.line, values)
        resolved[table.file] = rows
    return ConfigFiles(files.directory, files.revision, resolved)


def sync_config(
    engine: Engine,
    config: ConnectorConfig,
    files: ConfigFiles,
    *,
    apply: bool,
    allow_grants: bool = True,
) -> ConfigSync:
    """Merge ``files`` into the ``CFG_`` tables; with ``apply`` false, roll it all back.

    The result is validated in the same transaction; when ``validate`` fails it, nothing is
    committed.
    """
    require_tables(engine)
    selected = files.rows.get(PROJECTS.file, {})
    if set(selected) != {(config.project,)}:
        found = ", ".join(str(key[0]) for key in selected) or "no project"
        raise MetadataFileError(
            f"{files.directory / PROJECTS.file}: selected project is {config.project!r}, but "
            f"the file contains {found}; keep exactly the selected project row in projects.csv"
        )
    files = resolved_files(files, config)
    changes: list[Change] = []
    findings: tuple[Finding, ...] = ()
    applied = False
    with locks.MIGRATE.hold(engine), _labelled(files.revision):
        try:
            with engine.begin() as conn:
                changes.extend(normalized_changes(conn, config))
                changes.extend(_merge(conn, files, config.project))
                denied = [change.row for change in changes if change.file == "grants.csv"]
                if apply and denied and not allow_grants:
                    raise PermissionDeniedError(
                        "grants.csv: admin role required to change "
                        + "; ".join(denied)
                        + "; ask an administrator to apply these files; no changes were committed"
                    )
                project = conn.execute(
                    statement(conn, "project_by_code"), {"code": config.project}
                ).one_or_none()
                if project is None:
                    raise MetadataFileError(
                        f"projects.csv: project {config.project!r} was not created; expected "
                        "ACTIVE_FLAG=Y for a new project; activate it before applying metadata"
                    )
                with selected_project(int(project.project_id)):
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


def normalized_changes(conn: Connection, config: ConnectorConfig) -> list[Change]:
    """Resolve only this project's stored source identities before merging its CSV keys."""
    changes: list[Change] = []
    from etl_craft.engine.repository.source_contracts import resolve_project_sources
    from etl_craft.services.lineage import dialect_and_catalog

    dialect, catalog = dialect_and_catalog(config)
    existing_project = conn.execute(
        statement(conn, "project_by_code"), {"code": config.project}
    ).one_or_none()
    with selected_project(int(existing_project.project_id) if existing_project else None):
        resolved = resolve_project_sources(conn, catalog, dialect) if existing_project else []
    for before, after in resolved:
        values = dict(zip(OBJECT_TABLES.key, after, strict=True))
        changes.append(
            Change(
                OBJECT_TABLES.file,
                OBJECT_TABLES.table,
                "update",
                OBJECT_TABLES.label(values),
                tuple(
                    ColumnChange(name, cell(old), cell(new))
                    for name, old, new in zip(OBJECT_TABLES.key, before, after, strict=True)
                    if old != new
                ),
            )
        )
    return changes
