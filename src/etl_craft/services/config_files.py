"""Merge package-owned configuration from the project's CSV files into the Engine DB.

The Engine DB is the source of truth. Files in ``config/`` carry only submitted changes;
absent rows and files are untouched. Each row has an ``ACTIVE_FLAG``.
``read_config_files`` reads and checks every file. ``sync_config`` merges them by
key, in one transaction:

- a row the Engine DB lacks, flagged ``Y``, is inserted;
- a row whose values changed is updated in place, so its id and the history that refers to it
  stay; a row flagged ``Y`` whose row is retired is made active again, the same way;
- an existing row explicitly flagged ``N`` is retired; a new inactive key is refused.

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
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import migration as current_migration
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import EngineDbError, MetadataFileError
from etl_craft.core.text import suggest
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.config_rows import (
    TABLES,
    ConfigTable,
    Key,
    Value,
)
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.config_export import export_config as export_config
from etl_craft.services.config_models import (
    Change as Change,
)
from etl_craft.services.config_models import (
    ColumnChange as ColumnChange,
)
from etl_craft.services.config_models import (
    ConfigExport as ConfigExport,
)
from etl_craft.services.config_models import (
    ConfigFiles as ConfigFiles,
)
from etl_craft.services.config_models import (
    ConfigSync as ConfigSync,
)
from etl_craft.services.config_models import (
    FileRow as FileRow,
)
from etl_craft.services.config_models import (
    cell as cell,
)
from etl_craft.services.config_upserts import current_rows as current_rows
from etl_craft.services.config_upserts import merge_rows
from etl_craft.services.config_upserts import resolved_files as resolved_files
from etl_craft.services.config_validation import parse_cell, row_problem
from etl_craft.services.doctor import Status
from etl_craft.services.validate import Finding, validate_on

logger = logging.getLogger(__name__)

CONFIG_DIRNAME = "config"


def config_directory(config: ConnectorConfig, directory: Path | None = None) -> Path:
    """Return ``directory``, or ``config/`` in the project directory."""
    return directory if directory is not None else config.project_dir / CONFIG_DIRNAME


def read_config_files(directory: Path) -> ConfigFiles:
    """Read and check the config files; raise ``MetadataFileError`` naming every problem.

    Any subset of known files is valid; header-only files hold no changes. References and
    omitted fields resolve against the Engine DB in the applying transaction.
    """
    if not directory.is_dir():
        raise MetadataFileError(
            f"no config folder at {directory}: create it with `etl-craft config export`, or "
            f"write {', '.join(table.file for table in TABLES)} there"
        )
    digest = hashlib.sha256()
    rows: dict[str, dict[Key, FileRow]] = {}
    problems: list[str] = []
    known = {table.file for table in TABLES}
    for path in sorted(path for path in directory.iterdir() if path.suffix.lower() == ".csv"):
        if path.name not in known:
            closest = suggest(path.name, known, limit=1, cutoff=0)
            remedy = f"use {closest[0]}" if closest else f"choose one of {', '.join(sorted(known))}"
            problems.append(
                f"{path}: unknown config CSV {path.name!r}; expected a known filename; {remedy}"
            )
    for table in TABLES:
        path = directory / table.file
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            continue
        except OSError as error:
            problems.append(f"{path} cannot be read: {error}")
            continue
        digest.update(table.file.encode() + b"\0" + payload + b"\0")
        rows[table.file] = _read_table(table, path, payload, problems)
    if not rows:
        problems.append(f"{directory}: nothing to apply; supply at least one known config CSV")
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
        if column.required and column.name in table.key and column.name not in header
    ]
    if header_problems:
        problems.extend(header_problems)
        return {}
    rows: dict[Key, FileRow] = {}
    supplied = frozenset(header)
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
            if column.name not in given:
                values[column.name] = column.default
                continue
            try:
                values[column.name] = parse_cell(column, given[column.name])
            except MetadataFileError as error:
                problems.append(f"{path} line {line}, column {column.name}: {error}")
        if len(values) != len(table.columns):
            continue
        problem = row_problem(table, values, supplied)
        if problem:
            problems.append(f"{path} line {line}: {problem}")
            continue
        key = tuple(values[name] for name in table.key)
        if key in rows:
            problems.append(
                f"{path} lines {rows[key].line} and {line} both hold {table.label(values)}; "
                "keep one of them"
            )
            continue
        rows[key] = FileRow(line, values, supplied)
    return rows


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


def sync_config(
    engine: Engine,
    config: ConnectorConfig,
    files: ConfigFiles,
    *,
    apply: bool,
    authorize: Callable[[Connection, ConfigFiles], None] | None = None,
) -> ConfigSync:
    """Merge ``files`` into the ``CFG_`` tables; with ``apply`` false, roll it all back.

    The result is validated in the same transaction; when ``validate`` fails it, nothing is
    committed.
    """
    require_tables(engine)
    files = resolved_files(files, config, keys_only=True)
    changes: list[Change] = []
    findings: tuple[Finding, ...] = ()
    applied = False
    with locks.MIGRATE.hold(engine), _labelled(files.revision):
        try:
            with engine.begin() as conn:
                project = conn.execute(
                    statement(conn, "project_by_code"), {"code": config.project}
                ).one_or_none()
                if project is None or project.active_flag != YesNo.YES:
                    raise MetadataFileError(
                        f"project {config.project!r}: expected an active project; "
                        "use `etl-craft project add --project CODE --reason REASON` first"
                    )
                with selected_project(int(project.project_id)):
                    changes.extend(merge_rows(conn, files, config, authorize))
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
