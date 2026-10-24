"""Export selected project metadata and discovered warehouse shapes to CSV files."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MetadataFileError
from etl_craft.engine.repository.config_history import config_as_of
from etl_craft.engine.repository.config_rows import (
    ACTIVE_FLAG,
    BUSINESS_RULES,
    OBJECT_TABLES,
    PROJECTS,
    SOURCE_COLUMNS,
    TABLES,
    TASKS,
    Key,
    StoredRow,
    Value,
    read_rows,
)
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.source_columns import seed_sources

if TYPE_CHECKING:
    from etl_craft.services.config_files import ConfigExport


def export_config(
    engine: Engine,
    directory: Path,
    *,
    overwrite: bool = False,
    as_of: datetime | None = None,
    config: ConnectorConfig | None = None,
    project_code: str = "default",
) -> ConfigExport:
    """Write each key's current ``CFG_`` row, active or not, as the config files.

    With ``as_of``, the rows are those that stood then, rebuilt from the change history. Rows
    are sorted by key. Existing files are kept unless ``overwrite`` is set.
    """
    from etl_craft.services.config_files import ConfigExport, cell, current_rows, require_tables

    require_tables(engine)
    existing = [table.file for table in TABLES if (directory / table.file).exists()]
    if existing and not overwrite:
        raise MetadataFileError(
            f"{directory} already holds {', '.join(existing)}; move them away, or give "
            "`force` to overwrite them with the Engine DB's rows"
        )
    current: dict[str, dict[Key, StoredRow]] = {}
    current_ids: dict[str, set[int | str]] = {}
    source = engine
    if as_of is not None:
        with engine.connect() as conn:
            source = config_as_of(conn, as_of)
    with source.connect() as conn:
        project_id: int | None = None
        for table in TABLES:
            current[table.file], _ = current_rows(
                table,
                read_rows(conn, table, project_code=project_code, project_id=project_id),
                current_ids,
            )
            current_ids[table.file] = {row.row_id for row in current[table.file].values()}
            if table is PROJECTS:
                selected = current[table.file]
                if not selected:
                    raise MetadataFileError(
                        f"project {project_code!r}: no recorded project at the selected time; "
                        "expected an existing project; select another project or a later time"
                    )
                project_id = int(next(iter(selected.values())).row_id)
    if config is not None and as_of is None:
        with selected_project(project_id):
            _warehouse_rows(engine, config, directory, current)
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
            values = row.values | {ACTIVE_FLAG.name: YesNo.of(row.active)}
            writer.writerow([cell(values[column.name]) for column in table.columns])
        (directory / table.file).write_text(buffer.getvalue(), encoding="utf-8")
        counts[table.file] = len(rows)
    if as_of is not None:
        source.dispose()
    return ConfigExport(str(directory), counts, None if as_of is None else as_of.isoformat())


def _warehouse_rows(
    engine: Engine,
    config: ConnectorConfig,
    directory: Path,
    current: dict[str, dict[Key, StoredRow]],
) -> None:
    """Resolve configured targets and discover active warehouse column shapes."""
    from etl_craft.services.config_files import ConfigFiles, FileRow, resolved_files

    metadata_rows = {
        table.file: {
            key: FileRow(int(row.row_id), row.values) for key, row in current[table.file].items()
        }
        for table in (OBJECT_TABLES, SOURCE_COLUMNS, BUSINESS_RULES, TASKS)
    }
    resolved = resolved_files(ConfigFiles(directory, "", metadata_rows), config)
    for table in (OBJECT_TABLES, SOURCE_COLUMNS, BUSINESS_RULES, TASKS):
        original = {row.row_id: row for row in current[table.file].values()}
        current[table.file] = {
            key: StoredRow(
                row.line,
                original[row.line].active,
                original[row.line].parents,
                True,
                row.values,
            )
            for key, row in resolved.rows[table.file].items()
        }
    seeded = seed_sources(engine, config)
    # Preserve inactive rows while replacing active definitions with current warehouse shapes.
    declared = current[SOURCE_COLUMNS.file]
    current[SOURCE_COLUMNS.file] = {
        key: row for key, row in current[SOURCE_COLUMNS.file].items() if not row.active
    }
    from etl_craft.engine.repository.normalization import object_identity
    from etl_craft.services.lineage import dialect_and_catalog

    dialect, catalog = dialect_and_catalog(config)
    for index, column in enumerate(seeded):
        identity = object_identity(column.table_name, catalog, dialect)
        names: dict[str, Value] = dict(zip(OBJECT_TABLES.key, identity, strict=True))
        parent = current[OBJECT_TABLES.file].get(identity)
        if parent is None:
            parent = StoredRow(index, True, (), True, names | {"OBJECT_KIND": column.object_kind})
        parent = StoredRow(
            parent.row_id,
            True,
            parent.parents,
            True,
            parent.values | {"OBJECT_KIND": column.object_kind or parent.values["OBJECT_KIND"]},
        )
        current[OBJECT_TABLES.file][identity] = parent
        key = (*identity, column.column_name)
        previous = declared.get(key)
        values: dict[str, Value] = names | {
            "COLUMN_NAME": column.column_name,
            "DATA_TYPE": column.data_type,
            "COLUMN_POSITION": column.column_position,
            "IS_NULLABLE": column.is_nullable
            or (previous.values["IS_NULLABLE"] if previous else None),
        }
        current[SOURCE_COLUMNS.file][key] = StoredRow(
            index, True, (int(parent.row_id),), True, values
        )
