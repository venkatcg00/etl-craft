"""Apply submitted configuration changes against the selected Engine DB snapshot."""

from collections.abc import Callable, Mapping
from dataclasses import replace

from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MetadataFileError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.config_rows import (
    ACTIVE_FLAG,
    BUSINESS_RULES,
    OBJECT_TABLES,
    PIPELINE_DEPENDENCIES,
    PIPELINES,
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
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.services.config_models import Change, ColumnChange, ConfigFiles, FileRow, cell
from etl_craft.services.config_validation import row_problem
from etl_craft.services.validate import Finding


def _ends(table: ConfigTable, values: Mapping[str, Value]) -> list[tuple[str, Key]]:
    """Return the pipelines and tasks a row names, by code."""
    if table in (PIPELINES, OBJECT_TABLES):
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
    found: list[StoredRow] = []
    for kind, end in _ends(table, file_row.values):
        file = {"pipeline": PIPELINES.file, "task": TASKS.file, "table": OBJECT_TABLES.file}[kind]
        parent = current[file].get(end)
        if parent is None or not parent.active:
            named = ".".join(str(code) for code in end if code is not None)
            raise MetadataFileError(
                f"{table.file} line {file_row.line}: {table.label(file_row.values)} is active, "
                f"but {kind} {named} is missing or inactive in the Engine DB "
                "and submitted changes; "
                f"set the {kind}'s ACTIVE_FLAG to Y, or this row's to N"
            )
        found.append(parent)
    if (
        table in (PIPELINE_DEPENDENCIES, TASK_DEPENDENCIES)
        and file_row.values["DEPENDS_ON_PROJECT_CODE"]
    ):
        found.append(external_dependency(conn, table, file_row))
    if table in (PIPELINES, OBJECT_TABLES):
        from etl_craft.engine.repository.pipelines import current_project_id

        return {"PROJECT_ID": current_project_id()}
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


def resolved_files(
    files: ConfigFiles, config: ConnectorConfig, *, keys_only: bool = False
) -> ConfigFiles:
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
            elif keys_only and table in (TASKS, BUSINESS_RULES):
                pass
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
            rows[key] = FileRow(row.line, values, row.provided)
        resolved[table.file] = rows
    return ConfigFiles(files.directory, files.revision, resolved)


def snapshot(
    conn: Connection, files: ConfigFiles, config: ConnectorConfig
) -> dict[str, dict[Key, StoredRow]]:
    """Load current keys in parent order, including files absent from this submission."""
    current: dict[str, dict[Key, StoredRow]] = {}
    identities: dict[str, set[int | str]] = {}
    for table in TABLES:
        rows = read_rows(conn, table, project_code=config.project, project_id=current_project_id())
        current[table.file], malformed = current_rows(table, rows, identities)
        if malformed:
            raise MetadataFileError(
                f"{table.table}: inconsistent parent ids on rows {[r.row_id for r in malformed]}; "
                "expected matching pipeline/task identities; repair the metadata before applying"
            )
        identities[table.file] = {row.row_id for row in current[table.file].values()}
    image = ConfigFiles(
        files.directory,
        files.revision,
        {
            name: {key: FileRow(int(row.row_id), row.values) for key, row in rows.items()}
            for name, rows in current.items()
        },
    )
    normalized = resolved_files(image, config)
    for name, normalized_rows in normalized.rows.items():
        by_id = {row.row_id: row for row in current[name].values()}
        current[name] = {
            key: replace(by_id[row.line], values=row.values) for key, row in normalized_rows.items()
        }
    return current


def normalize_submitted_tables(
    conn: Connection, files: ConfigFiles, current: Mapping[str, Mapping[Key, StoredRow]]
) -> list[Change]:
    """Persist resolved identities only for table keys present in this submission."""
    wanted = files.rows.get(OBJECT_TABLES.file, {})
    if not wanted:
        return []
    raw = {
        row.table_id: row
        for row in conn.execute(statement(conn, "source_contract_select_cfg_tables"))
    }
    changes = []
    for key in wanted:
        old = current[OBJECT_TABLES.file].get(key)
        if old is None:
            continue
        before = raw[old.row_id]
        provided = files.rows[OBJECT_TABLES.file][key].provided
        identity = tuple(
            old.values[name]
            if provided is None or name in provided
            else before._mapping[name.lower()]
            for name in OBJECT_TABLES.key
        )
        differ = tuple(
            ColumnChange(name, cell(before._mapping[name.lower()]), cell(value))
            for name, value in zip(OBJECT_TABLES.key, identity, strict=True)
            if before._mapping[name.lower()] != value
        )
        if not differ:
            continue
        conn.execute(
            statement(conn, "source_contract_update_cfg_tables"),
            dict(zip(("catalog", "schema", "table"), identity, strict=True)) | {"id": old.row_id},
        )
        changes.append(
            Change(
                OBJECT_TABLES.file,
                OBJECT_TABLES.table,
                "update",
                OBJECT_TABLES.label(old.values),
                differ,
            )
        )
    return changes


def complete_row(table: ConfigTable, row: FileRow, existing: StoredRow | None) -> FileRow:
    """Keep omitted stored fields; insert defaults never overwrite an existing value."""
    values = row.values
    if existing is not None:
        given = row.provided if row.provided is not None else frozenset(values)
        values = (
            existing.values
            | {ACTIVE_FLAG.name: YesNo.of(existing.active)}
            | {name: values[name] for name in given}
        )
    completed = FileRow(row.line, values, row.provided)
    label = table.label(values)
    if not completed.active and existing is None:
        raise MetadataFileError(
            f"{table.file} line {row.line}: {label} has ACTIVE_FLAG=N but no stored key; "
            "expected an existing row to retire; insert it with ACTIVE_FLAG=Y or remove this change"
        )
    missing = [c.name for c in table.columns if c.required and values[c.name] is None]
    problem = row_problem(table, values) if not missing else f"missing required columns {missing}"
    if problem:
        raise MetadataFileError(
            f"{table.file} line {row.line}: {label}: {problem}; supply valid values in this file"
        )
    return completed


def complete_files(
    files: ConfigFiles, current: Mapping[str, Mapping[Key, StoredRow]]
) -> ConfigFiles:
    """Complete only submitted rows using the transaction's stored configuration."""
    rows = {
        table.file: {
            key: complete_row(table, row, current[table.file].get(key))
            for key, row in files.rows[table.file].items()
        }
        for table in TABLES
        if table.file in files.rows
    }
    return ConfigFiles(files.directory, files.revision, rows)


def final_rows(
    files: ConfigFiles, current: Mapping[str, Mapping[Key, StoredRow]]
) -> dict[str, dict[Key, FileRow]]:
    """Overlay submitted values without removing any unmentioned key."""
    return {
        table.file: {
            key: FileRow(0, row.values | {ACTIVE_FLAG.name: YesNo.of(row.active)})
            for key, row in current[table.file].items()
        }
        | files.rows.get(table.file, {})
        for table in TABLES
    }


def check_references(
    files: ConfigFiles,
    final: Mapping[str, Mapping[Key, FileRow]],
    current: Mapping[str, Mapping[Key, StoredRow]],
) -> None:
    """Require new or changed active rows to reference active final identities."""
    for table in TABLES:
        for identity, row in files.rows.get(table.file, {}).items():
            old = current[table.file].get(identity)
            if not row.active or (
                old is not None and old.active and not row_changes(table, old, row)
            ):
                continue
            for kind, key in _ends(table, row.values):
                file = {
                    "pipeline": PIPELINES.file,
                    "task": TASKS.file,
                    "table": OBJECT_TABLES.file,
                }[kind]
                parent = final[file].get(key)
                if parent is None or not parent.active:
                    name = ".".join(str(v) for v in key if v is not None)
                    raise MetadataFileError(
                        f"{table.file} line {row.line}: {kind} {name} is missing or inactive "
                        "in the Engine DB and submitted changes; expected an active reference; "
                        f"supply ACTIVE_FLAG=Y for it or correct this row's key"
                    )


def check_columns(files: ConfigFiles, final: Mapping[str, Mapping[Key, FileRow]]) -> None:
    """Refuse omitted-column ordinal collisions and active children of retired tables."""
    positions: dict[Key, tuple[Key, FileRow]] = {}
    for key, column in final[SOURCE_COLUMNS.file].items():
        if not column.active:
            continue
        parent = final[OBJECT_TABLES.file].get(key[:3])
        if parent is not None and not parent.active:
            submitted = files.rows.get(OBJECT_TABLES.file, {}).get(key[:3])
            if submitted is not None:
                raise MetadataFileError(
                    f"tables.csv line {submitted.line}: {OBJECT_TABLES.label(submitted.values)} "
                    f"has active column {key[-1]!r}; expected columns retired before their table; "
                    "submit those columns with ACTIVE_FLAG=N too"
                )
        position = (*key[:3], column.values["COLUMN_POSITION"])
        if position in positions:
            other_key, other = positions[position]
            changed = files.rows.get(SOURCE_COLUMNS.file, {}).get(key) or files.rows.get(
                SOURCE_COLUMNS.file, {}
            ).get(other_key)
            line = changed.line if changed is not None else column.line
            raise MetadataFileError(
                f"columns.csv line {line}: COLUMN_POSITION={position[-1]} is shared by "
                f"{SOURCE_COLUMNS.label(column.values)} and {SOURCE_COLUMNS.label(other.values)}; "
                "expected unique active positions including stored columns; explicitly move "
                "or retire the conflicting column"
            )
        positions[position] = key, column


def retire_explicit(
    conn: Connection, files: ConfigFiles, current: dict[str, dict[Key, StoredRow]]
) -> list[Change]:
    """Retire submitted N rows children first, preserving every other row."""
    changes = []
    for table in reversed(TABLES):
        for key, desired in files.rows.get(table.file, {}).items():
            old = current[table.file].get(key)
            if desired.active or old is None or not old.active:
                continue
            label = table.label(old.values)
            retire_row(conn, table, old.row_id, label)
            changes.append(
                Change(
                    table.file,
                    table.table,
                    "retire",
                    label,
                    (ColumnChange(ACTIVE_FLAG.name, "Y", "N"),),
                    "ACTIVE_FLAG is N",
                )
            )
            current[table.file][key] = replace(old, active=False)
    return changes


def upsert_table(
    conn: Connection,
    table: ConfigTable,
    wanted: Mapping[Key, FileRow],
    current: dict[str, dict[Key, StoredRow]],
) -> list[Change]:
    """Publish one table's supplied active rows, preserving IDs and omitted fields."""
    stored = current[table.file]
    changes = []
    if table is SOURCE_COLUMNS:
        retire_reordered_columns(conn, stored, wanted)
    for key, desired in wanted.items():
        if not desired.active:
            continue
        existing = stored.get(key)
        label = table.label(desired.values)
        if existing is not None and existing.active and not row_changes(table, existing, desired):
            continue
        links = _links(conn, table, desired, current)
        if existing is None:
            insert_row(conn, table, desired.values, links)
            changes.append(Change(table.file, table.table, "insert", label))
            continue
        differ = row_changes(table, existing, desired)
        if not existing.active or differ:
            update_row(conn, table, existing.row_id, desired.values, links)
            if existing.active:
                changes.append(Change(table.file, table.table, "update", label, differ))
            else:
                flag = ColumnChange(ACTIVE_FLAG.name, "N", "Y")
                changes.append(
                    Change(table.file, table.table, "reactivate", label, (flag, *differ))
                )
    return changes


def merge_rows(
    conn: Connection,
    files: ConfigFiles,
    config: ConnectorConfig,
    authorize: Callable[[Connection, ConfigFiles], None] | None = None,
) -> tuple[list[Change], tuple[Finding, ...]]:
    """Validate the final snapshot, then retire explicitly and upsert in one transaction."""
    from etl_craft.services.config_renames import likely_renames, refuse_stale_codes

    current = snapshot(conn, files, config)
    refuse_stale_codes(conn, files, current)
    files = resolved_files(complete_files(files, current), config)
    final = final_rows(files, current)
    warnings = likely_renames(files, current, final)
    if authorize is not None:
        authorize(conn, files)
    check_references(files, final, current)
    check_columns(files, final)
    changes = normalize_submitted_tables(conn, files, current)
    changes.extend(retire_explicit(conn, files, current))
    for table in TABLES:
        if table.file in files.rows:
            changes.extend(upsert_table(conn, table, files.rows[table.file], current))
            identities = {
                name: {row.row_id for row in rows.values()} for name, rows in current.items()
            }
            current[table.file], _ = current_rows(
                table,
                read_rows(
                    conn, table, project_code=config.project, project_id=current_project_id()
                ),
                identities,
            )
    return changes, warnings
