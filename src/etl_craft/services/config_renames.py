"""Refuse stale helper identities and explain likely rename plans without changing them."""

import re
from collections.abc import Mapping

from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import MetadataFileError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.config_rows import TABLES, TASKS, Key, StoredRow
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.services.config_models import ConfigFiles, FileRow
from etl_craft.services.doctor import Status
from etl_craft.services.validate import Finding


def mentioned_files(config: ConnectorConfig, code: str) -> tuple[str, ...]:
    """List project helper files still naming an old code, including JSON parameters."""
    found = []
    token = re.compile(r"(?<![A-Za-z0-9_])" + re.escape(code) + r"(?![A-Za-z0-9_])")
    for table in TABLES:
        path = config.project_dir / "config" / table.file
        if not path.is_file():
            continue
        try:
            content = path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as error:
            raise MetadataFileError(
                f"{path}: cannot inspect old-code references ({error}); expected a readable "
                "UTF-8 config file; correct its encoding or permissions before renaming"
            ) from error
        if token.search(content):
            found.append(str(path))
    return tuple(found)


def refuse_stale_codes(
    conn: Connection, files: ConfigFiles, current: Mapping[str, Mapping[Key, StoredRow]]
) -> None:
    """Reserve renamed-away insert keys using typed metadata history, never a second store."""
    for rename in conn.execute(
        statement(conn, "renamed_codes"), {"project_id": current_project_id()}
    ):
        key = (
            (rename.old_code,)
            if rename.file == "pipelines.csv"
            else (rename.pipeline_code, rename.old_code)
        )
        row = files.rows.get(rename.file, {}).get(key)
        if row is not None and key not in current[rename.file]:
            raise MetadataFileError(
                f"{files.directory / rename.file} line {row.line}: code {rename.old_code!r} "
                f"was renamed to {rename.new_code!r} by {rename.actor!r} "
                f"at {as_utc(rename.renamed_at)}; "
                "expected the renamed identity, not a new row under its old code; "
                "update the file, or use a different code"
            )


def likely_renames(
    files: ConfigFiles,
    current: Mapping[str, Mapping[Key, StoredRow]],
    final: Mapping[str, Mapping[Key, FileRow]],
) -> tuple[Finding, ...]:
    """Warn on explicit retire/insert pairs with identical task and parameter definitions."""
    findings = []
    submitted = files.rows.get(TASKS.file, {})
    for old_key, retired in submitted.items():
        existing = current[TASKS.file].get(old_key)
        if retired.active or existing is None or not existing.active:
            continue
        old_definition = _definition(existing.values, old_key, final, current)
        for new_key, inserted in submitted.items():
            if (
                inserted.active
                and new_key not in current[TASKS.file]
                and old_key[0] == new_key[0]
                and old_definition == _definition(inserted.values, new_key, final, current)
            ):
                findings.append(
                    Finding(
                        f"{files.directory / TASKS.file} line {inserted.line}",
                        Status.WARN,
                        f"likely rename {old_key[0]}.{old_key[1]} to {new_key[1]}: the same "
                        "task definition is retired and inserted; preserve offsets and consumed "
                        f"events with `etl-craft rename --pipeline {old_key[0]} "
                        f"--task {old_key[1]} --to {new_key[1]} --reason REASON` instead",
                    )
                )
    return tuple(findings)


def _definition(
    values: Mapping[str, object],
    key: Key,
    final: Mapping[str, Mapping[Key, FileRow]],
    current: Mapping[str, Mapping[Key, StoredRow]],
) -> tuple[dict[str, object], dict[object, object]]:
    parameters: dict[object, object] = {
        k[2]: r.values["PARAMETER_VALUE"]
        for k, r in current["task_parameters.csv"].items()
        if k[:2] == key and r.active
    }
    parameters.update(
        {
            k[2]: r.values["PARAMETER_VALUE"]
            for k, r in final["task_parameters.csv"].items()
            if k[:2] == key and r.active
        }
    )
    return ({name: values[name] for name in TASKS.updated}, parameters)
