"""Fingerprint catalog render inputs and reuse verified pages from the previous site."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import replace
from importlib import resources
from pathlib import Path

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import ingestion_script, sql_file
from etl_craft.core.errors import ConfigurationError, EtlCraftError
from etl_craft.core.text import sha256_hex
from etl_craft.services.catalog import Catalog, TaskAsset
from etl_craft.services.catalog_gaps import table_records
from etl_craft.services.catalog_html import (
    pipeline_url,
    rule_url,
    run_url,
    script_url,
    table_url,
    task_url,
)

MANIFEST = ".etl-craft-manifest.json"
SqlFiles = dict[str, str | EtlCraftError | OSError]


def fingerprint(value: object) -> str:
    """Hash internal render inputs without persisting their potentially sensitive values."""
    return sha256_hex(repr(value).encode())


def read_sql_files(catalog: Catalog, config: ConnectorConfig) -> SqlFiles:
    """Read each displayed SQL file once for both its fingerprint and its HTML."""
    files: SqlFiles = {}
    names = {
        task.params["SOURCE_SQL_FILE"]
        for task in catalog.tasks.values()
        if "SOURCE_SQL_FILE" in task.params
    }
    for name in sorted(names):
        try:
            files[name] = sql_file(config, name).read_text(encoding="utf-8")
        except (OSError, EtlCraftError) as error:
            files[name] = error
    return files


def _task_inputs(task: TaskAsset, config: ConnectorConfig, sql_files: SqlFiles) -> object:
    files = []
    if "SOURCE_SQL_FILE" in task.params:
        value = sql_files[task.params["SOURCE_SQL_FILE"]]
        files.append(fingerprint((type(value).__name__, str(value))))
    if task.script:
        try:
            files.append(sha256_hex(ingestion_script(config, task.script).read_bytes()))
        except (OSError, EtlCraftError) as error:
            files.append(f"{type(error).__name__}: {error}")
    return replace(task, row=replace(task.row, last_run=None), runs=[]), files


def _components(catalog: Catalog) -> list[set[str]]:
    adjacent: dict[str, set[str]] = {name: set() for name in catalog.tables}
    for edge in catalog.table_edges:
        adjacent[edge.source].add(edge.target)
        adjacent[edge.target].add(edge.source)
    components = []
    remaining = set(adjacent)
    while remaining:
        pending = [min(remaining)]
        component = set()
        while pending:
            name = pending.pop()
            if name in component:
                continue
            component.add(name)
            pending.extend(adjacent[name] - component)
        remaining -= component
        components.append(component)
    return components


def page_versions(catalog: Catalog, config: ConnectorConfig, sql_files: SqlFiles) -> dict[str, str]:
    """Version pages by metadata, history, referenced files and rendering code."""
    package = resources.files("etl_craft.services")
    renderer = fingerprint(
        [
            (p.name, p.read_bytes())
            for p in sorted(package.iterdir(), key=lambda p: p.name)
            if p.name.startswith("catalog") and p.name.endswith(".py")
        ]
        + [
            (name, package.joinpath("catalog_assets", name).read_bytes())
            for name in ("catalog.css", "catalog.js")
        ]
    )
    context = (
        renderer,
        catalog.database,
        catalog.warehouse_identity,
        catalog.sql_dialect,
        catalog.compared_at,
    )
    tasks = {label: _task_inputs(task, config, sql_files) for label, task in catalog.tasks.items()}
    versions: dict[str, str] = {}
    _pipeline_versions(catalog, context, tasks, versions)
    for rule_id, rule in catalog.rules.items():
        versions.setdefault(rule_url(rule_id), fingerprint((context, rule)))
    for name, script in catalog.scripts.items():
        versions[script_url(name)] = fingerprint(
            (context, script, [tasks[t] for t in script.tasks])
        )
    _table_versions(catalog, context, tasks, versions)
    return versions


def _pipeline_versions(
    catalog: Catalog, context: object, tasks: dict[str, object], versions: dict[str, str]
) -> None:
    shown_runs = {run.pipeline_run_id for p in catalog.pipelines.values() for run in p.runs}
    for code, pipeline in catalog.pipelines.items():
        metadata = replace(
            pipeline,
            row=replace(pipeline.row, last_run=None, last_run_id=None),
            runs=[],
            run_changes={},
        )
        labels = set(pipeline.tasks)
        for label in pipeline.tasks:
            labels.update(name for name, _ in catalog.tasks[label].upstream)
        history = [
            (catalog.tasks[label].row.last_run, catalog.tasks[label].runs)
            for label in pipeline.tasks
        ]
        consumption = [
            [
                (used, used.pipeline_run_id in shown_runs, used.upstream_run_id in shown_runs)
                for used in (
                    catalog.built_from.get(run.pipeline_run_id, [])
                    + catalog.consumed_by.get(run.pipeline_run_id, [])
                )
            ]
            for run in pipeline.runs
        ]
        version = fingerprint(
            (
                context,
                metadata,
                [(label, tasks[label]) for label in sorted(labels) if label in tasks],
                [
                    (r, catalog.rules[r])
                    for label in pipeline.tasks
                    for r in catalog.tasks[label].rules
                ],
                pipeline.row.last_run,
                pipeline.runs,
                pipeline.run_changes,
                history,
                consumption,
            )
        )
        versions[pipeline_url(code)] = version
        versions.update((task_url(label), version) for label in pipeline.tasks)
        versions.update((run_url(run.pipeline_run_id), version) for run in pipeline.runs)
        versions.update(
            (rule_url(rule), version)
            for label in pipeline.tasks
            for rule in catalog.tasks[label].rules
        )


def _table_versions(
    catalog: Catalog, context: object, tasks: dict[str, object], versions: dict[str, str]
) -> None:
    for component in _components(catalog):
        labels = sorted(
            {
                label
                for name in component
                for label in (*catalog.tables[name].writers, *catalog.tables[name].readers)
            }
        )
        component_version = fingerprint(
            (
                context,
                [
                    (name, catalog.tables[name], table_records(catalog, catalog.tables[name]))
                    for name in sorted(component)
                ],
                [(label, tasks[label]) for label in labels],
                [e for e in catalog.table_edges if e.source in component],
                [e for e in catalog.column_edges if e.target_object in component],
                [
                    catalog.rules[r]
                    for name in sorted(component)
                    for r in catalog.tables[name].rules
                ],
            )
        )
        for name in component:
            table = catalog.tables[name]
            if not table.external:
                versions[table_url(name)] = fingerprint(
                    (
                        component_version,
                        [catalog.tasks[label].row.last_run for label in table.writers],
                    )
                )


def _manifest_pages(previous: Path) -> dict[str, dict[str, str]]:
    """Read a complete generated manifest; malformed derived state must be regenerated."""
    manifest = previous / MANIFEST
    if not manifest.exists() and not manifest.is_symlink():
        return {}
    if manifest.is_symlink():
        raise ConfigurationError(
            f"catalog manifest {manifest}: found a symbolic link; expected a generated file; "
            f"remove only {MANIFEST} and regenerate the catalog"
        )
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConfigurationError(
            f"catalog manifest {manifest}: {error}; expected generated JSON; remove only "
            f"{MANIFEST} and regenerate the catalog"
        ) from error
    if (
        not isinstance(data, dict)
        or type(data.get("version")) is not int
        or data.get("version") != 1
        or not isinstance(data.get("pages"), dict)
    ):
        raise ConfigurationError(
            f"catalog manifest {manifest}: expected version 1 and page fingerprints; "
            f"found an incompatible structure; remove only {MANIFEST} and regenerate the catalog"
        )
    pages: dict[str, dict[str, str]] = {}
    for path, entry in data["pages"].items():
        if (
            not isinstance(entry, dict)
            or set(entry) != {"version", "content"}
            or not all(
                isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in entry.values()
            )
        ):
            raise ConfigurationError(
                f"catalog manifest {manifest} page {path}: found invalid fingerprints; "
                f"expected version/content hashes; remove only {MANIFEST} and regenerate"
            )
        pages[path] = entry
    return pages


def reuse_pages(previous: Path, building: Path, versions: dict[str, str]) -> None:
    """Copy unchanged pages; refuse corrupt manifests or altered cached HTML."""
    pages = _manifest_pages(previous)
    for path, version in versions.items():
        entry = pages.get(path)
        if entry is None:
            continue
        if entry["version"] != version:
            continue
        source = previous / path
        safe = not (
            source.is_symlink()
            or not source.is_file()
            or not source.resolve().is_relative_to(previous.resolve())
        )
        try:
            content = source.read_bytes() if safe else None
        except OSError as error:
            raise ConfigurationError(
                f"catalog page {source}: {error}; expected readable generated HTML; "
                "grant the generator read access and regenerate the catalog"
            ) from error
        if content is None or sha256_hex(content) != entry["content"]:
            raise ConfigurationError(
                f"catalog page {source}: found missing or changed cached HTML; expected manifest "
                f"hash {entry['content']!r}; remove only {MANIFEST} and regenerate the catalog"
            )
        target = building / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        shutil.copystat(source, target, follow_symlinks=False)


def write_manifest(folder: Path, versions: dict[str, str]) -> None:
    """Record per-page input versions and hashes of the completed generated HTML."""
    pages = {
        path: {"version": version, "content": sha256_hex((folder / path).read_bytes())}
        for path, version in versions.items()
    }
    (folder / MANIFEST).write_text(json.dumps({"version": 1, "pages": pages}), encoding="utf-8")
