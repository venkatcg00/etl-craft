"""The catalog as a static site: a page per asset, a search index, and the lineage graphs.

Pages are plain HTML that read well without JavaScript; ``catalog.js`` adds search as you type,
the full results with filters by kind on the home page, and the lineage graph's direction and
depth filters and column tracing. Everything the site needs is in its folder: no page loads
anything from elsewhere, so it can be opened from disk, served from any web server, or
published with ``publish-docs``.

An owned output folder is rebuilt beside the current site and exchanged only when complete.
Unchanged pages reuse verified HTML and keep their timestamps. Every page says when it was
generated and warns once its run details are more than a day old.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from importlib import resources
from pathlib import Path

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import UsageError
from etl_craft.core.filelock import file_lock
from etl_craft.services.catalog.collect import (
    Catalog,
    TaskAsset,
)
from etl_craft.services.catalog.gap_html import gaps_page
from etl_craft.services.catalog.gaps import gap_snapshot
from etl_craft.services.catalog.html import escape as _e
from etl_craft.services.catalog.html import pipeline_url as pipeline_url
from etl_craft.services.catalog.html import render_table as _table
from etl_craft.services.catalog.html import rule_url as rule_url
from etl_craft.services.catalog.html import run_url as run_url
from etl_craft.services.catalog.html import script_url as script_url
from etl_craft.services.catalog.html import slug as slug
from etl_craft.services.catalog.html import table_url as table_url
from etl_craft.services.catalog.html import tag_chips
from etl_craft.services.catalog.html import task_url as task_url
from etl_craft.services.catalog.incremental import (
    SqlFiles,
    page_versions,
    read_sql_scripts,
    reuse_pages,
    write_manifest,
)
from etl_craft.services.catalog.metrics import facts as _facts
from etl_craft.services.catalog.metrics import kpi_values
from etl_craft.services.catalog.metrics import status as _status
from etl_craft.services.catalog.metrics import when as _when
from etl_craft.services.catalog.pages import Pages, anchor
from etl_craft.services.catalog.storage import MARKER as MARKER
from etl_craft.services.catalog.storage import check_folder, swap_site
from etl_craft.services.catalog.trigger_html import trigger_edges
from etl_craft.services.operations.serialization import to_json

ASSETS = ("catalog.css", "catalog.js")
BACKFILL_BADGE = ' <span class="badge">backfill</span>'
TABS = (
    ("search", "index.html", "Search"),
    ("dags", "dags.html", "DAGs"),
    ("warehouse", "warehouse.html", "Warehouse"),
    ("gaps", "gaps.html", "Schema and lineage gaps"),
)
"""The tabs at the top of every page: key, page, label."""
ON = ' class="on"'


@dataclass(frozen=True)
class Site:
    """What was written: the folder and the number of pages."""

    folder: Path
    pages: int


def write_site(catalog: Catalog, config: ConnectorConfig, folder: Path) -> Site:
    """Write the catalog site into ``folder``; ``UsageError`` if it holds anything else.

    The site is written into a hidden folder beside ``folder``, then swapped in.
    """
    if not folder.name:
        raise UsageError(
            f"catalog output {folder} has no directory name; choose a named output folder"
        )
    folder.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(folder.with_name(f".{folder.name}.build.lock"), wait_seconds=30):
        return _write_site(catalog, config, folder)


def _write_site(catalog: Catalog, config: ConnectorConfig, folder: Path) -> Site:
    check_folder(folder)
    building = folder.with_name(f".{folder.name}.building")
    if building.exists():
        shutil.rmtree(building)
    building.mkdir(parents=True)
    (building / MARKER).write_text(
        "Written by `etl-craft generate-docs`, which replaces this folder each time.\n",
        encoding="utf-8",
    )
    try:
        sql_scripts = read_sql_scripts(catalog, config)
        writer = _Writer(catalog, building, sql_scripts)
        versions = page_versions(catalog, config, sql_scripts)
        reuse_pages(folder, building, versions)
        writer.write_all()
        write_manifest(building, versions)
    except BaseException:
        shutil.rmtree(building, ignore_errors=True)
        raise
    swap_site(building, folder)
    return Site(folder, writer.pages)


class _Writer(Pages):
    """Writes every page, keeping the links between them relative."""

    def __init__(self, catalog: Catalog, folder: Path, sql_scripts: SqlFiles) -> None:
        self.catalog = catalog
        self.folder = folder
        self.pages = 0
        self.sql_scripts = sql_scripts

    def write_all(self) -> None:
        for name in ASSETS:
            source = resources.files("etl_craft.services").joinpath("catalog_assets", name)
            (self.folder / name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        (self.folder / "search-index.js").write_text(
            "window.CATALOG_INDEX = " + json.dumps(self._index(), ensure_ascii=True) + ";\n",
            encoding="utf-8",
        )
        c = self.catalog
        self._gap_pages()
        self._page("index.html", "etl-craft catalog", "", self._home, "search", [])
        self._page("dags.html", "DAGs", "", self._dags, "dags", [])
        self._page("warehouse.html", "Warehouse", "", self._warehouse, "warehouse", [])
        for code in c.pipelines:
            crumbs = [("dags.html", "DAGs"), (None, code)]
            self._page(
                pipeline_url(code), code, "pipeline", partial(self._pipeline, code), "dags", crumbs
            )
        for label, task in c.tasks.items():
            self._page(
                task_url(label),
                label,
                "task",
                partial(self._task, label),
                "dags",
                self._task_crumbs(task),
            )
        self._run_pages()
        self._supporting_pages()

    def _run_pages(self) -> None:
        c = self.catalog
        for code, pipeline in c.pipelines.items():
            for run in pipeline.runs:
                crumbs = [
                    ("dags.html", "DAGs"),
                    (pipeline_url(code), code),
                    (f"{pipeline_url(code)}#runs", "Runs"),
                    (None, str(run.pipeline_run_id)),
                ]
                self._page(
                    run_url(run.pipeline_run_id),
                    f"{code} run {run.pipeline_run_id}",
                    "pipeline run",
                    partial(self._run, pipeline, run),
                    "dags",
                    crumbs,
                )

    def _supporting_pages(self) -> None:
        c = self.catalog
        for name, table in c.tables.items():
            if not table.external:
                self._page(
                    table_url(name),
                    name,
                    "table",
                    partial(self._table_page, name),
                    "warehouse",
                    self._table_crumbs(name),
                )
        for rule_id, rule in c.rules.items():
            crumbs = (
                [
                    *self._task_crumbs(c.tasks[rule.task])[:-1],
                    (task_url(rule.task), rule.row.task_code),
                    (None, rule.row.name),
                ]
                if rule.task in c.tasks
                else [(None, rule.row.name)]
            )
            self._page(
                rule_url(rule_id),
                rule.row.name,
                "business rule",
                partial(self._rule, rule_id),
                "dags",
                crumbs,
            )
        for name in c.scripts:
            crumbs = [
                ("dags.html", "DAGs"),
                ("dags.html#scripts", "Python scripts"),
                (None, name),
            ]
            self._page(
                script_url(name),
                name,
                "Python script",
                partial(self._script, name),
                "dags",
                crumbs,
            )

    def _gap_pages(self) -> None:
        c = self.catalog
        (self.folder / "gaps.json").write_text(
            json.dumps(to_json(gap_snapshot(c))), encoding="utf-8"
        )
        self._page(
            "gaps.html",
            "Schema and lineage gaps",
            "",
            lambda: gaps_page(
                c, lambda n: self._table_link(n, ""), lambda n: self._task_link(n, "")
            ),
            "gaps",
            [],
        )

    def _task_crumbs(self, task: TaskAsset) -> list[tuple[str | None, str]]:
        code = task.row.pipeline_code
        return [("dags.html", "DAGs"), (pipeline_url(code), code), (None, task.row.task_code)]

    def _table_crumbs(self, name: str) -> list[tuple[str | None, str]]:
        database, schema, table = self.catalog.where(name)
        return [
            ("warehouse.html", "Warehouse"),
            (f"warehouse.html#{anchor(database)}", database),
            (f"warehouse.html#{anchor(database, schema)}", schema),
            (None, table),
        ]

    # Page frame

    def _page(
        self,
        path: str,
        title: str,
        kind: str,
        body: Callable[[], str],
        tab: str,
        crumbs: Sequence[tuple[str | None, str]],
    ) -> None:
        if (self.folder / path).is_file():
            self.pages += 1
            return
        depth = path.count("/")
        root = "../" * depth
        nav = "".join(
            f'<a href="{root}{url}"{ON if key == tab else ""}>{label}</a>'
            for key, url, label in TABS
        )
        trail = " &rsaquo; ".join(
            f'<a href="{_e(root + url)}">{_e(text)}</a>' if url is not None else _e(text)
            for url, text in crumbs
        )
        crumb_bar = f'<nav class="crumbs">{trail}</nav>' if crumbs else ""
        kind_label = f'<div class="kind-label">{_e(kind)}</div>' if kind else ""
        generated = _e(self.catalog.generated_at.isoformat())
        compared = _e(self.catalog.compared_at.isoformat()) if self.catalog.compared_at else ""
        document = (
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<meta name="robots" content="noindex, nofollow">'
            f"<title>{_e(title)} · etl-craft catalog</title>"
            f'<link rel="stylesheet" href="{root}catalog.css"></head>'
            f'<body data-root="{root}" data-generated="{generated}" data-compared="{compared}">'
            '<header class="top">'
            f'<a class="brand" href="{root}index.html">etl-craft catalog</a><nav>{nav}</nav>'
            '<div class="search"><input type="search" placeholder="Search pipelines, tasks, '
            'tables, columns, rules…" aria-label="Search the catalog" autocomplete="off">'
            '<div class="results"></div></div></header>'
            f"<main>{crumb_bar}{kind_label}<h1>{_e(title)}</h1>{body()}</main>"
            f'<footer class="foot">Generated {_e(_when(self.catalog.generated_at))}; run '
            "details are as of then. <code>etl-craft generate-docs</code> writes the site "
            "again.</footer>"
            f'<script src="{root}search-index.js"></script>'
            f'<script src="{root}catalog.js"></script></body></html>'
        )
        target = self.folder / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(document, encoding="utf-8")
        self.pages += 1

    def _home(self) -> str:
        c = self.catalog
        search = (
            '<section id="search-page">'
            '<input type="search" class="big" placeholder="Search pipelines, tasks, tables, '
            'columns, rules, scripts and documentation" aria-label="Search everything">'
            '<div class="chips"></div><p class="note summary"></p>'
            '<div class="all-results"></div></section>'
        )
        tables = sum(1 for t in c.tables.values() if not t.external)
        return search + _facts(
            [
                ("DAGs", self._link("dags.html", f"{len(c.pipelines)} pipeline(s)", "")),
                ("Tasks", str(len(c.tasks))),
                ("Warehouse", self._link("warehouse.html", f"{tables} table(s)", "")),
                ("Business rules", str(len(c.rules))),
                ("Python scripts", self._link("dags.html#scripts", str(len(c.scripts)), "")),
                (
                    "Untraced SQL tasks",
                    self._link("gaps.html", f"{len(c.untraced)} task(s)", "")
                    if c.untraced
                    else "none",
                ),
                ("Generated", _e(_when(c.generated_at))),
            ]
        )

    def _dags(self) -> str:
        c = self.catalog
        pipelines = _table(
            [
                "Pipeline",
                "Name",
                "Tags",
                "Description",
                "Schedule",
                "Refresh",
                "Depends on",
                "Tasks",
                "Last run",
                "Succeeded",
                "Average run",
            ],
            (
                [
                    self._link(pipeline_url(code), code, ""),
                    _e(p.row.pipeline_name),
                    f'<span data-pipeline-tags="{_e(" ".join(p.row.tags))}">'
                    f"{tag_chips(p.row.tags)}</span>",
                    _e(p.row.description),
                    _e(p.row.run_schedule),
                    _e(p.row.refresh_type),
                    ", ".join(self._link(pipeline_url(u), u, "") for u, _ in p.depends_on),
                    str(len(p.tasks)),
                    f"{_status(p.row.last_run.status)} {_e(_when(p.row.last_run.end))}"
                    if p.row.last_run
                    else "never run",
                    _e(rates),
                    _e(durations),
                ]
                for code, p in c.pipelines.items()
                for rates, durations in [kpi_values(p.runs)]
            ),
            "No active pipelines.",
        )
        scripts = _table(
            ["Python script", "Run by", "Found"],
            (
                [
                    self._link(script_url(name), name, ""),
                    ", ".join(self._task_link(t, "") for t in script.tasks),
                    "yes" if script.exists else '<span class="badge warn">missing</span>',
                ]
                for name, script in sorted(c.scripts.items())
            ),
            "No Python scripts.",
        )
        untraced = _table(
            ["Task", "Why its columns cannot be traced"],
            ([self._task_link(t.label, ""), _e(t.lineage_error)] for t in c.untraced),
            "Every SQL task's columns are traced.",
        )
        labels = sorted({tag for p in c.pipelines.values() for tag in p.row.tags})
        filters = (
            '<form class="tag-filter"><fieldset><legend>Filter pipelines by tags</legend>'
            '<p id="tag-help">Show pipelines carrying every selected tag.</p>'
            + " ".join(
                f'<label><input type="checkbox" name="tag" value="{_e(tag)}" '
                f'aria-describedby="tag-help"> {_e(tag)}</label>'
                for tag in labels
            )
            + '<button type="submit">Apply tags</button></fieldset></form>'
            '<p class="tag-result" role="status"></p>'
        )
        return (
            f'{filters}<div class="tag-pipelines">{pipelines}</div>{trigger_edges(c)}'
            f'<h2 id="scripts">Python scripts</h2>{scripts}'
            f'<h2 id="untraced">Column lineage unavailable</h2>{untraced}'
        )

    def _index(self) -> list[list[str]]:
        c = self.catalog
        entries: list[list[str]] = []
        for code, p in c.pipelines.items():
            text = " ".join(filter(None, [p.row.pipeline_name, p.row.description]))
            entries.append(["pipeline", code, text, pipeline_url(code)])
        for label, task in c.tasks.items():
            text = " ".join(filter(None, [task.row.handler, task.target, task.documentation]))
            entries.append(["task", label, text, task_url(label)])
        for name, table in c.tables.items():
            if table.external:
                continue
            text = "written by " + ", ".join(table.writers) if table.writers else ""
            entries.append(["table", name, text, table_url(name)])
            for column in table.columns.values():
                text = " ".join(filter(None, [column.data_type, column.comment]))
                entries.append(
                    [
                        "column",
                        f"{name}.{column.name}",
                        text,
                        table_url(name) + f"#col={column.name}",
                    ]
                )
        for rule_id, rule in c.rules.items():
            text = f"{rule.row.rule_type} on {rule.table}"
            entries.append(["rule", rule.row.name, text, rule_url(rule_id)])
        for name, script in c.scripts.items():
            entries.append(["script", name, ", ".join(script.tasks), script_url(name)])
        return entries
