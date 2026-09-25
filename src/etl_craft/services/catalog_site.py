"""The catalog as a static site: a page per asset, a search index, and the lineage graphs.

Pages are plain HTML that read well without JavaScript; ``catalog.js`` adds search as you type,
the full results with filters by kind on the home page, and the lineage graph's direction and
depth filters and column tracing. Everything the site needs is in its folder: no page loads
anything from elsewhere, so it can be opened from disk, served from any web server, or
published with ``publish-docs``.

The site is written to a folder of its own, marked with ``.etl-craft-catalog``: a folder that
holds other files is refused, and a folder written before is emptied and written again.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import shutil
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from pathlib import Path

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import sql_file
from etl_craft.core.errors import EtlCraftError, UsageError
from etl_craft.engine.repository.catalog import LastRun
from etl_craft.services.catalog import Catalog, TableAsset, TaskAsset, latest_run
from etl_craft.services.catalog_graph import lineage_drawing, render_svg
from etl_craft.services.lineage import COPY

MARKER = ".etl-craft-catalog"
"""The file that marks a folder as a catalog site ``generate-docs`` may write over."""

ASSETS = ("catalog.css", "catalog.js")
SQL_PARAMETERS = frozenset({"SOURCE_SQL"})
DEFAULT_DEPTH = 3
"""Levels shown each way when a page opens, if the graph is deeper; the rest are a click away."""


@dataclass(frozen=True)
class Site:
    """What was written: the folder and the number of pages."""

    folder: Path
    pages: int


def write_site(catalog: Catalog, config: ConnectorConfig, folder: Path) -> Site:
    """Write the catalog site into ``folder``; ``UsageError`` if it holds anything else."""
    _prepare(folder)
    writer = _Writer(catalog, config, folder)
    writer.write_all()
    return Site(folder, writer.pages)


def _prepare(folder: Path) -> None:
    if folder.exists() and not folder.is_dir():
        raise UsageError(f"{folder} is a file; name a folder for the catalog site")
    if folder.is_dir() and any(folder.iterdir()):
        if not (folder / MARKER).is_file():
            raise UsageError(
                f"{folder} already holds files that generate-docs did not write; name an empty "
                "or new folder, or the folder of an earlier catalog site"
            )
        for child in folder.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
    folder.mkdir(parents=True, exist_ok=True)
    (folder / MARKER).write_text(
        "Written by `etl-craft generate-docs`, which empties and rewrites this folder.\n",
        encoding="utf-8",
    )


def slug(key: str) -> str:
    """Return a file name for ``key``: itself when safe, else made safe with a short hash."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)
    if safe == key and not key.startswith("."):
        return key
    return f"{safe.lstrip('.')}-{hashlib.sha256(key.encode()).hexdigest()[:8]}"


def table_url(name: str) -> str:
    """Return the URL of a table's page, from the site's root."""
    return f"tables/{slug(name)}.html"


def task_url(label: str) -> str:
    """Return the URL of a task's page, from the site's root."""
    return f"tasks/{slug(label)}.html"


def pipeline_url(code: str) -> str:
    """Return the URL of a pipeline's page, from the site's root."""
    return f"pipelines/{slug(code)}.html"


def rule_url(rule_id: int) -> str:
    """Return the URL of a business rule's page, from the site's root."""
    return f"rules/{rule_id}.html"


def script_url(name: str) -> str:
    """Return the URL of an ingestion script's page, from the site's root."""
    return f"scripts/{slug(name)}.html"


def _e(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _when(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S %Z").strip() if value is not None else ""


def _status(value: str | None) -> str:
    return f'<span class="status {_e(value)}">{_e(value)}</span>' if value else ""


def _facts(pairs: Iterable[tuple[str, str]]) -> str:
    rows = "".join(f"<dt>{_e(k)}</dt><dd>{v}</dd>" for k, v in pairs if v)
    return f'<dl class="facts">{rows}</dl>' if rows else ""


def _table(headers: Sequence[str], rows: Iterable[Sequence[str]], empty: str) -> str:
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    if not body:
        return f'<p class="note">{_e(empty)}</p>'
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    return f'<div class="scroll"><table class="list"><tr>{head}</tr>{body}</table></div>'


def _counts(run: LastRun) -> str:
    counts = [
        ("source", run.source_count),
        ("target", run.target_count),
        ("inserted", run.insert_count),
        ("updated", run.update_count),
        ("deleted", run.delete_count),
    ]
    return ", ".join(f"{name} {value:,}" for name, value in counts if value is not None)


class _Writer:
    """Writes every page, keeping the links between them relative."""

    def __init__(self, catalog: Catalog, config: ConnectorConfig, folder: Path) -> None:
        self.catalog = catalog
        self.config = config
        self.folder = folder
        self.pages = 0

    def write_all(self) -> None:
        for name in ASSETS:
            source = resources.files("etl_craft.services").joinpath("catalog_assets", name)
            (self.folder / name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        (self.folder / "search-index.js").write_text(
            "window.CATALOG_INDEX = " + json.dumps(self._index(), ensure_ascii=True) + ";\n",
            encoding="utf-8",
        )
        self._page("index.html", "etl-craft catalog", "", self._home())
        for code in self.catalog.pipelines:
            self._page(pipeline_url(code), code, "pipeline", self._pipeline(code))
        for label in self.catalog.tasks:
            self._page(task_url(label), label, "task", self._task(label))
        for name, table in self.catalog.tables.items():
            if not table.external:
                self._page(table_url(name), name, "table", self._table_page(name))
        for rule_id, rule in self.catalog.rules.items():
            self._page(rule_url(rule_id), rule.row.name, "business rule", self._rule(rule_id))
        for name in self.catalog.scripts:
            self._page(script_url(name), name, "ingestion script", self._script(name))

    # Page frame

    def _page(self, path: str, title: str, kind: str, body: str) -> None:
        depth = path.count("/")
        root = "../" * depth
        nav = "".join(
            f'<a href="{root}index.html#{anchor}">{label}</a>'
            for anchor, label in (
                ("pipelines", "Pipelines"),
                ("tables", "Tables"),
                ("untraced", "Lineage gaps"),
            )
        )
        kind_label = f'<div class="kind-label">{_e(kind)}</div>' if kind else ""
        document = (
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<meta name="robots" content="noindex, nofollow">'
            f"<title>{_e(title)} · etl-craft catalog</title>"
            f'<link rel="stylesheet" href="{root}catalog.css"></head>'
            f'<body data-root="{root}"><header class="top">'
            f'<a class="brand" href="{root}index.html">etl-craft catalog</a><nav>{nav}</nav>'
            '<div class="search"><input type="search" placeholder="Search pipelines, tasks, '
            'tables, columns, rules…" aria-label="Search the catalog" autocomplete="off">'
            '<div class="results"></div></div></header>'
            f"<main>{kind_label}<h1>{_e(title)}</h1>{body}</main>"
            f'<script src="{root}search-index.js"></script>'
            f'<script src="{root}catalog.js"></script></body></html>'
        )
        target = self.folder / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(document, encoding="utf-8")
        self.pages += 1

    def _link(self, url: str, text: str, root: str = "../") -> str:
        return f'<a href="{_e(root + url)}">{_e(text)}</a>'

    def _table_link(self, name: str, root: str = "../") -> str:
        table = self.catalog.tables.get(name)
        if table is not None and table.external:
            return f'{_e(table.label)} <span class="badge">external</span>'
        return self._link(table_url(name), name, root)

    def _task_link(self, label: str, root: str = "../") -> str:
        return self._link(task_url(label), label, root)

    # Home

    def _home(self) -> str:
        c = self.catalog
        search = (
            '<section id="search-page"><h2>Search</h2>'
            '<input type="search" class="big" placeholder="Search everything" '
            'aria-label="Search everything" style="width:100%;padding:8px;font:inherit">'
            '<div class="chips"></div><p class="note summary"></p>'
            '<div class="all-results"></div></section>'
        )
        stats = _facts(
            [
                ("Pipelines", str(len(c.pipelines))),
                ("Tasks", str(len(c.tasks))),
                ("Tables", str(sum(1 for t in c.tables.values() if not t.external))),
                ("Business rules", str(len(c.rules))),
                ("Ingestion scripts", str(len(c.scripts))),
                ("Generated", _e(_when(c.generated_at))),
            ]
        )
        pipelines = _table(
            ["Pipeline", "Name", "Schedule", "Last run", "Ended"],
            (
                [
                    self._link(pipeline_url(code), code, ""),
                    _e(p.row.pipeline_name),
                    _e(p.row.run_schedule),
                    _status(p.row.last_run.status) if p.row.last_run else "never run",
                    _e(_when(p.row.last_run.end)) if p.row.last_run else "",
                ]
                for code, p in c.pipelines.items()
            ),
            "No active pipelines.",
        )
        tables = _table(
            ["Table", "Written by", "Read by", "Columns"],
            (
                [
                    self._link(table_url(name), name, ""),
                    ", ".join(self._task_link(t, "") for t in table.writers),
                    str(len(table.readers)),
                    str(len(table.columns)),
                ]
                for name, table in sorted(c.tables.items())
                if not table.external
            ),
            "No tables yet.",
        )
        untraced = _table(
            ["Task", "Why its columns cannot be traced"],
            ([self._task_link(t.label, ""), _e(t.lineage_error)] for t in c.untraced),
            "Every SQL task's columns are traced.",
        )
        return (
            f'{stats}{search}<h2 id="pipelines">Pipelines</h2>{pipelines}'
            f'<h2 id="tables">Tables</h2>{tables}'
            f'<h2 id="untraced">Column lineage unavailable</h2>{untraced}'
        )

    # Pipelines and tasks

    def _pipeline(self, code: str) -> str:
        p = self.catalog.pipelines[code]
        row = p.row
        run = row.last_run
        facts = _facts(
            [
                ("Name", _e(row.pipeline_name)),
                ("Description", _e(row.description)),
                ("Schedule", _e(row.run_schedule)),
                ("Refresh", _e(row.refresh_type)),
                ("SLA", _e(f"{row.sla_in_hours:g} hours") if row.sla_in_hours else ""),
                (
                    "Last run",
                    f"{_status(run.status)} {_e(_when(run.start))} → {_e(_when(run.end))}"
                    + (f" · SLA {_e(run.sla_status)}" if run.sla_status else "")
                    if run
                    else "never run",
                ),
                (
                    "Depends on",
                    ", ".join(
                        f"{self._link(pipeline_url(u), u)} ({_e(kind)})" for u, kind in p.depends_on
                    ),
                ),
                (
                    "Needed by",
                    ", ".join(self._link(pipeline_url(d), d) for d in p.depended_on_by),
                ),
            ]
        )
        tasks = _table(
            ["Task", "Handler", "Writes", "Last run", "Counts"],
            (
                [
                    self._task_link(label),
                    _e(task.row.handler),
                    self._table_link(task.target) if task.target else "",
                    _status(task.row.last_run.status) if task.row.last_run else "never run",
                    _e(_counts(task.row.last_run)) if task.row.last_run else "",
                ]
                for label in p.tasks
                for task in [self.catalog.tasks[label]]
            ),
            "No active tasks.",
        )
        return f"{facts}<h2>Tasks</h2>{tasks}"

    def _task(self, label: str) -> str:
        task = self.catalog.tasks[label]
        row = task.row
        run = row.last_run
        lineage = ""
        if task.lineage_error:
            lineage = (
                '<p><span class="badge warn">column lineage unavailable</span> '
                f"{_e(task.lineage_error)}</p>"
            )
        facts = _facts(
            [
                ("Pipeline", self._link(pipeline_url(row.pipeline_code), row.pipeline_code)),
                ("Handler", _e(row.handler)),
                ("Type", _e(row.task_type)),
                ("Run condition", _e(row.run_condition or "ALL")),
                ("Writes", self._table_link(task.target) if task.target else ""),
                ("Reads", ", ".join(self._table_link(s) for s in task.sources)),
                ("Script", self._link(script_url(task.script), task.script) if task.script else ""),
                (
                    "Last run",
                    f"{_status(run.status)} {_e(_when(run.end))} · {_e(_counts(run))}"
                    if run
                    else "never run",
                ),
                ("Error", _e(run.error_message) if run else ""),
            ]
        )
        documentation = ""
        if task.documentation:
            version = (
                f' <span class="badge">version {task.documentation_version}</span>'
                if task.documentation_version
                else ""
            )
            documentation = (
                f'<h2>Documentation{version}</h2><p class="doc">{_e(task.documentation)}</p>'
            )
        mapping = ""
        if task.column_edges:
            mapping = "<h2>Columns</h2>" + _table(
                ["Column", "Made from", "How"],
                (
                    [
                        self._link(
                            table_url(e.target_object) + f"#col={e.target_column}", e.target_column
                        ),
                        f"{self._table_link(e.source_object)}.{_e(e.source_column)}"
                        if e.source_object
                        else '<span class="note">no source column</span>',
                        "copy"
                        if e.transformation == COPY
                        else f"<code>{_e(e.transformation)}</code>",
                    ]
                    for e in task.column_edges
                ),
                "",
            )
        return f"{facts}{lineage}{documentation}{mapping}{self._parameters(task)}"

    def _parameters(self, task: TaskAsset) -> str:
        rows = []
        for name, value in sorted(task.params.items()):
            if name == "DOCUMENTATION":
                continue
            shown = (
                f"<pre>{_e(value)}</pre>" if name in SQL_PARAMETERS else f"<code>{_e(value)}</code>"
            )
            if name == "SOURCE_SQL_FILE":
                shown += self._sql_file(value)
            rows.append([f"<code>{_e(name)}</code>", shown])
        return "<h2>Parameters</h2>" + _table(["Parameter", "Value"], rows, "No parameters.")

    def _sql_file(self, name: str) -> str:
        try:
            return f"<pre>{_e(sql_file(self.config, name).read_text(encoding='utf-8'))}</pre>"
        except (EtlCraftError, OSError) as error:
            return f'<p class="note">The file cannot be read: {_e(error)}</p>'

    # Tables

    def _table_page(self, name: str) -> str:
        table = self.catalog.tables[name]
        run = latest_run(self.catalog.tasks[w].row.last_run for w in table.writers)
        writer_of = {
            self.catalog.tasks[w].row.last_run: w
            for w in table.writers
            if self.catalog.tasks[w].row.last_run is not None
        }
        facts = _facts(
            [
                ("Written by", ", ".join(self._task_link(w) for w in table.writers)),
                ("Read by", ", ".join(self._task_link(r) for r in table.readers)),
                (
                    "Business rules",
                    ", ".join(
                        self._link(rule_url(r), self.catalog.rules[r].row.name) for r in table.rules
                    ),
                ),
                (
                    "Last written",
                    f"{_status(run.status)} {_e(_when(run.end))} by "
                    f"{self._task_link(writer_of[run])} · {_e(_counts(run))}"
                    if run
                    else "",
                ),
                ("In the warehouse", "yes" if table.in_warehouse else ""),
            ]
        )
        return f"{facts}{self._columns(table)}{self._graph(name)}"

    def _columns(self, table: TableAsset) -> str:
        sources: dict[str, list[str]] = {}
        for e in self.catalog.column_edges:
            if e.target_object != table.name:
                continue
            origin = (
                f"{self._table_link(e.source_object)}.{_e(e.source_column)}"
                if e.source_object
                else "no source column"
            )
            how = "" if e.transformation == COPY else f" <code>{_e(e.transformation)}</code>"
            sources.setdefault(e.target_column, []).append(f"{origin}{how} ({_e(e.task)})")
        typed = any(c.data_type for c in table.columns.values())
        headers = ["Column", *(["Type", "Comment"] if typed else []), "Made from"]
        rows = []
        for column in table.columns.values():
            cells = [f'<a href="#col={_e(column.name)}" title="Trace it">{_e(column.name)}</a>']
            if typed:
                cells += [_e(column.data_type), _e(column.comment)]
            cells.append("<br>".join(sources.get(column.name, [])))
            rows.append(cells)
        return "<h2>Columns</h2>" + _table(headers, rows, "No columns known yet.")

    def _graph(self, name: str) -> str:
        drawing = lineage_drawing(self.catalog, name)
        low, high = drawing.levels
        deepest = max(-low, high)
        if deepest == 0:
            return '<h2>Lineage</h2><p class="note">No task reads or writes this table yet.</p>'
        opening = DEFAULT_DEPTH if deepest > DEFAULT_DEPTH else None
        options = "".join(
            f'<option value="{d}"{" selected" if d == opening else ""}>{d}</option>'
            for d in range(1, deepest + 1)
        )
        all_selected = "" if opening else " selected"
        truncated = (
            f'<p class="note">This graph stops after {drawing.truncated_at} level(s): it '
            "reached its size limit. `etl-craft lineage --table` follows every level.</p>"
            if drawing.truncated_at is not None
            else ""
        )
        svg = render_svg(drawing, lambda table: "../" + table_url(table))
        return (
            f'<h2>Lineage</h2><div class="graph-panel" data-focus="{_e(name)}">'
            '<div class="controls"><label>Direction <select class="direction">'
            '<option value="both">both</option><option value="upstream">upstream</option>'
            '<option value="downstream">downstream</option></select></label>'
            f'<label>Depth <select class="depth">{options}'
            f'<option value="all"{all_selected}>all</option></select></label>'
            '<span class="zoom">'
            '<button type="button" class="zoom-out" title="Zoom out">&minus;</button>'
            '<button type="button" class="zoom-in" title="Zoom in">+</button>'
            '<button type="button" class="zoom-fit" title="Fit the graph in view">Fit</button>'
            "</span>"
            '<button type="button" class="expand-all">Expand all</button>'
            '<button type="button" class="collapse-all">Collapse all</button>'
            '<button type="button" class="reset">Clear trace</button></div>'
            '<div class="legend">'
            '<span><svg width="30" height="8"><path d="M0,4 H30" stroke="currentColor"/></svg> '
            "copy</span>"
            '<span><svg width="30" height="8"><path d="M0,4 H30" stroke="currentColor" '
            'stroke-dasharray="6 4"/></svg> derived</span>'
            '<span><svg width="30" height="8"><path d="M0,4 H30" stroke="currentColor" '
            'stroke-width="2" stroke-dasharray="2 4"/></svg> table level only</span>'
            "<span>ƒ made from no source column</span>"
            "<span>Click a table to show its columns, ↗ to open it, and a column to trace "
            "it upstream and downstream. Drag to pan; Ctrl or ⌘ with the wheel zooms.</span></div>"
            f'{truncated}<div class="graph">{svg}</div></div>'
        )

    # Rules and scripts

    def _rule(self, rule_id: int) -> str:
        rule = self.catalog.rules[rule_id]
        row = rule.row
        facts = _facts(
            [
                ("Type", _e(row.rule_type)),
                ("Table", self._table_link(rule.table)),
                ("Key column", f"<code>{_e(row.key_column)}</code>"),
                ("Wave", _e(row.sequence_number)),
                ("Task", self._task_link(rule.task)),
            ]
        )
        return f"{facts}<h2>Condition</h2><pre>{_e(row.sql)}</pre>"

    def _script(self, name: str) -> str:
        script = self.catalog.scripts[name]
        facts = _facts(
            [
                ("File", f"<code>ingestion_scripts/{_e(name)}</code>"),
                ("Found", "yes" if script.exists else '<span class="badge warn">missing</span>'),
                ("Run by", ", ".join(self._task_link(t) for t in script.tasks)),
            ]
        )
        rows = [
            [
                self._task_link(label),
                ", ".join(self._table_link(s) for s in task.sources),
                self._table_link(task.target) if task.target else "",
            ]
            for label in script.tasks
            for task in [self.catalog.tasks[label]]
        ]
        return (
            facts
            + "<h2>Reads and writes</h2>"
            + _table(
                ["Task", "Reads (SOURCE_OBJECT)", "Writes (TARGET_OBJECT)"],
                rows,
                "",
            )
        )

    # Search index

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
