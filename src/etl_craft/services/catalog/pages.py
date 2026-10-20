"""Catalog pages: the warehouse, pipeline, run, task, table, rule and script pages."""

from __future__ import annotations

from collections.abc import Sequence

from etl_craft.core.enums import DependencyType
from etl_craft.engine.repository.catalog import Consumption, RunSummary
from etl_craft.services.catalog.collect import (
    Catalog,
    PipelineAsset,
    TableAsset,
    TaskAsset,
    latest_run,
)
from etl_craft.services.catalog.gap_html import comparison_grid
from etl_craft.services.catalog.graph import (
    dag_drawing,
    lineage_drawing,
    lineage_history,
    render_dag_svg,
    render_svg,
)
from etl_craft.services.catalog.html import escape as _e
from etl_craft.services.catalog.html import pipeline_url as pipeline_url
from etl_craft.services.catalog.html import render_table as _table
from etl_craft.services.catalog.html import rule_url as rule_url
from etl_craft.services.catalog.html import run_url as run_url
from etl_craft.services.catalog.html import script_url as script_url
from etl_craft.services.catalog.html import slug as slug
from etl_craft.services.catalog.html import table_url as table_url
from etl_craft.services.catalog.html import task_url as task_url
from etl_craft.services.catalog.incremental import (
    SqlFiles,
)
from etl_craft.services.catalog.metrics import bars as _bars
from etl_craft.services.catalog.metrics import counts as _counts
from etl_craft.services.catalog.metrics import duration as _duration
from etl_craft.services.catalog.metrics import facts as _facts
from etl_craft.services.catalog.metrics import kpi_summary
from etl_craft.services.catalog.metrics import status as _status
from etl_craft.services.catalog.metrics import when as _when
from etl_craft.services.catalog.storage import MARKER as MARKER
from etl_craft.services.lineage import COPY

SQL_PARAMETERS = frozenset({"SOURCE_SQL"})
ZOOM_BUTTONS = (
    '<span class="zoom">'
    '<button type="button" class="zoom-out" title="Zoom out">&minus;</button>'
    '<button type="button" class="zoom-in" title="Zoom in">+</button>'
    '<button type="button" class="zoom-fit" title="Fit the graph in view">Fit</button>'
    "</span>"
)
DEFAULT_DEPTH = 3
"""Levels shown each way when a page opens, if the graph is deeper; the rest are a click away."""


def anchor(*parts: str) -> str:
    """Return the id of a database's or schema's heading on the warehouse page."""
    return "w-" + slug(".".join(parts))


class Pages:
    """Builds each catalog page's HTML from the catalog, with links relative to it."""

    catalog: Catalog
    sql_files: SqlFiles

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

    def _warehouse(self) -> str:
        c = self.catalog
        grouped: dict[str, dict[str, list[str]]] = {}
        for name, table in sorted(c.tables.items()):
            if table.external:
                continue
            database, schema, _ = c.where(name)
            grouped.setdefault(database, {}).setdefault(schema, []).append(name)
        parts = []
        contents = []
        for database, schemas in sorted(grouped.items()):
            contents.append(
                f'<li><a href="#{anchor(database)}">{_e(database)}</a>: '
                + ", ".join(
                    f'<a href="#{anchor(database, schema)}">{_e(schema)}</a>'
                    for schema in sorted(schemas)
                )
                + "</li>"
            )
            parts.append(f'<h2 id="{anchor(database)}">{_e(database)}</h2>')
            for schema, names in sorted(schemas.items()):
                parts.append(f'<h3 id="{anchor(database, schema)}">{_e(schema)}</h3>')
                parts.append(
                    _table(
                        ["Table", "Columns", "Written by", "Read by", "Pipelines", "Rules"],
                        (
                            [
                                self._link(table_url(name), c.where(name)[2], ""),
                                str(len(table.columns)),
                                ", ".join(self._task_link(t, "") for t in table.writers),
                                ", ".join(self._task_link(t, "") for t in table.readers),
                                ", ".join(
                                    self._link(pipeline_url(p), p, "")
                                    for p in self._pipelines_of(table)
                                ),
                                str(len(table.rules)) if table.rules else "",
                            ]
                            for name in names
                            for table in [c.tables[name]]
                        ),
                        "",
                    )
                )
        external = [t for t in c.tables.values() if t.external]
        if external:
            parts.append('<h2 id="external">External sources</h2>')
            parts.append(
                _table(
                    ["Source", "Read by"],
                    (
                        [_e(t.label), ", ".join(self._task_link(r, "") for r in t.readers)]
                        for t in external
                    ),
                    "",
                )
            )
        if not contents:
            return '<p class="note">No tables yet.</p>'
        return f'<ul class="contents">{"".join(contents)}</ul>' + "".join(parts)

    def _pipelines_of(self, table: TableAsset) -> list[str]:
        codes = {self.catalog.tasks[t].row.pipeline_code for t in (*table.writers, *table.readers)}
        return sorted(codes)

    # Pipelines and tasks

    def _pipeline(self, code: str) -> str:
        p = self.catalog.pipelines[code]
        row = p.row
        run = row.last_run
        description = (
            f'<p class="doc">{_e(row.description)}</p>'
            if row.description
            else '<p class="note">No description: set <code>CFG_PIPELINES.DESCRIPTION</code> '
            "to document this pipeline.</p>"
        )
        facts = _facts(
            [
                ("Name", _e(row.pipeline_name)),
                ("Schedule", _e(row.run_schedule)),
                ("Refresh", _e(row.refresh_type)),
                ("SLA", _e(f"{row.sla_in_hours:g} hours") if row.sla_in_hours else ""),
                (
                    "Paused",
                    f'<span class="status PAUSED">{_e(p.paused.describe())}</span>'
                    if p.paused
                    else "",
                ),
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
            ["Task", "Handler", "Waits for", "Writes", "Reads", "Last run", "Counts"],
            (
                [
                    self._task_link(label),
                    _e(task.row.handler),
                    self._waits(task.upstream),
                    self._table_link(task.target) if task.target else "",
                    ", ".join(self._table_link(t) for t in task.sources),
                    _status(task.row.last_run.status) if task.row.last_run else "never run",
                    _e(_counts(task.row.last_run)) if task.row.last_run else "",
                ]
                for label in p.tasks
                for task in [self.catalog.tasks[label]]
            ),
            "No active tasks.",
        )
        changes = ""
        if p.interventions:
            changes = "<h2>Interventions on the last run</h2>" + _table(
                ["Task", "Action", "From", "To", "By", "At", "Reason"],
                (
                    [
                        self._task_link(f"{code}.{c.task_code}") if c.task_code else "the run",
                        _e(c.action),
                        _status(c.from_status),
                        _status(c.to_status) if c.to_status else "reset",
                        _e(c.requested_by),
                        _e(_when(c.requested_at)),
                        _e(c.reason),
                    ]
                    for c in p.interventions
                ),
                "",
            )
        return (
            f"{description}{facts}{self._dag(code)}<h2>Tasks</h2>{tasks}{changes}"
            f"{self._pipeline_runs(p)}"
        )

    # Runs

    def _pipeline_runs(self, p: PipelineAsset) -> str:
        if not p.runs:
            return '<h2 id="runs">Runs</h2><p class="note">It has not run yet.</p>'
        summary = (
            f'<p class="note">Runs shown: the latest {len(p.runs)} logical runs; '
            "repairs are counted separately from their original trigger.</p>" + kpi_summary(p.runs)
        )
        chart = _bars(
            [
                (
                    run.seconds,
                    run.status,
                    f"run {run.pipeline_run_id} {run.status} {_duration(run.seconds)}",
                    "../" + run_url(run.pipeline_run_id),
                )
                for run in reversed(p.runs)
            ],
            "Duration",
        )
        rows = _table(
            [
                "Run",
                "Kind",
                "Run date",
                "Status",
                "Started",
                "Took",
                "SLA",
                "Built from",
                "Changes",
            ],
            (
                [
                    self._link(run_url(run.pipeline_run_id), str(run.pipeline_run_id)),
                    _e(run.run_kind.value.title()),
                    _e(run.run_date)
                    + (' <span class="badge">backfill</span>' if run.backfill else ""),
                    _status(run.status),
                    _e(_when(run.start)),
                    _e(_duration(run.seconds)),
                    _e(run.sla_status),
                    self._upstream_runs(self.catalog.built_from.get(run.pipeline_run_id, [])),
                    str(len(p.run_changes.get(run.pipeline_run_id, [])) or ""),
                ]
                for run in p.runs
            ),
            "",
        )
        return f'<h2 id="runs">Runs</h2>{summary}{chart}{rows}'

    def _upstream_runs(self, consumed: Sequence[Consumption]) -> str:
        return ", ".join(
            self._run_link(used.upstream_run_id, used.upstream_pipeline)
            + (f" ({_e(used.upstream_task)})" if used.upstream_task else "")
            for used in consumed
        )

    def _run_link(self, pipeline_run_id: int, pipeline_code: str, root: str = "../") -> str:
        pipeline = self.catalog.pipelines.get(pipeline_code)
        text = f"{pipeline_code} run {pipeline_run_id}"
        if pipeline is not None and any(
            r.pipeline_run_id == pipeline_run_id for r in pipeline.runs
        ):
            return self._link(run_url(pipeline_run_id), text, root)
        return _e(text)

    def _run(self, p: PipelineAsset, run: RunSummary) -> str:
        code = p.row.pipeline_code
        facts = _facts(
            [
                ("Pipeline", self._link(pipeline_url(code), code)),
                ("Status", _status(run.status)),
                ("Run date", _e(run.run_date) + (" (backfill)" if run.backfill else "")),
                ("Started", _e(_when(run.start))),
                ("Ended", _e(_when(run.end))),
                ("Started by", _e(run.started_by)),
                ("Starter kind", _e(run.started_by_kind)),
                ("Ended by", _e(run.ended_by)),
                ("Ending kind", _e(run.ended_by_kind)),
                ("Took", _e(_duration(run.seconds))),
                ("SLA", _e(run.sla_status)),
                (
                    "Built from",
                    self._upstream_runs(self.catalog.built_from.get(run.pipeline_run_id, [])),
                ),
                (
                    "Used by",
                    ", ".join(
                        self._run_link(used.pipeline_run_id, used.pipeline_code)
                        + (f" ({_e(used.task_code)})" if used.task_code else "")
                        for used in self.catalog.consumed_by.get(run.pipeline_run_id, [])
                    ),
                ),
            ]
        )
        tasks = _table(
            ["Task", "Status", "Attempts", "Took", "Counts", "Watermark", "Message"],
            (
                [
                    self._task_link(label),
                    _status(task_run.status),
                    str(task_run.attempts),
                    _e(_duration(task_run.seconds)),
                    _e(_counts(task_run)),
                    _e(
                        f"{task_run.watermark_from} → {task_run.watermark_to}"
                        if task_run.watermark_from is not None
                        else ""
                    ),
                    failure_messages(task_run.error_message, task_run.failure_reasons),
                ]
                for label in p.tasks
                for task_run in self.catalog.tasks[label].runs
                if task_run.pipeline_run_id == run.pipeline_run_id
            ),
            "No task ran in this run, or its task runs are older than the history kept here.",
        )
        changes = p.run_changes.get(run.pipeline_run_id, [])
        interventions = ""
        if changes:
            interventions = "<h2>Interventions</h2>" + _table(
                ["Task", "Action", "From", "To", "By", "At", "Reason"],
                (
                    [
                        self._task_link(f"{code}.{c.task_code}") if c.task_code else "the run",
                        _e(c.action),
                        _status(c.from_status),
                        _status(c.to_status) if c.to_status else "reset",
                        _e(c.requested_by),
                        _e(_when(c.requested_at)),
                        _e(c.reason),
                    ]
                    for c in changes
                ),
                "",
            )
        return f"{facts}<h2>Tasks</h2>{tasks}{interventions}"

    def _task_runs(self, task: TaskAsset) -> str:
        if not task.runs:
            return ""
        summary = (
            f'<p class="note">Runs shown: the latest {len(task.runs)} logical runs; '
            "repairs are counted separately from their original trigger.</p>"
            + kpi_summary(task.runs)
        )
        chart = _bars(
            [
                (
                    float(r.target_count) if r.target_count is not None else None,
                    r.status,
                    f"run {r.pipeline_run_id} {r.status} {r.target_count or 0} row(s)",
                    "../" + run_url(r.pipeline_run_id),
                )
                for r in reversed(task.runs)
            ],
            "Rows written",
        )
        rows = _table(
            ["Run", "Kind", "Status", "Attempts", "Took", "Counts", "Watermark", "Message"],
            (
                [
                    self._run_link(r.pipeline_run_id, task.row.pipeline_code),
                    _e(r.run_kind.value.title()),
                    _status(r.status),
                    str(r.attempts),
                    _e(_duration(r.seconds)),
                    _e(_counts(r)),
                    _e(
                        f"{r.watermark_from} → {r.watermark_to}"
                        if r.watermark_from is not None
                        else ""
                    ),
                    failure_messages(r.error_message, r.failure_reasons),
                ]
                for r in task.runs
            ),
            "",
        )
        return f'<h2 id="runs">Runs</h2>{summary}{chart}{rows}'

    def _checked(self, task: TaskAsset) -> list[str]:
        return sorted({self.catalog.rules[r].table for r in task.rules})

    def _waits(self, dependencies: Sequence[tuple[str, str]]) -> str:
        return ", ".join(f"{self._task_link(t)} ({_e(kind)})" for t, kind in dependencies)

    def _dag(self, code: str) -> str:
        if not self.catalog.pipelines[code].tasks:
            return ""
        drawing = dag_drawing(self.catalog, code)
        svg = render_dag_svg(
            drawing,
            self.catalog,
            lambda label: "../" + task_url(label),
            lambda table: "../" + table_url(table),
        )
        legend = "".join(
            f'<span><svg width="30" height="8"><path class="dep {style}" d="M0,4 H30"/></svg> '
            f"{kind}</span>"
            for kind, style in (
                (DependencyType.SUCCESS, "success"),
                (DependencyType.FAILURE, "failure"),
                (DependencyType.ALWAYS, "always"),
                (DependencyType.HAS_DATA, "has-data"),
            )
        )
        return (
            '<h2>DAG</h2><div class="graph-panel dag-panel"><div class="controls">'
            f"{ZOOM_BUTTONS}</div>"
            f'<div class="legend">{legend}<span>→ writes, ← reads, ✓ checks. Click a task or '
            "a table to open it. Drag to pan; Ctrl or ⌘ with the wheel zooms.</span></div>"
            f'<div class="graph">{svg}</div></div>'
        )

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
                ("Waits for", self._waits(task.upstream)),
                ("Needed by", self._waits(task.downstream)),
                ("Handler", _e(row.handler)),
                ("Type", _e(row.task_type)),
                ("Run condition", _e(row.run_condition or "ALL")),
                ("Writes", self._table_link(task.target) if task.target else ""),
                ("Reads", ", ".join(self._table_link(s) for s in task.sources)),
                ("Script", self._link(script_url(task.script), task.script) if task.script else ""),
                (
                    "Business rules",
                    ", ".join(
                        self._link(rule_url(r), self.catalog.rules[r].row.name) for r in task.rules
                    ),
                ),
                ("Checks", ", ".join(self._table_link(t) for t in self._checked(task))),
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
        return (
            f"{facts}{lineage}{documentation}{mapping}{self._parameters(task)}"
            f"{self._task_runs(task)}"
        )

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
        value = self.sql_files[name]
        if isinstance(value, str):
            return f"<pre>{_e(value)}</pre>"
        return f'<p class="note">The file cannot be read: {_e(value)}</p>'

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
                    "Pipelines",
                    ", ".join(self._link(pipeline_url(p), p) for p in self._pipelines_of(table)),
                ),
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
                (
                    "In the warehouse",
                    "yes"
                    if table.in_warehouse
                    else "no"
                    if table.warehouse_checked
                    else "not checked",
                ),
            ]
        )
        comparison = comparison_grid(self.catalog, table, self._task_link, self._table_link)
        history = lineage_history(table, self.catalog)
        return f"{facts}{comparison}{self._graph(name)}{history}"

    def _graph(self, name: str) -> str:
        drawing = lineage_drawing(self.catalog, name)
        low, high = drawing.levels
        deepest = max(-low, high)
        if deepest == 0 and not (
            self.catalog.tables[name].writers or self.catalog.tables[name].readers
        ):
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
            f'<h2>Lineage</h2><div class="graph-panel lineage-panel" data-focus="{_e(name)}">'
            '<div class="controls"><label>Direction <select class="direction">'
            '<option value="both">both</option><option value="upstream">upstream</option>'
            '<option value="downstream">downstream</option></select></label>'
            f'<label>Depth <select class="depth">{options}'
            f'<option value="all"{all_selected}>all</option></select></label>'
            f"{ZOOM_BUTTONS}"
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


def failure_messages(message: str | None, reasons: Sequence[str]) -> str:
    """Escape both the database message and each recorded retry decision."""
    return "<br>".join(_e(value) for value in (message, *reasons) if value)
