"""`generate-docs` -- a static, searchable documentation site built from the Engine DB.

[ADDITION] CLAUDE.md's own plan: "the same read-layer queries as above,
rendered as a static, searchable site (deployable to GitHub Pages or any
endpoint) rather than returned as CLI text ... since GitHub Pages has no
backend, 'searchable' means a client-side search index baked in at
generation time, not a server endpoint." This module is that generator --
plain HTML/CSS/JS files plus one JSON search index, all written to a local
output directory a team then serves or deploys however it likes (GitHub
Pages, any static host, or just opened from disk).

Reuses exactly the read-layer functions the CLI's own `list`/`graph`/
`steps`/`lineage` commands already use (cfg.py, resolver.py) -- this module
adds no new query logic of its own beyond assembling their results into
pages, per "the same read-layer queries as above."

[CHOICE] The client-side search is a small, dependency-free vanilla-JS
substring search over the generated search-index.json, not a vendored copy
of Fuse.js/Lunr.js. CLAUDE.md's own phrasing ("Fuse.js/Lunr.js-class") reads
as "a client-side index of this general shape," not a literal library
requirement, and a team's own copy of this site may end up served from an
internal network with no CDN access at all -- a zero-dependency search
stays functional wherever the static files themselves do. Flagged as a real
trade-off: no fuzzy matching, no relevance ranking, just substring matches
against each entry's own text blob -- plenty for a metadata site with at
most a few hundred pipelines/tasks, not built to scale past that.

[CHOICE] Every generated page is genuinely static HTML (fetch()-based search
excepted) -- no build step beyond running `etl-craft generate-docs` again.
Re-running it wholesale-overwrites the previous output directory's files
(never a merge), same "full parse + re-serialize, simple and robust" spirit
already established for craft-connector.yml writes in configure.py.
"""

from __future__ import annotations

import html
import json
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path

from sqlalchemy.engine import Connection

from etl_craft.cfg import (
    CrossPipelineTaskEdge,
    PipelineDependencyEdge,
    PipelineStep,
    PipelineSummary,
    fetch_all_pipelines,
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependencies,
    fetch_pipeline_graph,
    fetch_pipeline_steps,
    fetch_task_codes,
    resolve_pipeline_id,
)
from etl_craft.column_lineage import ColumnEdge, TaskLineage, lineage_for_tasks
from etl_craft.documentation import fetch_current_documentation, refresh_all
from etl_craft.resolver import build_graph


@dataclass(frozen=True)
class PipelineDocData:
    """Everything one pipeline's generated page needs, beyond its own PipelineSummary."""

    waves: list[list[str]]
    steps: list[PipelineStep]
    pipeline_dependencies: list[PipelineDependencyEdge]
    cross_task_dependencies: list[CrossPipelineTaskEdge]
    # [ADDITION, 2026-09-20] task_code -> (documentation, version), and
    # task_code -> its parsed column lineage. Both are what make these pages
    # documentation rather than a formatted dump of CFG_ rows.
    documentation: dict[str, tuple[str, int]] = field(default_factory=dict)
    column_lineage: dict[str, list[ColumnEdge]] = field(default_factory=dict)


PipelineDoc = tuple[PipelineSummary, PipelineDocData]


def _collect_pipeline_doc_data(
    conn: Connection, pipeline_code: str, lineage: list[TaskLineage]
) -> PipelineDocData:
    """Gather one pipeline's waves/steps/dependencies -- the same reads `graph`/`steps` use."""
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    graph_data = fetch_pipeline_graph(conn, pipeline_id)
    task_codes = fetch_task_codes(conn, pipeline_id)
    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    waves = [[task_codes[task_id] for task_id in wave] for wave in graph.waves()]
    docs_by_task_id = fetch_current_documentation(conn)
    return PipelineDocData(
        waves=waves,
        steps=fetch_pipeline_steps(conn, pipeline_id),
        pipeline_dependencies=fetch_pipeline_dependencies(conn, pipeline_id),
        cross_task_dependencies=fetch_cross_pipeline_task_edges(conn, pipeline_id),
        documentation={
            task_codes[task_id]: entry
            for task_id, entry in docs_by_task_id.items()
            if task_id in task_codes
        },
        column_lineage={
            task.task_code: task.edges
            for task in lineage
            if task.pipeline_code == pipeline_code and task.edges
        },
    )


def collect_docs(conn: Connection) -> list[PipelineDoc]:
    """Gather every active pipeline's doc data -- the one DB-touching entry point here.

    [ADDITION, 2026-09-20] Refreshes documentation versions and resolves column
    lineage once for the whole site, rather than per pipeline: both are global
    reads, and a page for pipeline A legitimately wants to show that its column
    came from a table pipeline B writes.
    """
    refresh_all(conn)
    lineage = lineage_for_tasks(conn)
    return [
        (summary, _collect_pipeline_doc_data(conn, summary.pipeline_code, lineage))
        for summary in fetch_all_pipelines(conn)
    ]


def build_search_index(docs: list[PipelineDoc]) -> list[dict]:
    """Build the flat, JSON-serializable search index -- one entry per pipeline and per task."""
    index: list[dict] = []
    for summary, data in docs:
        index.append(
            {
                "type": "pipeline",
                "pipeline_code": summary.pipeline_code,
                "text": f"{summary.pipeline_code} {summary.pipeline_name} {summary.refresh_type}",
                "url": f"{summary.pipeline_code}.html",
            }
        )
        for step in data.steps:
            params_text = " ".join(f"{name}={value}" for name, value in step.parameters.items())
            index.append(
                {
                    "type": "task",
                    "pipeline_code": summary.pipeline_code,
                    "task_code": step.task_code,
                    "handler": step.handler,
                    "documentation": (data.documentation.get(step.task_code) or ("", 0))[0],
                    "text": (
                        f"{summary.pipeline_code} {step.task_code} {step.handler} {params_text} "
                        + (data.documentation.get(step.task_code) or ("", 0))[0]
                    ),
                    "url": f"{summary.pipeline_code}.html#{step.task_code}",
                }
            )
    return index


def _page(title: str, body: str, *, with_search: bool = False) -> str:
    search_assets = (
        '<script src="fuse.min.js" defer></script>\n<script src="search.js" defer></script>'
        if with_search
        else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{html.escape(title)}</title>
<link rel="stylesheet" href="style.css">
{search_assets}
</head>
<body>
{body}
</body>
</html>
"""


def _render_index_html(docs: list[PipelineDoc]) -> str:
    rows = "\n".join(
        f'<li><a href="{html.escape(summary.pipeline_code)}.html">'
        f"{html.escape(summary.pipeline_code)}</a> -- "
        f"{html.escape(summary.pipeline_name)} ({html.escape(summary.refresh_type)})</li>"
        for summary, _ in docs
    )
    body = f"""<h1>etl-craft pipelines</h1>
<input id="search-box" type="search" placeholder="Search pipelines and tasks...">
<ul id="search-results"></ul>
<h2>All pipelines</h2>
<ul>
{rows}
</ul>
"""
    return _page("etl-craft docs", body, with_search=True)


def _render_step(step: PipelineStep, data: PipelineDocData) -> str:
    """Render one task: its documentation, its column lineage, then its parameters."""
    parts = [
        f'<li id="{html.escape(step.task_code)}">'
        f"<strong>{html.escape(step.task_code)}</strong> ({html.escape(step.handler)})"
    ]

    documented = data.documentation.get(step.task_code)
    if documented is not None:
        prose, version = documented
        parts.append(
            f'<p class="doc">{html.escape(prose)}'
            f'<span class="doc-version">docs v{version}</span></p>'
        )

    edges = data.column_lineage.get(step.task_code) or []
    if edges:
        rows = "".join(
            "<tr>"
            f"<td>{html.escape(edge.target_column)}</td>"
            f"<td>{html.escape(_origin_of(edge))}</td>"
            f"<td><code>{html.escape(edge.transformation or '')}</code></td>"
            "</tr>"
            for edge in edges
        )
        parts.append(
            "<details><summary>Column lineage</summary>"
            "<table><tr><th>Column</th><th>From</th><th>Expression</th></tr>"
            f"{rows}</table></details>"
        )

    if step.parameters:
        # DOCUMENTATION is rendered as prose above; repeating it verbatim in
        # the parameter list is noise.
        shown = {k: v for k, v in step.parameters.items() if k != "DOCUMENTATION"}
        if shown:
            parts.append(
                "<details><summary>Parameters</summary><ul>"
                + "".join(
                    f"<li>{html.escape(name)} = <code>{html.escape(value)}</code></li>"
                    for name, value in shown.items()
                )
                + "</ul></details>"
            )
    parts.append("</li>")
    return "".join(parts)


def _origin_of(edge: ColumnEdge) -> str:
    if edge.source_object and edge.source_column:
        return f"{edge.source_object}.{edge.source_column}"
    if edge.source_column:
        return edge.source_column
    return "(literal / computed)"


def _render_pipeline_html(summary: PipelineSummary, data: PipelineDocData) -> str:
    waves_html = "\n".join(
        f"<li>Wave {i}: {', '.join(html.escape(t) for t in wave)}</li>"
        for i, wave in enumerate(data.waves, start=1)
    )
    steps_html = "\n".join(_render_step(step, data) for step in data.steps)
    pipeline_deps_html = (
        "\n".join(
            f"<li>{html.escape(dep.depends_on_pipeline_code)} "
            f"({html.escape(dep.dependency_type)})</li>"
            for dep in data.pipeline_dependencies
        )
        or "<li>(none)</li>"
    )
    cross_task_deps_html = (
        "\n".join(
            f"<li>{html.escape(dep.task_code)} -&gt; "
            f"{html.escape(dep.depends_on_pipeline_code)}.{html.escape(dep.depends_on_task_code)} "
            f"({html.escape(dep.dependency_type)})</li>"
            for dep in data.cross_task_dependencies
        )
        or "<li>(none)</li>"
    )
    body = f"""<p><a href="index.html">&larr; all pipelines</a></p>
<h1>{html.escape(summary.pipeline_code)}</h1>
<p>{html.escape(summary.pipeline_name)} -- {html.escape(summary.refresh_type)}</p>
<h2>Task waves</h2>
<ol>
{waves_html or "<li>(no active tasks)</li>"}
</ol>
<h2>Steps</h2>
<ul>
{steps_html or "<li>(no active tasks)</li>"}
</ul>
<h2>Pipeline dependencies</h2>
<ul>
{pipeline_deps_html}
</ul>
<h2>Cross-pipeline task dependencies</h2>
<ul>
{cross_task_deps_html}
</ul>
"""
    return _page(summary.pipeline_code, body)


_STYLE_CSS = """
:root { --fg: #1f2328; --muted: #57606a; --line: #d0d7de; --accent: #0969da; }
body { font-family: system-ui, sans-serif; max-width: 62rem; margin: 2rem auto;
       padding: 0 1rem; color: var(--fg); line-height: 1.5; }
h1, h2 { border-bottom: 1px solid var(--line); padding-bottom: 0.2rem; }
code { background: #f6f8fa; padding: 0.1rem 0.3rem; border-radius: 3px; font-size: 0.9em; }
table { border-collapse: collapse; margin: 0.5rem 0; }
th, td { border: 1px solid var(--line); padding: 0.25rem 0.6rem; text-align: left;
         font-size: 0.9rem; }
details { margin: 0.35rem 0; }
summary { cursor: pointer; color: var(--muted); font-size: 0.9rem; }
input#search-box { width: 100%; padding: 0.6rem; font-size: 1rem;
                   border: 1px solid var(--line); border-radius: 6px; }
ul#search-results { padding-left: 0; }
ul#search-results li { list-style: none; padding: 0.25rem 0;
                       border-bottom: 1px solid var(--line); }
ul#search-results:empty { display: none; }
.hint { color: var(--muted); font-size: 0.85rem; }
.doc { margin: 0.35rem 0; }
.doc-version { color: var(--muted); font-size: 0.8rem; margin-left: 0.5rem;
               border: 1px solid var(--line); border-radius: 10px; padding: 0 0.4rem; }
.match { color: var(--muted); font-size: 0.85rem; display: block; }
@media (prefers-color-scheme: dark) {
  :root { --fg: #e6edf3; --muted: #9198a1; --line: #30363d; --accent: #4493f8; }
  body { background: #0d1117; }
  code { background: #161b22; }
}
"""

# [DEVIATION, 2026-09-20] Real fuzzy ranking via Fuse.js, replacing the
# dependency-free substring filter this used to do, per explicit instruction
# ("fuzzy matching ... dont re-invent the wheel use metadata and any existing
# package that can do this").
#
# Fuse is *vendored* (src/etl_craft/vendor/fuse.min.js) and copied into the
# output, not loaded from a CDN. The original reasoning for avoiding a
# dependency here still holds — this site gets published to internal networks
# with no outbound access, where a CDN script tag leaves the search box
# silently dead. 15 KB buys a site that works everywhere.
_SEARCH_JS = """
(function () {
  var box = document.getElementById("search-box");
  var results = document.getElementById("search-results");
  if (!box || !results) return;
  var fuse = null;

  fetch("search-index.json").then(function (r) { return r.json(); }).then(function (data) {
    fuse = new Fuse(data, {
      includeScore: true,
      ignoreLocation: true,
      threshold: 0.4,
      keys: [
        { name: "task_code", weight: 3 },
        { name: "pipeline_code", weight: 3 },
        { name: "documentation", weight: 2 },
        { name: "text", weight: 1 }
      ]
    });
  });

  function label(entry) {
    return entry.type === "pipeline"
      ? entry.pipeline_code
      : entry.pipeline_code + "." + entry.task_code + " (" + entry.handler + ")";
  }

  box.addEventListener("input", function () {
    var q = box.value.trim();
    results.innerHTML = "";
    if (!q || !fuse) return;
    fuse.search(q, { limit: 30 }).forEach(function (hit) {
      var li = document.createElement("li");
      var a = document.createElement("a");
      a.href = hit.item.url;
      a.textContent = label(hit.item);
      li.appendChild(a);
      if (hit.item.documentation) {
        var note = document.createElement("span");
        note.className = "match";
        note.textContent = hit.item.documentation.slice(0, 120);
        li.appendChild(note);
      }
      results.appendChild(li);
    });
  });
})();
"""


def generate_docs(conn: Connection, output_dir: Path) -> None:
    """Generate the full static site under `output_dir` (created if missing)."""
    docs = collect_docs(conn)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "search-index.json").write_text(json.dumps(build_search_index(docs), indent=2))
    (output_dir / "style.css").write_text(_STYLE_CSS)
    (output_dir / "fuse.min.js").write_text(
        (files("etl_craft") / "vendor" / "fuse.min.js").read_text()
    )
    (output_dir / "search.js").write_text(_SEARCH_JS)
    (output_dir / "index.html").write_text(_render_index_html(docs))
    for summary, data in docs:
        (output_dir / f"{summary.pipeline_code}.html").write_text(
            _render_pipeline_html(summary, data)
        )
