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
from dataclasses import dataclass
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
from etl_craft.resolver import build_graph


@dataclass(frozen=True)
class PipelineDocData:
    """Everything one pipeline's generated page needs, beyond its own PipelineSummary."""

    waves: list[list[str]]
    steps: list[PipelineStep]
    pipeline_dependencies: list[PipelineDependencyEdge]
    cross_task_dependencies: list[CrossPipelineTaskEdge]


PipelineDoc = tuple[PipelineSummary, PipelineDocData]


def _collect_pipeline_doc_data(conn: Connection, pipeline_code: str) -> PipelineDocData:
    """Gather one pipeline's waves/steps/dependencies -- the same reads `graph`/`steps` use."""
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    graph_data = fetch_pipeline_graph(conn, pipeline_id)
    task_codes = fetch_task_codes(conn, pipeline_id)
    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    waves = [[task_codes[task_id] for task_id in wave] for wave in graph.waves()]
    return PipelineDocData(
        waves=waves,
        steps=fetch_pipeline_steps(conn, pipeline_id),
        pipeline_dependencies=fetch_pipeline_dependencies(conn, pipeline_id),
        cross_task_dependencies=fetch_cross_pipeline_task_edges(conn, pipeline_id),
    )


def collect_docs(conn: Connection) -> list[PipelineDoc]:
    """Gather every active pipeline's doc data -- the one DB-touching entry point here."""
    return [
        (summary, _collect_pipeline_doc_data(conn, summary.pipeline_code))
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
                    "text": (
                        f"{summary.pipeline_code} {step.task_code} {step.handler} {params_text}"
                    ),
                    "url": f"{summary.pipeline_code}.html#{step.task_code}",
                }
            )
    return index


def _page(title: str, body: str, *, with_search: bool = False) -> str:
    search_assets = '<script src="search.js" defer></script>' if with_search else ""
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


def _render_pipeline_html(summary: PipelineSummary, data: PipelineDocData) -> str:
    waves_html = "\n".join(
        f"<li>Wave {i}: {', '.join(html.escape(t) for t in wave)}</li>"
        for i, wave in enumerate(data.waves, start=1)
    )
    steps_html = "\n".join(
        f'<li id="{html.escape(step.task_code)}"><strong>{html.escape(step.task_code)}</strong> '
        f"({html.escape(step.handler)})"
        + (
            "<ul>"
            + "".join(
                f"<li>{html.escape(name)} = {html.escape(value)}</li>"
                for name, value in step.parameters.items()
            )
            + "</ul>"
            if step.parameters
            else ""
        )
        + "</li>"
        for step in data.steps
    )
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
body { font-family: system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem; }
input#search-box { width: 100%; padding: 0.5rem; font-size: 1rem; }
ul#search-results li { list-style: none; }
ul#search-results:empty { display: none; }
"""

_SEARCH_JS = """
(function () {
  var box = document.getElementById("search-box");
  var results = document.getElementById("search-results");
  if (!box || !results) return;
  var index = [];
  fetch("search-index.json").then(function (r) { return r.json(); }).then(function (data) {
    index = data;
  });
  box.addEventListener("input", function () {
    var q = box.value.trim().toLowerCase();
    results.innerHTML = "";
    if (!q) return;
    index
      .filter(function (entry) { return entry.text.toLowerCase().indexOf(q) !== -1; })
      .slice(0, 50)
      .forEach(function (entry) {
        var li = document.createElement("li");
        var a = document.createElement("a");
        a.href = entry.url;
        a.textContent = entry.type === "pipeline"
          ? entry.pipeline_code
          : entry.pipeline_code + "." + entry.task_code + " (" + entry.handler + ")";
        li.appendChild(a);
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
    (output_dir / "search.js").write_text(_SEARCH_JS)
    (output_dir / "index.html").write_text(_render_index_html(docs))
    for summary, data in docs:
        (output_dir / f"{summary.pipeline_code}.html").write_text(
            _render_pipeline_html(summary, data)
        )
