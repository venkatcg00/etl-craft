"""The reference pages generated from the code: configuration, task parameters, Engine DB schema.

``docs/gen_reference_pages.py`` writes them into the documentation build, so each page lists
exactly what the release reads: the settings the loader accepts, the task parameters each
handler reads, and the tables and columns of the Engine DB schema. The descriptions stay in the
annotated ``craft-connector.example.yml``, the guides and the schema's own comments; the tests
check that each of them covers every name listed here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from etl_craft.config.loader import settings_by_section
from etl_craft.dialects.warehouse.base import STORAGE_PARAMETERS
from etl_craft.handlers import email_alert, python_scripts
from etl_craft.handlers.registry import COMMON_PARAMETERS
from etl_craft.handlers.sql.spec import FLAGS
from etl_craft.handlers.sql.spec import PARAMETERS as SQL_PARAMETERS

REPO = Path(__file__).resolve().parents[1]
ANNOTATED = REPO / "docs" / "craft-connector.example.yml"
EXAMPLES = REPO / "docs" / "examples"
SCHEMA = REPO / "src" / "etl_craft" / "dialects" / "engine" / "postgres" / "schema.sql"
HANDLER_GUIDES = {
    "Every task": "../guides/pipelines-and-tasks.md#cfg_task_parameters",
    "SQL": "../guides/sql-tasks.md",
    "PYTHON": "../guides/ingestion-scripts.md",
    "EMAIL_ALERT": "../guides/email-alerts.md",
}
OUTCOME_PATTERNS = {
    f"EMAIL_{part}_{outcome}": f"EMAIL_{part}_<OUTCOME>"
    for part in ("SUBJECT", "BODY")
    for outcome in email_alert.OUTCOMES
}
"""Per-outcome alert parameters, and the pattern their guide documents them under."""


def written_in(key: str, text: str) -> bool:
    """Whether ``text`` (a YAML file) sets ``key``, or shows it commented out."""
    return re.search(rf"(^|\s|#\s*){re.escape(key)}:", text, re.MULTILINE) is not None


def setting_sources(key: str) -> list[str]:
    """Return the annotated reference, or else the examples, that show ``key``."""
    if written_in(key, ANNOTATED.read_text(encoding="utf-8")):
        return [ANNOTATED.name]
    return sorted(
        path.name for path in EXAMPLES.glob("*.yml") if written_in(key, path.read_text("utf-8"))
    )


def configuration_page() -> str:
    """Return the configuration reference: every setting, by section, and where it is shown."""
    lines = [
        "# craft-connector.yml",
        "",
        "Every setting the loader accepts, by section, generated from the loader itself. A key "
        "it does not accept stops every command with the key named. Each setting is explained "
        "where it is shown: the [annotated reference](../craft-connector.example.yml), or the "
        "[example](../examples/README.md) that uses it.",
        "",
        "`Orchestration` settings, and each section's `Profile`, may be written at the section "
        "level or in a profile block (`dev:`, `prod:`, ...); `Engine` and `Warehouse` "
        "connection fields are written in a profile block.",
        "",
    ]
    for section, keys in settings_by_section().items():
        lines += [f"## `{section}`", "", "| Setting | Shown in |", "|---|---|"]
        for key in sorted(keys, key=str.lower):
            shown = ", ".join(
                f"[`{name}`](../{'examples/' if name != ANNOTATED.name else ''}{name})"
                for name in setting_sources(key)
            )
            lines.append(f"| `{key}` | {shown} |")
        lines.append("")
    return "\n".join(lines)


def handler_parameters() -> dict[str, list[tuple[str, str]]]:
    """Return each handler's parameters, with what each applies to."""
    sql = []
    for name in sorted(SQL_PARAMETERS):
        if name in FLAGS:
            applies = ", ".join(f"`{action}`" for action in sorted(FLAGS[name]))
        elif name in STORAGE_PARAMETERS:
            applies = "where the table's files live"
        else:
            applies = ""
        sql.append((name, applies))
    alert = []
    for name in sorted(email_alert.PARAMETERS):
        pattern = OUTCOME_PATTERNS.get(name)
        alert.append((name, f"one outcome: `{pattern}`" if pattern else ""))
    return {
        "Every task": [(name, "") for name in sorted(COMMON_PARAMETERS)],
        "SQL": sql,
        "PYTHON": [(name, "") for name in sorted(python_scripts.PARAMETERS)],
        "EMAIL_ALERT": alert,
    }


def task_parameters_page() -> str:
    """Return the task parameter reference: every name each handler reads."""
    lines = [
        "# Task parameters",
        "",
        "Every `CFG_TASK_PARAMETERS.PARAMETER_NAME` etl-craft reads, by handler, generated from "
        "the handlers themselves. Each is explained in its handler's guide. A `BUSINESS_RULES` "
        "task reads none of its own: its rules are rows of `CFG_BUSINESS_RULES`. A name no "
        "handler reads is a [`validate`](../guides/validating.md) warning.",
        "",
    ]
    for handler, params in handler_parameters().items():
        title = handler if handler == "Every task" else f"`{handler}`"
        lines += [
            f"## {title}",
            "",
            f"Explained in [its guide]({HANDLER_GUIDES[handler]}).",
            "",
            "| Parameter | Applies to |",
            "|---|---|",
        ]
        lines += [f"| `{name}` | {applies} |" for name, applies in params]
        lines.append("")
    return "\n".join(lines)


@dataclass
class Column:
    """One column of an Engine DB table."""

    name: str
    type: str
    nullable: bool = True
    default: str | None = None
    allowed: list[str] = field(default_factory=list)


@dataclass
class Table:
    """One Engine DB table, the comment above it, and its columns."""

    name: str
    comment: str
    columns: list[Column] = field(default_factory=list)
    unique: list[str] = field(default_factory=list)


_CREATE = re.compile(r"CREATE TABLE (\w+) \((.*?)\n\);", re.DOTALL)
_CHECK_IN = re.compile(r"CHECK \((\w+) IN \(([^)]*)\)\)")
_UNIQUE_INDEX = re.compile(r"CREATE UNIQUE INDEX \w+\s+ON (\w+) \(([^)]*)\)(?:\s+WHERE ([^;]+))?;")


def schema_tables(schema: Path = SCHEMA) -> list[Table]:
    """Read the tables of ``schema``: comments, columns, allowed values and unique keys."""
    text = schema.read_text(encoding="utf-8")
    tables: list[Table] = []
    for match in _CREATE.finditer(text):
        before = text[: match.start()].rstrip("\n").split("\n")
        comment: list[str] = []
        while before and before[-1].startswith("--"):
            comment.insert(0, before.pop()[2:].strip())
        table = Table(match.group(1), " ".join(comment))
        for raw in match.group(2).split("\n"):
            line = raw.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            if not re.match(r"^[A-Z_]+\s", line):
                continue
            name, rest = line.split(None, 1)
            if name in {"CONSTRAINT", "CHECK", "OR", "AND", "PRIMARY", "UNIQUE"}:
                continue
            type_ = re.split(r"\s+(NOT NULL|DEFAULT|REFERENCES|GENERATED|PRIMARY)", rest)[0]
            default = re.search(r"DEFAULT (.+?)(?:\s+REFERENCES|$)", rest)
            table.columns.append(
                Column(
                    name,
                    type_.strip(),
                    nullable="NOT NULL" not in rest and "PRIMARY KEY" not in rest,
                    default=default.group(1).strip() if default else None,
                )
            )
        for column, values in _CHECK_IN.findall(match.group(2)):
            for col in table.columns:
                if col.name == column:
                    col.allowed = [v.strip().strip("'") for v in values.split(",")]
        tables.append(table)
    by_name = {table.name: table for table in tables}
    for name, columns, where in _UNIQUE_INDEX.findall(text):
        if name in by_name:
            key = ", ".join(c.strip() for c in columns.split(","))
            by_name[name].unique.append(key + (f" where {where.strip()}" if where else ""))
    return tables


def schema_page() -> str:
    """Return the Engine DB schema reference: every table and column, from the schema itself."""
    lines = [
        "# Engine DB schema",
        "",
        "Every table and column of the Engine DB, generated from the PostgreSQL schema etl-craft "
        "creates; the SQLite schema has the same tables and columns, in the same order, with "
        "SQLite's types. Your team writes the `CFG_` tables; etl-craft writes only the `AUD_` "
        "tables and `SCHEMA_MIGRATIONS`. See [Pipelines and tasks]"
        "(../guides/pipelines-and-tasks.md) and [The Engine DB](../connectors/engine-db.md).",
        "",
    ]
    for table in schema_tables():
        lines += [f"## `{table.name}`", ""]
        if table.comment:
            lines += [table.comment, ""]
        lines += ["| Column | Type | Null | Default | Allowed values |", "|---|---|---|---|---|"]
        for col in table.columns:
            lines.append(
                f"| `{col.name}` | `{col.type}` | {'yes' if col.nullable else 'no'} | "
                f"{f'`{col.default}`' if col.default else ''} | "
                f"{', '.join(f'`{v}`' for v in col.allowed)} |"
            )
        if table.unique:
            lines += ["", "Unique: " + "; ".join(f"`{key}`" for key in table.unique) + "."]
        lines.append("")
    return "\n".join(lines)
