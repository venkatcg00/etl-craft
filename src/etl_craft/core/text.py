"""Text parsing shared across the layers: JDBC URLs, secrets files, SQL text and identifiers.

Everything here works on strings alone; reading files and opening connections belong to the
callers.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl

from etl_craft.core.enums import RefreshType
from etl_craft.core.errors import ConfigurationError, HandlerError

# JDBC URLs

_JDBC_SCHEME = re.compile(r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+):")
_JDBC_URL = re.compile(
    r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+)://(?P<host>[^:/?]+)(:(?P<port>\d+))?"
    r"/(?P<database>[^?]*)(\?(?P<query>.*))?$"
)


@dataclass(frozen=True)
class JdbcUrl:
    """The parts of a ``jdbc:<scheme>://host[:port]/database[?query]`` URL.

    ``database`` is the whole path, which some warehouses write as ``catalog/schema``.
    """

    scheme: str
    host: str
    port: int | None
    database: str
    query: dict[str, str] = field(default_factory=dict)

    @property
    def catalog(self) -> str:
        """The first segment of the path: the catalog in a ``catalog/schema`` path."""
        return self.database.split("/", 1)[0]


def jdbc_scheme(jdbc_url: str) -> str:
    """Return the lower-cased vendor of a ``jdbc:<vendor>:...`` URL.

    Raises ``ConfigurationError`` for anything that does not start that way.
    """
    match = _JDBC_SCHEME.match(jdbc_url)
    if not match:
        raise ConfigurationError(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected jdbc:<vendor>:..."
        )
    return match["scheme"].lower()


def parse_jdbc_url(jdbc_url: str, *, default_port: int | None = None) -> JdbcUrl:
    """Split a ``jdbc:<scheme>://host[:port]/database[?query]`` URL into its parts.

    The query string is kept, so settings such as ``sslmode=require`` reach the driver.
    ``default_port`` fills in a missing port. Raises ``ConfigurationError`` for another shape;
    vendors with their own URL form parse it in their dialect.
    """
    match = _JDBC_URL.match(jdbc_url)
    if not match:
        raise ConfigurationError(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected "
            "jdbc:<dialect>://host[:port]/database or jdbc:duckdb:<path>"
        )
    return JdbcUrl(
        scheme=match["scheme"],
        host=match["host"],
        port=int(match["port"]) if match["port"] else default_port,
        database=match["database"],
        query=dict(parse_qsl(match["query"])) if match["query"] else {},
    )


# Secrets files and environment variable names

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_env_name(name: str) -> bool:
    """Whether ``name`` can be an environment variable name."""
    return bool(_ENV_NAME.match(name))


def parse_env_file(contents: str) -> dict[str, str]:
    """Parse ``.env`` text: one ``KEY=VALUE`` per line.

    Blank lines, lines starting with ``#`` and lines without ``=`` are skipped. Keys and values
    are stripped, and one matching pair of quotes around a value is removed. There are no escape
    sequences, no multi-line values and no trailing comments.
    """
    values: dict[str, str] = {}
    for line in contents.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = unquote(value.strip())
    return values


def unquote(value: str) -> str:
    """Remove one matching pair of wrapping quotes, ``"`` or ``'``, and nothing else."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


# SQL statements

_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")


def split_statements(sql_text: str) -> list[str]:
    """Split SQL text into statements on ``;``, ignoring those inside quotes and comments.

    A small scanner, not a parser: it knows single-quoted strings with their ``''`` escape,
    dollar-quoted bodies (``$$ ... $$`` and tagged ``$fn$ ... $fn$``), ``--`` line comments and
    ``/* */`` block comments, and nothing else. Statements are stripped; empty ones and ones
    holding only comments are dropped.
    """
    statements: list[str] = []
    current: list[str] = []
    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        if sql_text.startswith("--", i):
            end = sql_text.find("\n", i)
            end = length if end == -1 else end
        elif sql_text.startswith("/*", i):
            end = sql_text.find("*/", i + 2)
            end = length if end == -1 else end + 2
        elif ch == "'":
            end = _end_of_string_literal(sql_text, i)
        elif ch == "$" and (tag := _dollar_tag_at(sql_text, i)) is not None:
            close = sql_text.find(tag, i + len(tag))
            end = length if close == -1 else close + len(tag)
        elif ch == ";":
            statements.append("".join(current))
            current = []
            i += 1
            continue
        else:
            end = i + 1
        current.append(sql_text[i:end])
        i = end
    statements.append("".join(current))
    return [stmt.strip() for stmt in statements if not is_only_comments(stmt)]


def _end_of_string_literal(sql_text: str, start: int) -> int:
    """Return the index just past the single-quoted literal opening at ``start``."""
    end = start + 1
    length = len(sql_text)
    while end < length:
        if sql_text[end] == "'":
            if end + 1 < length and sql_text[end + 1] == "'":
                end += 2
                continue
            return end + 1
        end += 1
    return length


def _dollar_tag_at(sql_text: str, index: int) -> str | None:
    """Return the dollar-quote tag starting at ``index`` (``$$`` or ``$fn$``), or ``None``.

    ``$1`` is a placeholder and ``$a-b$`` is not a valid tag, so neither opens a quote.
    """
    end = sql_text.find("$", index + 1)
    if end == -1:
        return None
    body = sql_text[index + 1 : end]
    if body and not (body[0].isalpha() or body[0] == "_"):
        return None
    if not all(c.isalnum() or c == "_" for c in body):
        return None
    return sql_text[index : end + 1]


def is_only_comments(statement: str) -> bool:
    """Whether ``statement`` holds nothing but comments and whitespace."""
    stripped = _LINE_COMMENT.sub("", _BLOCK_COMMENT.sub("", statement))
    return not stripped.strip()


# SQL task text

PIPELINE_ID_TOKEN = "$$pipeline_id"


def substitute_pipeline_id(
    sql: str, *, refresh_type: str, pipeline_run_id: int, force_all: bool = False
) -> str:
    """Replace every ``$$pipeline_id`` in ``sql`` with the run's scope condition.

    The token becomes ``pipeline_run_id = <id>`` for an ``INCREMENTAL`` pipeline, and ``1=1``
    for a ``FULL`` one or when ``force_all`` asks for every row. The author writes the
    surrounding ``WHERE``; SQL without the token is returned unchanged. ``pipeline_run_id`` is
    an integer the engine resolved, so it is written into the text directly.
    """
    full = force_all or refresh_type == RefreshType.FULL
    replacement = "1=1" if full else f"pipeline_run_id = {int(pipeline_run_id)}"
    return sql.replace(PIPELINE_ID_TOKEN, replacement)


WRITE_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "TRUNCATE",
    "DROP",
    "ALTER",
    "CREATE",
    "GRANT",
    "REVOKE",
    "COPY",
)
"""Statements a read-only SELECT has no business containing."""

_COMMENT = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
_LITERAL = re.compile(r"'(?:[^']|'')*'")
_FIRST_WORD = re.compile(r"[(\s]*(\w+)")
_READ_STARTS = frozenset({"SELECT", "WITH", "TABLE", "VALUES"})


def strip_comments_and_literals(sql: str) -> str:
    """Replace comments with a space and string literals with ``''``."""
    return _LITERAL.sub("''", _COMMENT.sub(" ", sql))


def read_only_problem(sql: str) -> str | None:
    """Return why ``sql`` does not look like a read-only SELECT, or ``None`` when it does.

    The SQL must start with ``SELECT``, ``WITH``, ``TABLE`` or ``VALUES`` and contain none of
    ``WRITE_KEYWORDS`` as a whole word outside comments and string literals, which catches a
    data-modifying CTE such as ``WITH x AS (DELETE ... RETURNING *) SELECT ...``. This is a lint
    for honest mistakes, not a security boundary: ``CFG_`` rows are reviewed, and a determined
    author can defeat any string check.
    """
    stripped = strip_comments_and_literals(sql).strip()
    if not stripped:
        return "is empty"
    first = _FIRST_WORD.match(stripped)
    if first is None or first.group(1).upper() not in _READ_STARTS:
        got = first.group(1) if first else stripped[:20]
        return f"starts with {got!r}, not SELECT/WITH"
    found = [kw for kw in WRITE_KEYWORDS if re.search(rf"\b{kw}\b", stripped, re.IGNORECASE)]
    if found:
        return f"contains {found} — a read-only SELECT should not"
    return None


# Identifiers

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_OBJECT_REF = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_ORDER_TERM = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\s+(ASC|DESC))?(\s+NULLS\s+(FIRST|LAST))?$",
    re.IGNORECASE,
)


def is_safe_identifier(name: str) -> bool:
    """Whether ``name`` can be written unquoted into SQL and into a shell command.

    Pipeline codes, task codes, column names and catalog names are interpolated as they are,
    so only letters, digits and underscores are allowed, not starting with a digit.
    """
    return bool(_SAFE_IDENTIFIER.match(name))


def is_safe_object_ref(object_ref: str) -> bool:
    """Whether ``object_ref`` is exactly ``schema.table`` with two safe identifiers."""
    return bool(_SAFE_OBJECT_REF.match(object_ref))


def is_safe_order_term(term: str) -> bool:
    """Whether ``term`` is one ``ORDER BY`` item: a column, then optional direction and nulls.

    For example ``updated_at DESC NULLS LAST``. Expressions are not accepted.
    """
    return bool(_SAFE_ORDER_TERM.match(term.strip()))


def split_object_ref(object_ref: str, *, param_name: str = "TARGET_OBJECT") -> tuple[str, str]:
    """Split a ``schema.table`` reference into its two names.

    Raises ``HandlerError`` for anything else: the catalog comes from the active warehouse
    profile, so a ``CFG_`` row never names one, and a bare table name has no schema.
    """
    parts = [part.strip() for part in object_ref.split(".")]
    if len(parts) != 2 or not all(parts):
        raise HandlerError(
            f"CFG_TASK_PARAMETERS.{param_name}={object_ref!r} must be exactly "
            "'schema.table' — no database/catalog prefix (that comes from the active "
            "[Warehouse] profile at runtime) and no bare table name"
        )
    return parts[0], parts[1]


def qualify(object_ref: str, catalog: str) -> str:
    """Return ``catalog.schema.table`` for a ``schema.table`` reference.

    ``CFG_`` rows name only ``schema.table``, so the same row resolves to the development,
    test or production object depending on the active warehouse profile's catalog.
    """
    schema_name, table_name = split_object_ref(object_ref)
    return f"{catalog}.{schema_name}.{table_name}"


def split_pipe_list(value: str | None, *, param_name: str) -> list[str]:
    """Split a ``|``-separated task parameter into its stripped, non-empty items.

    Raises ``HandlerError`` when the parameter is missing or empty.
    """
    if not value:
        raise HandlerError(f"CFG_TASK_PARAMETERS.{param_name} is required for this SQL_ACTION")
    return [part.strip() for part in value.split("|") if part.strip()]


# Checksums


def sha256_hex(payload: bytes) -> str:
    """Return the SHA-256 hex digest of ``payload``; migrations record it to detect edits."""
    return hashlib.sha256(payload).hexdigest()


def fingerprint(*parts: str) -> str:
    """Return an MD5 hex digest of ``parts`` for change detection, not security.

    The parts are joined with NUL, which no identifier or SQL text contains, so different inputs
    cannot collide by concatenating to the same string.
    """
    material = "\0".join(parts)
    return hashlib.md5(material.encode(), usedforsecurity=False).hexdigest()
