"""Text parsing shared across the layers: JDBC URLs, secrets files, SQL text and identifiers.

Everything here works on strings alone; reading files and opening connections belong to the
callers.
"""

from __future__ import annotations

import difflib
import hashlib
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date
from urllib.parse import parse_qsl

from etl_craft.core.enums import RefreshType
from etl_craft.core.errors import ConfigurationError, HandlerError

# JDBC URLs

URL_SECRET_KEYS = frozenset(
    {"password", "pwd", "passwd", "token", "access_token", "secret", "private_key_file_pwd"}
)


def public_url_query(query: dict[str, str]) -> dict[str, str]:
    """Keep driver settings while omitting credential-like keys from logged URLs."""
    return {
        key: value
        for key, value in query.items()
        if key.lower() not in URL_SECRET_KEYS
        and not any(
            part in key.lower() for part in ("password", "token", "secret", "api_key", "credential")
        )
    }


_JDBC_SCHEME = re.compile(r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+):")
_JDBC_URL = re.compile(
    r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+)://(?P<host>[^:/?]+)(:(?P<port>[0-9]+))?"
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
    port = int(match["port"]) if match["port"] else default_port
    if port is not None and not 1 <= port <= 65535:
        raise ConfigurationError(f"JDBC port must be 1 to 65535, got {port}")
    return JdbcUrl(
        scheme=match["scheme"],
        host=match["host"],
        port=port,
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
    for line in contents.lstrip("\ufeff").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        key, _, value = stripped.partition("=")
        values[key.strip()] = unquote(value.strip())
    return values


def unquote(value: str) -> str:
    """Remove one matching pair of wrapping quotes, ``"`` or ``'``, and nothing else."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


# SQL statements


def _sql_parts(sql_text: str, *, placeholders: bool = False) -> Iterator[tuple[str, bool]]:
    """Yield code and protected quotes/comments using one scanner."""
    i = start = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        end = i
        if sql_text.startswith("--", i):
            end = sql_text.find("\n", i)
            end = length if end == -1 else end
        elif sql_text.startswith("/*", i):
            depth = 1
            end = i + 2
            while end < length and depth:
                if sql_text.startswith("/*", end):
                    depth += 1
                    end += 2
                elif sql_text.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
        elif ch in ("'", '"', "`"):
            end = _end_of_string_literal(sql_text, i, ch)
        elif ch == "$" and (tag := _dollar_tag_at(sql_text, i)) is not None:
            close = sql_text.find(tag, i + len(tag))
            match = _TOKEN.match(sql_text, i) if placeholders and tag == "$$" else None
            token = match is not None and (
                close == -1
                or (close != match.end() and _TOKEN.match(sql_text, close) is not None)
                or _delimiter_in_quote_or_comment(sql_text, match.end(), close)
                or sql_text.startswith("$$$$", close)
            )
            if not token:
                end = length if close == -1 else close + len(tag)
        if end > i:
            if start < i:
                yield sql_text[start:i], False
            yield sql_text[i:end], True
            i = start = end
        else:
            i += 1
    if start < length:
        yield sql_text[start:], False


def _delimiter_in_quote_or_comment(sql: str, start: int, delimiter: int) -> bool:
    """Whether a prospective dollar delimiter lies inside quoted text or a comment."""
    offset = start
    for part, protected in _sql_parts(sql[start:]):
        end = offset + len(part)
        if offset <= delimiter < end:
            return protected and part.startswith(("'", '"', "`", "--", "/*"))
        offset = end
    return False


def split_statements(sql_text: str) -> list[str]:
    """Split SQL on semicolons outside quoted strings, identifiers, bodies and comments."""
    statements: list[str] = []
    current = ""
    for part, protected in _sql_parts(sql_text):
        if protected:
            current += part
            continue
        pieces = part.split(";")
        current += pieces[0]
        for piece in pieces[1:]:
            statements.append(current)
            current = piece
    statements.append(current)
    return [stmt.strip() for stmt in statements if not is_only_comments(stmt)]


def as_subquery(select_sql: str) -> str:
    """Return ``select_sql`` in parentheses, ready to embed in a larger statement.

    A trailing ``;`` is dropped, and each parenthesis is on its own line, so a SELECT that ends
    in a ``--`` comment does not comment out the closing one.
    """
    statements = split_statements(select_sql)
    body = statements[0] if len(statements) == 1 else select_sql.strip()
    return f"(\n{body}\n)"


def _end_of_string_literal(sql_text: str, start: int, quote: str = "'") -> int:
    """Return the end of quoted text, honoring doubled quotes and E-string escapes."""
    end = start + 1
    length = len(sql_text)
    escaped = quote == "'" and start > 0 and sql_text[start - 1] in "Ee"
    while end < length:
        if escaped and sql_text[end] == "\\":
            end += 2
            continue
        if sql_text[end] == quote:
            if end + 1 < length and sql_text[end + 1] == quote:
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
    return all(
        (protected and part.startswith(("--", "/*"))) or not part.strip()
        for part, protected in _sql_parts(statement)
    )


# SQL task text

PIPELINE_ID_TOKEN = "$$pipeline_id"
PIPELINE_RUN_ID_TOKEN = "$$pipeline_run_id"
TASK_RUN_ID_TOKEN = "$$task_run_id"
PIPELINE_RUN_ID_FILTER_TOKEN = "$$pipeline_run_id_filter"
RUN_DATE_TOKEN = "$$run_date"
LINEAGE_RUN_DATE = date(1970, 1, 1)
"""The ``$$run_date`` lineage and validation read a task's SELECT with: fixed, so a task's
lineage does not look changed every day."""
_TOKEN = re.compile(r"\$\$([A-Za-z_][A-Za-z0-9_]*)")


def substitute_task_tokens(
    sql: str,
    *,
    pipeline_run_id: int,
    refresh_type: str,
    pipeline_run_id_substitution: bool,
    filter_enabled: bool,
    source: str = "SOURCE_SQL",
    force_all: bool = False,
    run_date: date = LINEAGE_RUN_DATE,
    run_date_substitution: bool = False,
    pipeline_id: int = 0,
    task_run_id: int = 0,
    pipeline_id_substitution: bool = False,
    task_run_id_substitution: bool = False,
) -> str:
    """Replace execution-identity and run-date tokens, each only when its switch is on.

    - ``$$pipeline_id``, ``$$pipeline_run_id`` and ``$$task_run_id`` become their named ids,
      enabled by ``PIPELINE_ID_SUBSTITUTION``, ``PIPELINE_RUN_ID_SUBSTITUTION`` and
      ``TASK_RUN_ID_SUBSTITUTION`` respectively.
    - ``$$pipeline_run_id_filter`` becomes ``pipeline_run_id = <id>``, or ``1=1`` for a ``FULL``
      refresh or when ``force_all`` asks for every row, when ``filter_enabled`` is on (the
      task's ``PIPELINE_RUN_ID_FILTER``).
    - ``$$run_date`` becomes the date the run runs as of, as ``DATE 'YYYY-MM-DD'``, when
      ``run_date_substitution`` is on (the task's ``RUN_DATE_SUBSTITUTION``).

    Raises ``HandlerError``, naming ``source``, for any other ``$$`` token, for a token whose
    switch is off, and for a switch that is on with its token absent: each is a mistake in the
    task's definition, and running the SQL anyway would read the wrong rows. The id is an
    integer the engine resolved, so it is written into the text directly.
    """
    parts = list(_sql_parts(sql, placeholders=True))
    found = {
        match.group(1)
        for part, protected in parts
        if not protected
        for match in _TOKEN.finditer(part)
    }
    known = {
        PIPELINE_ID_TOKEN[2:]: pipeline_id_substitution,
        PIPELINE_RUN_ID_TOKEN[2:]: pipeline_run_id_substitution,
        TASK_RUN_ID_TOKEN[2:]: task_run_id_substitution,
        PIPELINE_RUN_ID_FILTER_TOKEN[2:]: filter_enabled,
        RUN_DATE_TOKEN[2:]: run_date_substitution,
    }
    switch = {
        PIPELINE_ID_TOKEN[2:]: "PIPELINE_ID_SUBSTITUTION",
        PIPELINE_RUN_ID_TOKEN[2:]: "PIPELINE_RUN_ID_SUBSTITUTION",
        TASK_RUN_ID_TOKEN[2:]: "TASK_RUN_ID_SUBSTITUTION",
        PIPELINE_RUN_ID_FILTER_TOKEN[2:]: "PIPELINE_RUN_ID_FILTER",
        RUN_DATE_TOKEN[2:]: "RUN_DATE_SUBSTITUTION",
    }
    unknown = sorted(found - known.keys())
    if unknown:
        raise HandlerError(
            f"{source} uses unknown token(s) {', '.join('$$' + t for t in unknown)}; the known "
            f"tokens are {', '.join('$$' + name for name in known)}"
        )
    for name, enabled in known.items():
        if name in found and not enabled:
            raise HandlerError(
                f"{source} uses $${name}, but {switch[name]} is not true for this task; set "
                f"{switch[name]}=true to have it replaced"
            )
        if enabled and name not in found:
            raise HandlerError(
                f"{switch[name]} is true, but {source} has no $${name} to replace; remove the "
                "parameter or add the token"
            )
    run_id = int(pipeline_run_id)
    full = force_all or refresh_type == RefreshType.FULL
    replacements = {
        PIPELINE_ID_TOKEN[2:]: str(int(pipeline_id)),
        PIPELINE_RUN_ID_TOKEN[2:]: str(run_id),
        TASK_RUN_ID_TOKEN[2:]: str(int(task_run_id)),
        PIPELINE_RUN_ID_FILTER_TOKEN[2:]: "1=1" if full else f"pipeline_run_id = {run_id}",
        RUN_DATE_TOKEN[2:]: f"DATE '{run_date.isoformat()}'",
    }
    return "".join(
        part if protected else _TOKEN.sub(lambda match: replacements[match.group(1)], part)
        for part, protected in parts
    )


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

METADATA_CODE_PATTERN = r"[A-Za-z][A-Za-z0-9_]{0,127}"
"""Pipeline and task codes: one leading ASCII letter, at most 128 characters."""


def is_metadata_code(code: str) -> bool:
    """Whether a code is safe in commands and cannot collide with control steps."""
    return re.fullmatch(METADATA_CODE_PATTERN, code) is not None


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_OBJECT_REF = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$"
)
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
    """Whether ``object_ref`` is ``schema.table`` or ``database.schema.table``, safe identifiers."""
    return bool(_SAFE_OBJECT_REF.match(object_ref))


def is_safe_order_term(term: str) -> bool:
    """Whether ``term`` is one ``ORDER BY`` item: a column, then optional direction and nulls.

    For example ``updated_at DESC NULLS LAST``. Expressions are not accepted.
    """
    return bool(_SAFE_ORDER_TERM.match(term.strip()))


def split_object_ref(
    object_ref: str, *, param_name: str = "TARGET_OBJECT"
) -> tuple[str | None, str, str]:
    """Split ``schema.table`` or ``database.schema.table`` into (database or None, schema, table).

    Raises ``HandlerError`` for anything else: a bare table name has no schema.
    """
    parts = [part.strip() for part in object_ref.split(".")]
    if len(parts) not in (2, 3) or not all(parts):
        raise HandlerError(
            f"{param_name}={object_ref!r} must be 'schema.table' or 'database.schema.table'; a "
            "bare table name has no schema"
        )
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], parts[2]


def qualify(object_ref: str, catalog: str) -> str:
    """Return ``database.schema.table`` for a table reference.

    A reference that names its database is kept as written; ``schema.table`` gets ``catalog``,
    the active warehouse profile's database, so one row resolves to the development, test or
    production table depending on the environment.
    """
    database, schema_name, table_name = split_object_ref(object_ref)
    return f"{database or catalog}.{schema_name}.{table_name}"


# Checksums


def sha256_hex(payload: bytes) -> str:
    """Return the SHA-256 hex digest of ``payload``; migrations record it to detect edits."""
    return hashlib.sha256(payload).hexdigest()


# Suggestions


def suggest(unknown: str, candidates: Iterable[str], *, limit: int = 3) -> list[str]:
    """Return the ``candidates`` closest to ``unknown``, for a "did you mean" hint.

    Case-insensitive, since codes are routinely typed in the wrong case; candidates starting
    with ``unknown`` follow the close matches.
    """
    folded = {candidate.lower(): candidate for candidate in candidates}
    matches = difflib.get_close_matches(unknown.lower(), list(folded), n=limit, cutoff=0.5)
    ranked = [folded[match] for match in matches]
    for lowered, candidate in folded.items():
        if lowered.startswith(unknown.lower()) and candidate not in ranked:
            ranked.append(candidate)
    return ranked[:limit]
