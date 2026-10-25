"""Bounded source projections reuse the retention catalog's exact eligibility predicates."""

from sqlalchemy import TextClause, text
from sqlglot import exp, parse_one

from etl_craft.core.archive import ArchivePart, Scalar
from etl_craft.core.text import quote_identifier
from etl_craft.dialects.engine.base import SHARED_QUERIES

BATCH_ROWS = 100


def key_filter(part: ArchivePart, alias: str = "") -> tuple[str, dict[str, Scalar]]:
    """Build a NULL-safe predicate for only the captured keys, including wait kinds."""
    conditions: list[str] = []
    params: dict[str, Scalar] = {}
    for number, row in enumerate(part.rows):
        fields = []
        for index, (key, value) in enumerate(zip(part.keys, part.key(row), strict=True)):
            name = f"k{number}_{index}"
            field = f"{alias}.{key}" if alias else key
            fields.append(f"{field} IS NULL" if value is None else f"{field}=:{name}")
            if value is not None:
                params[name] = value
        conditions.append("(" + " AND ".join(fields) + ")")
    return "(" + " OR ".join(conditions) + ")", params


def candidate_query(name: str, *, selected: ArchivePart | None = None) -> TextClause:
    """Project full rows from the same bounded candidate set as plain purge."""
    tree = parse_one((SHARED_QUERIES / f"{name}.sql").read_text())
    if isinstance(tree, exp.Select):
        candidate = tree
        candidate.set("expressions", [exp.column("ATTEMPT_ID", table="a")])
        predicate = exp.In(this=exp.column("ATTEMPT_ID"), query=exp.Subquery(this=candidate))
        table = "AUD_TASK_ATTEMPTS"
        alias = "a"
    else:
        predicate = tree.args["where"].this
        candidate = predicate.args["query"].this
        table = tree.this.sql()
        alias = "c" if name == "retention_expired_chunks" else "t"
    if selected is not None:
        condition, params = key_filter(selected, alias)
        candidate.where(condition, append=True, copy=False)
    else:
        params = {}
    for limit in candidate.find_all(exp.Limit):
        if limit.expression == exp.Literal.number(10000):
            limit.set("expression", exp.Literal.number(BATCH_ROWS))
    return text(f"SELECT * FROM {table} WHERE {predicate.sql()}").bindparams(**params)


def read_part(part: ArchivePart, suffix: str = "") -> TextClause:
    """Read only captured source rows for exact revalidation after the remote commit."""
    where, params = key_filter(part)
    return text(f"SELECT * FROM {part.table} WHERE {where}{suffix}").bindparams(**params)


def delete_part(part: ArchivePart) -> TextClause:
    """Delete only a verified, revalidated subset, never a fresh unbounded selection."""
    where, params = key_filter(part)
    return text(f"DELETE FROM {part.table} WHERE {where}").bindparams(**params)


def projected(query: TextClause, columns: list[str]) -> TextClause:
    """Alias reflected source columns consistently on both Engine DBs."""
    names = ", ".join(f"{quote_identifier(c)} AS {quote_identifier(c.lower())}" for c in columns)
    return text(query.text.replace("SELECT *", f"SELECT {names}", 1)).bindparams(
        **query.compile().params
    )


def log_removals(part: ArchivePart) -> TextClause:
    """Resolve canonical file identities for only the revalidated retired attempts."""
    tree = parse_one((SHARED_QUERIES / "retention_logs.sql").read_text())
    where, params = key_filter(part, "a")
    assert isinstance(tree, exp.Select)
    tree.where(where, append=True, copy=False)
    return text(tree.sql()).bindparams(**params)
