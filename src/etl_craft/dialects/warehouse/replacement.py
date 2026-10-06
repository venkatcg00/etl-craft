"""Preserve warehouse-provided table properties during atomic CTAS or identity clone publication."""

from collections.abc import Callable
from typing import Any, ClassVar

from sqlglot import exp, parse_one
from sqlglot.dialects.databricks import Databricks
from sqlglot.errors import ErrorLevel, SqlglotError
from sqlglot.generators.databricks import DatabricksGenerator
from sqlglot.parsers.databricks import DatabricksParser

from etl_craft.core.errors import HandlerError


class _DatabricksReplacement(Databricks):
    """Read and render the warehouse's DEFAULT COLLATION property without dropping it."""

    class Parser(DatabricksParser):
        PROPERTY_PARSERS: ClassVar[dict[str, Callable[..., Any]]] = {
            **Databricks.Parser.PROPERTY_PARSERS,
            "COLLATION": Databricks.Parser.PROPERTY_PARSERS["COLLATE"],
        }

    class Generator(DatabricksGenerator):
        PROPERTIES_LOCATION: ClassVar[dict[type[exp.Expression], exp.PropertiesLocation]] = {
            **Databricks.Generator.PROPERTIES_LOCATION,
            exp.CollateProperty: exp.Properties.Location.POST_SCHEMA,
        }
        TRANSFORMS: ClassVar[dict[type[exp.Expr], Callable[..., str]]] = {
            **Databricks.Generator.TRANSFORMS,
            exp.CollateProperty: lambda self, e: f"DEFAULT COLLATION {self.sql(e, 'this')}",
        }


def replacement_ddl(ddl: str, target: str, select_sql: str, dialect: str) -> str:
    """Keep table properties; refuse column metadata that CTAS cannot carry forward."""
    try:
        format_ = _DatabricksReplacement() if dialect == "databricks" else dialect
        create = parse_one(ddl, read=format_)
        if not isinstance(create, exp.Create) or not isinstance(create.this, exp.Schema):
            raise HandlerError(f"{target}: cannot read a complete table definition for replacement")
        for column in create.this.expressions:
            if not isinstance(column, exp.ColumnDef) or column.args.get("constraints"):
                raise HandlerError(
                    f"{target}: atomic CTAS cannot preserve column metadata; "
                    "use a table-preserving write instead"
                )
        create.set("this", exp.to_table(target))
        create.set("replace", True)
        create.set("exists", False)
        create.set("expression", parse_one(select_sql, read=format_))
        if dialect == "snowflake":
            properties = create.args.get("properties") or exp.Properties(expressions=[])
            properties.append("expressions", exp.CopyGrantsProperty())
            create.set("properties", properties)
        return create.sql(dialect=format_, unsupported_level=ErrorLevel.RAISE)
    except SqlglotError as error:
        raise HandlerError(
            f"{target}: cannot preserve its table definition; use a table-preserving write"
        ) from error


def identity_replacement(
    ddl: str, target: str, candidate: str, columns: str, dialect: str
) -> tuple[str, str]:
    """Carry table properties into an identity candidate and publish it with an atomic clone.

    The candidate uses independent managed storage. Only publication writes the original
    external location; column constraints other than the managed identity remain a refusal.
    """
    try:
        format_ = _DatabricksReplacement() if dialect == "databricks" else dialect
        create = parse_one(ddl, read=format_)
        if not isinstance(create, exp.Create) or not isinstance(create.this, exp.Schema):
            raise HandlerError(f"{target}: cannot read a complete table definition for replacement")
        for column in create.this.expressions:
            if not isinstance(column, exp.ColumnDef):
                raise HandlerError(f"{target}: replacement cannot preserve column metadata")
            constraints = column.args.get("constraints") or []
            managed = column.name.lower() == "row_id" and all(
                isinstance(
                    c.kind, (exp.GeneratedAsIdentityColumnConstraint, exp.NotNullColumnConstraint)
                )
                for c in constraints
            )
            if constraints and not managed:
                raise HandlerError(
                    f"{target}: replacement cannot preserve column metadata; "
                    "use a table-preserving write instead"
                )
        shape = parse_one(f"CREATE TABLE {candidate} ({columns})", read=format_)
        create.set("this", shape.this)
        create.set("replace", False)
        create.set("exists", False)
        create.set("expression", None)
        location = ""
        properties = create.args.get("properties")
        if properties is not None:
            kept = []
            for property_ in properties.expressions:
                if isinstance(property_, exp.LocationProperty):
                    location = f" LOCATION {property_.this.sql(dialect=format_)}"
                elif not isinstance(property_, exp.CopyGrantsProperty):
                    kept.append(property_)
            properties.set("expressions", kept)
        candidate_ddl = create.sql(dialect=format_, unsupported_level=ErrorLevel.RAISE)
        clone = (
            f"DEEP CLONE {candidate}{location}"
            if dialect == "databricks"
            else f"CLONE {candidate} COPY GRANTS"
        )
        return candidate_ddl, f"CREATE OR REPLACE TABLE {target} {clone}"
    except SqlglotError as error:
        raise HandlerError(
            f"{target}: cannot preserve its table definition; use a table-preserving write"
        ) from error
