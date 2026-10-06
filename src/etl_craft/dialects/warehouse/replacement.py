"""Preserve a warehouse-provided table definition's properties during atomic CTAS."""

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
