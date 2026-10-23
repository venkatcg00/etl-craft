"""Use Snowflake's authenticated session schema without another metadata query."""

from sqlalchemy.engine import URL, Connection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import Dialect


class SessionSchema(DefaultDialect):
    """Keep the driver's live version probe and its transaction cleanup unchanged."""

    def _get_default_schema_name(self, connection: Connection) -> str:
        schema = getattr(connection.connection.dbapi_connection, "schema", None)
        if isinstance(schema, str):
            driver: Dialect = self
            return driver.normalize_name(schema)
        return super()._get_default_schema_name(connection)


_vendor_dialect = URL.create("snowflake").get_dialect()
dialect = type(
    "SessionSchemaDialect",
    (SessionSchema, _vendor_dialect),
    {
        "supports_statement_cache": False,
        "__module__": __name__,
        "import_dbapi": classmethod(lambda cls: _vendor_dialect.import_dbapi()),
    },
)
