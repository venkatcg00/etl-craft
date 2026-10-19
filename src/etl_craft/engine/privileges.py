"""The grants that keep ordinary users from bypassing Engine DB write guards."""

from __future__ import annotations

from etl_craft.core.text import quote_identifier


def grants_sql(schema: str) -> str:
    """Return reviewable grants for an owner, engine and reader; execute nothing."""
    quoted = quote_identifier(schema)
    return (
        "-- Replace these role names with your deployment's roles.\n"
        "CREATE ROLE etl_craft_owner NOLOGIN;\n"
        "CREATE ROLE etl_craft_engine LOGIN;\n"
        "CREATE ROLE etl_craft_reader NOLOGIN;\n"
        f"ALTER SCHEMA {quoted} OWNER TO etl_craft_owner;\n"
        f"REVOKE ALL ON ALL TABLES IN SCHEMA {quoted} FROM PUBLIC;\n"
        f"GRANT USAGE ON SCHEMA {quoted} TO etl_craft_engine, etl_craft_reader;\n"
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {quoted} "
        "TO etl_craft_engine;\n"
        f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {quoted} TO etl_craft_engine;\n"
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {quoted} TO etl_craft_reader;\n"
        f"ALTER DEFAULT PRIVILEGES FOR ROLE etl_craft_owner IN SCHEMA {quoted} "
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_craft_engine;\n"
        f"ALTER DEFAULT PRIVILEGES FOR ROLE etl_craft_owner IN SCHEMA {quoted} "
        "GRANT USAGE, SELECT ON SEQUENCES TO etl_craft_engine;\n"
        f"ALTER DEFAULT PRIVILEGES FOR ROLE etl_craft_owner IN SCHEMA {quoted} "
        "GRANT SELECT ON TABLES TO etl_craft_reader;\n"
        "-- Run DDL and migrations as the owner; transfer existing tables and sequences "
        "to that role. People use reader membership. Revoke other write grants reported by doctor."
    )
