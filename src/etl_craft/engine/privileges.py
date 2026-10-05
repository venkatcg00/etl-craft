"""The grants that keep ordinary users from bypassing Engine DB write guards."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def extra_write_grants(engine: Engine) -> list[tuple[str, str, str]]:
    """Return explicit writes granted to PUBLIC or another login, including inherited roles."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT c.relname AS table_name, "
                "CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE login.rolname END AS role_name, "
                "a.privilege_type AS privilege FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a "
                "LEFT JOIN pg_roles login ON login.rolcanlogin AND "
                "CASE WHEN a.grantee=0 THEN false ELSE "
                "(login.oid=a.grantee OR pg_has_role(login.oid, a.grantee, 'MEMBER')) END "
                "WHERE n.nspname=current_schema() AND c.relkind='r' "
                "AND (c.relname LIKE 'aud\\_%' ESCAPE '\\' "
                "OR c.relname LIKE 'cfg\\_%' ESCAPE '\\') "
                "AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE') "
                "AND a.grantee<>c.relowner AND (a.grantee=0 OR "
                "(login.oid<>c.relowner AND login.rolname<>current_user)) "
                "ORDER BY table_name, role_name, privilege"
            )
        ).all()
    return [(row.table_name, row.role_name, row.privilege) for row in rows]


def grants_sql(schema: str) -> str:
    """Return reviewable grants for an owner, engine and reader; execute nothing."""
    quoted = '"' + schema.replace('"', '""') + '"'
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
