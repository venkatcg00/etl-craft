"""Semantic Engine DB catalog snapshots, excluding data and physical object identifiers."""

import json
import re

from sqlalchemy import inspect, text


def sql_tokens(sql):
    if sql is None:
        return None
    return tuple(
        token if token.startswith("'") else token.strip('"').lower()
        for token in re.findall(r"'(?:[^']|'')*'|\"[^\"]*\"|[A-Za-z_][A-Za-z_0-9]*|\S", sql)
    )


def snapshot(engine):
    with engine.connect() as conn:
        if engine.dialect.name == "postgresql":
            schema = conn.execute(text("SELECT current_schema() AS schema")).scalar_one()
            queries = {
                "columns": (
                    "SELECT c.relname, a.attnum, a.attname, "
                    "format_type(a.atttypid,a.atttypmod), a.attnotnull, a.attidentity, "
                    "a.attgenerated, pg_get_expr(d.adbin,d.adrelid), "
                    "col_description(c.oid,a.attnum) FROM pg_class c JOIN pg_namespace n ON "
                    "n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN "
                    "pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE "
                    "n.nspname=:schema AND c.relkind IN ('r','v') AND a.attnum>0 AND NOT "
                    "a.attisdropped ORDER BY c.relname,a.attnum"
                ),
                "constraints": (
                    "SELECT c.relname, k.conname, k.contype, pg_get_constraintdef(k.oid), "
                    "k.convalidated, k.condeferrable, k.condeferred FROM pg_constraint k "
                    "JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON "
                    "n.oid=c.relnamespace WHERE n.nspname=:schema ORDER BY "
                    "c.relname,k.conname"
                ),
                "indexes": (
                    "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE "
                    "schemaname=:schema ORDER BY tablename,indexname"
                ),
                "triggers": (
                    "SELECT c.relname,t.tgname,pg_get_triggerdef(t.oid),t.tgenabled FROM "
                    "pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON "
                    "n.oid=c.relnamespace WHERE n.nspname=:schema AND NOT t.tgisinternal "
                    "ORDER BY c.relname,t.tgname"
                ),
                "functions": (
                    "SELECT "
                    "p.proname, pg_get_function_identity_arguments(p.oid), "
                    "pg_get_functiondef(p.oid), obj_description(p.oid, 'pg_proc') "
                    "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE "
                    "n.nspname=:schema ORDER BY "
                    "p.proname,pg_get_function_identity_arguments(p.oid)"
                ),
                "sequences": (
                    "SELECT sequencename,data_type,start_value,min_value,max_value,increment_by,"
                    "cycle,cache_size FROM pg_sequences WHERE schemaname=:schema "
                    "ORDER BY sequencename"
                ),
                "views": (
                    "SELECT viewname,definition FROM pg_views WHERE schemaname=:schema "
                    "ORDER BY viewname"
                ),
                "comments": (
                    "SELECT c.relname,obj_description(c.oid,'pg_class') FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema "
                    "AND c.relkind IN ('r','v','S') ORDER BY c.relname"
                ),
            }
            return {
                name: [
                    tuple(
                        sql_tokens(value.replace(f"{schema}.", "engine."))
                        if isinstance(value, str)
                        and not (name in {"comments", "functions", "columns"} and i == len(row) - 1)
                        else value
                        for i, value in enumerate(row)
                    )
                    for row in conn.execute(text(query), {"schema": schema})
                ]
                for name, query in queries.items()
            }
        inspector = inspect(conn)
        tables = {}
        for table in inspector.get_table_names():
            if table.startswith("sqlite_"):
                continue
            tables[table] = {
                "columns": [
                    {
                        **column,
                        "type": str(column["type"]),
                        "default": sql_tokens(column.get("default")),
                    }
                    for column in inspector.get_columns(table)
                ],
                "primary_key": inspector.get_pk_constraint(table),
                "foreign_keys": sorted(
                    inspector.get_foreign_keys(table), key=lambda x: json.dumps(x, sort_keys=True)
                ),
                "checks": sorted(
                    [
                        {"name": c["name"], "sql": sql_tokens(c["sqltext"])}
                        for c in inspector.get_check_constraints(table)
                    ],
                    key=lambda x: json.dumps(x, sort_keys=True),
                ),
                "unique": inspector.get_unique_constraints(table),
                "autoincrement": "AUTOINCREMENT"
                in conn.execute(
                    text("SELECT sql FROM sqlite_schema WHERE type='table' AND name=:table"),
                    {"table": table},
                )
                .scalar_one()
                .upper(),
            }
        objects = [
            tuple(sql_tokens(value) if i == 3 else value for i, value in enumerate(row))
            for row in conn.execute(
                text(
                    "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE type IN "
                    "('index','trigger','view') AND sql IS NOT NULL ORDER BY type,name"
                )
            )
        ]
        return {"tables": tables, "objects": objects}
