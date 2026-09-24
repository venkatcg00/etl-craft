"""Everything that differs between databases, one file per database.

[ADDITION, 2026-09-24] Per explicit instruction ("standardise actions and
separate dialects. each dialect maintain its own file and the substitution
happens based on the engine and warehouse in craft connector"). The engine's
behaviour is written once -- the closed SQL action vocabulary in
sql_actions.py, run-id resolution, migrations -- and each dialect supplies
only the primitives that genuinely differ: a DDL clause, how a hash is
computed, whether temporary tables exist, how ROW_ID is generated.

* ``engine_dialects/`` -- the Engine DB: one directory per database, each
  owning its Python module, its full ``schema.sql`` and its migration stream.
* ``warehouse_dialects/`` -- the warehouse: one module per database *and*
  table format (``databricks`` / ``databricks_iceberg`` ...), chosen from the
  warehouse's connection and the task's resolved TABLE_FORMAT.

[CHOICE] Dialects supply primitives, not whole actions. Seven actions times
eight warehouses as separate copies would put the same bug in eight places --
the pattern every earlier review round found in practice (a fix applied to one
dialect's path and not another's). One action implementation calling a small
dialect interface keeps each difference in exactly one file.
"""
