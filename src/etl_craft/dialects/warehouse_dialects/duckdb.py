"""DuckDB warehouse, native tables in a local file -- for local development.

DuckDB is embedded: its JDBC URL names a file, not a server, and it admits
exactly one *writing process* at a time (E2-61), so the engine queues
warehouse access behind an Engine DB lock rather than failing a parallel wave.
It also rejects adding an identity column to an existing table, so ROW_ID is a
sequence default here instead.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from etl_craft.db import ConnectionError_
from etl_craft.dialects.warehouse_dialects.base import (
    SAFE_IDENTIFIER,
    SurrogateKey,
    WarehouseDialect,
)

# `jdbc:duckdb:<path>`, or bare `jdbc:duckdb:` for an in-memory database.
_DUCKDB_URL_RE = re.compile(r"^jdbc:duckdb:(?P<path>.*)$")
_SAFE_CATALOG = SAFE_IDENTIFIER


class DuckDBWarehouse(WarehouseDialect):
    """DuckDB, native tables in one file."""

    key = "duckdb"
    display_name = "DuckDB"
    sqlalchemy_name = "duckdb"
    surrogate_key: SurrogateKey = "sequence"
    single_writer = True
    per_task_format = False
    # A file has nobody to authenticate.
    auth_fields: Mapping[str, tuple[str, ...]] = {"none": ()}
    verified_auth_modes = frozenset({"none"})

    def parse_jdbc(self, jdbc_url: str) -> tuple[str, dict[str, Any]]:
        """Parse `jdbc:duckdb:<path>` -- a file, not a server."""
        return _parse_duckdb(jdbc_url)


def _parse_duckdb(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse `jdbc:duckdb:<path>` — a file, not a server."""
    duckdb = _DUCKDB_URL_RE.match(jdbc_url)
    if duckdb:
        # [ADDITION, 2026-09-20] DuckDB is embedded: its JDBC URL is
        # `jdbc:duckdb:<path>` (or bare `jdbc:duckdb:` for in-memory) with no
        # host, port or query string — exactly the "vendor whose JDBC URL
        # shape isn't scheme://host[:port]/database at all" case this
        # translator's own comment flagged as needing its own parsing once
        # such a vendor was actually chosen. It has been.
        #
        # `database` is the catalog name DuckDB derives from the file stem
        # (`/data/warehouse.duckdb` -> `warehouse`), which is what
        # qualify()'s three-part `catalog.schema.table` form needs. An
        # in-memory database's catalog is `memory`.
        path = duckdb["path"] or ""
        if not path:
            # [DEVIATION, 2026-09-21, E2-63] The bare form is in-memory, and
            # now genuinely is. This used to fall through with path="" so the
            # creator below reached for `database` instead -- the literal
            # string "memory" -- and built `duckdb:///memory`, which DuckDB
            # reads as *a file named `memory` in the current working
            # directory*. Reproduced: two task subprocesses against
            # `jdbc:duckdb:` left a 274 KB file called `memory` in the repo
            # root and the second saw the first's data, which a real
            # in-memory database could not have shared. Each process also
            # starts wherever it happened to start, so cwd differences
            # between the orchestrator, a task subprocess and an Airflow
            # worker could produce several unrelated "warehouses".
            return "duckdb", {
                "host": None,
                "port": None,
                "path": ":memory:",
                "database": "memory",
                "query": {},
            }
        stem = Path(path).stem
        # [ADDITION, 2026-09-21, E2-62] The catalog name is the file stem, and
        # qualify() interpolates it unquoted into `catalog.schema.table`. A
        # hyphen is not an exotic filename, but `my-warehouse.public.t` is a
        # parser error that never mentions the file -- so check it here, where
        # it is derived, rather than letting every SQL action fail obscurely.
        # validate's own identifier check cannot catch this: it checks CFG_
        # values, and this one comes from craft-connector.yml.
        #
        # [CHOICE] Reject rather than quote. Quoting would make the catalog
        # case-sensitive and diverge from how the Postgres path builds the
        # same name.
        if not _SAFE_CATALOG.match(stem):
            raise ConnectionError_(
                f"DuckDB warehouse file {path!r} gives the catalog name {stem!r}, which is not "
                "a usable SQL identifier — it is interpolated unquoted into "
                "database.schema.table. Rename the file to use only letters, digits and "
                "underscores, starting with a letter or underscore."
            )
        return "duckdb", {
            "host": None,
            "port": None,
            "path": path,
            "database": stem,
            "query": {},
        }
    raise ConnectionError_(f"not a recognized DuckDB JDBC URL: {jdbc_url!r}")
