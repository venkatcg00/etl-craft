"""Prepare once, verify remote content, then revalidate and mutate the exact source snapshot."""

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.actor import current_actor
from etl_craft.core.archive import ArchiveBatch
from etl_craft.core.enums import ActionName, ActionOutcome, ArchiveStep
from etl_craft.core.errors import ArchiveError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import is_safe_identifier, qualify, sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine.archive import candidate_query, delete_part, log_removals
from etl_craft.engine import locks, resource_locks
from etl_craft.engine.archive import capture, capture_query, unchanged, with_context
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.engine.retention_tables import TABLES
from etl_craft.handlers.resources import LocalLocker
from etl_craft.services.cloning import mirror_schema_problem
from etl_craft.services.credentials import database_key
from etl_craft.services.retention_files import LogRemoval, cleanup_files, retire_log_rows
from etl_craft.warehouse.archive import ArchiveWriter
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect


@dataclass
class ArchiveSession:
    """One project purge serialized by its operation lock, with a frozen destination."""

    engine: Engine
    config: ConnectorConfig
    schema: str
    warehouse: Engine
    locker: LocalLocker
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def destination(self) -> str:
        """Fingerprint the resolved database address without rotating credentials."""
        assert self.config.warehouse is not None
        identity = [
            database_key(self.warehouse, self.schema),
            active_catalog(self.config),
            self.config.warehouse.extra,
            self.config.warehouse_table_format,
            asdict(self.config.cloning),
        ]
        return sha256_hex(json.dumps(identity, sort_keys=True).encode())

    def run(self, now: datetime) -> dict[str, int]:
        """Drain admitted work, retire logs and then purge configured history categories."""
        self.resume()
        days = self.config.retention.attempt_log_days
        if days is not None:
            while self.batch(
                "AUD_TASK_ATTEMPTS", "retention_logs", now - timedelta(days=days), ArchiveStep.LOGS
            ):
                pass
        while self.batch("AUD_ATTEMPT_LOG_CHUNKS", "retention_expired_chunks", None):
            pass
        self.counts["files"] = cleanup_files(self.engine, self.config)
        while True:
            progress = 0
            for table in TABLES:
                days = getattr(self.config.retention, table.period)
                if days is not None:
                    self.counts.setdefault(table.table, 0)
                    progress += self.batch(table.table, table.query, now - timedelta(days=days))
            if not progress:
                return self.counts

    def resume(self) -> int:
        """Resolve admitted work before selecting another batch or cleaning files."""
        with self.engine.connect() as conn:
            row = conn.execute(statement(conn, "archive_pending")).one_or_none()
        if row is None:
            return 0
        batch = ArchiveBatch.load(row.body)
        if batch.destination != self.destination or batch.schema != self.schema:
            raise ArchiveError(
                f"pending archive {batch.batch_id}: destination differs; expected "
                f"its original warehouse and schema {batch.schema!r}; restore them before retry"
            )
        return self.finish(batch)

    def batch(
        self,
        table: str,
        query: str,
        cutoff: datetime | None,
        step: ArchiveStep = ArchiveStep.DELETE,
    ) -> int:
        """Commit the source snapshot before any warehouse mutation can occur."""
        with self.engine.begin() as conn:
            root = capture(conn, table, query, cutoff)
            if not root.rows:
                return 0
            parts = with_context(conn, root)
            version = conn.execute(statement(conn, "archive_version")).scalar_one()
            if version is None:
                raise ArchiveError(
                    "SCHEMA_MIGRATIONS has no Engine version; expected an applied "
                    "schema ledger; run etl-craft migrate before archive"
                )
            batch = ArchiveBatch(
                uuid4().hex,
                conn.execute(statement(conn, "archive_origin")).scalar_one(),
                version,
                self.destination,
                self.schema,
                step,
                query,
                None if cutoff is None else cutoff.isoformat(),
                parts,
            )
            conn.execute(
                statement(conn, "archive_prepare"),
                {
                    "batch_id": batch.batch_id,
                    "destination": self.destination,
                    "schema": self.schema,
                    "body": batch.dump(),
                    "now": datetime.now(UTC),
                },
            )
        fault_point("archive.after_prepare")
        return self.finish(batch)

    def finish(self, batch: ArchiveBatch) -> int:
        """No current source row is deleted on the strength of an older archived value."""
        writer = ArchiveWriter(
            self.warehouse,
            warehouse_dialect(self.config),
            active_catalog(self.config),
            self.config.cloning,
        )
        with ExitStack() as held:
            for part in sorted(batch.parts, key=lambda p: p.table):
                name = qualify(f"{batch.schema}.{part.table}", writer.catalog)
                held.enter_context(
                    locks.target(name, writer.catalog).hold(
                        self.locker, resource_locks.LEASE_SECONDS + resource_locks.WAIT_SECONDS
                    )
                )
            writer.copy(batch)
            return self.retire(batch)

    def retire(self, batch: ArchiveBatch) -> int:
        """Commit the verified source decision while destination locks remain held."""
        cutoff = None if batch.cutoff is None else as_utc(batch.cutoff)
        now = datetime.now(UTC)
        with self.engine.begin() as conn:
            unchanged_parts = [unchanged(conn, part) for part in reversed(batch.parts)]
            root = batch.parts[0]
            valid = root
            if any(
                len(a.rows) != len(b.rows)
                for a, b in zip(unchanged_parts, reversed(batch.parts), strict=True)
            ):
                valid = replace(root, rows=())
            else:
                eligible = capture_query(
                    conn, root.table, candidate_query(batch.query, selected=root), cutoff
                )
                valid = replace(
                    root, rows=tuple(row for row in root.rows if row in set(eligible.rows))
                )
            removed = 0
            if valid.rows:
                if batch.step == ArchiveStep.LOGS:
                    rows = conn.execute(
                        log_removals(valid),
                        {"cutoff": cutoff, "project_id": current_project_id()},
                    ).all()
                    retire_log_rows(
                        conn, self.config, [LogRemoval(**row._mapping) for row in rows], now
                    )
                    removed = len(rows)
                else:
                    removed = conn.execute(delete_part(valid)).rowcount
            key = "attempt_logs" if batch.step == ArchiveStep.LOGS else root.table
            self.counts[key] = self.counts.get(key, 0) + removed
            self.counts["retained_changed"] = (
                self.counts.get("retained_changed", 0) + len(root.rows) - removed
            )
            conn.execute(statement(conn, "archive_finish"), {"batch_id": batch.batch_id})
            ActionRequest(
                ActionName.PURGE,
                {
                    "archive_batch": batch.batch_id,
                    "table": root.table,
                    "rows": removed,
                    "retained_changed": len(root.rows) - removed,
                    "schema": batch.schema,
                    "schema_version": batch.version,
                    "engine_id": batch.origin,
                },
                now,
                actor=current_actor(),
                engine=self.engine,
                outcome=ActionOutcome.SUCCESS,
                project_id=current_project_id(),
            ).write(conn)
            fault_point("archive.before_source_commit")
        fault_point("archive.after_source_commit")
        return removed


@contextmanager
def archive_session(
    engine: Engine,
    config: ConnectorConfig,
    schema: str | None,
    *,
    dry_run: bool,
) -> Iterator[ArchiveSession | None]:
    """Validate a dedicated destination and forbid bypassing admitted pending work."""
    with engine.connect() as conn:
        pending = conn.execute(statement(conn, "archive_pending")).one_or_none()
    if schema is None:
        if pending is not None:
            raise ArchiveError(
                f"pending archive for schema {pending.schema_name!r}; expected "
                "purge --archive-to with its original warehouse; resume it before plain purge"
            )
        yield None
        return
    if not isinstance(schema, str) or not is_safe_identifier(schema) or config.warehouse is None:
        raise ArchiveError(
            f"archive_to={schema!r}; expected one schema identifier and a Warehouse "
            "profile; configure the warehouse and choose a dedicated archive schema"
        )
    if schema.lower() == config.warehouse.schema.lower():
        raise ArchiveError(
            f"archive_to={schema!r} is the working/mirror schema; expected a dedicated "
            "archive schema; create and select a separate schema"
        )
    scoped = replace(config, warehouse=replace(config.warehouse, schema=schema))
    if problem := mirror_schema_problem(scoped):
        raise ArchiveError(f"archive_to={schema!r}: {problem}")
    if problem := warehouse_dialect(config).cloning_storage_problem(config.cloning):
        raise ArchiveError(f"archive_to={schema!r}: {problem}; configure archive table storage")
    if dry_run:
        yield None
        return
    locker = LocalLocker(engine, config)
    with open_warehouse(
        config, locker, wait_seconds=resource_locks.LEASE_SECONDS + resource_locks.WAIT_SECONDS
    ) as warehouse:
        yield ArchiveSession(engine, config, schema, warehouse, locker)
