"""Bounded project history deletion; every incoming reference remains a hard barrier."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import current_actor, purpose
from etl_craft.core.enums import ActionName, ActionOutcome
from etl_craft.core.faults import fault_point
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.engine.retention_tables import TABLES
from etl_craft.services.archives import ArchiveSession
from etl_craft.services.retention_bundles import cleanup_bundles
from etl_craft.services.retention_files import cleanup_files, expire_logs


@contextmanager
def retaining() -> Iterator[None]:
    """Scope the existing audit-guard exception to transactions in this purge only."""
    token = purpose.set("retention")
    try:
        yield
    finally:
        purpose.reset(token)


def purge_history(
    engine: Engine,
    config: ConnectorConfig,
    now: datetime,
    *,
    dry_run: bool,
    archive: ArchiveSession | None = None,
) -> dict[str, int]:
    """Use the same category policy with verified snapshots when archiving is requested."""
    with retaining():
        counts = (
            plain_history(engine, config, now, dry_run=dry_run)
            if archive is None
            else archive.run(now)
        )
        counts["AUD_PROJECT_BUNDLES"] = cleanup_bundles(engine, config, now, dry_run=dry_run)
        return counts


def plain_history(
    engine: Engine, config: ConnectorConfig, now: datetime, *, dry_run: bool
) -> dict[str, int]:
    """Count currently removable rows, or drain bounded batches until no progress is possible."""
    counts: dict[str, int] = {}
    with retaining():
        counts.update(retire_logs(engine, config, now, dry_run=dry_run))
        while True:
            progress = 0
            for table in TABLES:
                days = getattr(config.retention, table.period)
                if days is None:
                    continue
                params = {"cutoff": now - timedelta(days=days)}
                with engine.begin() as conn:
                    if dry_run:
                        counts[table.table] = conn.execute(
                            statement(conn, table.query + "_count"),
                            params,
                        ).scalar_one()
                        continue
                    removed = conn.execute(statement(conn, table.query), params).rowcount
                    if removed:
                        ActionRequest(
                            ActionName.PURGE,
                            {"table": table.table, "rows": removed},
                            now,
                            actor=current_actor(),
                            engine=engine,
                            outcome=ActionOutcome.SUCCESS,
                            project_id=current_project_id(),
                        ).write(conn)
                counts[table.table] = counts.get(table.table, 0) + removed
                progress += removed
                if removed:
                    fault_point("retention.after_batch_commit")
            if dry_run or not progress:
                return counts


def retire_logs(
    engine: Engine, config: ConnectorConfig, now: datetime, *, dry_run: bool
) -> dict[str, int]:
    """Retire expired log content and finish previously committed file cleanup."""
    counts: dict[str, int] = {}
    days = config.retention.attempt_log_days
    if days is not None:
        cutoff = now - timedelta(days=days)
        if dry_run:
            with engine.connect() as conn:
                counts["attempt_logs"] = conn.execute(
                    statement(conn, "retention_logs_count"), {"cutoff": cutoff}
                ).scalar_one()
        else:
            counts["attempt_logs"] = expire_logs(engine, config, cutoff, now)
    if not dry_run:
        while True:
            with engine.begin() as conn:
                removed = conn.execute(statement(conn, "retention_expired_chunks")).rowcount
            counts["AUD_ATTEMPT_LOG_CHUNKS"] = counts.get("AUD_ATTEMPT_LOG_CHUNKS", 0) + removed
            if not removed:
                break
        counts["files"] = cleanup_files(engine, config)
    return counts
