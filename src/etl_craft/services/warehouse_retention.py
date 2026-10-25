"""Retain only registered reject rows and supported table-file metadata."""

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import TextClause
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ActionName, ActionOutcome, IcebergMaintenance
from etl_craft.core.errors import MetadataError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import is_safe_identifier, qualify
from etl_craft.dialects.warehouse.retention import rejects_statement, relation_kind
from etl_craft.engine import locks, resource_locks
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.config_rows import PIPELINES, read_rows
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.handlers.resources import LocalLocker
from etl_craft.handlers.sql.rejects import REJECT_AUDIT
from etl_craft.services.archives import ArchiveSession
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect


@dataclass(frozen=True)
class AuxiliaryTarget:
    """A table's explicitly configured operational lifetimes."""

    table_id: int
    catalog_name: str | None
    schema_name: str
    table_name: str
    object_kind: str | None
    reject_retention_days: int | None
    snapshot_retention_days: int | None
    orphan_retention_days: int | None

    def __post_init__(self) -> None:
        """Validate persisted periods before opening the warehouse or changing history."""
        for field, minimum in (
            ("reject_retention_days", 1),
            ("snapshot_retention_days", 7),
            ("orphan_retention_days", 7),
        ):
            days = getattr(self, field)
            if days is not None and (not isinstance(days, int) or not minimum <= days <= 36500):
                raise MetadataError(
                    f"CFG_TABLES TABLE_ID={self.table_id}: {field.upper()}={days!r}; expected "
                    f"whole days {minimum}..36500 or NULL for forever; correct tables.csv"
                )

    def name(self, catalog: str) -> str:
        """Resolve the same registered identity used by SQL tasks and their target lock."""
        for value in (
            self.catalog_name if self.catalog_name is not None else catalog,
            self.schema_name,
            self.table_name,
        ):
            if value is not None and not is_safe_identifier(value):
                raise MetadataError(
                    f"CFG_TABLES TABLE_ID={self.table_id}: identifier={value!r}; expected "
                    "separate SQL identifiers; correct tables.csv before purge"
                )
        return qualify(f"{self.schema_name}.{self.table_name}", self.catalog_name or catalog)

    def procedures(self) -> Iterator[tuple[IcebergMaintenance, int]]:
        """Enumerate configured file operations without inventing a provider fallback."""
        for action, days in (
            (IcebergMaintenance.EXPIRE_SNAPSHOTS, self.snapshot_retention_days),
            (IcebergMaintenance.REMOVE_ORPHAN_FILES, self.orphan_retention_days),
        ):
            if days is not None:
                yield action, days


@dataclass
class WarehouseRetention:
    """One preflighted project maintenance session, sharing archive reservations when present."""

    engine: Engine
    config: ConnectorConfig
    locker: LocalLocker
    targets: tuple[AuxiliaryTarget, ...]

    def preflight(self, warehouse: Engine) -> None:
        """Refuse unsafe policies before any Engine history or warehouse rows are removed."""
        dialect, catalog = warehouse_dialect(self.config), active_catalog(self.config)
        for target in self.targets:
            name = target.name(catalog)
            if target.object_kind == "VIEW":
                raise MetadataError(
                    f"{name}: OBJECT_KIND=VIEW; expected TABLE; unset its retention"
                )
            self.check_ownership(target, name)
            for action, days in target.procedures():
                if dialect.retention_maintenance(name, action, days) is None:
                    raise MetadataError(
                        f"{name}: {action} is unsupported by {dialect.spec.display_name}; expected "
                        "a supported Iceberg maintenance dialect; unset this table policy "
                        "or use Trino over Iceberg"
                    )
            with warehouse.connect() as conn:
                self.check_rejects(conn, target, name)
                if tuple(target.procedures()):
                    kind = relation_kind(dialect, conn, name)
                    if kind is None or "VIEW" in kind.upper():
                        raise MetadataError(
                            f"{name}: relation kind={kind!r}; "
                            "expected its registered Iceberg table; "
                            "restore the table or unset its maintenance periods"
                        )

    def check_ownership(self, target: AuxiliaryTarget, name: str) -> None:
        """Recheck registered ownership at admission and under both mutation locks."""
        with self.engine.connect() as conn:
            if target.reject_retention_days is not None:
                self.collision(conn, target, name + "__rejects")
            if tuple(target.procedures()):
                self.collision(conn, target, name)

    def collision(self, conn: Connection, target: AuxiliaryTarget, name: str) -> None:
        """Refuse suffix collisions with registered target business rows."""
        catalog, schema, table = name.split(".")
        row = conn.execute(
            statement(conn, "retention_warehouse_collision"),
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "id": target.table_id,
            },
        ).one_or_none()
        if row is not None:
            raise MetadataError(
                f"{name}: also registered as TABLE_ID={row.table_id} in project {row.project_id}; "
                "expected an exclusively owned maintenance object; unset the policy or "
                "use a separate target"
            )

    def check_rejects(self, conn: Connection, target: AuxiliaryTarget, name: str) -> bool:
        """Absent reject ledgers are normal; a differently shaped object is refused."""
        if target.reject_retention_days is None:
            return False
        dialect = warehouse_dialect(self.config)
        table = name + "__rejects"
        if not dialect.read_metadata(conn, table).columns:
            return False
        kind = relation_kind(dialect, conn, table)
        if kind is not None and "VIEW" in kind.upper():
            raise MetadataError(
                f"{table}: relation kind={kind!r}; expected a reject TABLE; "
                "restore its audit ledger"
            )
        types = dialect.full_column_types(conn, table)
        missing = {*REJECT_AUDIT, "REJECT_REASON", "REJECT_COLUMN"} - {
            column.upper() for column in types
        }
        if missing:
            raise MetadataError(
                f"{table}: missing columns {sorted(missing)}; expected the package reject "
                "ledger; restore its audit shape before purge"
            )
        for column in ("CREATE_DATE", "PIPELINE_ID"):
            actual = types.get(column.lower())
            expected = dialect.audit_column_type(column)
            if actual is None or not dialect.archive_type_matches(actual, expected):
                raise MetadataError(
                    f"{table}.{column}: type={actual!r}; expected {expected!r}; "
                    "restore the package reject ledger before purge"
                )
        return True

    def run(self, now: datetime, *, dry_run: bool) -> dict[str, int]:
        """Reserve the warehouse only for its maintenance phase, after Engine cleanup."""
        with open_warehouse(
            self.config,
            self.locker,
            wait_seconds=resource_locks.LEASE_SECONDS + resource_locks.WAIT_SECONDS,
        ) as warehouse:
            return self.sweep(warehouse, now, dry_run=dry_run)

    def sweep(self, warehouse: Engine, now: datetime, *, dry_run: bool) -> dict[str, int]:
        """Hold each managed target lock for reject deletion and all its file maintenance."""
        counts = {}
        with self.engine.connect() as conn:
            pipelines = [
                int(row.row_id)
                for row in read_rows(
                    conn,
                    PIPELINES,
                    project_code=self.config.project,
                    project_id=current_project_id(),
                )
            ]
        catalog = active_catalog(self.config)
        for target in self.targets:
            name = target.name(catalog)
            with ExitStack() as held:
                objects = (name, name + "__rejects") if target.reject_retention_days else (name,)
                for object_name in objects:
                    held.enter_context(
                        locks.target(object_name, catalog).hold(
                            self.locker, resource_locks.LEASE_SECONDS + resource_locks.WAIT_SECONDS
                        )
                    )
                self.check_ownership(target, name)
                try:
                    with warehouse.begin() as conn:
                        removed = self.clean(conn, target, pipelines, now, dry_run=dry_run)
                    if not dry_run:
                        fault_point("retention.warehouse_after_commit")
                        self.record(target, removed, now, ActionOutcome.SUCCESS)
                    counts.update(removed)
                except SQLAlchemyError as error:
                    raise MetadataError(
                        f"{name}: warehouse retention failed ({type(error).__name__}); "
                        "expected writable package auxiliary objects and supported provider "
                        "retention floors; check warehouse permissions/configuration and retry"
                    ) from error
        return counts

    def clean(
        self,
        conn: Connection,
        target: AuxiliaryTarget,
        pipelines: list[int],
        now: datetime,
        *,
        dry_run: bool,
    ) -> dict[str, int]:
        """Rows use ingestion audit time; procedure counts are operations, never file estimates."""
        name = target.name(active_catalog(self.config))
        counts: dict[str, int] = {}
        writes: list[TextClause] = []
        if target.reject_retention_days is not None:
            count = 0
            if self.check_rejects(conn, target, name) and pipelines:
                cutoff = now - timedelta(days=target.reject_retention_days)
                query = rejects_statement(name + "__rejects", cutoff, pipelines, delete=False)
                count = conn.execute(query).scalar_one()
                if count:
                    writes.append(
                        rejects_statement(
                            name + "__rejects",
                            cutoff,
                            pipelines,
                            delete=True,
                        )
                    )
            counts[name + "__rejects"] = count
        for action, days in target.procedures():
            maintenance = warehouse_dialect(self.config).retention_maintenance(name, action, days)
            assert maintenance is not None
            writes.append(maintenance)
            counts[f"{name}:{action}"] = 1
        if dry_run:
            return counts
        self.record(target, counts, now, ActionOutcome.REQUESTED)
        for query in writes:
            result = conn.execute(query)
            if result.returns_rows:
                result.all()
        if target.reject_retention_days is not None and counts[name + "__rejects"]:
            remaining = conn.execute(
                rejects_statement(
                    name + "__rejects",
                    now - timedelta(days=target.reject_retention_days),
                    pipelines,
                    delete=False,
                )
            ).scalar_one()
            if remaining:
                raise MetadataError(
                    f"{name}__rejects: {remaining} eligible rows remain; expected zero "
                    "after deletion; inspect warehouse row policies and writers, then retry"
                )
        fault_point("retention.warehouse_after_write")
        return counts

    def record(
        self,
        target: AuxiliaryTarget,
        counts: dict[str, int],
        now: datetime,
        outcome: ActionOutcome,
    ) -> None:
        """Keep admitted counts distinguishable from acknowledged warehouse commits."""
        ActionRequest(
            ActionName.PURGE,
            {
                "warehouse_retention": target.name(active_catalog(self.config)),
                "counts": counts,
                "reject_retention_days": target.reject_retention_days,
                "snapshot_retention_days": target.snapshot_retention_days,
                "orphan_retention_days": target.orphan_retention_days,
            },
            now,
            actor=current_actor(),
            engine=self.engine,
            project_id=current_project_id(),
            outcome=outcome,
        ).record()


@contextmanager
def auxiliary_session(
    engine: Engine,
    config: ConnectorConfig,
    archive: ArchiveSession | None,
) -> Iterator[WarehouseRetention | None]:
    """Open no warehouse when all per-target lifetimes are unset."""
    with engine.connect() as conn:
        targets = tuple(
            AuxiliaryTarget(**row)
            for row in conn.execute(statement(conn, "retention_warehouse_targets")).mappings()
        )
    if not targets:
        yield None
        return
    if config.warehouse is None:
        raise MetadataError(
            "CFG_TABLES retention is configured but Warehouse is absent; expected its "
            "project connection; configure Warehouse or unset the table retention periods"
        )
    locker = archive.locker if archive else LocalLocker(engine, config)
    opened = (
        nullcontext(archive.warehouse)
        if archive
        else open_warehouse(
            config, locker, wait_seconds=resource_locks.LEASE_SECONDS + resource_locks.WAIT_SECONDS
        )
    )
    with opened as warehouse:
        session = WarehouseRetention(engine, config, locker, targets)
        session.preflight(warehouse)
    yield session
