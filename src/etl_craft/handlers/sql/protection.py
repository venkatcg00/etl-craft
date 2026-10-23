"""Business-key locks and provenance committed with the warehouse mutation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from etl_craft.core.actor import Actor
from etl_craft.core.enums import ActorKind, ProtectionEvent, SqlAction
from etl_craft.core.errors import SqlGuardError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import is_safe_identifier, sha256_hex
from etl_craft.dialects.warehouse.protection import ProtectionSql
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import SqlTask
from etl_craft.handlers.sql.tables import ENGINE_COLUMNS, check_target_audit

MERGES = frozenset({SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE})


def evidence_names(session: Session) -> tuple[str, str]:
    """Bounded names share the target's resolved catalog/schema and identity rules."""
    suffix = sha256_hex(session.target.lower().encode())[:20]
    return (
        session.qualify(f"{session.schema}.etl_protection_{suffix}"),
        session.qualify(f"{session.schema}.etl_fingerprints_{suffix}"),
    )


def exists(session: Session) -> bool:
    """Refuse partial evidence instead of treating its target as unprotected."""
    found = session.relation_types(evidence_names(session))
    present = [0 in found, 1 in found]
    if bool(present[0]) != bool(present[1]):
        raise SqlGuardError(
            f"{session.target}: incomplete protection evidence; expected both journal and "
            "fingerprints; restore the target and its evidence from the same backup"
        )
    return bool(present[0])


def refuse_replacement(session: Session) -> None:
    """Maintenance cannot bypass a protected target's write boundary."""
    if exists(session):
        raise SqlGuardError(
            f"{session.target}: {session.action} cannot replace protected SCD data or evidence; "
            "use its SCD merge or the authorized edit/unlock service"
        )


@dataclass(frozen=True)
class ProtectionRequest:
    """One actual writer and the operation that explains its evidence."""

    actor: Actor
    reason: str
    request_id: str
    now: datetime

    @classmethod
    def create(cls, actor: Actor, reason: str) -> ProtectionRequest:
        """Give a deliberate edit a durable request identity before any write."""
        return cls(actor, reason, str(uuid4()), datetime.now(UTC))


class Protection:
    """One checked target under its engine ownership lock and warehouse transaction."""

    def __init__(self, session: Session, task: SqlTask) -> None:
        """Build typed fingerprints without retaining business values."""
        self.session = session
        self.task = task
        self.prevented_rows = 0
        self.prevented_values = 0
        columns = tuple(name for name, _ in check_target_audit(session, task.action))
        invalid = [c for c in columns if not is_safe_identifier(c)]
        if invalid or "row_id" not in {c.lower() for c in columns}:
            raise SqlGuardError(
                f"{session.target}: unsupported columns={invalid}, "
                f"ROW_ID present={'row_id' in {c.lower() for c in columns}}; expected plain "
                "column names and a unique generated ROW_ID; migrate the target before protection"
            )
        self.business = tuple(c for c in columns if c.lower() not in ENGINE_COLUMNS)
        missing = set(task.merge_key + task.merge_compare_columns) - set(self.business)
        if missing:
            raise SqlGuardError(
                f"{session.target}: missing merge columns={sorted(missing)}; expected every key "
                "and compare column in the target; restore the task/target contract"
            )
        journal, snapshot = evidence_names(session)
        types = session.hash_types(session.target)

        def hashed(names: tuple[str, ...]) -> str:
            return session.dialect.hash_expression(
                [f"t.{c}" for c in names], [types[c.lower()] for c in names]
            )

        self.sql = ProtectionSql(
            session.target,
            journal,
            snapshot,
            hashed(task.merge_key),
            {c: hashed((c,)) for c in self.business},
            hashed(columns),
        )
        self.contract = json.dumps(
            [task.action, task.merge_key, task.merge_compare_columns, self.business]
        )

    def acquire(self) -> None:
        """Refuse unsupported atomicity, then exclude concurrent outside DML where supported."""
        s = self.session
        if not s.dialect.atomic_protection:
            raise SqlGuardError(
                f"{s.target}: protected writes unsupported on {s.dialect.spec.display_name}; "
                "expected atomic data and evidence commits; use native PostgreSQL or DuckDB"
            )
        if statement := s.dialect.protection_lock_sql(s.target):
            s.run(statement, step="lock protected business data")

    def enable(self, request: ProtectionRequest) -> None:
        """Establish an explicitly unattributed baseline inside the first edit's transaction."""
        self.acquire()
        invalid = self.session.count(
            self.sql.invalid_keys(
                self.task.merge_key, scd2=self.task.action == SqlAction.SCD2_MERGE
            ),
            step="validate protected business keys",
        )
        if invalid:
            raise SqlGuardError(
                f"{self.session.target}: {invalid} NULL or duplicate current business keys; "
                "expected unique non-NULL keys; repair the target before enabling protection"
            )
        for statement in self.sql.ddl():
            self.session.run(statement, step="create protection evidence")
        params = self.parameters(request, ProtectionEvent.ENABLE, revision=1)
        self.session.run(
            self.sql.insert_event(
                "SELECT :revision, NULL, NULL, NULL, FALSE, NULL, NULL, :actor, :actor_kind, "
                ":now, :request_id, :reason, :event, :contract"
            ),
            {**params, "contract": self.contract},
            step="record protection contract",
        )
        self.session.run(
            self.sql.baseline(),
            self.parameters(request, ProtectionEvent.BASELINE, revision=2),
            step="record existing values with unknown writers",
        )
        self.save_snapshot()

    def check(self) -> None:
        """Unknown writes fail before an edit, unlock or ETL mutation can conceal them."""
        self.acquire()
        saved = (
            self.session.run(
                f"SELECT contract FROM {self.sql.journal} WHERE event_kind = :kind",
                {"kind": ProtectionEvent.ENABLE},
                step="read protection contract",
            )
            .scalars()
            .all()
        )
        if saved != [self.contract]:
            raise SqlGuardError(
                f"{self.session.target}: protection contract found {saved!r}; expected "
                f"{self.contract}; restore its merge keys, compare columns and business shape"
            )
        if self.session.count(self.sql.drift(), step="check unobserved writes"):
            raise SqlGuardError(
                f"{self.session.target}: unobserved warehouse writes differ from committed "
                "provenance; expected every writer to use the authenticated service; "
                "restore data and evidence together and route outside writers through the service"
            )

    def parameters(
        self, request: ProtectionRequest, event: ProtectionEvent, *, revision: int | None = None
    ) -> dict[str, object]:
        """Use a monotone revision under the shared ownership lock, independent of clocks."""
        if revision is None:
            revision = 1 + self.session.count(
                f"SELECT COALESCE(MAX(revision), 0) FROM {self.sql.journal}",
                step="evidence revision",
            )
        return {
            "revision": revision,
            "actor": request.actor.name,
            "actor_kind": request.actor.kind,
            "human": request.actor.kind == ActorKind.USER,
            "now": request.now,
            "request_id": request.request_id,
            "reason": request.reason,
            "event": event,
        }

    def save_snapshot(self) -> None:
        """Replace fingerprints only in the same transaction as the actual rows and evidence."""
        self.session.run(f"DELETE FROM {self.sql.snapshot}", step="replace row fingerprints")
        self.session.run(
            f"INSERT INTO {self.sql.snapshot} {self.sql.rows()}", step="save row fingerprints"
        )

    def finish(self, request: ProtectionRequest) -> None:
        """Record changed column writers without overwriting other columns' attribution."""
        s = self.session
        cells, state = s.scratch("cells"), s.scratch("provenance")
        s.create_scratch(cells, self.sql.cells(), step="fingerprint effective values")
        s.create_scratch(state, self.sql.state(), step="retain previous column provenance")
        params = self.parameters(request, ProtectionEvent.WRITE)
        fault_point("sql.protection.before_evidence")
        s.run(self.sql.changes(cells, state), params, step="record effective column writers")
        if request.actor.kind == ActorKind.USER:
            s.run(
                self.sql.inserted_locks(cells, state),
                {**params, "event": ProtectionEvent.LOCK},
                step="protect human inserts",
            )
        self.save_snapshot()
        s.drop(cells)
        s.drop(state)

    def stage(self, stage: str, *, delete: bool = False) -> str:
        """Apply locks before the merge's hash and change counts are computed."""
        s = self.session
        protected = s.scratch("protected")
        columns = tuple(name for name, _ in s.columns(stage))
        attempts = self.sql.prevented(stage, columns, delete=delete)
        self.prevented_rows += s.count(
            f"SELECT COUNT(DISTINCT key_hash) FROM ({attempts}) p", step="prevented row changes"
        )
        self.prevented_values += s.count(
            f"SELECT COUNT(*) FROM ({attempts}) p", step="prevented value changes"
        )
        s.create_scratch(
            protected,
            self.sql.protect_stage(stage, columns, delete=delete),
            step="respect human locks",
        )
        s.drop(stage)
        return protected


def for_task(session: Session, task: SqlTask) -> Protection | None:
    """Find an enabled contract, refusing actions that would bypass its invariants."""
    if not exists(session):
        return None
    if task.action not in MERGES | {SqlAction.DELETE_ROWS, SqlAction.SETUP_TABLE}:
        refuse_replacement(session)
    journal, _ = evidence_names(session)
    saved = session.run(
        f"SELECT contract FROM {journal} WHERE event_kind = :kind",
        {"kind": ProtectionEvent.ENABLE},
        step="read protected writer contract",
    ).scalar_one()
    action, keys, compare, _ = json.loads(saved)
    from dataclasses import replace

    contract = replace(
        task,
        action=SqlAction(action),
        merge_key=tuple(keys),
        merge_compare_columns=tuple(compare),
    )
    if task.action in MERGES and (
        task.action != contract.action
        or task.merge_key != contract.merge_key
        or task.merge_compare_columns != contract.merge_compare_columns
    ):
        raise SqlGuardError(
            f"{session.target}: writer contract differs from protection; expected {saved}; "
            "restore matching SCD action, MERGE_KEY and MERGE_COMPARE_COLUMNS"
        )
    if task.action == SqlAction.DELETE_ROWS and task.merge_key != contract.merge_key:
        raise SqlGuardError(
            f"{session.target}: DELETE_ROWS MERGE_KEY={task.merge_key}; expected "
            f"{contract.merge_key} from protection; configure the same business key"
        )
    guard = Protection(session, contract)
    guard.check()
    return guard


def refuse_evidence_target(session: Session) -> None:
    """Reserve the evidence namespace so SQL tasks cannot mutate another target's journal."""
    if re.fullmatch(r"etl_(protection|fingerprints)_[0-9a-f]{20}", session.table.lower()):
        raise SqlGuardError(
            f"{session.target}: reserved SCD protection evidence; expected a business target; "
            "choose another table and use the edit/unlock service for protected data"
        )
