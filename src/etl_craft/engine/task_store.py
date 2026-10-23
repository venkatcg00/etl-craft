"""Task metadata and progress without lending an Engine DB connection to a handler."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import RunStatus, SqlAction
from etl_craft.core.errors import StaleTransitionError
from etl_craft.core.ownership import ResourceLocker, attempt_holder
from etl_craft.engine import locks, transitions
from etl_craft.engine.agent_state import fence
from etl_craft.engine.alerts import AlertState, read_alert
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.business_rules import (
    BusinessRule,
    RuleRunBinding,
    begin_rule_run,
    clear_rule_keys,
    fetch_active_rule_keys,
    fetch_business_rules_for_task,
    finish_rule_run,
    flag_rule_keys,
)
from etl_craft.engine.repository.hash_versions import (
    clear_hash_version,
    fetch_target_contract,
    save_hash_version,
    save_view_statement,
)
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset
from etl_craft.engine.repository.tasks import (
    TargetTask,
)
from etl_craft.engine.resource_locks import currently_holds, holder_tag


@dataclass(frozen=True)
class TaskIdentity:
    """The exact run and attempt a caller may read and change."""

    pipeline_id: int
    task_id: int
    pipeline_run_id: int
    task_run_id: int
    attempt_id: int | None
    owner: str | None
    credential_digest: str | None = None

    @property
    def holder_id(self) -> str:
        """Use the credential namespace for agents and the process owner for local attempts."""
        assert self.attempt_id is not None
        return attempt_holder(self.attempt_id, self.credential_digest or self.owner or "")


@dataclass(frozen=True)
class TaskState:
    """A task's metadata, checkpoint and related writers at the time of the read."""

    offset: StoredOffset | None
    rules: tuple[BusinessRule, ...]
    peers: tuple[TargetTask, ...]
    peer_statuses: dict[str, str | None]
    merge_conflict: str | None
    target: str | None = None
    contract: TargetState | None = None


@dataclass(frozen=True)
class TargetState:
    """Published state is read under the same lock as the warehouse mutation."""

    hash_version: int | None
    view_statement: str | None


@dataclass(frozen=True)
class TargetPublication:
    """State to publish only after the corresponding warehouse transaction committed."""

    target: str
    hash_version: int | None = None
    clear_hash: bool = False
    view_statement: str | None = None


@dataclass(frozen=True)
class RuleEnding:
    """One rule's keys and ending commit together under its task attempt."""

    rule_id: int
    rule_run_id: int
    status: RunStatus
    new_keys: tuple[str, ...] = ()
    passing_keys: tuple[str, ...] = ()


class TaskStore(ResourceLocker, Protocol):
    """The local and HTTP task backends expose the same bounded operations."""

    def alert(self, pipelines: str | None) -> AlertState:
        """Read this task's run report and requested pipeline digests."""
        ...

    def state(self, sql_action: str | None = None) -> TaskState:
        """Read this task's metadata and recorded checkpoints."""
        ...

    def target(self, target: str) -> TargetState:
        """Read a target's published state under its resource ownership."""
        ...

    def publish(self, publication: TargetPublication) -> None:
        """Publish the warehouse commit's hash or view contract."""
        ...

    def watermark(self, start: str, end: str) -> None:
        """Record a staged range without advancing its checkpoint."""
        ...

    def begin_rule(self, rule_id: int) -> RuleRunBinding:
        """Bind a rule to this task run, retaining a successful result on retry."""
        ...

    def rule_keys(self, rule_id: int) -> set[str]:
        """Read a rule's currently active flagged keys."""
        ...

    def end_rule(self, ending: RuleEnding) -> None:
        """Record a rule's flags and ending together."""
        ...


@dataclass(frozen=True)
class LocalTaskStore:
    """Keep SQL and fenced lifecycle decisions in the Engine DB service."""

    engine: Engine
    config: ConnectorConfig
    identity: TaskIdentity
    locks: ResourceLocker
    _contracts: dict[str, TargetState] = field(default_factory=dict, compare=False)

    @property
    def session_tag(self) -> str:
        """Tag the task independently of its metadata service connection."""
        if self.identity.attempt_id is not None:
            return holder_tag(self.identity.holder_id)
        return self.locks.session_tag

    def _assert(self, conn: Connection) -> None:
        identity = self.identity
        if identity.attempt_id is not None:
            task_run_id = fence(conn, identity.attempt_id, identity.owner or "")
            if task_run_id != identity.task_run_id:
                raise StaleTransitionError(
                    f"attempt {identity.attempt_id}: task run {identity.task_run_id} differs "
                    f"from admitted {task_run_id}; use its admitted identity"
                )

    @contextmanager
    def hold(self, key: int, name: str, wait_seconds: float) -> Iterator[None]:
        """Use the common target, event and warehouse ownership service."""
        with self.locks.hold(key, name, wait_seconds):
            self._contracts.clear()
            try:
                yield
            finally:
                self._contracts.clear()

    def alert(self, pipelines: str | None) -> AlertState:
        """Read the exact run's report, using the deployment's SLA policy."""
        identity = self.identity
        with self.engine.connect() as conn:
            self._assert(conn)
            return read_alert(
                conn,
                identity.pipeline_id,
                identity.pipeline_run_id,
                identity.task_id,
                self.config.limits.enforce_sla,
                pipelines,
            )

    def state(self, sql_action: str | None = None) -> TaskState:
        """Read only metadata belonging to the selected task and its run."""
        identity = self.identity
        with self.engine.connect() as conn:
            self._assert(conn)
            if sql_action is not None:
                return self._sql_state(conn, sql_action)
            return TaskState(
                fetch_task_offset(conn, identity.task_id),
                tuple(fetch_business_rules_for_task(conn, identity.task_id)),
                (),
                {},
                None,
            )

    def _sql_state(self, conn: Connection, sql_action: str) -> TaskState:
        """Read checkpoint, peer outcomes and target contract from one database snapshot."""
        identity = self.identity
        rows = conn.execute(
            statement(conn, "sql_task_state"),
            {
                "task_id": identity.task_id,
                "pipeline_id": identity.pipeline_id,
                "pipeline_run_id": identity.pipeline_run_id
                if sql_action == SqlAction.DROP_TABLE
                else None,
                "sql_action": sql_action,
                "catalog": active_catalog(self.config)
                if self.config.warehouse is not None
                else None,
            },
        ).all()
        row = rows[0]
        target = f"{row.catalog_name}.{row.schema_name}.{row.table_name}"
        contract = TargetState(row.hash_version, row.statement_sha256)
        if currently_holds(locks.target(target, row.catalog_name).key):
            self._contracts[target] = contract
        return TaskState(
            StoredOffset(row.offset_type, row.offset_value)
            if row.offset_type is not None
            else None,
            (),
            tuple(
                TargetTask(peer.task_id, peer.task_code, peer.sql_action)
                for peer in rows
                if peer.task_id is not None
            ),
            {str(peer.task_id): peer.status for peer in rows if peer.task_id is not None}
            if sql_action == SqlAction.DROP_TABLE
            else {},
            row.merge_conflict
            if sql_action in {SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE}
            else None,
            target,
            contract,
        )

    def target(self, target: str) -> TargetState:
        """Read the published contract; warehouse state is never guessed."""
        if target.lower() in self._contracts:
            return self._contracts[target.lower()]
        with self.engine.connect() as conn:
            self._assert(conn)
            return TargetState(*fetch_target_contract(conn, target))

    def publish(self, publication: TargetPublication) -> None:
        """Publish only after the caller's warehouse commit."""
        known = self._contracts.get(publication.target.lower())
        if (
            publication.clear_hash
            and known is not None
            and known.hash_version is None
            and publication.hash_version is None
            and publication.view_statement is None
        ):
            return
        with self.engine.begin() as conn:
            self._assert(conn)
            if self.identity.attempt_id is not None:
                key = locks.target(publication.target, active_catalog(self.config)).key
                owned = conn.execute(
                    statement(conn, "resource_lock_owned"),
                    {
                        "key": key,
                        "holder": self.identity.holder_id,
                        "owner": self.identity.owner,
                    },
                ).scalar_one_or_none()
                if owned is None:
                    raise StaleTransitionError(
                        f"target {publication.target}: attempt has no live resource lock; "
                        "stop this writer"
                    )
            if publication.clear_hash:
                clear_hash_version(conn, publication.target)
            if publication.hash_version is not None:
                save_hash_version(conn, publication.target, publication.hash_version)
            if publication.view_statement is not None:
                save_view_statement(conn, publication.target, publication.view_statement)
        self._contracts.pop(publication.target.lower(), None)

    def watermark(self, start: str, end: str) -> None:
        """Fence the staged range to the current attempt without moving the offset."""
        identity = self.identity
        with self.engine.begin() as conn:
            self._assert(conn)
            transitions.record_watermark_range(
                conn,
                identity.task_run_id,
                start,
                end,
                attempt_id=identity.attempt_id,
                owner=identity.owner,
            )

    def _rule(self, conn: Connection, rule_id: int) -> BusinessRule:
        from etl_craft.core.errors import PermissionDeniedError

        rules = fetch_business_rules_for_task(conn, self.identity.task_id)
        for rule in rules:
            if rule.business_rule_id == rule_id:
                return rule
        raise PermissionDeniedError(
            f"rule {rule_id}: not an active rule of task {self.identity.task_id}; "
            "report only this attempt's rules"
        )

    def begin_rule(self, rule_id: int) -> RuleRunBinding:
        """Bind a validated rule under this attempt's task run."""
        with self.engine.begin() as conn:
            self._assert(conn)
            self._rule(conn, rule_id)
            return begin_rule_run(conn, rule_id, self.identity.task_run_id, datetime.now(UTC))

    def rule_keys(self, rule_id: int) -> set[str]:
        """Read flags only for a rule owned by this task."""
        with self.engine.connect() as conn:
            self._assert(conn)
            self._rule(conn, rule_id)
            return fetch_active_rule_keys(conn, rule_id)

    def end_rule(self, ending: RuleEnding) -> None:
        """Commit rule keys and ending together, rejecting another task's run id."""
        from etl_craft.core.errors import PermissionDeniedError
        from etl_craft.engine.queries import statement

        now = datetime.now(UTC)
        with self.engine.begin() as conn:
            self._assert(conn)
            rule = self._rule(conn, ending.rule_id)
            row = conn.execute(
                statement(conn, "business_rule_run"),
                {"business_rule_id": ending.rule_id, "task_run_id": self.identity.task_run_id},
            ).one_or_none()
            if row is None or row.business_rule_run_id != ending.rule_run_id:
                raise PermissionDeniedError(
                    f"rule run {ending.rule_run_id}: not bound to task run "
                    f"{self.identity.task_run_id}; use the binding returned to this attempt"
                )
            if ending.status == RunStatus.SUCCESS:
                flag_rule_keys(conn, rule, ending.rule_run_id, ending.new_keys, now)
                clear_rule_keys(conn, ending.rule_id, ending.passing_keys, now)
            finish_rule_run(conn, ending.rule_run_id, ending.status, now)
