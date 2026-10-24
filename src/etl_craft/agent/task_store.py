"""Handler metadata and ownership through the agent API; never through an Engine DB engine."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace

from etl_craft import __version__
from etl_craft.agent.client import AgentClient
from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import ActionName, AgentReport
from etl_craft.core.errors import ServerError
from etl_craft.core.items import ItemRecord, ItemState
from etl_craft.core.ownership import attempt_holder
from etl_craft.core.text import sha256_hex
from etl_craft.engine.alerts import AlertState
from etl_craft.engine.repository.business_rules import RuleRunBinding
from etl_craft.engine.resource_locks import confirm_stale, holder_tag
from etl_craft.engine.task_store import RuleEnding, TargetPublication, TargetState, TaskState
from etl_craft.services.operations.agent_models import AgentTask
from etl_craft.services.operations.serialization import from_json


@dataclass
class RemoteTaskStore:
    """The HTTP implementation of the same task store the local provider supplies.

    ``confirm`` stops a stale holder's tagged warehouse statements from this agent, which
    holds the warehouse credentials the API tier does not need.
    """

    client: AgentClient
    attempt_id: int
    confirm: Callable[[str], None]
    held: set[int] = field(default_factory=set)
    contracts: dict[str, TargetState] = field(default_factory=dict)

    @property
    def holder_id(self) -> str:
        """Identify the attempt within its credential namespace."""
        assert self.client.token is not None
        return attempt_holder(self.attempt_id, sha256_hex(self.client.token.encode()))

    @property
    def session_tag(self) -> str:
        """Tag every connection with its admitted attempt identity."""
        return holder_tag(self.holder_id)

    def request(self, operation: AgentReport) -> AgentTask:
        """Bind every sub-operation to the exact attempt and package version."""
        return AgentTask(version=__version__, attempt_id=self.attempt_id, operation=operation)

    def call(self, request: AgentTask) -> object:
        """Leave all ownership and lifecycle decisions at the API tier."""
        return self.client.call(ActionName.AGENT_REPORT, request).data

    def state(self, sql_action: str | None = None) -> TaskState:
        """Read the admitted task's metadata, checkpoints and related writers."""
        state = from_json(TaskState, self.call(self.request(AgentReport.STATE)))
        if state.target is not None and state.contract is not None:
            self.contracts[state.target.lower()] = state.contract
        return state

    def target(self, target: str) -> TargetState:
        """Read only the locked target's published contract."""
        if target.lower() in self.contracts:
            return self.contracts[target.lower()]
        return from_json(
            TargetState, self.call(replace(self.request(AgentReport.TARGET), target=target))
        )

    def publish(self, publication: TargetPublication) -> None:
        """Publish the contract after the warehouse commit, under owned target fencing."""
        self.call(replace(self.request(AgentReport.PUBLISH), publication=publication))

    def watermark(self, start: str, end: str) -> None:
        """Report a staged range; only the successful ending advances the checkpoint."""
        self.call(
            replace(self.request(AgentReport.WATERMARK), watermark_from=start, watermark_to=end)
        )

    def items(self) -> ItemState:
        """Read item evidence through the API tier, including previous task attempts."""
        return from_json(ItemState, self.call(self.request(AgentReport.ITEMS)))

    def prepare_items(self, keys: tuple[str, ...]) -> None:
        """Send one bounded registration batch instead of one API call per queued key."""
        self.call(replace(self.request(AgentReport.ITEM_PREPARE), item_keys=keys))

    def item_event(self, event: ItemRecord) -> ItemRecord:
        """Retry the same report identity without duplicating immutable evidence."""
        return from_json(
            ItemRecord,
            self.call(
                replace(
                    self.request(AgentReport.ITEM_EVENT),
                    item=event,
                )
            ),
        )

    def begin_rule(self, rule_id: int) -> RuleRunBinding:
        """Bind one of this task's rules to its recorded run."""
        return from_json(
            RuleRunBinding,
            self.call(replace(self.request(AgentReport.RULE_BEGIN), rule_id=rule_id)),
        )

    def rule_keys(self, rule_id: int) -> set[str]:
        """Read only flags for the admitted task's rule."""
        result = self.call(replace(self.request(AgentReport.RULE_KEYS), rule_id=rule_id))
        if not isinstance(result, list) or any(not isinstance(key, str) for key in result):
            raise ServerError(
                "agent rule keys: expected a list of strings; upgrade the API and agent together"
            )
        return set(result)

    def end_rule(self, ending: RuleEnding) -> None:
        """Commit the rule's keys and ending through its fenced task service."""
        self.call(replace(self.request(AgentReport.RULE_END), rule_ending=ending))

    def alert(self, pipelines: str | None) -> AlertState:
        """Derive the report scope from admitted metadata through the API."""
        return from_json(AlertState, self.call(self.request(AgentReport.ALERT)))

    @contextmanager
    def hold(self, key: int, name: str, wait_seconds: float) -> Iterator[None]:
        """Hold a shared resource without retaining a server connection between calls."""
        if key in self.held:
            yield
            return
        request = replace(
            self.request(AgentReport.LOCK),
            lock_key=key,
            lock_wait_seconds=min(wait_seconds if wait_seconds > 0 else 30, 30),
        )
        while True:
            result = self.client.call(ActionName.AGENT_REPORT, request, timeout=35).data
            if not isinstance(result, dict):
                raise ServerError(f"{name}: expected a lock answer; check the API version")
            stale = result.get("stale_holder")
            if result.get("acquired") is True:
                break
            if not isinstance(stale, str) or stale == request.confirmed_holder:
                raise ServerError(f"{name}: expected acquired ownership; check the API version")
            confirm_stale(self.confirm, name, stale)
            request = replace(request, confirmed_holder=stale)
        self.held.add(key)
        self.contracts.clear()
        completed = False
        try:
            yield
            completed = True
        finally:
            self.held.remove(key)
            self.contracts.clear()
            if not completed:
                confirm_stale(self.confirm, name, self.holder_id)
            self.call(replace(self.request(AgentReport.UNLOCK), lock_key=key))

    def project(self, config: ConnectorConfig, bundle_id: str) -> ConnectorConfig:
        """Download only this attempt's bundle through the API, retaining host credentials."""
        import base64

        from etl_craft.config.project import project_config
        from etl_craft.engine.project_files import materialize

        answer = self.call(self.request(AgentReport.BUNDLE))
        if not isinstance(answer, dict) or answer.get("bundle_id") != bundle_id:
            raise ServerError(
                f"attempt {self.attempt_id}: bundle identity differs; expected "
                f"{bundle_id}; refresh the claim and inspect the API"
            )
        files = answer.get("files")
        if not isinstance(files, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in files.items()
        ):
            raise ServerError(
                f"bundle {bundle_id}: invalid manifest; expected paths and "
                f"hashes; check the API version"
            )

        def read(path: str) -> bytes:
            value = self.call(replace(self.request(AgentReport.BUNDLE), bundle_path=path))
            if not isinstance(value, dict) or not isinstance(value.get("content"), str):
                raise ServerError(
                    f"bundle {bundle_id}, file {path}: invalid bytes; expected "
                    f"base64 content; check the API"
                )
            try:
                return base64.b64decode(value["content"], validate=True)
            except ValueError as error:
                raise ServerError(
                    f"bundle {bundle_id}, file {path}: invalid encoding; expected "
                    f"base64; restore its artifact"
                ) from error

        root = materialize(bundle_id, files, read)
        return project_config(config, root / "craft-connector.yml")
