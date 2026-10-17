"""What a pipeline run returns, the hooks it calls and how its steps are driven."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import TypeVar

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core import interrupts
from etl_craft.core.enums import (
    RunStatus,
)
from etl_craft.engine import runlog
from etl_craft.engine.repository.pipelines import (
    fetch_pipeline_detail,
    resolve_pipeline_id,
)
from etl_craft.execution.gates import (
    Clock,
)
from etl_craft.execution.sla import SlaLapse
from etl_craft.handlers.mail import send_sla_lapse_email

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class PipelineOutcome:
    """How a pipeline run, or one step of it, ended.

    ``status`` is ``IN-PROGRESS`` after ``--init-only`` started the run, or when the pipeline
    was paused while it ran. ``pipeline_run_id`` is ``None`` when a paused pipeline started
    nothing.
    """

    status: RunStatus
    message: str
    pipeline_run_id: int | None
    sla: runlog.SlaResult | None = None


@dataclass(frozen=True)
class RunHooks:
    """What other layers do at points of a run.

    ``on_sla_lapse`` is called once per run, when it is first seen past its SLA.
    ``on_finalized`` is called after the run has ended. A hook that raises is logged and does
    not change how the run ended. A run given no hooks uses ``default_hooks``.
    """

    on_sla_lapse: Callable[[SlaLapse], None] | None = None
    on_finalized: Callable[[PipelineOutcome], None] | None = None


def default_hooks(config: ConnectorConfig, engine: Engine) -> RunHooks:
    """Return the hooks a run gets unless given others: the SLA email, with ``Enforce_sla`` on.

    The email goes to the pipeline's ``EMAIL_RECIPIENTS``, else the DAG default recipients,
    through the Email settings.
    """
    if not config.limits.enforce_sla:
        return RunHooks()

    def email_the_lapse(lapse: SlaLapse) -> None:
        with engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, lapse.pipeline_code)
            recipients = fetch_pipeline_detail(conn, pipeline_id).email_recipients
        send_sla_lapse_email(
            config,
            recipients,
            pipeline_code=lapse.pipeline_code,
            pipeline_run_id=lapse.pipeline_run_id,
            sla_hours=lapse.sla_hours,
            elapsed_hours=lapse.elapsed_hours,
        )

    return RunHooks(on_sla_lapse=email_the_lapse)


def drive(steps: Generator[float, None, T], clock: Clock) -> T:
    """Wait only in the foreground supervisor; workers never wait on gate deadlines.

    A signal held while a step worked is raised between steps, so it never interrupts one.
    """
    try:
        while True:
            interrupts.checkpoint()
            try:
                delay = next(steps)
            except StopIteration as done:
                return done.value  # type: ignore[no-any-return]
            clock.sleep(delay)
    finally:
        steps.close()


def call_hook(name: str, hook: Callable[[T], None] | None, value: T) -> None:
    """Call ``hook`` with ``value``; a failing hook is logged and leaves the outcome alone."""
    if hook is None:
        return
    try:
        hook(value)
    except Exception:
        logger.exception("the %s hook failed; the run's outcome is unchanged", name)
