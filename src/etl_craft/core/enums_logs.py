"""Closed values for the pipeline run log.

Split from ``core.enums`` by concern; the same rules apply to every closed set here.
"""

from __future__ import annotations

from enum import StrEnum


class RunLogCode(StrEnum):
    """The stable message code of one run log line."""

    RUN_STARTED = "RUN_STARTED"
    TASK_QUEUED = "TASK_QUEUED"
    TASK_RETRY_DUE = "TASK_RETRY_DUE"
    TASK_ADMITTED = "TASK_ADMITTED"
    TASK_STARTED = "TASK_STARTED"
    TASK_CANCEL_REQUESTED = "TASK_CANCEL_REQUESTED"
    TASK_CANCEL_ACKNOWLEDGED = "TASK_CANCEL_ACKNOWLEDGED"
    TASK_ENDED = "TASK_ENDED"
    TASK_LOST = "TASK_LOST"
    TASK_SETTLED = "TASK_SETTLED"
    WAIT_OPENED = "WAIT_OPENED"
    GATE_DECIDED = "GATE_DECIDED"
    CONTROL_REQUESTED = "CONTROL_REQUESTED"
    CONTROL_ACKNOWLEDGED = "CONTROL_ACKNOWLEDGED"
    CONTROL_COMPLETED = "CONTROL_COMPLETED"
    INTERVENTION = "INTERVENTION"
    SLA_BREACHED = "SLA_BREACHED"
    SLA_MET = "SLA_MET"
    RUN_ENDED = "RUN_ENDED"
