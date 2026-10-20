"""The controller drops a run it cannot go on with, and serves only as a service account."""

import contextvars
import logging
import threading

import pytest

from etl_craft.controller.supervisor import _Supervisor, serve
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.errors import ConfigurationError, RunStateError
from etl_craft.services.operations import OperationContext

pytestmark = pytest.mark.unit


def test_a_controller_signed_in_as_a_person_is_refused_before_it_starts():
    ctx = OperationContext(None, None, Actor("pat", ActorKind.USER))
    with pytest.raises(
        ConfigurationError, match="kind=USER; expected SERVICE; set ETL_CRAFT_TOKEN"
    ):
        serve(ctx, threading.Event())


def steps(closed, error=None):
    try:
        if error is not None:
            raise error
        yield 1.0
    finally:
        closed.append(True)


def test_a_run_that_fails_to_advance_is_dropped_closed_and_logged(caplog):
    supervisor = _Supervisor(None, threading.Event(), None)
    closed = []
    supervisor.jobs[7] = (steps(closed, RunStateError("lease lost")), contextvars.copy_context())
    with caplog.at_level(logging.ERROR, "etl_craft.controller.supervisor"):
        supervisor.advance(7)
    assert supervisor.jobs == {}
    assert closed == [True]
    assert "pipeline_run_id=7: controller could not finish run" in caplog.text


def test_a_run_that_takes_a_step_stays_in_the_working_set():
    supervisor = _Supervisor(None, threading.Event(), None)
    closed = []
    supervisor.jobs[7] = (steps(closed), contextvars.copy_context())
    supervisor.advance(7)
    assert list(supervisor.jobs) == [7]
    assert closed == []
