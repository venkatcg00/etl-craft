"""Interactive resume asks only after the shared operation returns the missed-tick plan."""

import argparse
import io
from types import SimpleNamespace

import pytest

from etl_craft.cli.commands import pause
from etl_craft.cli.output import Output
from etl_craft.core.enums import ControlAction, ResumeChoice, RunStatus
from etl_craft.core.errors import UsageError
from etl_craft.services.operations.models import OperationResult

pytestmark = pytest.mark.unit


def test_interactive_scheduled_resume_prompt_preserves_json(monkeypatch):
    args = argparse.Namespace(
        pipeline_code="P",
        task_code=None,
        project="default",
        tag=[],
        then=None,
        reason="maintenance done",
        output_format="json",
        yes=False,
    )
    stdout, stderr = io.StringIO(), io.StringIO()
    requests = []

    def call(_args, _action, request):
        requests.append(request)
        if request.then is None:
            raise UsageError("P: scheduled resume requires --then; catch-up would create 2 run(s)")
        return OperationResult(RunStatus.SUCCESS, "P: resumed", 1, "P")

    monkeypatch.setattr(pause, "call", call)
    monkeypatch.setattr("sys.stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("builtins.input", lambda: "wait")
    command = pause.command(ControlAction.RESUME)
    assert command.run(args, Output(stdout=stdout, stderr=stderr)) == 0
    assert requests[-1].then == ResumeChoice.WAIT
    assert "catch-up would create 2" in stderr.getvalue()
    assert "catch-up would create 2" not in stdout.getvalue()
    assert stdout.getvalue().startswith('{"schema":')
