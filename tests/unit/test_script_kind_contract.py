"""Python kinds distinguish unmeasured utility work from measured ingestion."""

from dataclasses import replace

import pytest

from etl_craft.core.enums import RetryOn, ScriptKind
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.core.script_kinds import reported_count, script_kind
from etl_craft.execution.retries import task_retry_policy
from etl_craft.scripting import ScriptResult
from unit.test_execution_limits import CONFIG

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("value", [None, "OTHER", "", "utility"])
def test_missing_or_invalid_script_kind_names_task_value_and_remedy(value):
    params = {} if value is None else {"SCRIPT_KIND": value}
    with pytest.raises(MetadataError) as caught:
        script_kind(params, "P.load")
    for expected in ("P.load", repr(value), "INGESTION", "UTILITY", "task_parameters.csv"):
        assert expected in str(caught.value)


@pytest.mark.parametrize("count", [None, 0, 7])
def test_utility_keeps_unknown_counts_distinct_from_measured_zero(count):
    assert reported_count(ScriptResult(count).row_count, ScriptKind.UTILITY, "P.utility") == count


@pytest.mark.parametrize("count", [None, -1, True, "3"])
def test_ingestion_requires_a_reported_nonnegative_count(count):
    with pytest.raises(HandlerError) as caught:
        reported_count(ScriptResult(count).row_count, ScriptKind.INGESTION, "P.load")
    for expected in ("P.load", "row_count", repr(count), "INGESTION", "return row_count"):
        assert expected in str(caught.value)


@pytest.mark.parametrize(
    "handler,kind,expected",
    [
        ("PYTHON", "UTILITY", RetryOn.NONE),
        ("SQL_SCRIPT", None, RetryOn.NONE),
        ("PYTHON", "INGESTION", RetryOn.TRANSIENT),
        ("SQL", None, RetryOn.TRANSIENT),
    ],
)
def test_unmanaged_default_retry_policy(handler, kind, expected):
    config = replace(CONFIG, retry_on=RetryOn.TRANSIENT)
    params = {} if kind is None else {"SCRIPT_KIND": kind}
    policy = task_retry_policy(params, config, handler)
    assert policy.retry_on == expected
    assert "default" in policy.source
    overridden = task_retry_policy({**params, "RETRY_ON": RetryOn.ANY}, config, handler)
    assert overridden.retry_on == RetryOn.ANY
    assert "parameter" in overridden.source


@pytest.mark.parametrize("legacy_kind", [None, "UTILITY", "CUSTOM"])
def test_legacy_admitted_python_images_keep_ingestion_without_live_defaulting(legacy_kind):
    import json

    from etl_craft.core.definitions import Definition
    from etl_craft.engine.repository.tasks import admitted_parameters

    image = Definition.read(
        json.dumps(
            {
                "CFG_TASKS": {
                    "columns": ["task_id", "handler"],
                    "rows": [{"task_id": 7, "handler": "PYTHON"}],
                },
                "CFG_TASK_PARAMETERS": {
                    "columns": ["task_id", "parameter_name", "parameter_value", "active_flag"],
                    "rows": (
                        []
                        if legacy_kind is None
                        else [
                            {
                                "task_id": 7,
                                "parameter_name": "SCRIPT_KIND",
                                "parameter_value": legacy_kind,
                                "active_flag": "Y",
                            }
                        ]
                    ),
                },
            }
        )
    )
    assert admitted_parameters(image, 7)["SCRIPT_KIND"] == ScriptKind.INGESTION
    with pytest.raises(MetadataError, match="SCRIPT_KIND=None"):
        script_kind({}, "P.new_task")


def test_new_snapshot_contract_keeps_explicit_utility_and_refuses_a_missing_kind():
    import json

    from etl_craft.core.definitions import Definition
    from etl_craft.core.errors import RunStateError
    from etl_craft.engine.repository.tasks import admitted_parameters

    payload = {
        "_version": 2,
        "CFG_TASKS": {
            "columns": ["task_id", "handler"],
            "rows": [{"task_id": 7, "handler": "PYTHON"}],
        },
        "CFG_TASK_PARAMETERS": {
            "columns": ["task_id", "parameter_name", "parameter_value", "active_flag"],
            "rows": [
                {
                    "task_id": 7,
                    "parameter_name": "SCRIPT_KIND",
                    "parameter_value": "UTILITY",
                    "active_flag": "Y",
                }
            ],
        },
    }
    image = Definition.read(json.dumps(payload))
    assert script_kind(admitted_parameters(image, 7), "P.utility") == ScriptKind.UTILITY
    payload["CFG_TASK_PARAMETERS"]["rows"] = []
    with pytest.raises(MetadataError, match="SCRIPT_KIND=None"):
        script_kind(admitted_parameters(Definition.read(json.dumps(payload)), 7), "P.bad")
    payload["_version"] = 99
    with pytest.raises(RunStateError, match="snapshot is malformed"):
        Definition.read(json.dumps(payload))
