"""Requests, declared capacity and the retired YAML limit fail before work starts."""

import pytest
import yaml

from etl_craft.config import load_config
from etl_craft.core.agent_resources import DeclaredCapacity
from etl_craft.core.enums import Handler
from etl_craft.core.errors import ConfigurationError, MetadataError
from etl_craft.core.placement import resource_request
from etl_craft.core.resources import MIB
from fixtures.agents import resources

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("raw", ["0", "-1", "1.5", "²", "bad", ""])
@pytest.mark.parametrize("key", ["CPU_REQUEST", "MEMORY_REQUEST_MB"])
def test_bad_resource_request_names_task_value_expectation_and_remedy(key, raw):
    with pytest.raises(MetadataError) as caught:
        resource_request(Handler.PYTHON, {key: raw}, "SALES.load")
    for part in ("SALES.load", key, repr(raw), "positive whole number", "set", "remove"):
        assert part in str(caught.value)


def test_resource_defaults_share_the_measured_task_costs():
    from etl_craft.core.resources import TASK_COST

    for handler, kind in ((Handler.SQL, "warehouse"), (Handler.PYTHON, "python")):
        request = resource_request(handler, {}, "P.T")
        assert request.cpu == TASK_COST[kind][0]
        assert request.memory_mb * MIB == TASK_COST[kind][1]


def test_declared_local_capacity_can_only_lower_measurements():
    measured = resources(cpus=4).allocation
    assert DeclaredCapacity(cpu=8, memory_mb=4096).allocation(measured) == measured
    bounded = DeclaredCapacity(cpu=1, memory_mb=256).allocation(measured)
    assert bounded.cpus.value == 1
    assert bounded.memory.value == 256 * MIB


def test_external_capacity_requires_both_dimensions_and_keeps_its_quota():
    measured = resources().allocation
    with pytest.raises(ConfigurationError, match="--memory-mb"):
        DeclaredCapacity(cpu=32, external=True).allocation(measured)
    external = DeclaredCapacity(cpu=32, memory_mb=40960, external=True).allocation(measured)
    assert external.cpus.value == 32
    assert external.memory.value == 40960 * MIB


def old_config(tmp_path, value=12):
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Max_parallel_tasks": value},
            },
            sort_keys=False,
        )
    )
    return path


@pytest.mark.parametrize("value", [12, None])
def test_retired_parallel_key_names_the_explicit_move(tmp_path, value):
    path = old_config(tmp_path, value)
    with pytest.raises(ConfigurationError) as caught:
        load_config(path)
    for part in (
        str(path),
        "Orchestration.Max_parallel_tasks",
        repr(value),
        "pipelines.csv",
        "MAX_PARALLEL_TASKS",
        "each pipeline",
        "remove",
    ):
        assert part in str(caught.value)


@pytest.mark.parametrize("command", ["validate", "doctor", "migrate", "upgrade-targets"])
def test_commands_refuse_the_old_key_before_opening_a_database(tmp_path, capsys, command):
    from etl_craft.cli import main
    from etl_craft.core.errors import ExitCode

    path = old_config(tmp_path)
    assert main(["--config", str(path), command]) == ExitCode.CONFIGURATION
    error = capsys.readouterr().err
    assert "Max_parallel_tasks" in error
    assert "pipelines.csv" in error
    assert "remove" in error


@pytest.mark.parametrize("pool,state", [("missing", None), ("disabled", "N")])
def test_pool_admission_refuses_missing_and_inactive_rows(pool, state):
    from etl_craft.engine.placement import check_pool

    with pytest.raises(MetadataError) as caught:
        check_pool(pool, state, "SALES.load")
    for part in ("SALES.load", pool, "active CFG_POOLS", "pools.csv", "config apply"):
        assert part in str(caught.value)


def test_validation_never_sums_agents_and_allows_an_absent_pool_of_agents():
    from etl_craft.core.placement import ResourceRequest
    from etl_craft.services.placement import Placement

    placement = Placement(
        {"default": "Y"},
        {"default": {1: ResourceRequest(2, 1024), 2: ResourceRequest(2, 1024)}},
        None,
    )
    with pytest.raises(MetadataError) as caught:
        placement.check("SALES.load", ResourceRequest(3, 1536), "default")
    for part in ("SALES.load", "1536", "agent", "1024", "Reduce", "add capacity"):
        assert part in str(caught.value)
    Placement({"default": "Y"}, {}, None).check("SALES.load", ResourceRequest(3, 1536), "default")


def test_local_validation_uses_one_measured_allocation():
    from etl_craft.core.placement import ResourceRequest
    from etl_craft.services.placement import Placement

    placement = Placement({"default": "Y"}, {}, ResourceRequest(1, 256))
    with pytest.raises(MetadataError, match="local pool"):
        placement.check("SALES.load", ResourceRequest(1, 512), "default")
    placement.check("SALES.load", ResourceRequest(1, 256), "default")
