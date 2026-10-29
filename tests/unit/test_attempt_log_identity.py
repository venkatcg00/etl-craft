"""Recorded attempt path boundaries remain safe after mutable codes change."""

from dataclasses import replace

import pytest

from etl_craft.config import ConnectionProfile
from etl_craft.core.enums import AttemptStatus
from etl_craft.core.errors import ResourceNotFoundError
from etl_craft.engine.logs import LogAttempt
from etl_craft.execution.attempt_records import local_attempt_log
from fixtures.engine_db import engine_config

pytestmark = pytest.mark.unit


@pytest.fixture
def log_identity(tmp_path):
    config = replace(
        engine_config(ConnectionProfile("ENGINE", "test", "jdbc:sqlite:x.db", "", "NONE")),
        log_dir=tmp_path / "logs",
    )
    path = config.log_dir / "project-7" / "OLD" / "run-11" / "OLD_TASK.attempt-2.log"
    path.parent.mkdir(parents=True)
    path.write_text("old bytes")
    row = LogAttempt(
        19, "engine", 7, 11, 13, 2, "NEW", "NEW_TASK", AttemptStatus.SUCCESS, None, str(path), None
    )
    return config, row, path


@pytest.mark.parametrize("legacy", [False, True])
def test_recorded_old_codes_and_legacy_layout_remain_readable(log_identity, legacy):
    config, row, path = log_identity
    if legacy:
        path = config.log_dir / "OLD" / "run-11" / "OLD_TASK.attempt-2.log"
        path.parent.mkdir(parents=True)
        path.write_text("legacy bytes")
        row = replace(row, log_path=str(path))
    assert local_attempt_log(config, row) == path


@pytest.mark.parametrize(
    "invalid",
    ["outside", "traversal", "run", "attempt", "project", "symlink", "bad_pipeline", "bad_task"],
)
def test_invalid_recorded_paths_name_identity_shape_and_remedy(log_identity, tmp_path, invalid):
    config, row, path = log_identity
    assert local_attempt_log(config, row) == path
    if invalid == "outside":
        path = tmp_path / "OLD" / "run-11" / path.name
    elif invalid == "traversal":
        path = path.parent / ".." / "run-11" / path.name
    elif invalid == "run":
        path = path.parent.parent / "run-12" / path.name
    elif invalid == "attempt":
        path = path.with_name("OLD_TASK.attempt-3.log")
    elif invalid == "project":
        path = config.log_dir / "project-8" / "OLD" / "run-11" / path.name
    elif invalid == "symlink":
        outside = tmp_path / "outside"
        outside.mkdir()
        link = config.log_dir / "project-7" / "LINK"
        link.symlink_to(outside, target_is_directory=True)
        path = link / "run-11" / path.name
    elif invalid == "bad_pipeline":
        path = config.log_dir / "project-7" / "bad-code" / "run-11" / path.name
    else:
        path = path.with_name("bad-code.attempt-2.log")
    with pytest.raises(ResourceNotFoundError) as error:
        local_attempt_log(config, replace(row, log_path=str(path)))
    for part in (
        "attempt_id=19",
        str(path),
        "expected",
        "run-11",
        ".attempt-2.log",
        "no traversal",
        "restore",
    ):
        assert part in str(error.value)
