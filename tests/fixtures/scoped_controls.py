"""A real Engine DB and connector for scoped operation acceptance."""

from pathlib import Path

import pytest
import yaml

from etl_craft.config import load_config
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ApiRole
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.services.operations import OperationContext

CHILD = ChildOptions(module="fixtures.task_child", kill_grace_seconds=1, cancel_poll_seconds=0.05)


@pytest.fixture
def ctx(engine_db, tmp_path, monkeypatch):
    profile = engine_db.config.engine
    block = {"jdbc_url": profile.jdbc_url, "schema": profile.schema or "public"}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "environment"},
                "Orchestration": {"Mode": "local"},
                "Engine": {"test": block},
            },
            sort_keys=False,
        )
    )
    # SQLite fixture paths remain relative to the directory of this connector.
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).parents[1]))
    return OperationContext(
        engine_db.engine,
        load_config(path),
        Actor("operator", ActorKind.USER),
        child=CHILD,
        role=ApiRole.OPERATOR,
    )
