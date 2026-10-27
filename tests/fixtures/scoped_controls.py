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
    block = {"Jdbc_url": profile.jdbc_url, "Schema": profile.schema or "public"}
    if profile.auth_mode != "NONE":
        block |= {
            "User": profile.user,
            "Auth_mode": profile.auth_mode,
            "Secret": profile.secret_var,
        }
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
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
