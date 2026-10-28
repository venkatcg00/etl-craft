"""Real controller loss between mirror commit and durable source acknowledgement."""

import signal

import pytest
import yaml
from sqlalchemy import text

from etl_craft.warehouse.connection import open_warehouse
from fixtures.cloning import insert_pause

pytestmark = pytest.mark.chaos


def test_controller_killed_after_clone_commit_recovers_a_single_mirror(cli_project):
    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Cloning"] = {"Enabled": True, "Scope": "AUD", "Every_minutes": 1}
    raw["Artifacts"] = raw.pop("Artifacts")
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    assert p.run("pipelines", "refresh")[0] == 0
    controller = p.start("controller", fault="clone.after_mirror_commit:kill")
    assert controller.wait(timeout=40) == 137, controller.output
    # The first table committed; its baseline was never acknowledged.
    with p.engine.connect() as conn:
        incomplete = conn.execute(
            text(
                "SELECT COUNT(*) FROM AUD_CLONE_STATE "
                "WHERE TABLE_NAME='AUD_ACTIONS' AND INITIALIZED=0"
            )
        ).scalar_one()
    assert incomplete == 1
    from etl_craft.config import load_config

    config = load_config(p.config.config_path)
    with p.engine.begin() as conn:
        insert_pause(conn, p.pipeline_id, "survives controller loss")
        # A killed holder must be reconciled through the existing stale-owner fencing path.
        conn.execute(
            text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT='2000-01-01 00:00:00+00:00'")
        )
    replacement = p.start("controller")
    p.wait_for(
        "SELECT COUNT(*) FROM AUD_CLONE_STATE "
        "WHERE TABLE_NAME='AUD_PIPELINE_PAUSES' AND INITIALIZED=1",
        expected=1,
        timeout=40,
    )
    replacement.signal(signal.SIGTERM)
    assert replacement.wait(timeout=40) == 0, replacement.output
    with open_warehouse(config) as warehouse, warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT PIPELINE_PAUSE_ID AS pause_id, REASON AS reason "
                    "FROM AUD_PIPELINE_PAUSES"
                )
            )
            .mappings()
            .all()
        )
    assert len(rows) == 1 and rows[0].reason == "survives controller loss"
