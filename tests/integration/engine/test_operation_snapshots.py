"""Related operation views retain one metadata version during PostgreSQL writes."""

import pytest
from sqlalchemy import text

from etl_craft.engine.connection import read_snapshot
from etl_craft.services.operations.snapshots import pipeline_view
from fixtures.engine_db import apply_schema
from fixtures.metadata import add_pipeline

pytestmark = pytest.mark.engine_postgres


def test_related_views_keep_one_version_while_metadata_changes(postgres_database):
    engine = postgres_database.engine
    apply_schema(engine)
    with engine.begin() as conn:
        pipeline_id = add_pipeline(conn, "P")
    with read_snapshot(engine) as snapshot:
        assert pipeline_view(snapshot, pipeline_id).pipeline_name == "P"
        with engine.begin() as writer:
            writer.execute(
                text("UPDATE CFG_PIPELINES SET PIPELINE_NAME='renamed' WHERE PIPELINE_ID=:id"),
                {"id": pipeline_id},
            )
        assert pipeline_view(snapshot, pipeline_id).pipeline_name == "P"
    with read_snapshot(engine) as snapshot:
        assert pipeline_view(snapshot, pipeline_id).pipeline_name == "renamed"
