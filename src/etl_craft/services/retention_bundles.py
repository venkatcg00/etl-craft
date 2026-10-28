"""Release obsolete bundles only after their last run and refresh reference disappears."""

import json
from datetime import datetime, timedelta

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.engine import locks
from etl_craft.engine.artifacts import ArtifactStore
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id


def cleanup_bundles(
    engine: Engine, config: ConnectorConfig, now: datetime, *, dry_run: bool
) -> int:
    """Keep each manifest until all its object removals finish, so a restart retries safely."""
    days = config.retention.config_history_days
    if days is None:
        return 0
    removed = 0
    params = {"project_id": current_project_id(), "cutoff": now - timedelta(days=days)}
    with locks.MIGRATE.hold(engine), locks.BUNDLES.hold(engine):
        while True:
            with engine.connect() as conn:
                if dry_run:
                    return int(
                        conn.execute(
                            statement(conn, "retention_bundle_candidates_count"), params
                        ).scalar_one()
                    )
                rows = conn.execute(statement(conn, "retention_bundle_candidates"), params).all()
            if not rows:
                return removed
            for row in rows:
                with engine.connect() as conn:
                    bundle = Bundle.load(conn, row.bundle_id)
                    store = ArtifactStore(config, bundle.store, bundle.profile)
                    for digest in set(bundle.files.values()):
                        # Profiles and URL aliases can share bytes; retain any referenced digest.
                        references = conn.execute(
                            statement(conn, "retention_bundle_object_references"),
                            {
                                "bundle_id": bundle.bundle_id,
                                "digest": f'%"{digest}"%',
                                "digests": json.dumps([digest]),
                            },
                        ).scalar_one()
                        if not references:
                            store.remove(digest)
                with engine.begin() as conn:
                    removed += conn.execute(
                        statement(conn, "retention_bundle_delete"), {"bundle_id": bundle.bundle_id}
                    ).rowcount
