"""Durable refresh images and the last passing definition used at admission."""

import json
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import RunRefusedError
from etl_craft.core.text import sha256_hex
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class RefreshWrite:
    """A validation outcome and its immutable metadata and optional project publication."""

    project_id: int
    pipeline_id: int
    snapshot: str
    bundle_id: str | None
    findings: tuple[dict[str, object], ...]
    now: datetime
    passed: bool


def record(conn: Connection, result: RefreshWrite) -> int:
    """Store one image per changed hash; unchanged outcomes reference its owning record."""
    from etl_craft.core.enums import RunStatus

    digest = sha256_hex(result.snapshot.encode())
    previous = conn.execute(
        statement(conn, "refresh_latest_snapshot"), {"pipeline_id": result.pipeline_id}
    ).one_or_none()
    reference = (
        int(previous.refresh_id)
        if previous is not None and previous.metadata_version == digest
        else None
    )
    return int(
        conn.execute(
            statement(conn, "refresh_insert"),
            {
                "project_id": result.project_id,
                "pipeline_id": result.pipeline_id,
                "now": result.now,
                "actor": current_actor().name,
                "actor_kind": current_actor().kind,
                "status": RunStatus.SUCCESS if result.passed else RunStatus.FAILED,
                "metadata_version": digest,
                "snapshot": result.snapshot if reference is None else None,
                "snapshot_refresh_id": reference,
                "bundle_id": result.bundle_id,
                "findings": json.dumps(result.findings),
            },
        ).scalar_one()
    )


def admitted(conn: Connection, pipeline_id: int) -> tuple[str, str, str]:
    """Never substitute current rows or files for a missing or blocked last-good version."""
    from etl_craft.core.enums import RunStatus, YesNo

    params = {"pipeline_id": pipeline_id}
    identity = conn.execute(statement(conn, "refresh_identity"), params).one()
    block = identity.block_on_refresh_error == YesNo.YES
    good = conn.execute(statement(conn, "refresh_good"), params).one_or_none()
    latest = conn.execute(statement(conn, "refresh_latest"), params).one_or_none()
    if good is None or (block and latest is not None and latest.status == RunStatus.FAILED):
        findings = [] if latest is None else json.loads(latest.findings)
        messages = (
            "; ".join(str(finding["message"]) for finding in findings) or "no refresh recorded"
        )
        reason = "no version has passed yet" if good is None else "BLOCK_ON_REFRESH_ERROR=Y"
        raise RunRefusedError(
            f"pipeline {identity.pipeline_code!r}: {reason}; findings: {messages}; "
            "correct the findings "
            "and run `etl-craft pipelines refresh` before admitting a new run"
        )
    return str(good.snapshot), str(good.metadata_version), str(good.bundle_id)
