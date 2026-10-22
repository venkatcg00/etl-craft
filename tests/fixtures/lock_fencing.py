"""An expired lock holder whose warehouse statement is still running, for fencing tests."""

import threading
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.engine import resource_locks
from etl_craft.engine.resource_locks import Holder, holder_tag
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect

KEY = 4242
OLD = "action:expired-holder"


def expired_lock(world):
    """Give the expired holder the lock, then let its lease run out."""
    with world.engine_db.begin() as conn:
        acquired = resource_locks.acquire(
            conn, KEY, "fenced target", Holder(OLD, OLD, SYSTEM_ACTOR)
        )
        assert acquired.budget is not None
        conn.execute(
            text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=:past"),
            {"past": datetime.now(UTC) - timedelta(seconds=1)},
        )


def lock_holder(world):
    """Return the lock's current holder id."""
    with world.engine_db.connect() as conn:
        return conn.execute(
            text("SELECT HOLDER_ID AS holder_id FROM AUD_RESOURCE_LOCKS")
        ).scalar_one()


def start_old_write(world, sql, wait_seconds=20):
    """Run the expired holder's statement in the background, tagged as that holder.

    Returns once the warehouse reports the statement running, with the engine, the thread and
    the list that receives the statement's error when it is stopped.
    """
    engine = build_warehouse_engine(world.config, session_tag=holder_tag(OLD))
    errors = []

    def write():
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
        # The stopped statement's error is the evidence the tests look for.
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    dialect = warehouse_dialect(world.config)
    deadline = time.monotonic() + wait_seconds
    while True:
        with world.warehouse.begin() as conn:
            if dialect.running_tagged(conn, holder_tag(OLD)):
                break
        assert time.monotonic() < deadline, f"the expired holder's statement never ran: {errors}"
        time.sleep(0.2)
    return engine, thread, errors


def expire_leases(engine_db):
    """Let every lock lease run out, as it does after its killed holder stops renewing it."""
    with engine_db.begin() as conn:
        conn.execute(
            text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=:past"),
            {"past": datetime.now(UTC) - timedelta(seconds=1)},
        )
