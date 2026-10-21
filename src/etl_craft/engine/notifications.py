"""One resolved session connection for controller and API notifications."""

import asyncio
import logging
import threading
from dataclasses import replace
from hashlib import sha256
from uuid import uuid4
from weakref import WeakKeyDictionary

import psycopg
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import mask_arguments
from etl_craft.dialects.engine import build_engine, for_engine
from etl_craft.engine.queries import statement

logger = logging.getLogger(__name__)

CHANNEL = "etl_craft_events"
PROBE_SECONDS = 2.0


def pool_channel(pool: str) -> str:
    """Keep arbitrary pool names within PostgreSQL's identifier length."""
    return "etl_craft_pool_" + sha256(pool.encode()).hexdigest()[:32]


def agent_channel(agent_id: int) -> str:
    """Address commands and cancellations to one token-derived agent identity."""
    return f"etl_craft_agent_{agent_id}"


def listener_engine(config: ConnectorConfig) -> Engine:
    """Reuse Engine authentication with the independently resolved session address."""
    profile = config.engine
    return build_engine(
        config,
        replace(profile, jdbc_url=profile.listen_jdbc_url or profile.jdbc_url),
        poolclass=NullPool,
    )


def check_notifications(config: ConnectorConfig, sender: Engine) -> None:
    """Prove that a separate Engine DB transaction reaches the listening session."""
    if not for_engine(sender).supports_api_replicas:
        return
    profile = config.engine
    setting = "Listen_jdbc_url" if profile.listen_jdbc_url is not None else "Jdbc_url"
    address = mask_arguments({"url": profile.listen_jdbc_url or profile.jdbc_url})["url"]
    engine = None
    try:
        engine = listener_engine(config)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as listener:
            listener.execute(statement(listener, "controller_listen"))
            payload = uuid4().hex
            with sender.begin() as conn:
                conn.execute(
                    statement(conn, "execution_notify"), {"channel": CHANNEL, "payload": payload}
                )
            for channel, received in for_engine(listener).notifications(listener, PROBE_SECONDS):
                if channel == CHANNEL and received == payload:
                    return
        detail = f"no matching notification arrived within {PROBE_SECONDS:g} seconds"
    except (ConfigurationError, SQLAlchemyError, psycopg.Error) as error:
        detail = f"notification connection failed ({type(error).__name__})"
    finally:
        if engine is not None:
            engine.dispose()
    raise ConfigurationError(
        f"Engine.{profile.name}.{setting}={address!r}: {detail}; "
        "point it at a direct PostgreSQL connection or a session-mode pool, "
        "connected to the same Engine DB as Jdbc_url"
    )


class NotificationHub:
    """One listening session wakes all claim and resource waiters on this API copy."""

    def __init__(self, config: ConnectorConfig, sender: Engine) -> None:
        """Start a dedicated listener only for PostgreSQL."""
        self.changed = threading.Condition()
        self.generation = 0
        self.channels: set[str] = {CHANNEL}
        self.waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = set()
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None
        self.engine = listener_engine(config) if for_engine(sender).supports_api_replicas else None
        if self.engine is not None:
            self.thread = threading.Thread(target=self._listen, name="engine-listen", daemon=True)
            self.thread.start()

    def _listen(self) -> None:
        assert self.engine is not None
        while not self.stop.is_set():
            try:
                with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    subscribed: set[str] = set()
                    while not self.stop.is_set():
                        with self.changed:
                            added = self.channels - subscribed
                        if added:
                            for_engine(conn).listen(conn, tuple(sorted(added)))
                            subscribed.update(added)
                            self.wake()
                        for _ in for_engine(conn).notifications(conn, 0.25):
                            self.wake()
                            break
            except (SQLAlchemyError, psycopg.Error):
                logger.warning("Engine DB notification session lost; reconnecting")
                self.stop.wait(1)

    def subscribe(self, *channels: str) -> None:
        """Share this session across every pool and agent served by the copy."""
        with self.changed:
            self.channels.update(channels)

    def wake(self) -> None:
        """Publish an edge after LISTEN starts or a committed notification arrives."""
        with self.changed:
            self.generation += 1
            self.changed.notify_all()
            for loop, pending in tuple(self.waiters):
                if not loop.is_closed():
                    loop.call_soon_threadsafe(pending.set)

    def wait(self, generation: int, seconds: float) -> None:
        """Never lose a notification arriving between a query and its wait."""
        with self.changed:
            self.changed.wait_for(
                lambda: self.generation != generation or self.stop.is_set(), timeout=seconds
            )

    async def wait_async(
        self, generation: int, seconds: float, shutdown: threading.Event | None
    ) -> None:
        """Wake all polls without assigning a blocked thread or a DB connection to each."""
        pending = asyncio.Event()
        waiter = (asyncio.get_running_loop(), pending)
        with self.changed:
            if self.generation != generation or self.stop.is_set():
                return
            self.waiters.add(waiter)
        deadline = waiter[0].time() + seconds
        try:
            while not pending.is_set() and not self.stop.is_set():
                if shutdown is not None and shutdown.is_set():
                    return
                remaining = deadline - waiter[0].time()
                if remaining <= 0:
                    return
                try:
                    await asyncio.wait_for(pending.wait(), timeout=min(0.25, remaining))
                except TimeoutError:
                    continue
        finally:
            with self.changed:
                self.waiters.discard(waiter)

    def close(self) -> None:
        """Stop the listener when its owning Engine is disposed."""
        self.stop.set()
        self.wake()
        if self.thread is not None:
            self.thread.join(timeout=2)
        if self.engine is not None:
            self.engine.dispose()


_hubs: WeakKeyDictionary[Engine, NotificationHub] = WeakKeyDictionary()
_hubs_lock = threading.Lock()


def notification_hub(config: ConnectorConfig, engine: Engine) -> NotificationHub:
    """Share a single notification connection across all requests using this Engine."""
    with _hubs_lock:
        hub = _hubs.get(engine)
        if hub is None:
            hub = NotificationHub(config, engine)
            _hubs[engine] = hub
            event.listen(engine, "engine_disposed", lambda _: hub.close())
        return hub
