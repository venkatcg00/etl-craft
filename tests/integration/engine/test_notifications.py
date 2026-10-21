"""Startup proves notification delivery instead of trusting a successful LISTEN."""

from dataclasses import replace

import pytest

from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.notifications import check_notifications, notification_hub


def test_notification_round_trip_on_the_resolved_connection(engine_db):
    check_notifications(engine_db.config, engine_db.engine)


@pytest.mark.parametrize("explicit", [False, True])
def test_a_listener_that_drops_notifications_is_refused(engine_db, monkeypatch, explicit):
    if not engine_db.dialect.supports_api_replicas:
        pytest.skip("SQLite has no session notifications")
    cfg = engine_db.config
    if explicit:
        cfg = replace(cfg, engine=replace(cfg.engine, listen_jdbc_url=cfg.engine.jdbc_url))
    monkeypatch.setattr(engine_db.dialect, "notifications", lambda conn, timeout: iter(()))
    with pytest.raises(ConfigurationError) as caught:
        check_notifications(cfg, engine_db.engine)
    message = str(caught.value)
    assert ("Listen_jdbc_url" if explicit else "Jdbc_url") in message
    assert "no matching notification" in message
    assert "direct PostgreSQL connection or a session-mode pool" in message
    assert cfg.engine.jdbc_url in message


def test_all_waiters_share_one_listener_and_dispose_stops_it(engine_db):
    hub = notification_hub(engine_db.config, engine_db.engine)
    assert notification_hub(engine_db.config, engine_db.engine) is hub
    engine_db.engine.dispose()
    assert hub.stop.is_set()
    assert hub.thread is None or not hub.thread.is_alive()
